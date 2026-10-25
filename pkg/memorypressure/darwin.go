//go:build darwin

package memorypressure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// ReadMemoryPressure は macOS 向けのメモリ逼迫情報を返します。
//
// macOS では Linux の MemAvailable のような単一の明確な値がないため、
// vm_stat の free / inactive / speculative pages を合算して
// available の近似値として扱います。
//
// 返す意味:
//   - LimitBytes:     hw.memsize
//   - AvailableBytes: (free + inactive + speculative) * pageSize
//   - UsedBytes:      LimitBytes - AvailableBytes
//   - Ratio:          UsedBytes / LimitBytes
//   - RSSBytes:       ps の rss から取得した現在プロセスのRSS相当値
//
// 注意:
//   - available は近似値
//   - container / cgroup は darwin では扱わない
//   - Docker Desktop 上の Linux container 内で実行される場合は、
//     この darwin 実装ではなく linux 実装が使われる
func (defaultProvider) ReadMemoryPressure(ctx context.Context) (MemoryPressure, error) {
	total, err := readDarwinTotalMemory(ctx)
	if err != nil {
		return MemoryPressure{}, err
	}
	if total == 0 {
		return MemoryPressure{}, errors.New("total memory is zero")
	}

	free, inactive, speculative, pageSize, err := readDarwinVMStat(ctx)
	if err != nil {
		return MemoryPressure{}, err
	}

	available := (free + inactive + speculative) * pageSize
	if available > total {
		available = total
	}

	used := total - available
	ratio := float64(used) / float64(total)

	// RSS は process 自身の補助指標。
	// 取得失敗しても pressure 全体は失敗にしない。
	rssBytes, rssReliable := readDarwinRSSBytesBestEffort(ctx)

	p := MemoryPressure{
		Ratio:          ratio,
		AvailableBytes: available,
		LimitBytes:     total,
		UsedBytes:      used,
		Source:         SourceMacOSVMStat,

		// macOS の available は近似だが、ローカル開発・通常監視では
		// 実用上使える値として扱う。
		Reliable: true,

		// macOS ネイティブ実行なので host 視点。
		// Linux container は Docker Desktop 内の Linux VM 側で動くため、
		// その場合は linux build が使われる。
		DirectHost: true,
		Container:  false,

		RSSBytes:    rssBytes,
		RSSReliable: rssReliable,
	}

	if err := p.Validate(); err != nil {
		return MemoryPressure{}, err
	}
	return p, nil
}

// readDarwinTotalMemory は macOS の総物理メモリ量を取得します。
//
// hw.memsize は bytes 単位の総物理メモリ量です。
func readDarwinTotalMemory(ctx context.Context) (uint64, error) {
	cmd := exec.CommandContext(ctx, "sysctl", "-n", "hw.memsize")
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("sysctl hw.memsize: %w", err)
	}

	s := strings.TrimSpace(string(out))
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse hw.memsize: %w", err)
	}
	return v, nil
}

// readDarwinVMStat は vm_stat から free / inactive / speculative pages と
// page size を取得します。
//
// macOS の vm_stat はページ数を返すため、bytes にするには pageSize を掛けます。
//
// available 近似に使う値:
//   - Pages free
//   - Pages inactive
//   - Pages speculative
//
// 注意:
//   - inactive を available に含めるかは設計判断です
//   - ここでは「必要に応じて再利用されうるメモリ」とみなし、
//     available 近似に含めています
func readDarwinVMStat(ctx context.Context) (
	freePages uint64,
	inactivePages uint64,
	speculativePages uint64,
	pageSize uint64,
	err error,
) {
	cmd := exec.CommandContext(ctx, "vm_stat")
	out, err := cmd.Output()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("vm_stat: %w", err)
	}

	pageSize = 4096 // 取得できなかった場合の fallback

	lines := bytes.Split(out, []byte{'\n'})
	for _, line := range lines {
		s := strings.TrimSpace(string(line))
		if s == "" {
			continue
		}

		if strings.HasPrefix(s, "Mach Virtual Memory Statistics:") {
			if v, ok := parseDarwinPageSize(s); ok {
				pageSize = v
			}
			continue
		}

		key, val, ok := splitDarwinVMStatLine(s)
		if !ok {
			continue
		}

		switch key {
		case "Pages free":
			freePages = val
		case "Pages inactive":
			inactivePages = val
		case "Pages speculative":
			speculativePages = val
		}
	}

	return freePages, inactivePages, speculativePages, pageSize, nil
}

// parseDarwinPageSize は vm_stat の先頭行から page size を取り出します。
//
// 例:
//
//	Mach Virtual Memory Statistics: (page size of 16384 bytes)
func parseDarwinPageSize(line string) (uint64, bool) {
	start := strings.Index(line, "page size of ")
	end := strings.Index(line, " bytes")
	if start < 0 || end <= start {
		return 0, false
	}

	raw := line[start+len("page size of ") : end]
	v, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil || v == 0 {
		return 0, false
	}

	return v, true
}

// splitDarwinVMStatLine は vm_stat の1行から key と page count を取り出します。
//
// 例:
//
//	"Pages free:                               12345."
//	"Pages speculative:                        67890."
//
// vm_stat の数値には末尾 "." や桁区切り "," が入ることがあるため除去します。
func splitDarwinVMStatLine(line string) (string, uint64, bool) {
	idx := strings.Index(line, ":")
	if idx < 0 {
		return "", 0, false
	}

	key := strings.TrimSpace(line[:idx])
	raw := strings.TrimSpace(line[idx+1:])
	raw = strings.TrimSuffix(raw, ".")
	raw = strings.ReplaceAll(raw, ".", "")
	raw = strings.ReplaceAll(raw, ",", "")

	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return "", 0, false
	}

	return key, v, true
}

// readDarwinRSSBytesBestEffort は現在プロセスの RSS を ps から取得します。
//
// macOS では ps -o rss= -p <pid> が通常 KiB 単位の RSS を返します。
// 取得失敗しても MemoryPressure 全体の取得失敗にはせず、(0, false) を返します。
func readDarwinRSSBytesBestEffort(ctx context.Context) (uint64, bool) {
	cmd := exec.CommandContext(
		ctx,
		"ps",
		"-o",
		"rss=",
		"-p",
		strconv.Itoa(os.Getpid()),
	)

	out, err := cmd.Output()
	if err != nil {
		return 0, false
	}

	s := strings.TrimSpace(string(out))
	if s == "" {
		return 0, false
	}

	fields := strings.Fields(s)
	if len(fields) == 0 {
		return 0, false
	}

	// ps の rss は通常 KiB。
	v, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, false
	}

	return v * 1024, true
}
