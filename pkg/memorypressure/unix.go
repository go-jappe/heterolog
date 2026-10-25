//go:build unix && !linux && !darwin

package memorypressure

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// ReadMemoryPressure は Linux / macOS 以外の Unix 系 OS 向けの
// best-effort fallback 実装です。
//
// 想定対象:
//   - freebsd
//   - openbsd
//   - netbsd
//   - solaris
//   - aix など
//
// 注意:
//   - container / cgroup はここでは扱わない
//   - 実用的な container 対応は基本的に linux 実装側で扱う
//   - Unix 系 OS はメモリ統計の取得方法が OS ごとに大きく異なるため、
//     この実装では「取れる範囲で取る」方針にする
//   - available が安全に取れない場合は Reliable=false を返す
func (defaultProvider) ReadMemoryPressure(ctx context.Context) (MemoryPressure, error) {
	total, err := readUnixTotalMemory(ctx)
	if err != nil {
		return MemoryPressure{}, err
	}
	if total == 0 {
		return MemoryPressure{}, errors.New("total memory is zero")
	}

	// RSS は process 自身の補助指標なので、取得失敗しても全体を失敗にしない。
	rssBytes, rssReliable := readUnixRSSBytesBestEffort(ctx)

	available, ok := readUnixAvailableMemory(ctx)
	if !ok {
		// 総メモリは取れたが、available を安全に取れない場合。
		//
		// この状態で ratio を無理に作ると誤判定につながるため、
		// overload 判定には使わせない。
		p := MemoryPressure{
			Ratio:          0,
			AvailableBytes: 0,
			LimitBytes:     total,
			UsedBytes:      0,
			Source:         SourceUnknown,
			Reliable:       false,

			// この generic Unix 版では host 視点として扱う。
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

	if available > total {
		available = total
	}

	used := total - available
	ratio := float64(used) / float64(total)

	p := MemoryPressure{
		Ratio:          ratio,
		AvailableBytes: available,
		LimitBytes:     total,
		UsedBytes:      used,
		Source:         SourceUnknown,
		Reliable:       true,

		// Linux 以外の generic Unix では container / cgroup を扱わない。
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

// readUnixTotalMemory は sysctl から総物理メモリを取得する。
//
// Unix 系では OS ごとに key が異なるため、代表的な候補を順に試す。
// この generic 実装では、どれか1つでも取れれば採用する。
func readUnixTotalMemory(ctx context.Context) (uint64, error) {
	keys := []string{
		"hw.physmem",   // FreeBSD など
		"hw.physmem64", // 一部 BSD
		"hw.realmem",   // 一部 BSD
		"hw.memsize",   // macOS 系だが、generic fallback として残す
	}

	for _, key := range keys {
		v, err := readSysctlUint64(ctx, key)
		if err == nil && v > 0 {
			return v, nil
		}
	}

	return 0, fmt.Errorf("failed to read total memory via sysctl on %s", runtime.GOOS)
}

// readUnixAvailableMemory は利用可能メモリを best-effort で取得する。
//
// 現状では FreeBSD 系で使われる v_free_count を主に見る。
// OpenBSD / NetBSD / Solaris / AIX などでは失敗する可能性がある。
//
// 注意:
//   - generic Unix で available を正確に共通取得するのは難しい
//   - 曖昧な vmstat の列解釈は誤判定につながるため使わない
//   - 取れない場合は false を返し、呼び出し側で Reliable=false にする
func readUnixAvailableMemory(ctx context.Context) (uint64, bool) {
	freePages, err := readSysctlUint64(ctx, "vm.stats.vm.v_free_count")
	if err != nil || freePages == 0 {
		return 0, false
	}

	pageSize := uint64(4096)

	if v, err := readSysctlUint64(ctx, "hw.pagesize"); err == nil && v > 0 {
		pageSize = v
	}

	return freePages * pageSize, true
}

// readUnixRSSBytesBestEffort は ps コマンドから process RSS を取得する。
//
// 多くの Unix 系では
//
//	ps -o rss= -p <pid>
//
// が KiB 単位の RSS を返す。
// ただし OS 差があるため、失敗しても全体の取得失敗にはしない。
func readUnixRSSBytesBestEffort(ctx context.Context) (uint64, bool) {
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

// readSysctlUint64 は sysctl -n <key> の結果を uint64 として読む。
//
// x/sys/unix で OS ごとに分岐するより、generic fallback では
// 外部コマンドに寄せたほうが実装を単純に保てる。
func readSysctlUint64(ctx context.Context, key string) (uint64, error) {
	cmd := exec.CommandContext(ctx, "sysctl", "-n", key)

	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("sysctl %s: %w", key, err)
	}

	s := strings.TrimSpace(string(out))
	if s == "" {
		return 0, fmt.Errorf("sysctl %s returned empty value", key)
	}

	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse sysctl %s: %w", key, err)
	}

	return v, nil
}
