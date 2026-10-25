//go:build linux

package memorypressure

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

func (defaultProvider) ReadMemoryPressure(ctx context.Context) (MemoryPressure, error) {
	return readLinuxMemoryPressure(ctx, os.DirFS("/"))
}

// filesystemはprocfs/cgroupfsを含むルートです。観測ごとに所属とmountを解決し、
// cgroup移動やnamespace変更後に古いscopeの値を再利用しません。
func readLinuxMemoryPressure(ctx context.Context, filesystem fs.FS) (MemoryPressure, error) {
	if err := ctx.Err(); err != nil {
		return MemoryPressure{}, err
	}
	p, err := readLinuxCgroupV2Pressure(ctx, filesystem)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return MemoryPressure{}, ctxErr
		}
		// 有限のcgroup制約を取得できない場合は、従来どおりhostを観測します。
		// Source/DirectHostでscopeを区別し、containerの上限とはみなしません。
		p, err = readLinuxProcMeminfoPressure(filesystem)
		if err != nil {
			return MemoryPressure{}, err
		}
	}
	p.RSSBytes, p.RSSReliable = readLinuxRSSBytesBestEffort(filesystem)
	return p, nil
}

// readLinuxProcMeminfoPressure は /proc/meminfo から host 視点の
// メモリ逼迫情報を作る。
//
// 返す意味:
//   - LimitBytes:     MemTotal
//   - AvailableBytes: MemAvailable
//   - UsedBytes:      MemTotal - MemAvailable
//   - Ratio:          UsedBytes / LimitBytes
//   - RSSBytes:       /proc/self/status の VmRSS
//
// 注意:
//   - これは host 視点の値であり、container limit を直接表すものではない
func readLinuxProcMeminfoPressure(filesystem fs.FS) (MemoryPressure, error) {
	f, err := filesystem.Open("proc/meminfo")
	if err != nil {
		return MemoryPressure{}, fmt.Errorf("open /proc/meminfo: %w", err)
	}
	defer f.Close()

	var memTotal uint64
	var memAvailable uint64

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "MemTotal:") {
			v, err := parseMeminfoKiB(line)
			if err != nil {
				return MemoryPressure{}, err
			}
			memTotal = v
			continue
		}
		if strings.HasPrefix(line, "MemAvailable:") {
			v, err := parseMeminfoKiB(line)
			if err != nil {
				return MemoryPressure{}, err
			}
			memAvailable = v
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		return MemoryPressure{}, fmt.Errorf("scan /proc/meminfo: %w", err)
	}

	if memTotal == 0 {
		return MemoryPressure{}, errors.New("MemTotal not found")
	}
	if memAvailable > memTotal {
		memAvailable = memTotal
	}

	used := memTotal - memAvailable
	ratio := float64(used) / float64(memTotal)

	p := MemoryPressure{
		Ratio:          ratio,
		AvailableBytes: memAvailable,
		LimitBytes:     memTotal,
		UsedBytes:      used,
		Source:         SourceLinuxProcMem,
		Reliable:       true,

		// /proc/meminfo は host 視点。
		DirectHost: true,
		Container:  false,
	}
	if err := p.Validate(); err != nil {
		return MemoryPressure{}, err
	}
	return p, nil
}

// readLinuxRSSBytesBestEffort は /proc/self/status の VmRSS を読んで
// RSSBytes を返す。
//
// RSS は process 自身の常駐メモリ量であり、container 全体の used/limit とは
// 役割が異なる補助指標。
// 取得に失敗しても MemoryPressure 全体の取得失敗にはしないため、
// (0, false) を返す best-effort にしている。
func readLinuxRSSBytesBestEffort(filesystem fs.FS) (uint64, bool) {
	f, err := filesystem.Open("proc/self/status")
	if err != nil {
		return 0, false
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// 例:
		// VmRSS:	  123456 kB
		if strings.HasPrefix(line, "VmRSS:") {
			v, err := parseMeminfoKiB(line)
			if err != nil {
				return 0, false
			}
			return v, true
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, false
	}
	return 0, false
}

// parseMeminfoKiB は
//
//	"MemTotal: 12345 kB"
//	"VmRSS:    67890 kB"
//
// のような行から KiB 値を読み取り、bytes に変換して返す。
func parseMeminfoKiB(line string) (uint64, error) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, fmt.Errorf("invalid meminfo/status line: %s", line)
	}

	v, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse KiB value: %w", err)
	}

	return v * 1024, nil
}
