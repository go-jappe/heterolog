package spoolstorehandler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/pkg/disk"
)

type DirStats struct {
	TotalSize     uint64
	OldestModTime time.Time
	FileCount     int
}

type dirStatsSnapshot struct {
	Stats          DirStats
	ScannedAt      time.Time
	CallsSinceScan uint64

	// LastScanFailedAt は scan 失敗後の即時再試行を抑制するために使う。
	LastScanFailedAt time.Time
}

type dirStatsCache struct {
	mu       sync.RWMutex
	snapshot dirStatsSnapshot
	// activeScanDoneはnilならscan停止中、非nilなら現在実行中のactive spool
	// directory scanの完了channelです。
	// cache更新とnilへの復帰後にmuを解放し、最後にchannelをcloseして完了を通知します。
	activeScanDone chan struct{}
}

func scanDirStats(root spool.StateDir) (DirStats, error) {
	if root.IsZero() {
		return DirStats{}, errors.New("state dir is zero")
	}

	dir := root.Path()

	// ルートが存在しない場合は空ディレクトリ相当として扱う。
	// 過負荷判定では「まだ何もない」状態を正常に扱いたいため。
	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return DirStats{}, nil
		}
		return DirStats{}, err
	}
	if !info.IsDir() {
		return DirStats{}, fmt.Errorf("%s is not a directory", dir)
	}

	// 1 ディレクトリごとの読み込み件数。
	// 全件を一度に保持せず、小分けに読む。
	const batchSize = 256

	var stats DirStats

	// 再帰ではなく明示スタックでディレクトリを走査する。
	stack := []string{dir}

	for len(stack) > 0 {
		currentDir := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// ディレクトリを開いて配下エントリ名を分割読み込みする。
		f, err := os.Open(currentDir)
		if err != nil {
			// 走査中に消えていた場合は無視して続行する。
			if os.IsNotExist(err) {
				continue
			}
			return DirStats{}, err
		}

		readErr := func() error {
			for {
				names, err := f.Readdirnames(batchSize)
				if err != nil && err != io.EOF {
					return err
				}

				for _, name := range names {
					// macOSのAppleDoubleの付随fileは、spoolの使用量に含めません。
					if fsstore.IsAppleDoubleCompanion(currentDir, name) {
						continue
					}
					path := filepath.Join(currentDir, name)

					// symlink を辿らず、そのエントリ自体を判定する。
					entryInfo, statErr := os.Lstat(path)
					if statErr != nil {
						// 走査中に既に消えていた場合は無視して続行する。
						if os.IsNotExist(statErr) {
							continue
						}
						return statErr
					}

					// 子ディレクトリは後続ループで処理する。
					if entryInfo.IsDir() {
						stack = append(stack, path)
						continue
					}

					// 通常ファイルだけ集計対象にする。
					if !entryInfo.Mode().IsRegular() {
						continue
					}

					size := uint64(entryInfo.Size())
					modTime := entryInfo.ModTime()

					stats.FileCount++
					stats.TotalSize += size

					// 最古更新時刻を更新する。
					if stats.OldestModTime.IsZero() || modTime.Before(stats.OldestModTime) {
						stats.OldestModTime = modTime
					}
				}

				// このディレクトリを最後まで読み終えた。
				if err == io.EOF {
					return nil
				}
			}
		}()

		// ディレクトリハンドルは各ディレクトリごとに必ず閉じる。
		closeErr := f.Close()
		if readErr != nil {
			return DirStats{}, readErr
		}
		if closeErr != nil {
			return DirStats{}, closeErr
		}
	}

	return stats, nil
}

// spool用ディレクトリの過負荷判定
// ディレクトリのscopeはactiveのみ
func (s *SpoolStoreHandler) isActiveSpoolStorageOverloaded(ctx context.Context, activeDir spool.StateDir) (bool, error) {
	_ = ctx

	// 過負荷判定用設定を取得
	overloadCfg := s.Config.OverloadPolicyConfig()

	// 利用可能ディスク容量が下限を下回っていれば過負荷
	// 書き込み継続によりディスク枯渇を起こす前に検知する
	if minAvailableDiskSpace := overloadCfg.MinAvailableDiskSpace(); minAvailableDiskSpace > 0 {
		freeBytes, err := availableDiskSpace(activeDir.Path())
		if err != nil {
			return false, fmt.Errorf("failed to get available disk space: %w", err)
		}
		if freeBytes < minAvailableDiskSpace {
			return true, nil
		}
	}

	// active 配下のファイル一覧・総サイズ・最古更新時刻を収集
	// retryable の退避先は active のため、過負荷判定対象も active を基準にする
	stats, err := s.getDirStats(activeDir)
	if err != nil {
		return false, fmt.Errorf("failed to get active dir stats: %w", err)
	}

	// ディレクトリ総サイズが上限を超えていれば過負荷
	if maxDirSize := overloadCfg.MaxDirSize(); maxDirSize > 0 && stats.TotalSize > maxDirSize {
		return true, nil
	}

	// 最古ファイルの滞留時間が警告閾値を超えていれば、active spool が
	// 正常に drain できていない可能性を通知する。この条件はoverloadとしては扱わず、
	// retryableデータのspool保存は継続する。
	if warnAfterDays := overloadCfg.ActiveBacklogWarnAfterDays(); warnAfterDays > 0 && !stats.OldestModTime.IsZero() {
		now := s.clock.Now()
		cutoff := now.Add(-time.Duration(warnAfterDays) * 24 * time.Hour)
		if stats.OldestModTime.Before(cutoff) {
			s.notifyActiveBacklogStaleOncePerDay(ctx, stats, warnAfterDays, now)
		}
	}

	// いずれのoverload条件にも該当しなければ過負荷ではない。
	return false, nil
}

func availableDiskSpace(path string) (uint64, error) {
	p, err := nearestExistingPath(path)
	if err != nil {
		return 0, err
	}

	free, _, err := disk.FreeAndTotal(p)
	if err != nil {
		return 0, err
	}

	return free, nil
}

func nearestExistingPath(path string) (string, error) {
	p := filepath.Clean(strings.TrimSpace(path))
	if p == "" || p == "." {
		return "", errors.New("path is empty")
	}

	for {
		_, err := os.Stat(p)
		if err == nil {
			return p, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}

		parent := filepath.Dir(p)
		if parent == p {
			return "", fmt.Errorf("no existing parent path found: %s", path)
		}
		p = parent
	}
}

// activeDir は active state が末尾のフルパス
func (s *SpoolStoreHandler) getDirStats(activeDir spool.StateDir) (DirStats, error) {
	now := s.clock.Now()
	cfg := s.Config.OverloadPolicyConfig()

	interval := cfg.StatsScanInterval()
	maxCallsSinceScan := cfg.MaxCallsSinceScan()

	// scan 失敗時の連続 retry 抑制。
	// 設定化してもよいが、まずは固定値でもよい。
	const failedScanRetryInterval = 5 * time.Second

	s.overloadCache.mu.Lock()
	snapshot := s.overloadCache.snapshot
	scanInProgress := s.overloadCache.activeScanDone != nil

	// 初回だけは同期 scan する。
	// 初回にゼロ値を返すと、再起動直後に既存 active spool を過小評価するため。
	if snapshot.ScannedAt.IsZero() && !scanInProgress {
		scanDone := make(chan struct{})
		s.overloadCache.activeScanDone = scanDone
		s.overloadCache.mu.Unlock()

		stats, err := s.dirStatsScanner(activeDir)
		scannedAt := s.clock.Now()

		s.overloadCache.mu.Lock()
		defer s.finishDirStatsScanLocked(scanDone)

		if err != nil {
			s.overloadCache.snapshot.LastScanFailedAt = scannedAt
			return DirStats{}, err
		}

		s.overloadCache.snapshot = dirStatsSnapshot{
			Stats:          stats,
			ScannedAt:      scannedAt,
			CallsSinceScan: 0,
		}

		return stats, nil
	}

	nextCallsSinceScan := snapshot.CallsSinceScan + 1

	needsRescan := shouldRefreshDirStats(
		snapshot,
		now,
		interval,
		maxCallsSinceScan,
		nextCallsSinceScan,
	)

	// cache がまだ有効なら、そのまま返す。
	if !needsRescan {
		s.overloadCache.snapshot.CallsSinceScan = nextCallsSinceScan
		stats := s.overloadCache.snapshot.Stats
		s.overloadCache.mu.Unlock()
		return stats, nil
	}

	// scan 中なら待たない。
	// 呼び出し回数だけ進めて、古い cache を返す。
	if scanInProgress {
		s.overloadCache.snapshot.CallsSinceScan = nextCallsSinceScan
		stats := s.overloadCache.snapshot.Stats
		s.overloadCache.mu.Unlock()
		return stats, nil
	}

	// 直近の scan が失敗している場合、短時間は再 scan を起動しない。
	// 高RPS時に scan 失敗 goroutine を連発しないため。
	if !snapshot.LastScanFailedAt.IsZero() &&
		now.Sub(snapshot.LastScanFailedAt) < failedScanRetryInterval {
		s.overloadCache.snapshot.CallsSinceScan = nextCallsSinceScan
		stats := s.overloadCache.snapshot.Stats
		s.overloadCache.mu.Unlock()
		return stats, nil
	}

	// refresh が必要。
	// ただし呼び出し元は待たせず、古い cache を返す。
	scanDone := make(chan struct{})
	s.overloadCache.activeScanDone = scanDone
	s.overloadCache.snapshot.CallsSinceScan = nextCallsSinceScan

	stats := s.overloadCache.snapshot.Stats
	s.overloadCache.mu.Unlock()

	// scanDoneをcacheへ登録済みのため、Shutdownはこの非同期scanの完了を待てます。
	go s.refreshDirStats(activeDir, scanDone)

	return stats, nil
}

// shouldRefreshDirStats は、dir stats の再走査が必要かを判定する。
//
// 再走査が必要になる条件:
//   - まだ一度も scan していない
//   - interval が 0 以下で、統計値を判定ごとに更新する設定である
//   - 前回 scan から interval 以上経過している
//   - 前回 scan 以降の呼び出し回数が maxCallsSinceScan に到達している
//
// nextCallsSinceScan は、今回の呼び出しを反映した呼び出し回数。
// 呼び出し回数ベースの refresh 判定に使う。
func shouldRefreshDirStats(
	snapshot dirStatsSnapshot,
	now time.Time,
	interval time.Duration,
	maxCallsSinceScan uint64,
	nextCallsSinceScan uint64,
) bool {
	if snapshot.ScannedAt.IsZero() {
		return true
	}

	if interval <= 0 || now.Sub(snapshot.ScannedAt) >= interval {
		return true
	}

	if maxCallsSinceScan > 0 && nextCallsSinceScan >= maxCallsSinceScan {
		return true
	}

	return false
}

// refreshDirStatsはactive spool配下をfull scanし、dir stats cacheを更新します。
// scan中はoverloadCacheのlockを保持しないため、呼び出し側は完了を待たず
// 既存のstale cacheを使って判定を継続できます。scan失敗時は古いcacheを維持し、
// LastScanFailedAtを記録して短時間の再scan連発を防ぎます。
func (s *SpoolStoreHandler) refreshDirStats(activeDir spool.StateDir, scanDone chan struct{}) {
	stats, err := s.dirStatsScanner(activeDir)
	scannedAt := s.clock.Now()

	s.overloadCache.mu.Lock()
	defer s.finishDirStatsScanLocked(scanDone)

	if err != nil {
		// scan 失敗時は、既存の Stats / ScannedAt / CallsSinceScan は維持する。
		// LastScanFailedAt は、高RPS時に失敗 scan が連発することを防ぐために使う。
		s.overloadCache.snapshot.LastScanFailedAt = scannedAt
		return
	}

	// scan 成功時は snapshot を新しい集計結果で置き換える。
	// CallsSinceScan は scan 直後なので 0 に戻す。
	s.overloadCache.snapshot = dirStatsSnapshot{
		Stats:          stats,
		ScannedAt:      scannedAt,
		CallsSinceScan: 0,
	}
}

// finishDirStatsScanLockedはscan結果をcacheへ反映した後に呼び、完了を通知して
// 次のscanを開始可能にします。呼び出し元はs.overloadCache.muを保持します。
func (s *SpoolStoreHandler) finishDirStatsScanLocked(scanDone chan struct{}) {
	s.overloadCache.activeScanDone = nil
	s.overloadCache.mu.Unlock()
	close(scanDone)
}

func (s *SpoolStoreHandler) isMemoryOverloaded(ctx context.Context) (bool, error) {
	if s.MemoryMonitor == nil {
		return false, nil
	}
	if s.Config == nil {
		return false, nil
	}

	overloadCfg := s.Config.OverloadPolicyConfig()
	if overloadCfg == nil || !overloadCfg.UseMemoryPressureForOverload() {
		return false, nil
	}

	critical, err := s.MemoryMonitor.IsCritical(ctx)
	if err != nil {
		return false, err
	}

	return critical, nil
}

// ShouldNotifyはClockから受け取ったnowの日付で、一日に一度だけ通知を許可します。
func (s *dailyWarnState) ShouldNotify(
	now time.Time,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sameCalendarDay(s.lastWarnedAt, now) {
		return false
	}

	s.lastWarnedAt = now
	return true
}

func sameCalendarDay(a, b time.Time) bool {
	if a.IsZero() || b.IsZero() {
		return false
	}

	ay, am, ad := a.In(b.Location()).Date()
	by, bm, bd := b.Date()

	return ay == by && am == bm && ad == bd
}

func (s *SpoolStoreHandler) notifyActiveBacklogStaleOncePerDay(
	ctx context.Context,
	stats DirStats,
	warnAfterDays int,
	now time.Time,
) {
	if s == nil {
		return
	}
	if stats.OldestModTime.IsZero() {
		return
	}
	if warnAfterDays <= 0 {
		return
	}

	if !s.activeBacklogStaleWarn.ShouldNotify(now) {
		return
	}

	age := now.Sub(stats.OldestModTime)

	msg := fmt.Sprintf(
		"active spool backlog is stale: oldest_mod_time=%s, age=%s, active_backlog_warn_after_days=%d, total_size=%d, file_count=%d. The active spool may not be draining properly.",
		stats.OldestModTime.Format(time.RFC3339),
		age.String(),
		warnAfterDays,
		stats.TotalSize,
		stats.FileCount,
	)

	s.notifyWarn(ctx, msg)
}
