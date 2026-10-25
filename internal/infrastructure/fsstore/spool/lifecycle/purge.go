package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	lifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

var errStopStateFileWalk = errors.New("stop state file walk")

type SizePurgeDecision struct {
	Mode       SizePurgeMode
	SkipReason lifecyclereport.PurgeSkipReason
}

// state 配下のファイルを purge する。
// 実行順は by-age -> by-size。
// 先に by-age を行うのは、古い day bucket 配下の managed spool file を先に落とし、
// by-size の走査・削除対象を減らせるため。
func (f *FileOperator) purgeStateIfNeed(
	ctx context.Context,
	state pkgspool.State,
) (lifecyclereport.PurgeReport, error) {
	report := lifecyclereport.PurgeReport{}

	if f == nil {
		return report, errors.New("spool lifecycle store is nil")
	}

	now := f.clock.Now()

	stateDir, err := f.stateDir(state)
	if err != nil {
		return report, err
	}

	if !f.StatePurgeEnabled(state) {
		report.SkipReason = lifecyclereport.PurgeSkipReasonStateDisabled
		return report, nil
	}

	statePurge := f.purgeTrackerFor(state)
	maxFileAge := f.StateMaxFileAge(state)

	// 1. by-age purge
	// 古い day bucket を purge 対象にする。
	// 古いからと言って day bucket を RemoveAll はしない。
	// managed spool file と空 bucket dir だけを削除する。
	// 実行は最大で1日に1回まで。
	if !shouldRunAgePurge(now, statePurge.LastAgePurgeAt) {
		report.ByAge.SkipReason = lifecyclereport.PurgeSkipReasonAgeAlreadyRunToday
	} else {
		ageReport, err := f.purgeByAgeUsingDayDirs(
			ctx,
			stateDir.Path(),
			now,
			maxFileAge,
		)
		if err != nil {
			return mergePurgeReport(report, ageReport), err
		}

		statePurge.MarkAgePurgeExecuted(now)
		report = mergePurgeReport(report, ageReport)

		// by-age の結果を size estimate cache に反映する。
		f.applyAgePurgeResultToSizeEstimate(statePurge, ageReport)
	}

	// 2. by-size purge
	// by-age 後の残存 managed spool file だけを対象に、古い順でサイズ超過を解消する。
	sizeReport, err := f.runSizePurgeIfNeed(ctx, state, stateDir.Path(), now, statePurge)
	if err != nil {
		return mergePurgeReport(report, sizeReport), err
	}
	report = mergePurgeReport(report, sizeReport)

	return report, nil
}

// mergePurgeReport は、purge 処理ごとの PurgeReport を加算マージする。
func mergePurgeReport(acc, current lifecyclereport.PurgeReport) lifecyclereport.PurgeReport {
	merged := lifecyclereport.PurgeReport{
		SkipReason: acc.SkipReason,
		ByAge:      mergePurgeStepReport(acc.ByAge, current.ByAge),
		BySize:     mergePurgeStepReport(acc.BySize, current.BySize),
	}

	if merged.SkipReason == lifecyclereport.PurgeSkipReasonNone {
		merged.SkipReason = current.SkipReason
	}

	return merged
}

func mergePurgeStepReport(
	acc lifecyclereport.PurgeStepReport,
	current lifecyclereport.PurgeStepReport,
) lifecyclereport.PurgeStepReport {
	merged := lifecyclereport.PurgeStepReport{
		Executed:             acc.Executed || current.Executed,
		SkipReason:           acc.SkipReason,
		DeletedFileCount:     acc.DeletedFileCount + current.DeletedFileCount,
		DeletedTotalSize:     acc.DeletedTotalSize + current.DeletedTotalSize,
		RemovedEmptyDirCount: acc.RemovedEmptyDirCount + current.RemovedEmptyDirCount,
		InvalidBucketDirCount: acc.InvalidBucketDirCount +
			current.InvalidBucketDirCount,
	}

	if merged.SkipReason == lifecyclereport.PurgeSkipReasonNone {
		merged.SkipReason = current.SkipReason
	}

	// 実行された step に skip reason が混ざらないようにする。
	if merged.Executed {
		merged.SkipReason = lifecyclereport.PurgeSkipReasonNone
	}

	if acc.FirstInvalidBucketDir != "" {
		merged.FirstInvalidBucketDir = acc.FirstInvalidBucketDir
		merged.FirstInvalidBucketDirReason = acc.FirstInvalidBucketDirReason
	} else {
		merged.FirstInvalidBucketDir = current.FirstInvalidBucketDir
		merged.FirstInvalidBucketDirReason = current.FirstInvalidBucketDirReason
	}

	return merged
}

// 残っている managed spool file を古い順に削除して、容量上限以下にする。
// by-age と違い bucket 単位ではなく、最終的には個別ファイル単位で調整する。
//
// 削除対象は validateSpoolFileName を満たす通常ファイルだけ。
// managed spool file 以外の通常ファイル、symlink、socket、device file などは削除しない。
//
// 1st pass は総サイズ計算だけなので順序不要。
// 2nd pass は削除対象の選択に順序が関係するため oldest-first で処理する。
func (f *FileOperator) purgeBySizeOldestFirst(
	ctx context.Context,
	stateDir string,
	maxDirSize uint64,
) (lifecyclereport.PurgeReport, uint64, error) {
	report := lifecyclereport.PurgeReport{}
	report.BySize.Executed = true

	var totalSize uint64

	// 1st pass: managed spool file の総サイズだけ集計する。
	// 順序は結果に影響しないため、lexical walk ではなく chunk scan で読む。
	if err := walkStateFilesUnordered(
		ctx,
		stateDir,
		&report.BySize,
		func(path string, info fs.FileInfo) error {
			_ = path
			totalSize += uint64(info.Size())
			return nil
		},
	); err != nil {
		return report, 0, fmt.Errorf("failed to scan files for size purge: %w", err)
	}

	if totalSize <= maxDirSize {
		return report, totalSize, nil
	}

	remainingSize := totalSize

	// 2nd pass: 辞書順 = oldest-first で順次削除する。
	// invalid bucket は 1st pass で記録済みなので、ここでは二重計上しない。
	err := walkStateFilesLexical(
		ctx,
		stateDir,
		nil,
		func(path string, info fs.FileInfo) error {
			// 上限以下まで削除できたので、walk を正常打ち切りする。
			// errStopStateFileWalk は walkStateFilesLexical で nil に変換される。
			if remainingSize <= maxDirSize {
				return errStopStateFileWalk
			}

			size := uint64(info.Size())
			protected, err := terminalHasProcessingSource(stateDir, path)
			if err != nil {
				return err
			}
			if protected {
				return nil
			}

			if err := os.Remove(path); err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return fmt.Errorf("failed to remove file for size purge %s: %w", path, err)
			}

			report.BySize.DeletedFileCount++
			report.BySize.DeletedTotalSize += size

			if remainingSize >= size {
				remainingSize -= size
			} else {
				remainingSize = 0
			}

			removedDirs, err := removeEmptyParents(filepath.Dir(path), stateDir)
			if err != nil {
				return fmt.Errorf("failed to remove empty parent dirs after size purge: %w", err)
			}
			report.BySize.RemovedEmptyDirCount += removedDirs

			return nil
		},
	)
	if err != nil {
		return report, remainingSize, err
	}

	return report, remainingSize, nil
}

// stateDir 配下の managed spool file を順序保証なしで処理する。
//
// 用途:
//   - 総サイズ計算
//   - 件数計算
//   - 順序に意味がない scan
//
// 前提:
//   - ディレクトリ構造は yyyy/mm/dd/hh/mm
//   - minute dir 配下に spool file が置かれる
//
// 注意:
//   - oldest-first は保証しない
//   - 削除順が意味を持つ処理では walkStateFilesLexical を使う
//   - os.ReadDir ではなく Readdirnames で分割読み込みし、
//     1 ディレクトリ内の全 entry を一度に保持しない
//   - validateSpoolFileName を満たす通常ファイルだけを fn に渡す
func walkStateFilesUnordered(
	ctx context.Context,
	stateDir string,
	invalid *lifecyclereport.PurgeStepReport,
	fn func(path string, info fs.FileInfo) error,
) error {
	return walkStateMinuteDirsUnordered(ctx, stateDir, nil, invalid, fn)
}

func walkStateMinuteDirsUnordered(
	ctx context.Context,
	dir string,
	parts []string,
	invalid *lifecyclereport.PurgeStepReport,
	fn func(path string, info fs.FileInfo) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(parts) == 5 {
		return walkMinuteDirFilesUnordered(ctx, dir, invalid, fn)
	}

	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	readErr := func() error {
		const batchSize = 256

		for {
			names, err := f.Readdirnames(batchSize)
			if err != nil && err != io.EOF {
				return err
			}

			for _, name := range names {
				if err := ctx.Err(); err != nil {
					return err
				}

				path := filepath.Join(dir, name)

				info, statErr := os.Lstat(path)
				if statErr != nil {
					if os.IsNotExist(statErr) {
						continue
					}
					return statErr
				}
				if !info.IsDir() {
					continue
				}

				childParts := appendParts(parts, name)
				if err := validateBucketDirParts(childParts); err != nil {
					if invalid != nil {
						invalid.AddInvalidBucketDir(path, err)
					}
					continue
				}

				if err := walkStateMinuteDirsUnordered(ctx, path, childParts, invalid, fn); err != nil {
					return err
				}
			}

			if err == io.EOF {
				return nil
			}
		}
	}()

	closeErr := f.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}

	return nil
}

func walkMinuteDirFilesUnordered(
	ctx context.Context,
	minuteDir string,
	invalid *lifecyclereport.PurgeStepReport,
	fn func(path string, info fs.FileInfo) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	f, err := os.Open(minuteDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	readErr := func() error {
		const batchSize = 256

		for {
			names, err := f.Readdirnames(batchSize)
			if err != nil && err != io.EOF {
				return err
			}

			for _, name := range names {
				if err := ctx.Err(); err != nil {
					return err
				}

				path := filepath.Join(minuteDir, name)

				info, statErr := os.Lstat(path)
				if statErr != nil {
					if os.IsNotExist(statErr) {
						continue
					}
					return statErr
				}

				if info.IsDir() {
					if invalid != nil {
						invalid.AddInvalidBucketDir(path, fmt.Errorf("bucket depth is too deep: 6"))
					}
					continue
				}

				if !info.Mode().IsRegular() {
					continue
				}

				if err := validateSpoolFileName(name); err != nil {
					continue
				}

				if err := fn(path, info); err != nil {
					return err
				}
			}

			if err == io.EOF {
				return nil
			}
		}
	}()

	closeErr := f.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}

	return nil
}

// removeManagedBucketDirFiles は、rootDir 配下の bucket dir を再帰的に走査し、
// managed spool file だけを削除する。
//
// 削除対象:
//   - validateSpoolFileName を満たす通常ファイル
//   - 削除後に空になったディレクトリ
//
// 削除しないもの:
//   - spool 拡張子以外の通常ファイル
//   - symlink / socket / device file などの非通常ファイル
//   - bucket 構造として不正なディレクトリ配下
//
// 削除した managed spool file の件数・サイズを集計する。
// 空ディレクトリ削除数も集計する。
func removeManagedBucketDirFiles(
	ctx context.Context,
	rootDir string,
	dir string,
	report *lifecyclereport.PurgeStepReport,
) (deletedCount int, deletedSize uint64, removedDirCount int, err error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, 0, err
	}

	if err := validateBucketDir(rootDir, dir); err != nil {
		if report != nil {
			report.AddInvalidBucketDir(dir, err)
		}
		return 0, 0, 0, nil
	}

	info, err := os.Lstat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, err
	}
	if !info.IsDir() {
		return 0, 0, 0, fmt.Errorf("%s is not a directory", dir)
	}

	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, 0, nil
		}
		return 0, 0, 0, err
	}

	readErr := func() error {
		const batchSize = 256

		for {
			names, err := f.Readdirnames(batchSize)
			if err != nil && err != io.EOF {
				return err
			}

			for _, name := range names {
				if err := ctx.Err(); err != nil {
					return err
				}

				path := filepath.Join(dir, name)

				entryInfo, statErr := os.Lstat(path)
				if statErr != nil {
					if os.IsNotExist(statErr) {
						continue
					}
					return statErr
				}

				if entryInfo.IsDir() {
					childCount, childSize, childRemoved, rmErr := removeManagedBucketDirFiles(
						ctx,
						rootDir,
						path,
						report,
					)
					if rmErr != nil {
						return rmErr
					}

					deletedCount += childCount
					deletedSize += childSize
					removedDirCount += childRemoved
					continue
				}

				// symlink / socket / device file などは削除しない。
				if !entryInfo.Mode().IsRegular() {
					continue
				}

				// managed spool file だけ削除する。
				// 想定外の通常ファイルは事故防止のため残す。
				if err := validateSpoolFileName(name); err != nil {
					continue
				}

				size := uint64(entryInfo.Size())
				protected, err := terminalHasProcessingSource(rootDir, path)
				if err != nil {
					return err
				}
				if protected {
					continue
				}

				if rmErr := os.Remove(path); rmErr != nil {
					if os.IsNotExist(rmErr) {
						continue
					}
					return rmErr
				}

				deletedCount++
				deletedSize += size
			}

			if err == io.EOF {
				return nil
			}
		}
	}()

	closeErr := f.Close()
	if readErr != nil {
		return 0, 0, 0, readErr
	}
	if closeErr != nil {
		return 0, 0, 0, closeErr
	}

	empty, err := isDirEmpty(dir)
	if err != nil {
		return 0, 0, 0, err
	}
	if !empty {
		return deletedCount, deletedSize, removedDirCount, nil
	}

	if err := os.Remove(dir); err != nil {
		if os.IsNotExist(err) {
			return deletedCount, deletedSize, removedDirCount, nil
		}
		return 0, 0, 0, err
	}

	removedDirCount++

	return deletedCount, deletedSize, removedDirCount, nil
}

// empty parent を stateDir 直下まで best-effort で削除する。
// 削除できるのは空ディレクトリだけ。
// 空でない・既に存在しないケースは正常系として打ち切る。
func removeEmptyParents(startDir, stopDir string) (int, error) {
	dir := startDir
	removed := 0

	for {
		if dir == "" {
			return removed, nil
		}

		cleanDir := filepath.Clean(dir)
		cleanStop := filepath.Clean(stopDir)

		if cleanDir == cleanStop {
			return removed, nil
		}

		err := os.Remove(cleanDir)
		if err != nil {
			if os.IsNotExist(err) {
				return removed, nil
			}

			// 空でないディレクトリは掃除できないだけなので正常終了扱いにする。
			// OS 依存で ENOTEMPTY / EEXIST などが PathError に包まれるため、
			// best-effort cleanup ではここで打ち切る。
			var pathErr *os.PathError
			if errors.As(err, &pathErr) {
				return removed, nil
			}

			return removed, err
		}

		removed++

		parent := filepath.Dir(cleanDir)
		if parent == cleanDir {
			return removed, nil
		}
		dir = parent
	}
}

// 相対パスを OS 非依存で分割する。
func splitRelPath(rel string) []string {
	rel = filepath.ToSlash(rel)
	raw := strings.Split(rel, "/")

	parts := make([]string, 0, len(raw))
	for _, p := range raw {
		if p == "" || p == "." {
			continue
		}
		parts = append(parts, p)
	}
	return parts
}

// StateMaxFileAge は state ごとの保持日数設定を返す。
func (f *FileOperator) StateMaxFileAge(state pkgspool.State) int {
	if f == nil || f.Config == nil {
		return 0
	}

	switch state {
	case pkgspool.StateDone:
		return f.Config.DoneMaxFileAge()
	case pkgspool.StateDead:
		return f.Config.DeadMaxFileAge()
	case pkgspool.StateQuarantine:
		return f.Config.QuarantineMaxFileAge()
	default:
		return 0
	}
}

// StateMaxDirSize は state ごとの最大ディレクトリサイズ設定を返す。
func (f *FileOperator) StateMaxDirSize(state pkgspool.State) uint64 {
	if f == nil || f.Config == nil {
		return 0
	}

	switch state {
	case pkgspool.StateDone:
		return f.Config.DoneMaxDirSize()
	case pkgspool.StateDead:
		return f.Config.DeadMaxDirSize()
	case pkgspool.StateQuarantine:
		return f.Config.QuarantineMaxDirSize()
	default:
		return 0
	}
}

// shouldRunAgePurge は、今回 by-age purge を走らせるべきかを返す。
// by-age は「日付が変わってから 1 回だけ」実行する。
func shouldRunAgePurge(
	now time.Time,
	lastRunAt time.Time,
) bool {
	if lastRunAt.IsZero() {
		return true
	}

	ny, nm, nd := now.Date()
	ly, lm, ld := lastRunAt.In(now.Location()).Date()

	return ny != ly || nm != lm || nd != ld
}

// ShouldRunSizePurge は、今回 by-size check を走らせるべきかを返す。
func (f *FileOperator) ShouldRunSizePurge(
	state pkgspool.State,
	now time.Time,
	lastRunAt time.Time,
) bool {
	if f == nil || f.Config == nil {
		return false
	}
	interval := f.Config.SizePurgeInterval()
	if lastRunAt.IsZero() {
		return true
	}

	return !now.Before(lastRunAt.Add(interval))
}

// DecideSizePurge は、今回の by-size 処理方針を返す。
//
// 戻り値:
//   - SizePurgeSkip:
//     今回は size check / purge を行わない
//   - SizePurgeFullScan:
//     stateDir 全体を実測し、必要なら oldest-first で削除する
//   - SizePurgeDiffScan:
//     前回 check 以降の増加分だけを見て、full scan が必要か判断する
func (f *FileOperator) DecideSizePurge(
	state pkgspool.State,
	now time.Time,
	rt StatePurgeTracker,
) SizePurgeDecision {
	if f == nil || f.Config == nil {
		return SizePurgeDecision{
			Mode: SizePurgeSkip,
		}
	}

	// estimate cache を使わない設定なら、
	// interval 条件を満たしたタイミングで毎回 full scan する。
	if !f.Config.EnableSizeEstimateCache() {
		if !f.ShouldRunSizePurge(state, now, rt.LastSizeCheckAt) {
			return SizePurgeDecision{
				Mode:       SizePurgeSkip,
				SkipReason: lifecyclereport.PurgeSkipReasonSizeIntervalNotDue,
			}
		}

		return SizePurgeDecision{
			Mode: SizePurgeFullScan,
		}
	}

	// まだ一度も full scan に成功していない場合、
	// 差分推定の起点がないため interval に関係なく full scan する。
	if rt.LastSizeFullScanAt.IsZero() || rt.LastSizeCheckAt.IsZero() {
		return SizePurgeDecision{
			Mode: SizePurgeFullScan,
		}
	}

	// clock rollback などで前回 check 時刻より now が古い場合、
	// diff scan の基準が壊れるため full scan に戻す。
	if now.Before(rt.LastSizeCheckAt) {
		return SizePurgeDecision{
			Mode: SizePurgeFullScan,
		}
	}

	// estimate が初期化済みの場合だけ、interval による skip が意味を持つ。
	if !f.ShouldRunSizePurge(state, now, rt.LastSizeCheckAt) {
		return SizePurgeDecision{
			Mode:       SizePurgeSkip,
			SkipReason: lifecyclereport.PurgeSkipReasonSizeIntervalNotDue,
		}
	}

	// SizeFullScanEveryNChecks は、
	// diff scan を何回続けたら full scan を挟むかを決める設定。
	// 1なら diff scan を使わず、毎回 full scan する。
	every := f.Config.SizeFullScanEveryNChecks()
	if every == 1 {
		return SizePurgeDecision{
			Mode: SizePurgeFullScan,
		}
	}
	if rt.SizeCheckCountSinceFullScan+1 >= every {
		return SizePurgeDecision{
			Mode: SizePurgeFullScan,
		}
	}

	// SizeFullScanMaxInterval は、
	// 回数条件とは別に、一定時間ごとに full scan を強制するための設定。
	// diff scan だけが長期間続いて estimate が古くなりすぎるのを防ぐ。
	maxInterval := f.Config.SizeFullScanMaxInterval()
	if !now.Before(rt.LastSizeFullScanAt.Add(maxInterval)) {
		return SizePurgeDecision{
			Mode: SizePurgeFullScan,
		}
	}

	return SizePurgeDecision{
		Mode: SizePurgeDiffScan,
	}
}

// CanSkipSizeFullScanByEstimate は、推定サイズを見るだけで
// 今回の by-size full scan をスキップしてよいかを返す。
func (f *FileOperator) CanSkipSizeFullScanByEstimate(
	state pkgspool.State,
	rt StatePurgeTracker,
) bool {
	if f == nil || f.Config == nil {
		return false
	}
	if !f.Config.EnableSizeEstimateCache() {
		return false
	}

	// full scan 成功済みでなければ、estimate を使う根拠がない。
	if rt.LastSizeFullScanAt.IsZero() || rt.LastSizeCheckAt.IsZero() {
		return false
	}

	return rt.EstimatedTotalSize <= f.StateMaxDirSize(state)
}

// MarkAgePurgeExecuted は by-age purge 実行後に呼ぶ。
func (rt *StatePurgeTracker) MarkAgePurgeExecuted(now time.Time) {
	if rt == nil {
		return
	}
	rt.LastAgePurgeAt = now
}

// MarkSizeFullScanned は full scan 成功後に呼ぶ。
// 実測値を estimate cache に反映し、full scan 起点のカウンタをリセットする。
func (rt *StatePurgeTracker) MarkSizeFullScanned(
	now time.Time,
	remainingTotalSize uint64,
) {
	if rt == nil {
		return
	}

	rt.LastSizeCheckAt = now
	rt.LastSizeFullScanAt = now
	rt.SizeCheckCountSinceFullScan = 0
	rt.EstimatedTotalSize = remainingTotalSize
}

// MarkSizeEstimateCheckedWithTotal は、estimate による size check 成功後に呼ぶ。
// full scan は実行していないため、LastSizeFullScanAt は更新しない。
func (rt *StatePurgeTracker) MarkSizeEstimateCheckedWithTotal(
	now time.Time,
	estimatedTotalSize uint64,
) {
	if rt == nil {
		return
	}

	rt.LastSizeCheckAt = now
	rt.SizeCheckCountSinceFullScan++
	rt.EstimatedTotalSize = estimatedTotalSize
}

func walkExpiredDayDirsChunked(
	ctx context.Context,
	stateDir string,
	cutoffDate time.Time,
	report *lifecyclereport.PurgeStepReport,
	fn func(dayDir string) error,
) error {
	cutoffYear := cutoffDate.Year()
	cutoffMonth := int(cutoffDate.Month())

	return forEachChildDirNameChunked(ctx, stateDir, func(yearName string, yearDir string) error {
		if err := validateBucketDirPart(1, yearName); err != nil {
			report.AddInvalidBucketDir(yearDir, err)
			return nil
		}

		year, _ := parsePathInt(yearName)
		if year > cutoffYear {
			return nil
		}

		return forEachChildDirNameChunked(ctx, yearDir, func(monthName string, monthDir string) error {
			if err := validateBucketDirPart(2, monthName); err != nil {
				report.AddInvalidBucketDir(monthDir, err)
				return nil
			}

			month, _ := parsePathInt(monthName)
			if year == cutoffYear && month > cutoffMonth {
				return nil
			}

			return forEachChildDirNameChunked(ctx, monthDir, func(dayName string, dayDir string) error {
				parts := []string{yearName, monthName, dayName}

				if err := validateBucketDirParts(parts); err != nil {
					report.AddInvalidBucketDir(dayDir, err)
					return nil
				}

				dayDate, err := parseDayDirDate(parts, cutoffDate.Location())
				if err != nil {
					report.AddInvalidBucketDir(dayDir, err)
					return nil
				}

				if !dayDate.Before(cutoffDate) {
					return nil
				}

				return fn(dayDir)
			})
		})
	})
}

func forEachChildDirNameChunked(
	ctx context.Context,
	dir string,
	fn func(name string, path string) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	readErr := func() error {
		const batchSize = 256

		for {
			names, err := f.Readdirnames(batchSize)
			// The purge callback can remove the now-empty month/year being
			// scanned. Its disappearance means this traversal is complete.
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil && err != io.EOF {
				return err
			}

			for _, name := range names {
				if err := ctx.Err(); err != nil {
					return err
				}

				path := filepath.Join(dir, name)

				info, statErr := os.Lstat(path)
				if statErr != nil {
					if os.IsNotExist(statErr) {
						continue
					}
					return statErr
				}

				if !info.IsDir() {
					continue
				}

				if err := fn(name, path); err != nil {
					return err
				}
			}

			if err == io.EOF {
				return nil
			}
		}
	}()

	closeErr := f.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}

	return nil
}

// parseDayDirDate は、stateDir からの相対パス要素
// [yyyy, mm, dd] を日付として解釈する。
//
// 注意:
//   - parts の長さは必ず 3 を期待する
//   - 数値に変換できない場合は error を返す
//   - purge 側では事前に bucket part validation を行う
//   - loc は purge 判定の timezone 基準を揃えるために使う
func parseDayDirDate(parts []string, loc *time.Location) (time.Time, error) {
	if len(parts) != 3 {
		return time.Time{}, errors.New("invalid day dir depth")
	}

	year, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, err
	}
	month, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, err
	}
	day, err := strconv.Atoi(parts[2])
	if err != nil {
		return time.Time{}, err
	}

	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, loc), nil
}

// parsePathInt は、bucket path の 1 要素を int に変換する。
func parsePathInt(s string) (int, bool) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return v, true
}

// startOfDayは、atが持つタイムゾーンにおける日付の
// 00:00:00を返します。
func startOfDay(at time.Time) time.Time {
	y, m, d := at.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, at.Location())
}

// stateDir 配下の managed spool file を oldest-first で順次処理する。
// 前提:
//   - ディレクトリ構造は yyyy/mm/dd/hh/mm
//   - 各階層名はゼロ埋めされており、辞書順 = 時系列順
//   - minute dir 配下のファイル名は ssmmm_*<spool.SpoolExt> 形式で、辞書順 = 時系列順
//
// 注意:
//   - minute dir 内は oldest-first を守るため、ReadDirectoryEntries で名前順に読む
//   - validateSpoolFileName を満たす通常ファイルだけを fn に渡す
func walkStateFilesLexical(
	ctx context.Context,
	stateDir string,
	invalid *lifecyclereport.PurgeStepReport,
	fn func(path string, info fs.FileInfo) error,
) error {
	err := walkStateMinuteDirsLexical(ctx, stateDir, nil, invalid, fn)
	if errors.Is(err, errStopStateFileWalk) {
		return nil
	}
	return err
}

func walkStateMinuteDirsLexical(
	ctx context.Context,
	dir string,
	parts []string,
	invalid *lifecyclereport.PurgeStepReport,
	fn func(path string, info fs.FileInfo) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(parts) == 5 {
		return walkMinuteDirFilesLexical(ctx, dir, invalid, fn)
	}

	// ReadDirectoryEntries は名前の順でディレクトリ群を返す。
	entries, err := fsstore.ReadDirectoryEntries(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() {
			continue
		}

		childDir := filepath.Join(dir, entry.Name())
		childParts := appendParts(parts, entry.Name())

		if err := validateBucketDirParts(childParts); err != nil {
			if invalid != nil {
				invalid.AddInvalidBucketDir(childDir, err)
			}
			continue
		}

		if err := walkStateMinuteDirsLexical(ctx, childDir, childParts, invalid, fn); err != nil {
			return err
		}
	}

	return nil
}

func walkMinuteDirFilesLexical(
	ctx context.Context,
	minuteDir string,
	invalid *lifecyclereport.PurgeStepReport,
	fn func(path string, info fs.FileInfo) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	entries, err := fsstore.ReadDirectoryEntries(minuteDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		name := entry.Name()
		path := filepath.Join(minuteDir, name)

		if entry.IsDir() {
			if invalid != nil {
				invalid.AddInvalidBucketDir(path, fmt.Errorf("bucket depth is too deep: 6"))
			}
			continue
		}

		if err := validateSpoolFileName(name); err != nil {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to stat %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			continue
		}

		if err := fn(path, info); err != nil {
			return err
		}
	}

	return nil
}

func countStateFilesAndSize(
	ctx context.Context,
	stateDir string,
) (int, uint64, error) {
	var count int
	var totalSize uint64

	err := walkStateFilesUnordered(ctx, stateDir, nil, func(path string, info fs.FileInfo) error {
		_ = path

		count++
		totalSize += uint64(info.Size())
		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	return count, totalSize, nil
}

func (f *FileOperator) applyAgePurgeResultToSizeEstimate(
	rt *StatePurgeTracker,
	ageReport lifecyclereport.PurgeReport,
) {
	if rt == nil {
		return
	}

	age := ageReport.ByAge
	if !age.Executed {
		return
	}

	// まだ full scan に成功していない場合、差し引く元の estimate がない。
	if rt.LastSizeFullScanAt.IsZero() {
		return
	}

	if rt.EstimatedTotalSize >= age.DeletedTotalSize {
		rt.EstimatedTotalSize -= age.DeletedTotalSize
	} else {
		rt.EstimatedTotalSize = 0
	}
}

func (f *FileOperator) runSizePurgeIfNeed(
	ctx context.Context,
	state pkgspool.State,
	stateDir string,
	now time.Time,
	rt *StatePurgeTracker,
) (lifecyclereport.PurgeReport, error) {
	report := lifecyclereport.PurgeReport{}

	if f == nil || f.Config == nil {
		return report, nil
	}
	if rt == nil {
		return report, errors.New("state purge tracker is nil")
	}

	maxDirSize := f.StateMaxDirSize(state)

	// full scan を実行し、実測した残サイズを size estimate cache に反映する。
	//
	// purgeBySizeOldestFirst は、削除後に残った managed spool file の総サイズを返す。
	// その値は呼び出し元へ返さず、StatePurgeTracker の更新に使う。
	runFullScan := func() (lifecyclereport.PurgeReport, error) {
		fullReport, remainingTotalSize, err := f.purgeBySizeOldestFirst(ctx, stateDir, maxDirSize)
		if err != nil {
			// full scan / purge に失敗した場合、tracker は更新しない。
			// 古い estimate は残すが、LastSizeCheckAt / LastSizeFullScanAt も進めないため、
			// 次回も full scan 条件に入る。
			return fullReport, err
		}

		rt.MarkSizeFullScanned(now, remainingTotalSize)
		return fullReport, nil
	}

	decision := f.DecideSizePurge(state, now, *rt)

	switch decision.Mode {
	case SizePurgeSkip:
		report.BySize.SkipReason = decision.SkipReason
		return report, nil

	case SizePurgeFullScan:
		// estimate cache を使わない、または full scan が必要なタイミング。
		return runFullScan()

	case SizePurgeDiffScan:
		report.BySize.Executed = true

		// 念のための防御。
		// 通常は DecideSizePurge 側で full scan に振り分け済み。
		if rt.LastSizeFullScanAt.IsZero() || rt.LastSizeCheckAt.IsZero() {
			return runFullScan()
		}

		diffTotalSize, err := countStateFileSizeSince(
			ctx,
			stateDir,
			rt.LastSizeCheckAt,
			&report.BySize,
		)
		if err != nil {
			return report, fmt.Errorf("failed to scan delta files for size purge: %w", err)
		}

		estimatedTotal := rt.EstimatedTotalSize
		if diffTotalSize > 0 {
			estimatedTotal += diffTotalSize
		}

		// 推定後の値で skip 判定するため、tracker 本体はまだ更新しない。
		rtCandidate := *rt
		rtCandidate.EstimatedTotalSize = estimatedTotal

		if f.CanSkipSizeFullScanByEstimate(state, rtCandidate) {
			rt.MarkSizeEstimateCheckedWithTotal(now, estimatedTotal)
			return report, nil
		}

		return runFullScan()

	default:
		return report, fmt.Errorf("unknown size purge mode: %d", decision.Mode)
	}
}

// countStateFileSizeSince は、since より後に追加・更新された
// managed spool file の合計サイズを返す。
// 差分推定のため、判定には record の OccurredAt ではなく ModTime を使う。
func countStateFileSizeSince(
	ctx context.Context,
	stateDir string,
	since time.Time,
	invalid *lifecyclereport.PurgeStepReport,
) (uint64, error) {
	if since.IsZero() {
		_, totalSize, err := countStateFilesAndSize(ctx, stateDir)
		return totalSize, err
	}

	sy, sm, sd := since.Date()
	sh, smin, _ := since.Clock()

	var totalSize uint64

	err := walkStateFilesSinceChunked(
		ctx,
		stateDir,
		nil,
		sy,
		int(sm),
		sd,
		sh,
		smin,
		since,
		invalid,
		func(path string, info fs.FileInfo) error {
			_ = path

			if !info.ModTime().After(since) {
				return nil
			}
			totalSize += uint64(info.Size())
			return nil
		},
	)
	if err != nil {
		return 0, err
	}

	return totalSize, nil
}

// stateDir 配下を chunk 読みしながら再帰走査する。
// yyyy/mm/dd/hh/ii 構造を前提に、since より古い側の subtree は早めに降下打ち切りする。
// validateSpoolFileName を満たす通常ファイルだけを fn に渡す。
func walkStateFilesSinceChunked(
	ctx context.Context,
	dir string,
	parts []string,
	sy, sm, sd, sh, smin int,
	since time.Time,
	invalid *lifecyclereport.PurgeStepReport,
	fn func(path string, info fs.FileInfo) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	readErr := func() error {
		const batchSize = 256

		for {
			entries, err := f.ReadDir(batchSize)
			if err != nil && err != io.EOF {
				return err
			}

			for _, entry := range entries {
				if err := ctx.Err(); err != nil {
					return err
				}

				name := entry.Name()
				path := filepath.Join(dir, name)

				if entry.IsDir() {
					childParts := appendParts(parts, name)

					if err := validateBucketDirParts(childParts); err != nil {
						if invalid != nil {
							invalid.AddInvalidBucketDir(path, err)
						}
						continue
					}

					if !shouldDescendSincePath(childParts, sy, sm, sd, sh, smin) {
						continue
					}

					if err := walkStateFilesSinceChunked(
						ctx,
						path,
						childParts,
						sy,
						sm,
						sd,
						sh,
						smin,
						since,
						invalid,
						fn,
					); err != nil {
						return err
					}
					continue
				}

				if len(parts) != 5 {
					continue
				}

				if err := validateSpoolFileName(name); err != nil {
					continue
				}

				info, err := entry.Info()
				if err != nil {
					if os.IsNotExist(err) {
						continue
					}
					return fmt.Errorf("failed to stat %s: %w", path, err)
				}
				if !info.Mode().IsRegular() {
					continue
				}

				if err := fn(path, info); err != nil {
					return err
				}
			}

			if err == io.EOF {
				return nil
			}
		}
	}()

	closeErr := f.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}

	return nil
}

// since より明らかに古い subtree なら false を返す。
// parts は stateDir からの相対ディレクトリ要素:
// [yyyy], [yyyy mm], [yyyy mm dd], [yyyy mm dd hh], [yyyy mm dd hh ii]
func shouldDescendSincePath(parts []string, sy, sm, sd, sh, smin int) bool {
	switch len(parts) {
	case 1:
		year, ok := parsePathInt(parts[0])
		if !ok {
			return false
		}
		return year >= sy

	case 2:
		year, ok := parsePathInt(parts[0])
		if !ok {
			return false
		}
		month, ok := parsePathInt(parts[1])
		if !ok {
			return false
		}
		return !(year < sy || (year == sy && month < sm))

	case 3:
		year, ok := parsePathInt(parts[0])
		if !ok {
			return false
		}
		month, ok := parsePathInt(parts[1])
		if !ok {
			return false
		}
		day, ok := parsePathInt(parts[2])
		if !ok {
			return false
		}
		return compareYMD(year, month, day, sy, sm, sd) >= 0

	case 4:
		year, ok := parsePathInt(parts[0])
		if !ok {
			return false
		}
		month, ok := parsePathInt(parts[1])
		if !ok {
			return false
		}
		day, ok := parsePathInt(parts[2])
		if !ok {
			return false
		}
		hour, ok := parsePathInt(parts[3])
		if !ok {
			return false
		}
		return compareYMDH(year, month, day, hour, sy, sm, sd, sh) >= 0

	case 5:
		year, ok := parsePathInt(parts[0])
		if !ok {
			return false
		}
		month, ok := parsePathInt(parts[1])
		if !ok {
			return false
		}
		day, ok := parsePathInt(parts[2])
		if !ok {
			return false
		}
		hour, ok := parsePathInt(parts[3])
		if !ok {
			return false
		}
		minute, ok := parsePathInt(parts[4])
		if !ok {
			return false
		}
		return compareYMDHM(year, month, day, hour, minute, sy, sm, sd, sh, smin) >= 0

	default:
		return false
	}
}

// appendParts は、現在の bucket path 要素に child directory 名を追加した
// 新しい slice を返す。
//
// name は、現在走査中のディレクトリ直下で見つかった子ディレクトリ名。
// 例えば parts が ["2026", "04"]、name が "27" なら、
// ["2026", "04", "27"] を返す。
//
// 再帰走査中に親 slice の backing array を共有しないよう、
// append ではなく make + copy で分離する。
func appendParts(parts []string, name string) []string {
	next := make([]string, len(parts)+1)
	copy(next, parts)
	next[len(parts)] = name
	return next
}

func compareYMD(y1, m1, d1, y2, m2, d2 int) int {
	if y1 != y2 {
		if y1 < y2 {
			return -1
		}
		return 1
	}
	if m1 != m2 {
		if m1 < m2 {
			return -1
		}
		return 1
	}
	if d1 != d2 {
		if d1 < d2 {
			return -1
		}
		return 1
	}
	return 0
}

func compareYMDH(y1, m1, d1, h1, y2, m2, d2, h2 int) int {
	if v := compareYMD(y1, m1, d1, y2, m2, d2); v != 0 {
		return v
	}
	if h1 < h2 {
		return -1
	}
	if h1 > h2 {
		return 1
	}
	return 0
}

func compareYMDHM(y1, m1, d1, h1, min1, y2, m2, d2, h2, min2 int) int {
	if v := compareYMDH(y1, m1, d1, h1, y2, m2, d2, h2); v != 0 {
		return v
	}
	if min1 < min2 {
		return -1
	}
	if min1 > min2 {
		return 1
	}
	return 0
}

// validateBucketDir は、dir が rootDir 配下であり、かつ想定 bucket 構造の
// ディレクトリであることを確認する。
func validateBucketDir(rootDir, dir string) error {
	rootDir = strings.TrimSpace(rootDir)
	dir = strings.TrimSpace(dir)

	if rootDir == "" {
		return fmt.Errorf("root dir is empty")
	}
	if dir == "" {
		return fmt.Errorf("dir is empty")
	}

	rel, err := filepath.Rel(rootDir, dir)
	if err != nil {
		return fmt.Errorf("failed to resolve relative path: %w", err)
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("%s is outside root dir %s", dir, rootDir)
	}

	parts := splitRelPath(rel)
	if err := validateBucketDirParts(parts); err != nil {
		return fmt.Errorf("invalid bucket dir %s: %w", dir, err)
	}

	return nil
}

// validateBucketDirParts は、bucket ディレクトリの各階層が
// YYYY/MM/DD/HH/mm の途中または末端であることを確認する。
func validateBucketDirParts(parts []string) error {
	if len(parts) == 0 {
		return fmt.Errorf("bucket path is empty")
	}
	if len(parts) > 5 {
		return fmt.Errorf("bucket depth is too deep: %d", len(parts))
	}

	for i, p := range parts {
		if err := validateBucketDirPart(i+1, p); err != nil {
			return err
		}
	}

	if len(parts) >= 3 {
		year, _ := strconv.Atoi(parts[0])
		month, _ := strconv.Atoi(parts[1])
		day, _ := strconv.Atoi(parts[2])

		if err := validateCalendarDay(year, month, day); err != nil {
			return err
		}
	}

	return nil
}

// validateBucketDirPart は、bucket path の 1 階層分が想定形式か確認する。
func validateBucketDirPart(depth int, name string) error {
	switch depth {
	case 1: // YYYY
		if len(name) != 4 {
			return fmt.Errorf("year must be 4 digits: %s", name)
		}
		if _, err := strconv.Atoi(name); err != nil {
			return fmt.Errorf("year must be numeric: %s", name)
		}
		return nil

	case 2: // MM
		return validateFixedWidthInt(name, 2, 1, 12, "month")

	case 3: // DD
		return validateFixedWidthInt(name, 2, 1, 31, "day")

	case 4: // HH
		return validateFixedWidthInt(name, 2, 0, 23, "hour")

	case 5: // mm
		return validateFixedWidthInt(name, 2, 0, 59, "minute")

	default:
		return fmt.Errorf("invalid bucket depth: %d", depth)
	}
}

func validateCalendarDay(year int, month int, day int) error {
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if t.Year() != year || int(t.Month()) != month || t.Day() != day {
		return fmt.Errorf("invalid calendar date: %04d/%02d/%02d", year, month, day)
	}
	return nil
}

func validateFixedWidthInt(s string, width int, min int, max int, name string) error {
	if len(s) != width {
		return fmt.Errorf("%s must be %d digits: %s", name, width, s)
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("%s must be numeric: %s", name, s)
	}
	if n < min || n > max {
		return fmt.Errorf("%s is out of range: %s", name, s)
	}

	return nil
}

// validateSpoolFileName は、lifecycle が削除してよい managed spool file 名か確認する。
// 想定形式は <ssmmm>_<random><spool.SpoolExt>。
// random には区切り文字 '_' を含めない。
func validateSpoolFileName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("file name is empty")
	}

	// file name のみを受け付ける。
	// filepath.Base だけでは OS によって '\' の扱いが変わるため、
	// '/' と '\' を明示的に拒否する。
	if strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("file name must not contain path separators: %s", name)
	}
	if filepath.Base(name) != name {
		return fmt.Errorf("file name must not contain path separators: %s", name)
	}

	if filepath.Ext(name) != spool.SpoolExt {
		return fmt.Errorf("unexpected extension: %s", name)
	}

	base := strings.TrimSuffix(name, spool.SpoolExt)

	timePart, randPart, ok := strings.Cut(base, "_")
	if !ok {
		return fmt.Errorf("file name must be <ssmmm>_<random>%s: %s", spool.SpoolExt, name)
	}

	// 区切り文字 '_' は timePart と randPart の間に1つだけ許可する。
	if strings.Contains(randPart, "_") {
		return fmt.Errorf("random suffix must not contain underscore: %s", name)
	}

	if len(timePart) != 5 {
		return fmt.Errorf("time prefix must be 5 digits ssmmm: %s", name)
	}

	secPart := timePart[:2]
	if err := validateFixedWidthInt(secPart, 2, 0, 59, "second"); err != nil {
		return fmt.Errorf("invalid second prefix: %w", err)
	}

	msPart := timePart[2:]
	if err := validateFixedWidthInt(msPart, 3, 0, 999, "millisecond"); err != nil {
		return fmt.Errorf("invalid millisecond prefix: %w", err)
	}

	if randPart == "" {
		return fmt.Errorf("random suffix is empty: %s", name)
	}

	for _, r := range randPart {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' {
			continue
		}

		return fmt.Errorf("random suffix contains invalid character %q: %s", r, name)
	}

	return nil
}

func isDirEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	defer f.Close()

	names, err := f.Readdirnames(1)
	if err == io.EOF && len(names) == 0 {
		return true, nil
	}
	if err != nil && err != io.EOF {
		return false, err
	}

	return len(names) == 0, nil
}

func (f *FileOperator) stateDir(state pkgspool.State) (spool.StateDir, error) {
	switch state {
	case pkgspool.StateActive:
		return f.RootDir.ActiveDir(), nil
	case pkgspool.StateProcessing:
		return f.RootDir.ProcessingDir(), nil
	case pkgspool.StateDone:
		return f.RootDir.DoneDir(), nil
	case pkgspool.StateDead:
		return f.RootDir.DeadDir(), nil
	case pkgspool.StateQuarantine:
		return f.RootDir.QuarantineDir(), nil
	default:
		return spool.StateDir{}, fmt.Errorf("invalid spool state: %q", state)
	}
}

func (f *FileOperator) purgeByAgeUsingDayDirs(
	ctx context.Context,
	fullDirIncludeState string,
	now time.Time,
	maxFileAge int,
) (lifecyclereport.PurgeReport, error) {
	report := lifecyclereport.PurgeReport{}

	if maxFileAge <= 0 {
		return report, fmt.Errorf("max file age must be positive: %d", maxFileAge)
	}

	report.ByAge.Executed = true

	cutoffDate := startOfDay(now).AddDate(0, 0, -maxFileAge)

	err := walkExpiredDayDirsChunked(
		ctx,
		fullDirIncludeState,
		cutoffDate,
		&report.ByAge,
		func(dayDir string) error {
			deletedCount, deletedSize, removedDirCount, err := removeManagedBucketDirFiles(
				ctx,
				fullDirIncludeState,
				dayDir,
				&report.ByAge,
			)
			if err != nil {
				return fmt.Errorf("failed to remove expired day dir %s: %w", dayDir, err)
			}

			report.ByAge.DeletedFileCount += deletedCount
			report.ByAge.DeletedTotalSize += deletedSize
			report.ByAge.RemovedEmptyDirCount += removedDirCount

			removedParents, err := removeEmptyParents(filepath.Dir(dayDir), fullDirIncludeState)
			if err != nil {
				return fmt.Errorf("failed to remove empty parent dirs after age purge: %w", err)
			}
			report.ByAge.RemovedEmptyDirCount += removedParents

			return nil
		},
	)
	if err != nil {
		return report, err
	}

	return report, nil
}

func (f *FileOperator) StatePurgeEnabled(state pkgspool.State) bool {
	if f == nil || f.Config == nil {
		return false
	}

	switch state {
	case pkgspool.StateDone:
		return f.Config.EnablePurgeDone()
	case pkgspool.StateDead:
		return f.Config.EnablePurgeDead()
	case pkgspool.StateQuarantine:
		return f.Config.EnablePurgeQuarantine()
	default:
		return false
	}
}
