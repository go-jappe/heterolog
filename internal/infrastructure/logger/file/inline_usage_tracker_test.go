package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgtime "github.com/go-jappe/heterolog/pkg/time"

	"github.com/stretchr/testify/require"
)

type cleanupScanCounts struct {
	byTarget atomic.Int32
	all      atomic.Int32
	reads    atomic.Int32
}

type fakePurgeFileScanner struct {
	scan func(
		logpattern.FileNamePattern,
		logpattern.GlobMode,
	) ([]fsstore.FileInfo, error)
	observe func(
		logpattern.FileNamePattern,
		logpattern.GlobMode,
	) (fsstore.FileInspection, error)
}

func (f *fakePurgeFileScanner) Scan(
	fileName logpattern.FileNamePattern,
	mode logpattern.GlobMode,
) (fsstore.FileInspection, error) {
	if f.observe != nil {
		return f.observe(fileName, mode)
	}
	files, err := f.scan(fileName, mode)
	if err != nil {
		return fsstore.FileInspection{}, err
	}
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	return fsstore.FileInspection{Paths: paths, RegularFiles: files}, nil
}

func observeCleanupScans(logger *FileLogger) *cleanupScanCounts {
	counts := &cleanupScanCounts{}
	production := filesystemPurgeFileScanner{}
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{observe: func(
		fileName logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) (fsstore.FileInspection, error) {
		switch mode {
		case logpattern.GlobByTarget:
			counts.byTarget.Add(1)
		case logpattern.GlobAllFiles:
			counts.all.Add(1)
		}
		counts.reads.Add(1)
		return production.Scan(fileName, mode)
	}}
	return counts
}

func newInlineUsageTestLogger(t *testing.T, now time.Time, name string) *FileLogger {
	t.Helper()
	cfg := newTestFileLoggerConfig(t.TempDir(), name, name)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })
	return logger
}

func newInlineUsageDebugTestLogger(t *testing.T, now time.Time, name string) *FileLogger {
	t.Helper()
	cfg := newTestFileLoggerConfig(t.TempDir(), name, name)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.RuntimeConfig.IsDebug = true
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })
	return logger
}

func TestInlineAgePurgeStateUsesClockCalendarDay(t *testing.T) {
	ctx := context.Background()
	location, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	firstAttempt := time.Date(2026, time.August, 22, 0, 30, 0, 0, location)
	logger := newInlineUsageTestLogger(t, firstAttempt, "inline-age-calendar-state")

	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	firstAgePurgeReferenceDate := pkgtime.StartOfDay(firstAttempt)
	require.Equal(t, firstAgePurgeReferenceDate, logger.inlinePurge.lastAgePurgeDate)
	require.Same(t, location, logger.inlinePurge.lastAgePurgeDate.Location())

	// UTC上では日付が変わっても、Clockの同じcalendar dayなら再実行しません。
	sameCalendarDay := firstAttempt.Add(10 * time.Hour)
	logger.Clock = pkgclock.NewFixedClock(sameCalendarDay)
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.Equal(t, firstAgePurgeReferenceDate, logger.inlinePurge.lastAgePurgeDate)

	nextCalendarDay := firstAttempt.Add(24 * time.Hour)
	logger.Clock = pkgclock.NewFixedClock(nextCalendarDay)
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.Equal(
		t,
		pkgtime.StartOfDay(nextCalendarDay),
		logger.inlinePurge.lastAgePurgeDate,
	)
}

func TestMergeRemainingTargetFilesOmitsMissingTargetAndKeepsOtherFiles(t *testing.T) {
	allFiles := []fsstore.FileInfo{
		{Path: filepath.Join("logs", "missing.log"), Size: 10},
		{Path: filepath.Join("logs", "failed.log"), Size: 20},
		{Path: filepath.Join("logs", "other.log"), Size: 30},
	}
	targetFiles := allFiles[:2]
	remainingTargetFiles := []fsstore.FileInfo{allFiles[1]}

	remaining := mergeRemainingTargetFiles(allFiles, targetFiles, remainingTargetFiles)

	require.Equal(t, []fsstore.FileInfo{allFiles[1], allFiles[2]}, remaining)
}

func TestSelectFileSeriesInfoFiltersDirectoryResultWithoutAnotherScan(t *testing.T) {
	fileName := logpattern.FileNamePattern{
		Prefix:    "error_",
		Target:    "application",
		Extension: "log",
	}
	files := []fsstore.FileInfo{
		{Path: "/logs/error_application_20260827_1.log"},
		{Path: "/logs/error_application_20260827_2.log.custom"},
		{Path: "/logs/debug_application_20260827_1.log"},
		{Path: "/logs/error_other_20260827_1.log"},
		{Path: "/logs/error_application_20260827_3.log.gz.tmp"},
	}

	selected := selectFileSeriesInfo(files, fileName)
	require.Equal(t, files[:2], selected)
}

func TestSelectFileSeriesInfoAcceptsLiteralBracketWithoutFiles(t *testing.T) {
	selected := selectFileSeriesInfo(nil, logpattern.FileNamePattern{
		Target:    "[",
		Extension: "log",
	})
	require.Empty(t, selected)
}

func TestSelectFileSeriesInfoUsesDirectoryCaseRulesAndPreservesPaths(t *testing.T) {
	t.Parallel()
	files := []fsstore.FileInfo{
		{Path: filepath.Join("Logs", "Error_Application[1]_20260915_1.log.gz"), Size: 10},
		{Path: filepath.Join("Logs", "error_application[1]_20260915_2.log"), Size: 20},
		{Path: filepath.Join("Logs", "ERROR_APPLICATION[1]_20260915_3.LOG.ZST"), Size: 30},
		{Path: filepath.Join("Logs", "Error_Application1_20260915_4.log.gz"), Size: 40},
		{Path: filepath.Join("Logs", "Debug_Application[1]_20260915_5.log.gz"), Size: 50},
	}
	original := append([]fsstore.FileInfo(nil), files...)
	// OS名ではなく保存先directoryの規則を指定し、両方の規則をどのOSでも確認します。
	for _, test := range []struct {
		nameCase pathidentity.NameCase
		want     []fsstore.FileInfo
	}{
		{nameCase: pathidentity.NameCaseSensitive, want: files[:1]},
		{nameCase: pathidentity.NameCaseInsensitive, want: files[:3]},
	} {
		fileName := logpattern.FileNamePattern{
			Prefix:    "Error_",
			Target:    "Application[1]",
			Extension: "log",
			NameCase:  test.nameCase,
		}
		require.Equal(t, test.want, selectFileSeriesInfo(files, fileName), "name case %d", test.nameCase)
	}
	require.Equal(t, original, files, "comparison must not rewrite input paths")
}

func TestSelectFileSeriesInfoMatchesGlobByTarget(t *testing.T) {
	directory := t.TempDir()
	// 大小文字を区別しないfilesystemでも共存できる名前だけを作成します。
	for _, name := range []string{
		"error_application1_20260827_1.log",
		"error_application1_20260827_2.log.custom",
		"error_application[1]_20260827_1.log",
		"error_application[1]_20260827_2.log.custom",
		"ERROR_APPLICATION[1]_20260827_3.LOG.CUSTOM",
		"debug_application1_20260827_1.log",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), nil, 0o600))
	}

	// 走査と系列の選択が同じ規則を使うことを、directoryの両方の規則で確認します。
	// 規則を指定しない場合は、保存先directoryの判定を使います。
	for _, test := range []struct {
		name     string
		nameCase pathidentity.NameCase
	}{
		{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
		{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive},
		{name: "directory rule", nameCase: pathidentity.NameCaseUnspecified},
	} {
		t.Run(test.name, func(t *testing.T) {
			fileName := logpattern.FileNamePattern{
				Dir:       directory,
				Prefix:    "error_",
				Target:    "application[1]",
				Extension: "log",
				NameCase:  test.nameCase,
			}
			scanner := filesystemPurgeFileScanner{}
			allFiles, err := scanner.Scan(fileName, logpattern.GlobAllFiles)
			require.NoError(t, err)
			selected := selectFileSeriesInfo(allFiles.RegularFiles, fileName)
			byTarget, err := scanner.Scan(fileName, logpattern.GlobByTarget)
			require.NoError(t, err)

			selectedPaths := make([]string, 0, len(selected))
			for _, file := range selected {
				selectedPaths = append(selectedPaths, file.Path)
			}
			byTargetPaths := make([]string, 0, len(byTarget.RegularFiles))
			for _, file := range byTarget.RegularFiles {
				byTargetPaths = append(byTargetPaths, file.Path)
			}
			require.Equal(t, byTargetPaths, selectedPaths)
			want := []string{
				filepath.Join(directory, "error_application[1]_20260827_1.log"),
				filepath.Join(directory, "error_application[1]_20260827_2.log.custom"),
			}
			if test.nameCase.ForDirectory(directory) == pathidentity.NameCaseInsensitive {
				want = append(want, filepath.Join(directory, "ERROR_APPLICATION[1]_20260827_3.LOG.CUSTOM"))
			}
			require.ElementsMatch(t, want, selectedPaths)
		})
	}
}

func TestPurgeObservedFileSeriesStopsAfterAgeDeleteFailure(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-age-delete-failure")
	directory := t.TempDir()

	old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
	failingPath := filepath.Join(directory, "old-non-empty_"+old.Format(pkgfsstore.FileDateFormat)+"_1.log")
	require.NoError(t, os.Mkdir(failingPath, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(failingPath, "child"),
		[]byte("keep"),
		0o600,
	))
	require.NoError(t, os.Chtimes(failingPath, old, old))

	files := []fsstore.FileInfo{{
		Path:    failingPath,
		Size:    1,
		ModTime: old,
	}}
	for index := 0; index < logger.inlinePurge.config.MaxFileBackups(); index++ {
		path := filepath.Join(directory, fmt.Sprintf("recent_%s_%d.log", now.Format(pkgfsstore.FileDateFormat), index+1))
		require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
		files = append(files, fsstore.FileInfo{
			Path:    path,
			Size:    4,
			ModTime: now,
		})
	}

	result := logger.purgeObservedFileSeries(
		context.Background(),
		logger.inlinePurge,
		files,
		newPurgeFileNames(logger.Fsstore.FileName),
		pkgtime.StartOfDay(now),
		true,
		nil,
		runNotificationImmediately,
	)

	require.True(t, result.Ran)
	require.Len(t, result.RemovalResults, 1)
	require.Equal(t, failingPath, result.RemovalResults[0].File.Path)
	require.Error(t, result.RemovalResults[0].Err)
	require.Len(t, result.Remaining, len(files))
	require.Equal(t, pkgtime.StartOfDay(now), logger.inlinePurge.lastAgePurgeDate)
	for _, file := range files[1:] {
		require.FileExists(t, file.Path)
	}
}

func TestPurgeLogsByGenerationKeepsProtectedFileWhenLimitIsNotExceeded(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "generation-noop-protected")
	activeInfo, err := os.Stat(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	inactivePath := filepath.Join(filepath.Dir(logger.Fsstore.ActiveFilePath), "inactive.log")
	require.NoError(t, os.WriteFile(inactivePath, []byte("inactive"), 0o600))
	inactiveInfo, err := os.Stat(inactivePath)
	require.NoError(t, err)

	files := []fsstore.FileInfo{
		{
			Path:    logger.Fsstore.ActiveFilePath,
			Size:    uint64(activeInfo.Size()),
			ModTime: activeInfo.ModTime(),
		},
		{
			Path:    inactivePath,
			Size:    uint64(inactiveInfo.Size()),
			ModTime: inactiveInfo.ModTime(),
		},
	}

	result := logger.purgeLogsByGeneration(
		context.Background(),
		logger.inlinePurge,
		files,
		newPurgeFileNames(logger.Fsstore.FileName),
		nil,
		runNotificationImmediately,
	)

	require.False(t, result.Ran)
	require.Equal(t, files, result.Remaining)
	require.Empty(t, result.RemovalResults)
}

// 保持数は圧縮形式ごとではなく、日付・番号で決まる世代単位で数えます。
func TestPurgeLogsByGenerationCountsRawAndCompressedFilesAsOneGeneration(t *testing.T) {
	now := time.Date(2026, time.August, 27, 13, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "generation-by-compression-suffix")
	maxBackups := logger.inlinePurge.config.MaxFileBackups()
	directory := logger.Fsstore.FileName.Dir

	var (
		files     []fsstore.FileInfo
		rawPaths  []string
		gzipPaths []string
	)
	writeSeriesFile := func(name string, modifiedAt time.Time) string {
		path := filepath.Join(directory, name)
		require.NoError(t, os.WriteFile(path, []byte("retained"), 0o600))
		require.NoError(t, os.Chtimes(path, modifiedAt, modifiedAt))
		info, err := os.Stat(path)
		require.NoError(t, err)
		files = append(files, fsstore.FileInfo{
			Path:    path,
			Size:    uint64(info.Size()),
			ModTime: info.ModTime(),
		})
		return path
	}

	for index := 0; index < maxBackups+1; index++ {
		rawPaths = append(rawPaths, writeSeriesFile(
			fmt.Sprintf("application_20260827_%d.log", index+1),
			now.Add(time.Duration(index)*time.Minute),
		))
	}
	for index := 0; index < maxBackups; index++ {
		gzipPaths = append(gzipPaths, writeSeriesFile(
			fmt.Sprintf("application_20260827_%d.log.gz", index+1),
			now.Add(time.Duration(maxBackups+index+1)*time.Minute),
		))
	}

	result := logger.purgeLogsByGeneration(
		context.Background(),
		logger.inlinePurge,
		files,
		newPurgeFileNames(logger.Fsstore.FileName),
		nil,
		runNotificationImmediately,
	)

	require.True(t, result.Ran)
	// 上限を一世代超えたため、最も古い世代の未圧縮・圧縮fileをまとめて削除します。
	removedPaths := make([]string, 0, len(result.RemovalResults))
	for _, removal := range result.RemovalResults {
		require.NoError(t, removal.Err)
		removedPaths = append(removedPaths, removal.File.Path)
	}
	require.ElementsMatch(t, []string{rawPaths[0], gzipPaths[0]}, removedPaths,
		"raw and gzip files of one generation must count once and expire together")
	require.NoFileExists(t, rawPaths[0])
	require.NoFileExists(t, gzipPaths[0])
	for _, path := range append(rawPaths[1:], gzipPaths[1:]...) {
		require.FileExists(t, path)
	}

}

func TestInlinePurgeStopsDirectorySizeAfterAgeDeleteFailure(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-age-failure-stops-size")
	fileName := logger.Fsstore.FileName
	maxDirectoryBytes := logger.inlinePurge.config.MaxDirSize()

	failingPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260820_10."+fileName.Extension,
	)
	require.NoError(t, os.Mkdir(failingPath, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(failingPath, "child"),
		[]byte("keep"),
		0o600,
	))
	old := now.AddDate(0, 0, -2)
	require.NoError(t, os.Chtimes(failingPath, old, old))

	recentPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260827_10."+fileName.Extension,
	)
	require.NoError(t, os.WriteFile(recentPath, []byte("keep"), 0o600))
	activeInfo, err := os.Stat(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)

	scanResult := []fsstore.FileInfo{
		{
			Path:    failingPath,
			Size:    maxDirectoryBytes + 1,
			ModTime: old,
		},
		{
			Path:    recentPath,
			Size:    4,
			ModTime: now,
		},
		{
			Path:    logger.Fsstore.ActiveFilePath,
			Size:    uint64(activeInfo.Size()),
			ModTime: activeInfo.ModTime(),
		},
	}
	var targetScans atomic.Int32
	var directoryScans atomic.Int32
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
		_ logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		switch mode {
		case logpattern.GlobByTarget:
			targetScans.Add(1)
		case logpattern.GlobAllFiles:
			directoryScans.Add(1)
		}
		return append([]fsstore.FileInfo(nil), scanResult...), nil
	}}

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.EqualValues(t, 0, targetScans.Load())
	require.EqualValues(t, 1, directoryScans.Load(), "combined cleanup must reuse one directory scan")
	require.FileExists(t, recentPath)
	require.Equal(t, pkgtime.StartOfDay(now), logger.inlinePurge.lastAgePurgeDate)
	require.False(t, logger.inlinePurge.directorySizeTracker.TryStartObservation(
		now.UTC(),
		logger.inlinePurge.config,
		false,
	), "age removal failure must not rescan the unchanged over-limit directory immediately")
	require.True(t, logger.inlinePurge.directorySizeTracker.TryStartObservation(
		now.Add(logger.inlinePurge.config.UsageCacheTTL()).UTC(),
		logger.inlinePurge.config,
		false,
	))
	logger.inlinePurge.directorySizeTracker.AbortObservation()
}

func TestInlinePurgeStopsDirectorySizeAfterGenerationDeleteFailure(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(
		t.TempDir(),
		"inline-generation-failure-stops-size",
		"inline-generation-failure-stops-size",
	)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.MaxFileBackups = fsstore.MinAllowedMaxFileBackups
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	logger.inlinePurge.lastAgePurgeDate = pkgtime.StartOfDay(now)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	fileName := logger.Fsstore.FileName
	failingPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260826_100."+fileName.Extension,
	)
	require.NoError(t, os.Mkdir(failingPath, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(failingPath, "child"),
		[]byte("keep"),
		0o600,
	))
	recentPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260827_100."+fileName.Extension,
	)
	require.NoError(t, os.WriteFile(recentPath, []byte("keep"), 0o600))
	newestPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260827_101."+fileName.Extension,
	)
	require.NoError(t, os.WriteFile(newestPath, []byte("keep"), 0o600))
	scanResult := []fsstore.FileInfo{
		{
			Path:    failingPath,
			Size:    logger.inlinePurge.config.MaxDirSize() + 1,
			ModTime: now.Add(-time.Hour),
		},
		{Path: recentPath, Size: 4, ModTime: now},
		{Path: newestPath, Size: 4, ModTime: now.Add(time.Hour)},
	}

	var targetScans atomic.Int32
	var directoryScans atomic.Int32
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
		_ logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		switch mode {
		case logpattern.GlobByTarget:
			targetScans.Add(1)
		case logpattern.GlobAllFiles:
			directoryScans.Add(1)
		}
		return append([]fsstore.FileInfo(nil), scanResult...), nil
	}}

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.EqualValues(t, 0, targetScans.Load())
	require.EqualValues(t, 1, directoryScans.Load(), "combined cleanup must reuse one directory scan")
	require.DirExists(t, failingPath)
	require.FileExists(t, recentPath)
	require.FileExists(t, newestPath)
	require.False(t, logger.inlinePurge.directorySizeTracker.TryStartObservation(
		now.UTC(),
		logger.inlinePurge.config,
		false,
	), "generation removal failure must not rescan the unchanged over-limit directory immediately")
	require.True(t, logger.inlinePurge.directorySizeTracker.TryStartObservation(
		now.Add(logger.inlinePurge.config.UsageCacheTTL()).UTC(),
		logger.inlinePurge.config,
		false,
	))
	logger.inlinePurge.directorySizeTracker.AbortObservation()
}

func TestInlineSeriesDeleteFailureCompletesUnderLimitDirectoryObservation(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-series-failure-finishes-observation")
	fileName := logger.Fsstore.FileName
	failingPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260820_100."+fileName.Extension,
	)
	require.NoError(t, os.Mkdir(failingPath, 0o700))
	require.NoError(t, os.WriteFile(
		filepath.Join(failingPath, "child"),
		[]byte("keep"),
		0o600,
	))
	old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
		_ logpattern.FileNamePattern,
		_ logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		return []fsstore.FileInfo{{
			Path:    failingPath,
			Size:    4,
			ModTime: old,
		}}, nil
	}}

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.False(t, logger.inlinePurge.directorySizeTracker.TryStartObservation(
		now.UTC(),
		logger.inlinePurge.config,
		false,
	), "a successful shared scan must finish the under-limit directory observation")
}

func TestInlinePurgeRemovesMissingTargetFromDirectoryUsageEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 27, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-missing-target-usage")
	fileName := logger.Fsstore.FileName
	missingPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260820_10."+fileName.Extension,
	)
	activeInfo, err := os.Stat(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	scanResult := []fsstore.FileInfo{
		{
			Path:    missingPath,
			Size:    logger.inlinePurge.config.MaxDirSize() + 1,
			ModTime: now.AddDate(0, 0, -2),
		},
		{
			Path:    logger.Fsstore.ActiveFilePath,
			Size:    uint64(activeInfo.Size()),
			ModTime: activeInfo.ModTime(),
		},
	}
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
		_ logpattern.FileNamePattern,
		_ logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		return append([]fsstore.FileInfo(nil), scanResult...), nil
	}}

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.False(t, logger.inlinePurge.directorySizeTracker.TryStartObservation(
		now.UTC(),
		logger.inlinePurge.config,
		false,
	))
}

func TestFileLoggerEntersInlinePurgeRuntimeOnlyForInlineLifecycle(t *testing.T) {
	tests := []struct {
		name        string
		mode        pkgfsstore.FileLifecycleMode
		wantRuntime bool
	}{
		{
			name:        "inline",
			mode:        pkgfsstore.FileLifecycleModeInline,
			wantRuntime: true,
		},
		{
			name: "runner",
			mode: pkgfsstore.FileLifecycleModeRunner,
		},
		{
			name: "disable",
			mode: pkgfsstore.FileLifecycleModeDisable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, time.August, 25, 17, 0, 0, 0, time.UTC)
			cfg := newTestFileLoggerConfig(t.TempDir(), "usage-growth-"+tt.name, "usage-growth-"+tt.name)
			cfg.LifecycleMode = tt.mode
			cfg.FsstoreConfig.Lifecycle.EnableRotate = true
			logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
			require.NoError(t, err)
			logger.Clock = pkgclock.NewFixedClock(now)
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

			inlinePurge := logger.inlinePurge
			if tt.wantRuntime {
				require.NotNil(t, inlinePurge)
			} else {
				require.Nil(t, inlinePurge)
			}

			usageLimitConfig := logger.config.Fsstore().Purge()

			if inlinePurge != nil {
				directoryTracker := inlinePurge.directorySizeTracker
				require.True(t, directoryTracker.TryStartObservation(
					now,
					usageLimitConfig,
					true,
				))
				directoryTracker.FinishObservation(usageLimitConfig.MaxDirSize())
			}

			// inlineのbackground cleanupが観測値を先に消費しないよう、今回のwrite中だけ
			// cleanup gateを取得済みにします。runner・disableではruntimeがnilのままでも
			// rotationを伴うwriteがinline処理へ入らないことも同じ経路で検証します。
			if inlinePurge != nil {
				inlinePurge.cleanupRunning.Store(true)
			}
			require.NoError(t, os.Truncate(
				logger.Fsstore.ActiveFilePath,
				int64(logger.config.Fsstore().Lifecycle().MaxFileSize()),
			))
			require.NoError(t, logger.testInfo(ctx, "record usage only for inline lifecycle"))
			waitBackgroundForTest(t, logger)
			if inlinePurge != nil {
				inlinePurge.cleanupRunning.Store(false)
			}

			probeAt := now
			if inlinePurge != nil {
				directoryObservation := inlinePurge.directorySizeTracker.TryStartObservation(
					probeAt,
					usageLimitConfig,
					false,
				)
				require.True(t, directoryObservation)
				inlinePurge.directorySizeTracker.AbortObservation()

			}
		})
	}
}

func TestInlineDirectoryCleanupReconcilesGenerationAtUsageCacheTTL(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-usage-fast-path")
	counts := observeCleanupScans(logger)

	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.EqualValues(t, 0, counts.byTarget.Load())
	require.EqualValues(t, 1, counts.all.Load())
	require.EqualValues(t, 1, counts.reads.Load())

	require.NoError(t, logger.testInfo(ctx, "ordinary append stays below max directory size"))
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, counts.reads.Load(), "an ordinary under-limit append must not observe files")

	fileName := logger.Fsstore.FileName
	var generationPaths []string
	for index := 0; index <= logger.inlinePurge.config.MaxFileBackups(); index++ {
		path := filepath.Join(
			fileName.Dir,
			fmt.Sprintf(
				"%s%s_%s_%d.%s",
				fileName.Prefix,
				fileName.Target,
				fileName.Date,
				100+index,
				fileName.Extension,
			),
		)
		require.NoError(t, os.WriteFile(path, []byte("external generation"), 0o600))
		modifiedAt := now.Add(
			time.Duration(index-logger.inlinePurge.config.MaxFileBackups()-1) * time.Minute,
		)
		require.NoError(t, os.Chtimes(path, modifiedAt, modifiedAt))
		generationPaths = append(generationPaths, path)
	}

	usageCacheTTL := logger.config.Fsstore().Purge().UsageCacheTTL()
	logger.Clock = pkgclock.NewFixedClock(now.Add(usageCacheTTL - time.Nanosecond))
	require.NoError(t, logger.testInfo(ctx, "append before directory usage cache TTL"))
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, counts.reads.Load(), "a still-fresh estimate must not observe files")
	require.FileExists(t, generationPaths[0],
		"rotation count is not tracked and must not start a precision-only scan")

	// timerは使わず、UsageCacheTTL期限切れ後に到着した次のinline cleanupが
	// directory全体の一回のsnapshotで使用byte数とgeneration保持数を再評価します。
	logger.Clock = pkgclock.NewFixedClock(now.Add(usageCacheTTL))
	require.NoError(t, logger.testInfo(ctx, "append at directory usage cache TTL"))
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 0, counts.byTarget.Load(),
		"generation cleanup must reuse the directory snapshot without a target scan")
	require.EqualValues(t, 2, counts.all.Load())
	require.EqualValues(t, 2, counts.reads.Load())
	require.NoFileExists(t, generationPaths[0])
	for _, path := range generationPaths[1:] {
		require.FileExists(t, path)
	}
}

func TestInlineCleanupFastPathDoesNotRegisterBackgroundOperation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-cleanup-no-background-fast-path")
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))

	logger.Fsstore.Mu.Lock()
	fileNameSnapshot := logger.Fsstore.FileName
	logger.Fsstore.Mu.Unlock()

	// beginBackgroundOperationはbackgroundMuを取得します。上限以下・TTL内・同日で
	// 走査理由がない要求がこのlockを待たずに返れば、background登録より前に
	// fast pathが終了したことを確認できます。
	logger.backgroundMu.Lock()
	// 待機を検出して失敗した場合も、cleanupが待つlockを解放してからloggerを閉じます。
	unlockBackground := sync.OnceFunc(logger.backgroundMu.Unlock)
	t.Cleanup(unlockBackground)
	returned := make(chan struct{})
	go func() {
		logger.triggerCleanup(ctx, fileNameSnapshot)
		close(returned)
	}()
	receiveForTest(t, returned, "cleanup fast path waited for background operation registration")
	unlockBackground()

	require.False(t, logger.inlinePurge.cleanupRunning.Load())
}

func TestInlineDailyAgePurgeReconcilesFileGenerationBeforeDirectoryUsageCacheExpires(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(
		t.TempDir(),
		"inline-generation-daily-reconciliation",
		"inline-generation-daily-reconciliation",
	)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	// 日付変更によるrotationではなく、通常writeの日次age判定だけを検証します。
	cfg.FsstoreConfig.Lifecycle.EnableRotate = false
	cfg.FsstoreConfig.Lifecycle.UsageCacheTTL = 48 * time.Hour
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	counts := observeCleanupScans(logger)

	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.EqualValues(t, 1, counts.all.Load())
	require.EqualValues(t, 0, counts.byTarget.Load())

	fileName := logger.Fsstore.FileName
	manuallyAddedPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260824_999."+fileName.Extension,
	)
	require.NoError(t, os.WriteFile(manuallyAddedPath, []byte("manually restored log"), 0o600))
	require.NoError(t, os.Chtimes(manuallyAddedPath, now, now))

	// 手動追加は、同じ日かつdirectory概算が上限以下では走査を起動しません。
	logger.Clock = pkgclock.NewFixedClock(now.Add(time.Hour))
	require.NoError(t, logger.testInfo(ctx, "same-day append does not scan file generations"))
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, counts.reads.Load())

	// directory使用量のTTLは有効なままでも、日付変更後のage purgeはfile系列だけを
	// 走査し、手動追加も含めて保持世代数を日次で補正します。
	logger.Clock = pkgclock.NewFixedClock(now.Add(24 * time.Hour))
	require.NoError(t, logger.testInfo(ctx, "next-day append starts age and generation purge"))
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, counts.all.Load())
	require.EqualValues(t, 1, counts.byTarget.Load())
	require.EqualValues(t, 2, counts.reads.Load())
}

func TestInlineCleanupObservesEstimatedThresholdExcessImmediately(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-usage-threshold-excess")
	counts := observeCleanupScans(logger)
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))

	maxDirSize := logger.config.Fsstore().Purge().MaxDirSize()
	logger.inlinePurge.directorySizeTracker.RecordGrowth(maxDirSize + 1)

	require.NoError(t, logger.testInfo(ctx, "append after estimated directory threshold excess"))
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 2, counts.all.Load())
	require.EqualValues(t, 2, counts.reads.Load(), "a new threshold crossing must observe immediately")
	require.FileExists(t, logger.Fsstore.ActiveFilePath)
}

func TestInlineDirectoryLimitUsesObservedUsageForDeletion(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-usage-soft-limit")
	counts := observeCleanupScans(logger)
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))

	maxDirSize := logger.config.Fsstore().Purge().MaxDirSize()
	inactivePath := filepath.Join(filepath.Dir(logger.Fsstore.ActiveFilePath), "inactive-size_20260824_1.log")
	file, err := os.Create(inactivePath)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, os.Truncate(inactivePath, int64(maxDirSize+1)))
	old := now.Add(-time.Hour)
	require.NoError(t, os.Chtimes(inactivePath, old, old))
	logger.inlinePurge.directorySizeTracker.RecordGrowth(maxDirSize + 1)

	require.NoError(t, logger.testInfo(ctx, "append starts observed directory-size purge"))
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 2, counts.all.Load())
	require.NoFileExists(t, inactivePath)
	require.FileExists(t, logger.Fsstore.ActiveFilePath)
}

func TestInlineDirectorySizePurgeWarnsWhenActiveFilesAloneExceedLimit(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 30, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-active-files-over-limit")
	maxDirSize := logger.inlinePurge.config.MaxDirSize()
	var warnings []*logging.LogEntry
	logger.OnWarn = func(
		_ context.Context,
		entry *logging.LogEntry,
		_ pkgobserver.Tracer,
		_ pkgobserver.Meter,
	) {
		warnings = append(warnings, entry)
	}

	remaining := logger.purgeByDirectorySize(
		ctx,
		logger.inlinePurge,
		[]fsstore.FileInfo{{
			Path: logger.Fsstore.ActiveFilePath,
			Size: maxDirSize + 1,
		}},
		newPurgeFileNames(logger.Fsstore.FileName),
		new(bool),
		runNotificationImmediately,
	)

	require.Len(t, remaining, 1)
	require.Len(t, warnings, 1)
	require.Equal(t, WarnMsgActiveLogFilesExceedMaxDirSize, warnings[0].Message)
	require.Equal(t, maxDirSize+1, warnings[0].Fields["activeFileSize"])
	require.Equal(t, 1, warnings[0].Fields["activeFiles"])
	require.Equal(t, maxDirSize, warnings[0].Fields["maxDirSize"])
}

func TestInlineDirectorySizePurgeUsesFileNameSnapshotWhileLiveNameChanges(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 30, 10, 15, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-directory-file-name-snapshot")
	originalFileName := logger.Fsstore.FileName
	fileNameSnapshot := originalFileName
	fileNameSnapshot.Dir = filepath.Join(fileNameSnapshot.Dir, "observed-snapshot")
	maxDirSize := logger.inlinePurge.config.MaxDirSize()

	var warningDirectory string
	logger.OnWarn = func(
		_ context.Context,
		entry *logging.LogEntry,
		_ pkgobserver.Tracer,
		_ pkgobserver.Meter,
	) {
		warningDirectory, _ = entry.Fields["directoryPath"].(string)
	}

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	// 途中で失敗しても、名前を書き換え続けるgoroutineを止め、元の名前に戻してからloggerを閉じます。
	stopWriter := sync.OnceFunc(func() {
		close(stop)
		receiveForTest(t, writerDone, "live file name writer stop")
		logger.Fsstore.Mu.Lock()
		logger.Fsstore.FileName = originalFileName
		logger.Fsstore.Mu.Unlock()
	})
	t.Cleanup(stopWriter)
	go func() {
		defer close(writerDone)
		for index := 0; ; index++ {
			select {
			case <-stop:
				return
			default:
			}
			logger.Fsstore.Mu.Lock()
			logger.Fsstore.FileName.Dir = fmt.Sprintf("live-directory-%d", index%2)
			logger.Fsstore.Mu.Unlock()
		}
	}()

	for range 100 {
		remaining := logger.purgeByDirectorySize(
			ctx,
			logger.inlinePurge,
			[]fsstore.FileInfo{{
				Path: logger.Fsstore.ActiveFilePath,
				Size: maxDirSize + 1,
			}},
			newPurgeFileNames(fileNameSnapshot),
			new(bool),
			runNotificationImmediately,
		)
		require.Len(t, remaining, 1)
	}
	stopWriter()

	require.Equal(t, fileNameSnapshot.Dir, warningDirectory)
}

func TestInlineDirectorySizePurgeWarnsWhenNoFileIsEligible(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 30, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-no-file-eligible")
	maxDirSize := logger.inlinePurge.config.MaxDirSize()
	retainedPath := filepath.Join(filepath.Dir(logger.Fsstore.ActiveFilePath), "retained.log")
	require.NoError(t, os.WriteFile(retainedPath, []byte("retained"), 0o600))
	logger.lifecycleGuard.ProtectPaths(logger.lifecycleToken, retainedPath)
	t.Cleanup(func() {
		logger.lifecycleGuard.UnprotectPaths(logger.lifecycleToken, retainedPath)
	})
	var warnings []*logging.LogEntry
	logger.OnWarn = func(
		_ context.Context,
		entry *logging.LogEntry,
		_ pkgobserver.Tracer,
		_ pkgobserver.Meter,
	) {
		warnings = append(warnings, entry)
	}

	remaining := logger.purgeByDirectorySize(
		ctx,
		logger.inlinePurge,
		[]fsstore.FileInfo{{
			Path: retainedPath,
			Size: maxDirSize + 1,
		}},
		newPurgeFileNames(logger.Fsstore.FileName),
		new(bool),
		runNotificationImmediately,
	)

	require.Len(t, remaining, 1)
	require.Len(t, warnings, 1)
	require.Equal(t, WarnMsgNoFilesEligibleForDirSizePurge, warnings[0].Message)
	require.Equal(t, maxDirSize+1, warnings[0].Fields["directorySize"])
	require.Equal(t, 1, warnings[0].Fields["remainingFiles"])
	require.FileExists(t, retainedPath)
}

func TestInlineDirectorySizePurgeDispatchesRemoveFailureToOnErrorOnce(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 30, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-directory-remove-failure")
	maxDirSize := logger.inlinePurge.config.MaxDirSize()
	failingPath := filepath.Join(filepath.Dir(logger.Fsstore.ActiveFilePath), "non-empty.log")
	require.NoError(t, os.Mkdir(failingPath, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(failingPath, "child"), []byte("keep"), 0o600))
	var (
		dispatchedErrors []error
		errorEntries     []*logging.LogEntry
		warnings         []*logging.LogEntry
	)
	logger.OnError = func(
		_ context.Context,
		err error,
		entry *logging.LogEntry,
		_ pkgobserver.Tracer,
		_ pkgobserver.Meter,
	) {
		dispatchedErrors = append(dispatchedErrors, err)
		errorEntries = append(errorEntries, entry)
	}
	logger.OnWarn = func(
		_ context.Context,
		entry *logging.LogEntry,
		_ pkgobserver.Tracer,
		_ pkgobserver.Meter,
	) {
		warnings = append(warnings, entry)
	}

	remaining := logger.purgeByDirectorySize(
		ctx,
		logger.inlinePurge,
		[]fsstore.FileInfo{{
			Path: failingPath,
			Size: maxDirSize + 1,
		}},
		newPurgeFileNames(logger.Fsstore.FileName),
		new(bool),
		runNotificationImmediately,
	)

	require.Len(t, remaining, 1)
	require.Len(t, dispatchedErrors, 1)
	require.Len(t, errorEntries, 1)
	var removalFailure *fsstore.FileRemovalFailure
	require.ErrorAs(t, dispatchedErrors[0], &removalFailure)
	require.Equal(t, failingPath, removalFailure.File.Path)
	require.Equal(t, ErrorMsgPurgeLogsByDirectorySize, errorEntries[0].Message)
	require.Empty(t, warnings, "a removal failure is an OnError event, not a no-candidate warning")
}

func TestInlineRotationStartsDirectoryObservationAfterArchiveProtectionEnds(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(
		t.TempDir(),
		"inline-rotation-generation-cleanup",
		"inline-rotation-generation-cleanup",
	)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	counts := observeCleanupScans(logger)

	fileName := logger.Fsstore.FileName
	for index := 100; index < 102; index++ {
		path := filepath.Join(
			fileName.Dir,
			fmt.Sprintf(
				"%s%s_%s_%d.%s",
				fileName.Prefix,
				fileName.Target,
				fileName.Date,
				index,
				fileName.Extension,
			),
		)
		require.NoError(t, os.WriteFile(path, []byte("rotated"), 0o600))
		modifiedAt := now.Add(-time.Duration(102-index) * time.Hour)
		require.NoError(t, os.Chtimes(path, modifiedAt, modifiedAt))
	}

	// 初回のdirectory観測で既存世代も実fileから確認します。
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.EqualValues(t, 1, counts.all.Load())
	require.EqualValues(t, 0, counts.byTarget.Load())

	oldActivePath := logger.Fsstore.ActiveFilePath
	require.NoError(t, os.Truncate(
		oldActivePath,
		int64(logger.config.Fsstore().Lifecycle().MaxFileSize()),
	))
	require.NoError(t, logger.testInfo(ctx, "rotate and observe directory after protection ends"))
	waitBackgroundForTest(t, logger)

	require.NotEqual(t, oldActivePath, logger.Fsstore.ActiveFilePath)
	// rotationは低頻度なので、archive保護解除後にdirectory全体を一度観測し、
	// 同じsnapshotでgenerationも再評価します。
	require.EqualValues(t, 2, counts.all.Load())
	require.EqualValues(t, 0, counts.byTarget.Load())
	require.EqualValues(t, 2, counts.reads.Load())
}

func TestInlineCleanupRetriesFailedObservationOnNextCleanup(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-usage-observation-failure")
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))

	attempt := now.AddDate(0, 0, 1)
	logger.Clock = pkgclock.NewFixedClock(attempt)
	var reads atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	releaseScan := releaseOnCleanupForTest(t, release)
	sentinel := errors.New("purge file scan unavailable")
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
		logpattern.FileNamePattern,
		logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		if reads.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil, sentinel
	}}

	logger.Fsstore.Mu.Lock()
	fileNameSnapshot := logger.Fsstore.FileName
	logger.Fsstore.Mu.Unlock()
	logger.triggerCleanup(ctx, fileNameSnapshot)
	receiveForTest(t, started, "first cleanup observation start")
	// 実行中のcleanup要求は待機・pending化せず破棄します。
	logger.triggerCleanup(ctx, fileNameSnapshot)
	logger.triggerCleanup(ctx, fileNameSnapshot)
	releaseScan()
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, reads.Load(), "an in-flight owner must drop concurrent cleanup requests")

	// 観測失敗はAbortされるため、完了後の次のcleanupで直ちに再試行します。
	logger.triggerCleanup(ctx, fileNameSnapshot)
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 2, reads.Load(), "a failed observation must retry on the next cleanup")
}

func TestInlineCleanupRetriesCombinedObservationAfterDirectoryScanFailure(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-directory-scan-retry")
	production := filesystemPurgeFileScanner{}
	sentinel := errors.New("directory scan unavailable")
	var targetScans atomic.Int32
	var directoryScans atomic.Int32
	var failFirstDirectoryScan atomic.Bool
	failFirstDirectoryScan.Store(true)
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{observe: func(
		fileName logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) (fsstore.FileInspection, error) {
		switch mode {
		case logpattern.GlobByTarget:
			targetScans.Add(1)
		case logpattern.GlobAllFiles:
			directoryScans.Add(1)
			if failFirstDirectoryScan.CompareAndSwap(true, false) {
				return fsstore.FileInspection{}, sentinel
			}
		}
		return production.Scan(fileName, mode)
	}}

	err := purgeFileLoggerForTest(ctx, logger)
	require.ErrorIs(t, err, sentinel)
	require.EqualValues(t, 0, targetScans.Load())
	require.EqualValues(t, 1, directoryScans.Load())

	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.EqualValues(t, 0, targetScans.Load())
	require.EqualValues(t, 2, directoryScans.Load())
}

func TestInlineCleanupReleasesDirectoryObservationAfterScannerPanic(t *testing.T) {
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-scanner-panic")
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
		logpattern.FileNamePattern,
		logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		panic("purge scanner panic")
	}}

	require.PanicsWithValue(t, "purge scanner panic", func() {
		_ = purgeFileLoggerForTest(context.Background(), logger)
	})
	require.True(t, logger.inlinePurge.directorySizeTracker.TryStartObservation(
		now.UTC(),
		logger.inlinePurge.config,
		false,
	), "a panic after reservation must release the directory observation")
	logger.inlinePurge.directorySizeTracker.AbortObservation()
}

func TestTargetOnlyPurgePanicInvalidatesEstimateAndReleasesRetainedUploadPath(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 30, 0, 0, time.UTC)
	logger := newInlineUsageDebugTestLogger(t, now, "target-purge-panic")
	logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploadErrorUploader{err: errors.New("not reached")})
	tracker := logger.inlinePurge.directorySizeTracker
	require.True(t, tracker.TryStartObservation(now, logger.inlinePurge.config, true))
	tracker.FinishObservation(0)

	old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
	pendingPath := writePendingCompressedLog(t, logger, "20260820", 1, old)
	fileName := logger.Fsstore.FileName
	deletablePath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260819_1."+fileName.Extension,
	)
	require.NoError(t, os.WriteFile(deletablePath, []byte("delete"), 0o600))
	require.NoError(t, os.Chtimes(deletablePath, old, old))
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
		_ logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		require.Equal(t, logpattern.GlobByTarget, mode)
		return []fsstore.FileInfo{
			{Path: deletablePath, Size: 6, ModTime: old},
			{Path: pendingPath, Size: 1, ModTime: old},
		}, nil
	}}
	logger.OnDebug = func(
		context.Context,
		*logging.LogEntry,
		pkgobserver.Tracer,
		pkgobserver.Meter,
	) {
		panic("target purge report panic")
	}

	plan := inlinePurgePlan{
		agePurgeReferenceDate: now,
		dailyAgePurgeDue:      true,
	}
	require.PanicsWithValue(t, "target purge report panic", func() {
		_ = logger.cleanupOldLogs(context.Background(), fileName, plan)
	})
	require.NoFileExists(t, deletablePath)
	require.FileExists(t, pendingPath)
	require.True(t, tracker.TryStartObservation(now, logger.inlinePurge.config, false),
		"a target-only deletion followed by panic must invalidate the fresh estimate")
	tracker.AbortObservation()
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.NotContains(t, protected, filepath.Clean(pendingPath),
			"panic must release the pending upload retain")
	})
}

func TestDirectorySizePurgePanicInvalidatesAndAbortsObservation(t *testing.T) {
	now := time.Date(2026, time.August, 29, 11, 0, 0, 0, time.UTC)
	logger := newInlineUsageDebugTestLogger(t, now, "directory-purge-panic")
	tracker := logger.inlinePurge.directorySizeTracker
	require.True(t, tracker.TryStartObservation(now, logger.inlinePurge.config, true))
	tracker.FinishObservation(0)
	require.True(t, tracker.TryStartObservation(now, logger.inlinePurge.config, true))

	fileName := logger.Fsstore.FileName
	deletablePath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260819_1."+fileName.Extension,
	)
	require.NoError(t, os.WriteFile(deletablePath, []byte("delete"), 0o600))
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
		_ logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		require.Equal(t, logpattern.GlobAllFiles, mode)
		return []fsstore.FileInfo{{
			Path:    deletablePath,
			Size:    logger.inlinePurge.config.MaxDirSize() + 1,
			ModTime: now.Add(-time.Hour),
		}}, nil
	}}
	logger.OnDebug = func(
		context.Context,
		*logging.LogEntry,
		pkgobserver.Tracer,
		pkgobserver.Meter,
	) {
		panic("directory purge report panic")
	}

	plan := inlinePurgePlan{
		agePurgeReferenceDate:     now,
		directorySizeScanAcquired: true,
	}
	require.PanicsWithValue(t, "directory purge report panic", func() {
		_ = logger.cleanupOldLogs(context.Background(), fileName, plan)
	})
	require.NoFileExists(t, deletablePath)
	require.True(t, tracker.TryStartObservation(now, logger.inlinePurge.config, false),
		"a directory deletion followed by panic must invalidate the aborted observation")
	tracker.AbortObservation()
}

func TestPurgeMutationTrackerRestoresCleanStateWhenNoRemovalWasAttempted(t *testing.T) {
	dirty := false
	result := runPurgeAndTrackDirectoryMutation(&dirty, func() fsstore.PurgeResult {
		return fsstore.PurgeResult{Ran: true}
	})

	require.True(t, result.Ran)
	require.False(t, dirty)
}

func TestInlineCleanupReleasesObservationWhenClosePreventsBackgroundStart(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-cleanup-close-before-background")
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))

	tracker := logger.inlinePurge.directorySizeTracker
	config := logger.config.Fsstore().Purge()
	tracker.RecordGrowth(config.MaxDirSize() + 1)
	logger.backgroundMu.Lock()
	logger.closeState = loggercommon.LoggerCloseStateStarted
	logger.backgroundMu.Unlock()

	logger.Fsstore.Mu.Lock()
	fileNameSnapshot := logger.Fsstore.FileName
	logger.Fsstore.Mu.Unlock()
	logger.triggerCleanup(ctx, fileNameSnapshot)

	require.False(t, logger.inlinePurge.cleanupRunning.Load())
	require.True(
		t,
		tracker.TryStartObservation(now, config, false),
		"a directory observation reserved before background registration must be released",
	)
	tracker.AbortObservation()
}

func TestInlineCleanupReleasesGateAfterDirectoryObservationStartPanics(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-usage-observation-start-panic")

	originalConfig := logger.inlinePurge.config
	logger.inlinePurge.config = panickingUsageLimitConfig{}
	t.Cleanup(func() { logger.inlinePurge.config = originalConfig })

	require.PanicsWithValue(t, directoryObservationStartPanic, func() {
		logger.Fsstore.Mu.Lock()
		fileNameSnapshot := logger.Fsstore.FileName
		logger.Fsstore.Mu.Unlock()
		logger.triggerCleanup(ctx, fileNameSnapshot)
	})
	logger.inlinePurge.config = originalConfig
	require.False(t, logger.inlinePurge.cleanupRunning.Load())
}

func TestInlineCleanupDropsConcurrentRequestsWithoutGenerationRecheck(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "inline-usage-single-observation")
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	// 日次age purgeでscanへ入らせます。
	logger.Clock = pkgclock.NewFixedClock(now.Add(24 * time.Hour))

	started := make(chan struct{})
	release := make(chan struct{})
	releaseScan := releaseOnCleanupForTest(t, release)
	var reads atomic.Int32
	production := filesystemPurgeFileScanner{}
	logger.inlinePurge.fileScanner = &fakePurgeFileScanner{observe: func(
		fileName logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) (fsstore.FileInspection, error) {
		if reads.Add(1) == 1 {
			close(started)
			<-release
		}
		return production.Scan(fileName, mode)
	}}

	logger.Fsstore.Mu.Lock()
	fileNameSnapshot := logger.Fsstore.FileName
	logger.Fsstore.Mu.Unlock()
	logger.triggerCleanup(ctx, fileNameSnapshot)
	receiveForTest(t, started, "first cleanup observation start")

	// 実行中のcleanup要求は保持せず破棄します。
	logger.triggerCleanup(ctx, fileNameSnapshot)
	logger.triggerCleanup(ctx, fileNameSnapshot)
	logger.triggerCleanup(ctx, fileNameSnapshot)
	releaseScan()
	waitBackgroundForTest(t, logger)

	require.EqualValues(t, 1, reads.Load(),
		"requests received during an observation must not start a follow-up observation")

	logger.triggerCleanup(ctx, fileNameSnapshot)
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, reads.Load(),
		"the next cleanup request must not recheck a discarded concurrent request")
	require.False(t, logger.inlinePurge.cleanupRunning.Load())
}

const directoryObservationStartPanic = "directory observation start panic"

func TestInlinePurgePlanSeparatesCleanupFromFileSeriesPurgeReasons(t *testing.T) {
	tests := []struct {
		name                        string
		dailyAgePurgeDue            bool
		directorySizeScanAcquired   bool
		seriesUploadRetryDue        bool
		wantCleanupRequired         bool
		wantFileSeriesPurgeRequired bool
	}{
		{name: "no reason"},
		{
			name:                        "daily age",
			dailyAgePurgeDue:            true,
			wantCleanupRequired:         true,
			wantFileSeriesPurgeRequired: true,
		},
		{
			name:                        "directory observation",
			directorySizeScanAcquired:   true,
			wantCleanupRequired:         true,
			wantFileSeriesPurgeRequired: true,
		},
		{
			name:                 "series upload retry",
			seriesUploadRetryDue: true,
			wantCleanupRequired:  true,
		},
		{
			name:                 "series upload retry defers daily purge",
			dailyAgePurgeDue:     true,
			seriesUploadRetryDue: true,
			wantCleanupRequired:  true,
		},
		{
			name:                        "both reasons",
			dailyAgePurgeDue:            true,
			directorySizeScanAcquired:   true,
			wantCleanupRequired:         true,
			wantFileSeriesPurgeRequired: true,
		},
		{
			name:                        "upload retry reuses acquired directory observation",
			directorySizeScanAcquired:   true,
			seriesUploadRetryDue:        true,
			wantCleanupRequired:         true,
			wantFileSeriesPurgeRequired: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := inlinePurgePlan{
				dailyAgePurgeDue:          test.dailyAgePurgeDue,
				directorySizeScanAcquired: test.directorySizeScanAcquired,
				seriesUploadRetryDue:      test.seriesUploadRetryDue,
			}
			require.Equal(t, test.wantCleanupRequired, plan.cleanupRequired())
			require.Equal(
				t,
				test.wantFileSeriesPurgeRequired,
				plan.fileSeriesPurgeRequired(),
			)
		})
	}
}

type panickingUsageLimitConfig struct{}

func (panickingUsageLimitConfig) MaxDirSize() uint64 {
	panic(directoryObservationStartPanic)
}

func (panickingUsageLimitConfig) MaxFileBackups() int {
	return 1
}

func (panickingUsageLimitConfig) MaxFileAge() int {
	return 1
}

func (panickingUsageLimitConfig) UsageCacheTTL() time.Duration {
	return time.Hour
}
