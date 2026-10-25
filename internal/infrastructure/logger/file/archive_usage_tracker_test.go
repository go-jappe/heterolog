package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgtime "github.com/go-jappe/heterolog/pkg/time"

	"github.com/stretchr/testify/require"
)

type failingUsageCompressionCodec struct {
	err error
}

type panickingUsageCompressionCodec struct{}

func (failingUsageCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressGzip
}

func (c failingUsageCompressionCodec) Compress(_, _ *os.File) error {
	return c.err
}

func (failingUsageCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	return nil
}

func (panickingUsageCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressGzip
}

func (panickingUsageCompressionCodec) Compress(_, _ *os.File) error {
	panic("compression codec panic")
}

func (panickingUsageCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	return nil
}

func TestFileLoggerCompressionInvalidatesDirectoryEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 29, 8, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "actual-compression-usage", "actual-compression-usage")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	sourcePath := filepath.Join(logger.Fsstore.FileName.Dir, "actual-compression-rotated.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("compressible archive payload"), 0o600))
	runtime := logger.inlinePurge
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(28)
	require.False(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, false))

	logger.tryArchiveRotatedFile(context.Background(), sourcePath, logger.Fsstore.FileName)
	require.NoFileExists(t, sourcePath)
	require.FileExists(t, sourcePath+pkgfsstore.ExtensionCompressGzip)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, false))
}

func TestFileLoggerCompressionFailureAlsoInvalidatesDirectoryEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 29, 8, 30, 0, 0, time.UTC)
	sentinel := errors.New("compression failed before publish")
	cfg := newTestFileLoggerConfig(t.TempDir(), "failed-compression-usage", "failed-compression-usage")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.CompressionCodec = failingUsageCompressionCodec{err: sentinel}
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })

	sourcePath := filepath.Join(logger.Fsstore.FileName.Dir, "failed-compression-rotated.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("source remains"), 0o600))
	runtime := logger.inlinePurge
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(14)

	logger.tryArchiveRotatedFile(context.Background(), sourcePath, logger.Fsstore.FileName)
	require.FileExists(t, sourcePath)
	require.NoFileExists(t, sourcePath+pkgfsstore.ExtensionCompressGzip)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, false))
}

func TestFileLoggerStandaloneArchivePanicSchedulesOnlyCompressedUploadRetry(t *testing.T) {
	now := time.Date(2026, time.August, 29, 8, 45, 0, 0, time.UTC)

	t.Run("compressed upload panic", func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "compressed-upload-panic", "compressed-upload-panic")
		cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
		cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
		cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
		logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		logger.Clock = pkgclock.NewFixedClock(now)
		logger.ArchiveUploader = panickingDestinationArchiveUploader{}
		t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })

		sourcePath := filepath.Join(
			logger.Fsstore.FileName.Dir,
			logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_20260828_1.log",
		)
		require.NoError(t, os.WriteFile(sourcePath, []byte("archive payload"), 0o600))
		require.Panics(t, func() {
			logger.tryArchiveRotatedFile(context.Background(), sourcePath, logger.Fsstore.FileName)
		})
		require.FileExists(t, sourcePath+pkgfsstore.ExtensionCompressGzip)
		scheduled, retryAt := fileSeriesUploadRetryScheduleSnapshot(logger.inlinePurge)
		require.True(t, scheduled)
		require.Equal(t, now.Add(logger.inlinePurge.config.UsageCacheTTL()), retryAt)
	})

	t.Run("uploader disabled compression panic", func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "disabled-upload-panic", "disabled-upload-panic")
		cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
		cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
		cfg.FsstoreConfig.CompressionCodec = panickingUsageCompressionCodec{}
		logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		logger.Clock = pkgclock.NewFixedClock(now)
		t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })

		sourcePath := filepath.Join(logger.Fsstore.FileName.Dir, "disabled-upload-source.log")
		require.NoError(t, os.WriteFile(sourcePath, []byte("archive payload"), 0o600))
		require.Panics(t, func() {
			logger.tryArchiveRotatedFile(context.Background(), sourcePath, logger.Fsstore.FileName)
		})
		scheduled, _ := fileSeriesUploadRetryScheduleSnapshot(logger.inlinePurge)
		require.False(t, scheduled)
	})

	t.Run("raw upload panic", func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "raw-upload-panic", "raw-upload-panic")
		cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
		cfg.FsstoreConfig.Lifecycle.EnableFileCompress = false
		logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		logger.Clock = pkgclock.NewFixedClock(now)
		logger.ArchiveUploader = panickingDestinationArchiveUploader{}
		t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })

		sourcePath := filepath.Join(logger.Fsstore.FileName.Dir, "raw-upload-source.log")
		require.NoError(t, os.WriteFile(sourcePath, []byte("archive payload"), 0o600))
		require.Panics(t, func() {
			logger.tryArchiveRotatedFile(context.Background(), sourcePath, logger.Fsstore.FileName)
		})
		scheduled, _ := fileSeriesUploadRetryScheduleSnapshot(logger.inlinePurge)
		require.False(t, scheduled)
	})
}

func TestFileLoggerRotationArchivePanicSchedulesRetryBeforeUnprotect(t *testing.T) {
	now := time.Date(2026, time.August, 29, 8, 50, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "rotation-upload-panic", "rotation-upload-panic")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	logger.ArchiveUploader = panickingDestinationArchiveUploader{}
	logger.inlinePurge.lastAgePurgeDate = pkgtime.StartOfDay(now)
	t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })

	oldFilePath := filepath.Join(
		logger.Fsstore.FileName.Dir,
		logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_20260828_1.log",
	)
	require.NoError(t, os.WriteFile(oldFilePath, []byte("archive payload"), 0o600))
	protectedPaths := logger.pathsToProtectForRotatedFile(oldFilePath)
	archiveToken := appendlog.NewDirectoryGuardToken()
	logger.lifecycleGuard.ProtectPaths(archiveToken, protectedPaths...)

	require.Panics(t, func() {
		logger.processRotatedLogFile(
			context.Background(),
			oldFilePath,
			logger.Fsstore.FileName,
			protectedPaths,
			archiveToken,
			runNotificationImmediately,
		)
	})
	waitBackgroundForTest(t, logger)

	compressedPath := oldFilePath + pkgfsstore.ExtensionCompressGzip
	require.FileExists(t, compressedPath)
	scheduled, retryAt := fileSeriesUploadRetryScheduleSnapshot(logger.inlinePurge)
	require.True(t, scheduled)
	require.Equal(t, now.Add(logger.inlinePurge.config.UsageCacheTTL()), retryAt)
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		for _, path := range protectedPaths {
			require.Contains(t, protected, filepath.Clean(path), "復旧処理が受理済みpathの保護を引き継ぐ")
		}
	})
}

func TestFileLoggerRotationObservesDirectoryWhenCompressionIsDisabled(t *testing.T) {
	rotationTime := time.Date(2026, time.August, 30, 9, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "rotation-directory-observation", "rotation-directory-observation")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = false
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(rotationTime)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	runtime := logger.inlinePurge
	runtime.lastAgePurgeDate = time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(rotationTime, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(0)
	counts := observeCleanupScans(logger)
	require.NoError(t, os.Truncate(
		logger.Fsstore.ActiveFilePath,
		int64(logger.config.Fsstore().Lifecycle().MaxFileSize()),
	))

	err = rotateFileLoggerForTest(context.Background(), logger)
	require.NoError(t, err)
	waitBackgroundForTest(t, logger)

	require.EqualValues(t, 1, counts.all.Load(),
		"rotation must refresh directory usage even when compression is disabled")
}

func TestFileLoggerRotationDefersEstimateInvalidationUntilArchiveTaskCompletes(t *testing.T) {
	rotationTime := time.Date(2026, time.August, 30, 9, 30, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "rotation-coalesced-observation", "rotation-coalesced-observation")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(rotationTime)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	runtime := logger.inlinePurge
	runtime.lastAgePurgeDate = time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(rotationTime, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(0)
	// このtestの手動baselineは起動時full scan完了後の状態を表します。
	runtime.rawArchiveRecoveryBootstrapPending.Store(false)
	counts := observeCleanupScans(logger)

	uploadStarted := make(chan struct{}, 1)
	uploadRelease := make(chan struct{})
	releaseUpload := releaseOnCleanupForTest(t, uploadRelease)
	archiveUploader := &blockingSuccessfulArchiveUploader{
		started: uploadStarted,
		release: uploadRelease,
	}
	logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploader)
	require.NoError(t, os.Truncate(
		logger.Fsstore.ActiveFilePath,
		int64(logger.config.Fsstore().Lifecycle().MaxFileSize()),
	))

	err = rotateFileLoggerForTest(context.Background(), logger)
	require.NoError(t, err)
	receiveForTest(t, uploadStarted, "rotation upload start")

	// compression完了後でもrotation taskがupload中なら、cacheはまだ有効です。
	// 通常writeと同じ判定を行っても、中間状態での全体観測は開始しません。
	plan := logger.planInlineCleanup()
	require.False(t, plan.directorySizeScanAcquired)
	require.False(t, plan.fileSeriesPurgeRequired())

	releaseUpload()
	waitBackgroundForTest(t, logger)

	require.EqualValues(t, 1, archiveUploader.calls.Load())
	require.EqualValues(t, 1, counts.all.Load(),
		"compression and uploaded-file removal must be reconciled by one post-task observation")
	require.EqualValues(t, 0, counts.byTarget.Load())
}

func TestFileLoggerRotationUploadFailureIsNotRetriedByImmediateObservation(t *testing.T) {
	rotationTime := time.Date(2026, time.August, 30, 10, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "rotation-upload-retry-deferred", "rotation-upload-retry-deferred")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(rotationTime)
	t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })

	runtime := logger.inlinePurge
	runtime.lastAgePurgeDate = time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(rotationTime, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(0)
	// このtestの手動baselineは起動時full scan完了後の状態を表します。
	runtime.rawArchiveRecoveryBootstrapPending.Store(false)
	counts := observeCleanupScans(logger)

	archiveUploader := &countingFailingArchiveUploader{err: errors.New("upload unavailable")}
	logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploader)
	oldFilePath := logger.Fsstore.ActiveFilePath
	require.NoError(t, os.Truncate(
		oldFilePath,
		int64(logger.config.Fsstore().Lifecycle().MaxFileSize()),
	))

	err = rotateFileLoggerForTest(context.Background(), logger)
	require.NoError(t, err)
	waitBackgroundForTest(t, logger)

	compressedPath := oldFilePath + pkgfsstore.ExtensionCompressGzip
	require.FileExists(t, compressedPath)
	require.EqualValues(t, 1, archiveUploader.calls.Load(),
		"the forced post-rotation observation must not immediately repeat the failed upload")
	require.EqualValues(t, 1, counts.all.Load())

	// schedule期限前のwriteでは、target-onlyもdirectory全体も走査しません。
	logger.Clock = pkgclock.NewFixedClock(
		rotationTime.Add(runtime.config.UsageCacheTTL() - time.Nanosecond),
	)
	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, archiveUploader.calls.Load())
	require.EqualValues(t, 1, counts.all.Load())
	require.EqualValues(t, 0, counts.byTarget.Load())

	// TTL後の走査も、登録済みの復旧処理が所有するarchiveを重複実行しません。
	logger.Clock = pkgclock.NewFixedClock(rotationTime.Add(runtime.config.UsageCacheTTL()))
	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, archiveUploader.calls.Load())
	require.EqualValues(t, 1, counts.all.Load())
	require.EqualValues(t, 1, counts.byTarget.Load())

	// 次の期限でも復旧処理の所有権を保ちます。
	logger.Clock = pkgclock.NewFixedClock(rotationTime.Add(2 * runtime.config.UsageCacheTTL()))
	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, archiveUploader.calls.Load())
	require.EqualValues(t, 2, counts.all.Load())
	require.EqualValues(t, 1, counts.byTarget.Load())
}

func TestFileLoggerPostRotationObservationDefersEveryPendingUpload(t *testing.T) {
	now := time.Date(2026, time.August, 30, 10, 30, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "post-rotation-all-uploads-deferred")
	runtime := logger.inlinePurge
	runtime.lastAgePurgeDate = time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	counts := observeCleanupScans(logger)

	archiveUploader := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploader)
	pendingPaths := []string{
		writePendingCompressedLog(t, logger, "20260828", 1, now.Add(-2*time.Hour)),
		writePendingCompressedLog(t, logger, "20260829", 2, now.Add(-time.Hour)),
	}

	// 複数rotation taskが近接完了した場合と同じく、post-rotation強制観測が
	// 複数のlocal compressed archiveを一度に再発見しても、どれもuploadしません。
	runtime.seriesUploadRetry.scheduleRetry(now, runtime.config.UsageCacheTTL())
	runtime.invalidateDirectorySizeEstimate()
	logger.triggerPostRotationCleanup(context.Background(), logger.Fsstore.FileName, "")
	waitBackgroundForTest(t, logger)

	require.Empty(t, archiveUploader.inputsSnapshot())
	require.EqualValues(t, 1, counts.all.Load())
	for _, path := range pendingPaths {
		require.FileExists(t, path)
	}

	// TTL後は、保留したpending archiveをこのseriesの
	// target-only観測ですべて再試行します。
	logger.Clock = pkgclock.NewFixedClock(now.Add(runtime.config.UsageCacheTTL()))
	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)

	inputs := archiveUploader.inputsSnapshot()
	require.Len(t, inputs, len(pendingPaths))
	require.ElementsMatch(t, pendingPaths, []string{inputs[0].FilePath(), inputs[1].FilePath()})
	require.EqualValues(t, 1, counts.all.Load())
	require.EqualValues(t, 1, counts.byTarget.Load())
	for _, path := range pendingPaths {
		require.NoFileExists(t, path)
	}
}

func TestFileLoggerUnscheduledDirectoryObservationDoesNotStartUpload(t *testing.T) {
	now := time.Date(2026, time.August, 30, 10, 40, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "unscheduled-directory-observation")
	runtime := logger.inlinePurge
	runtime.lastAgePurgeDate = pkgtime.StartOfDay(now)
	uploader := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, uploader)
	pendingPath := writePendingCompressedLog(t, logger, "20260820", 1, now.Add(-time.Hour))
	counts := observeCleanupScans(logger)

	// directory cacheの観測とseries uploadの開始条件を分離します。
	// scheduleのないfull observationはcompressed pathをpurgeからprotectするだけです。
	runtime.invalidateDirectorySizeEstimate()
	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)

	require.Empty(t, uploader.inputsSnapshot())
	require.FileExists(t, pendingPath)
	require.EqualValues(t, 1, counts.all.Load())
	require.EqualValues(t, 0, counts.byTarget.Load())
}

func TestFileLoggerSeriesUploadRetryDefersAgeAndGenerationPurge(t *testing.T) {
	now := time.Date(2026, time.August, 30, 10, 42, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "series-retry-without-generation-purge")
	runtime := logger.inlinePurge
	uploader := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, uploader)
	runtime.seriesUploadRetry.scheduleImmediateRetry()

	// MaxFileBackupsを超えるraw generationを用意します。retry-only観測が
	// generation purgeまで兼ねると、このうち最古のfileが削除されます。
	var rawPaths []string
	for index := 1; index <= runtime.config.MaxFileBackups()+1; index++ {
		path := filepath.Join(
			logger.Fsstore.FileName.Dir,
			logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+
				"_20260819_"+fmt.Sprint(index)+".log",
		)
		require.NoError(t, os.WriteFile(path, []byte("raw generation"), 0o600))
		modifiedAt := now.
			AddDate(0, 0, -runtime.config.MaxFileAge()-1).
			Add(-time.Duration(index) * time.Hour)
		require.NoError(t, os.Chtimes(path, modifiedAt, modifiedAt))
		rawPaths = append(rawPaths, path)
	}
	pendingPath := writePendingCompressedLog(t, logger, "20260820", 1, now.Add(-time.Hour))
	counts := observeCleanupScans(logger)

	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)

	require.Len(t, uploader.inputsSnapshot(), 1)
	require.NoFileExists(t, pendingPath)
	require.EqualValues(t, 1, counts.byTarget.Load())
	require.EqualValues(t, 0, counts.all.Load())
	for _, path := range rawPaths {
		require.FileExists(t, path,
			"an upload-retry-only scan must not apply age or generation retention")
	}
	require.True(t, runtime.lastAgePurgeDate.IsZero())

	// upload後削除が無効化したdirectory cacheを、次の自然なwriteで全体観測します。
	// ここで初めてage・generation purgeを実行します。
	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, counts.byTarget.Load())
	require.EqualValues(t, 1, counts.all.Load())
	for _, path := range rawPaths {
		require.NoFileExists(t, path)
	}
	require.Equal(t, pkgtime.StartOfDay(now), runtime.lastAgePurgeDate)
}

func TestFileLoggerSeriesUploadRetryRejectsDanglingLinkBeforeUpload(t *testing.T) {
	now := time.Date(2026, time.August, 30, 10, 44, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "series-retry-without-stat")
	runtime := logger.inlinePurge
	runtime.lastAgePurgeDate = pkgtime.StartOfDay(now)
	uploader := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, uploader)
	runtime.seriesUploadRetry.scheduleImmediateRetry()

	pendingPath := filepath.Join(
		logger.Fsstore.FileName.Dir,
		logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+
			"_20260820_1.log.gz",
	)
	testutil.Symlink(
		t,
		filepath.Join(logger.Fsstore.FileName.Dir, "missing-archive-target"),
		pendingPath,
	)
	_, statErr := os.Stat(pendingPath)
	require.Error(t, statErr, "the dangling archive must reproduce an individual Stat failure")
	_, lstatErr := os.Lstat(pendingPath)
	require.NoError(t, lstatErr)

	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)

	inputs := uploader.inputsSnapshot()
	require.Empty(t, inputs)
	_, lstatErr = os.Lstat(pendingPath)
	require.NoError(t, lstatErr, "candidate inspection must preserve the dangling link")
	scheduled, _ := fileSeriesUploadRetryScheduleSnapshot(runtime)
	require.True(t, scheduled, "a rejected candidate must not complete the pending retry")
}

func TestFileLoggerSeriesBootstrapRetryIsIndependentOfSharedDirectoryBaseline(t *testing.T) {
	now := time.Date(2026, time.August, 30, 10, 45, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(
		t.TempDir(),
		"shared-bootstrap-retry",
		"shared-bootstrap-retry",
	)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	configured := NewConfigAdapter(cfg)
	defaultLogger, err := newStandaloneFileLogger(context.Background(), configured, nil, nil)
	require.NoError(t, err)
	debugLogger, err := newStandaloneDebugFileLogger(context.Background(), configured, nil, nil)
	require.NoError(t, err)
	defaultLogger.Clock = pkgclock.NewFixedClock(now)
	debugLogger.Clock = pkgclock.NewFixedClock(now)
	t.Cleanup(func() {
		closeWithinBoundForTest(t, boundedwait.HangTimeout, "debugLogger Close", debugLogger, true)
		closeWithinBoundForTest(t, boundedwait.HangTimeout, "defaultLogger Close", defaultLogger, true)
	})

	// production constructorと同じくArchiveUploaderが存在する状態でruntimeを
	// 初期化し、debug physical seriesのbootstrap scheduleを作ります。
	uploader := &recordingArchiveUploader{}
	debugLogger.ArchiveUploader = newSingleArchiveUploader(t, uploader)
	debugLogger.initInlinePurgeRuntime()
	debugRuntime := debugLogger.inlinePurge
	debugRuntime.lastAgePurgeDate = pkgtime.StartOfDay(now)

	require.Same(
		t,
		defaultLogger.inlinePurge.directorySizeTracker,
		debugRuntime.directorySizeTracker,
	)
	pendingPath := writePendingCompressedLog(t, debugLogger, "20260820", 1, now.Add(-time.Hour))

	// 別seriesの実際のGlobAllが先にshared directory baselineを確定しても、
	// debug series固有のbootstrap scheduleは消費されません。
	defaultRuntime := defaultLogger.inlinePurge
	defaultRuntime.lastAgePurgeDate = pkgtime.StartOfDay(now)
	defaultCounts := observeCleanupScans(defaultLogger)
	defaultRuntime.invalidateDirectorySizeEstimate()
	defaultLogger.triggerCleanup(context.Background(), defaultLogger.Fsstore.FileName)
	waitBackgroundForTest(t, defaultLogger)
	require.EqualValues(t, 1, defaultCounts.all.Load())
	require.EqualValues(t, 0, defaultCounts.byTarget.Load())
	require.FileExists(t, pendingPath)

	debugCounts := observeCleanupScans(debugLogger)
	debugLogger.triggerCleanup(context.Background(), debugLogger.Fsstore.FileName)
	waitBackgroundForTest(t, debugLogger)

	inputs := uploader.inputsSnapshot()
	require.Len(t, inputs, 1)
	require.Equal(t, pendingPath, inputs[0].FilePath())
	require.NoFileExists(t, pendingPath)
	require.EqualValues(t, 1, debugCounts.byTarget.Load())
	require.EqualValues(t, 0, debugCounts.all.Load(),
		"bootstrap discovery must not depend on or repeat the shared directory scan")
}

func TestFileLoggerSuccessfulOldRetryDoesNotClearConcurrentRotationSchedule(t *testing.T) {
	now := time.Date(2026, time.August, 30, 11, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "old-retry-preserves-new-schedule")
	runtime := logger.inlinePurge
	runtime.lastAgePurgeDate = pkgtime.StartOfDay(now)
	interval := runtime.config.UsageCacheTTL()
	runtime.seriesUploadRetry.scheduleRetry(now.Add(-interval), interval)
	pendingPath := writePendingCompressedLog(t, logger, "20260820", 1, now.Add(-time.Hour))

	uploadStarted := make(chan struct{}, 1)
	uploadRelease := make(chan struct{})
	releaseUpload := releaseOnCleanupForTest(t, uploadRelease)
	uploader := &blockingSuccessfulArchiveUploader{
		started: uploadStarted,
		release: uploadRelease,
	}
	logger.ArchiveUploader = newSingleArchiveUploader(t, uploader)
	counts := observeCleanupScans(logger)
	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	receiveForTest(t, uploadStarted, "scheduled series retry start")

	// old retryはsnapshot成功後にscheduleをclaim済みで、remote I/O中です。
	// この間に別のrotationが失敗し、新しい再試行を予約します。
	newFailureAt := now.Add(time.Minute)
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		runtime.seriesUploadRetry.scheduleRetry(newFailureAt, interval)
		return nil
	}))
	releaseUpload()
	waitBackgroundForTest(t, logger)

	require.NoFileExists(t, pendingPath)
	require.EqualValues(t, 1, uploader.calls.Load())
	require.EqualValues(t, 1, counts.byTarget.Load())
	require.EqualValues(t, 0, counts.all.Load())
	scheduled, retryAt := fileSeriesUploadRetryScheduleSnapshot(runtime)
	require.True(t, scheduled,
		"old retry success must not clear a retry scheduled during remote I/O")
	require.Equal(t, newFailureAt.Add(interval), retryAt)
}

func TestFileLoggerSnapshotBeforeRotationUnprotectPreservesSeriesRetrySchedule(t *testing.T) {
	// lifecycle lock待ちに入ったことをsynctest.Waitで確かめ、短い実時間の窓に頼りません。
	synctestFromWallClock(t, func(t *testing.T) {
		now := time.Date(2026, time.August, 30, 11, 30, 0, 0, time.UTC)
		cfg := newTestFileLoggerConfig(
			t.TempDir(),
			"snapshot-before-rotation-unprotect",
			"snapshot-before-rotation-unprotect",
		)
		cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
		cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
		cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
		logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		logger.Clock = pkgclock.NewFixedClock(now)
		t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })

		runtime := logger.inlinePurge
		runtime.lastAgePurgeDate = time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
		productionScanner := filesystemPurgeFileScanner{}
		snapshotTaken := make(chan struct{}, 1)
		releaseSnapshot := make(chan struct{})
		releaseCleanupSnapshot := releaseOnCleanupForTest(t, releaseSnapshot)
		var scanCount atomic.Int32
		runtime.fileScanner = &fakePurgeFileScanner{observe: func(
			fileName logpattern.FileNamePattern,
			mode logpattern.GlobMode,
		) (fsstore.FileInspection, error) {
			files, scanErr := productionScanner.Scan(fileName, mode)
			if scanCount.Add(1) == 1 {
				snapshotTaken <- struct{}{}
				<-releaseSnapshot
			}
			return files, scanErr
		}}

		uploadStarted := make(chan struct{}, 1)
		uploadReturned := make(chan struct{}, 1)
		uploadRelease := make(chan struct{})
		releaseUpload := releaseOnCleanupForTest(t, uploadRelease)
		archiveUploader := &blockingFirstFailingArchiveUploader{
			started:  uploadStarted,
			returned: uploadReturned,
			release:  uploadRelease,
			err:      errors.New("upload unavailable"),
		}
		logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploader)
		oldFilePath := logger.Fsstore.ActiveFilePath
		require.NoError(t, os.Truncate(
			oldFilePath,
			int64(logger.config.Fsstore().Lifecycle().MaxFileSize()),
		))

		err = rotateFileLoggerForTest(context.Background(), logger)
		require.NoError(t, err)
		receiveForTest(t, uploadStarted, "rotation upload start")

		// rotation pathがまだ保護中のうちに、通常cleanupがdirectory snapshotを
		// 取得してlifecycle lockを保持します。post-rotation triggerはこの
		// cleanup ownerによって破棄させ、series scheduleの引き継ぎを検証します。
		require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, true))
		plan := inlinePurgePlan{
			cleanupTriggeredAt:        now,
			agePurgeReferenceDate:     now,
			directorySizeScanAcquired: true,
		}
		runtime.cleanupRunning.Store(true)
		cleanupGateOwned := true
		defer func() {
			if cleanupGateOwned {
				runtime.cleanupRunning.Store(false)
			}
		}()
		cleanupDone := make(chan error, 1)
		go func() {
			cleanupDone <- logger.cleanupOldLogs(context.Background(), logger.Fsstore.FileName, plan)
		}()
		receiveForTest(t, snapshotTaken, "pre-unprotect cleanup snapshot")

		releaseUpload()
		receiveForTest(t, uploadReturned, "rotation upload return")
		// uploadが戻っても先行cleanupがlifecycle lockを持つ間は、rotation側は
		// 再試行を予約できません。予約がlock外にあると、保護中の古いsnapshotと
		// 新しい失敗scheduleの順序が逆転します。bubble内の処理がすべて待機に入るまで
		// 進めてから、rotation側がlockの外で予約していないことを確認します。
		synctest.Wait()
		scheduledDuringCleanup, _ := fileSeriesUploadRetryScheduleSnapshot(runtime)
		require.False(t, scheduledDuringCleanup,
			"rotation must not schedule the series retry while the earlier cleanup holds the lifecycle lock")

		releaseCleanupSnapshot()
		require.NoError(t, receiveForTest(t, cleanupDone, "pre-unprotect cleanup completion"))
		waitBackgroundForTest(t, logger)

		compressedPath := oldFilePath + pkgfsstore.ExtensionCompressGzip
		require.FileExists(t, compressedPath)
		require.EqualValues(t, 1, archiveUploader.calls.Load())
		scheduled, retryAt := fileSeriesUploadRetryScheduleSnapshot(runtime)
		require.True(t, scheduled)
		require.Equal(t, now.Add(runtime.config.UsageCacheTTL()), retryAt,
			"the pre-unprotect snapshot must not consume or alter the new rotation schedule")

		// rotationのInvalidateを受けた直後の全体観測は、pending archiveを
		// protectするだけで期限前scheduleをclaimしません。
		runtime.cleanupRunning.Store(false)
		cleanupGateOwned = false
		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
		waitBackgroundForTest(t, logger)
		require.EqualValues(t, 1, archiveUploader.calls.Load())
		require.FileExists(t, compressedPath)
		scheduled, retryAt = fileSeriesUploadRetryScheduleSnapshot(runtime)
		require.True(t, scheduled)
		require.Equal(t, now.Add(runtime.config.UsageCacheTTL()), retryAt)

		// TTL後の自然なwriteも、登録済みの復旧処理が所有するarchiveを重複実行しません。
		logger.Clock = pkgclock.NewFixedClock(now.Add(runtime.config.UsageCacheTTL()))
		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
		waitBackgroundForTest(t, logger)
		require.EqualValues(t, 1, archiveUploader.calls.Load())
		require.FileExists(t, compressedPath)
		scheduled, retryAt = fileSeriesUploadRetryScheduleSnapshot(runtime)
		require.False(t, scheduled)
	})
}

func TestFileLoggerSeriesRetryScanErrorBacksOffSchedule(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	logger := newInlineUsageTestLogger(t, now, "purge-error-keeps-retry-deferral")
	runtime := logger.inlinePurge
	interval := runtime.config.UsageCacheTTL()
	failureAt := now.Add(2 * interval)
	scanErr := errors.New("directory scan failed")
	runtime.fileScanner = &fakePurgeFileScanner{scan: func(
		logpattern.FileNamePattern,
		logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		// lifecycle lock待ちやfilesystem走査がTTLより長くかかった状況を
		// fixed clockで再現します。backoffはplan開始時ではなく失敗時から数えます。
		logger.Clock = pkgclock.NewFixedClock(failureAt)
		return nil, scanErr
	}}
	runtime.seriesUploadRetry.scheduleRetry(
		now.Add(-interval),
		interval,
	)

	uploadCompletionCalled := false
	err := logger.cleanupOldLogsWithPendingUploadCompletion(
		context.Background(),
		logger.Fsstore.FileName,
		inlinePurgePlan{
			cleanupTriggeredAt:    now,
			agePurgeReferenceDate: now,
			seriesUploadRetryDue:  true,
		},
		func(context.Context, []archiveUploadGroup) bool {
			uploadCompletionCalled = true
			return false
		},
	)

	require.ErrorIs(t, err, scanErr)
	require.False(t, uploadCompletionCalled)
	scheduled, retryAt := fileSeriesUploadRetryScheduleSnapshot(runtime)
	require.True(t, scheduled)
	require.Equal(t, failureAt.Add(interval), retryAt)
	require.False(t, runtime.seriesUploadRetry.isRetryDue(
		failureAt,
		interval,
	), "a failed target-only scan must not run again on every write")
}

func fileSeriesUploadRetryScheduleSnapshot(
	runtime *inlinePurgeRuntime,
) (bool, time.Time) {
	return fileSeriesUploadRetryScheduleValue(&runtime.seriesUploadRetry)
}

type blockingSuccessfulArchiveUploader struct {
	started chan<- struct{}
	release <-chan struct{}
	calls   atomic.Int32
}

func (u *blockingSuccessfulArchiveUploader) Upload(
	_ context.Context,
	_ uploader.UploadRequest,
) error {
	u.calls.Add(1)
	u.started <- struct{}{}
	<-u.release
	return nil
}

type countingFailingArchiveUploader struct {
	err   error
	calls atomic.Int32
}

func (u *countingFailingArchiveUploader) Upload(
	_ context.Context,
	_ uploader.UploadRequest,
) error {
	u.calls.Add(1)
	return u.err
}

type blockingFirstFailingArchiveUploader struct {
	started  chan<- struct{}
	returned chan<- struct{}
	release  <-chan struct{}
	err      error
	calls    atomic.Int32
}

func (u *blockingFirstFailingArchiveUploader) Upload(
	_ context.Context,
	_ uploader.UploadRequest,
) error {
	if u.calls.Add(1) == 1 {
		u.started <- struct{}{}
		<-u.release
		u.returned <- struct{}{}
	}
	return u.err
}

func TestTargetOnlyPurgeInvalidatesDirectoryEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 29, 0, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "target-only-membership")
	runtime := logger.inlinePurge
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(100)

	expiredAt := now.AddDate(0, 0, -runtime.config.MaxFileAge()-1)
	oldPath := filepath.Join(
		logger.Fsstore.FileName.Dir,
		logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_"+expiredAt.Format("20060102")+"_1."+
			logger.Fsstore.FileName.Extension,
	)
	require.NoError(t, os.WriteFile(oldPath, []byte("old"), 0o600))
	runtime.fileScanner = &fakePurgeFileScanner{scan: func(
		_ logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]fsstore.FileInfo, error) {
		require.Equal(t, logpattern.GlobByTarget, mode)
		return []fsstore.FileInfo{{
			Path:    oldPath,
			Size:    3,
			ModTime: expiredAt,
		}}, nil
	}}

	plan := inlinePurgePlan{agePurgeReferenceDate: now, dailyAgePurgeDue: true}
	require.NoError(t, logger.cleanupOldLogs(context.Background(), logger.Fsstore.FileName, plan))
	require.NoFileExists(t, oldPath)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, false))
}
