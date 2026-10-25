package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

type closeContractMultiLoggerConfig struct{}

func (closeContractMultiLoggerConfig) Timeout() time.Duration {
	return time.Second
}

func (closeContractMultiLoggerConfig) MaxLogEntryBytes() int {
	return pkglogger.DefaultMaxLogEntryBytes
}

func TestFileLoggerRejectsDirectWritesOnceCloseStarts(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "file-close-contract")
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	activePath := logger.Fsstore.ActiveFilePath

	// file lockを保持してCloseを終了待ちにし、Close開始後かつ完了前の契約を検証します。
	logger.Fsstore.Mu.Lock()
	fileLocked := true
	defer func() {
		if fileLocked {
			logger.Fsstore.Mu.Unlock()
		}
	}()
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- logger.Close()
	}()

	require.Eventually(t, func() bool {
		logger.backgroundMu.Lock()
		defer logger.backgroundMu.Unlock()
		return logger.closeState == common.LoggerCloseStateStarted
	}, hangTimeoutForTest, time.Millisecond)
	require.ErrorIs(t, logger.testInfo(context.Background(), "during close"), applogging.ErrLoggerClosed)

	logger.Fsstore.Mu.Unlock()
	fileLocked = false
	require.NoError(t, receiveForTest(t, closeDone, "Close after the file lock is released"))
	require.Nil(t, logger.Fsstore.File)
	logger.backgroundMu.Lock()
	require.Equal(t, common.LoggerCloseStateCompleted, logger.closeState)
	logger.backgroundMu.Unlock()

	checks := []struct {
		name string
		log  func() error
	}{
		{name: "debug", log: func() error { return logger.testDebug(context.Background(), "after close") }},
		{name: "info", log: func() error { return logger.testInfo(context.Background(), "after close") }},
		{name: "warn", log: func() error { return logger.testWarn(context.Background(), "after close") }},
		{name: "error", log: func() error { return logger.testError(context.Background(), "after close") }},
		{name: "error with stack", log: func() error {
			return logger.testErrorWithStack(context.Background(), "after close", errors.New("cause"))
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			require.ErrorIs(t, check.log(), applogging.ErrLoggerClosed)
			require.Nil(t, logger.Fsstore.File)
			require.Equal(t, activePath, logger.Fsstore.ActiveFilePath)
		})
	}
}

func TestFileLoggerCloseWaitsForRotationArchiveFromWriteAcceptedBeforeClose(t *testing.T) {
	rotationTime := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "close-accepted-rotation", "close-accepted-rotation")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	cfg.FsstoreConfig.Duration.CompressWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.CompressErrorThresholdDuration = 2 * time.Hour
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	clockEntered := make(chan struct{})
	clockRelease := make(chan struct{})
	uploadStarted := make(chan struct{}, 1)
	uploadRelease := make(chan struct{})
	var releaseClockOnce sync.Once
	var releaseUploadOnce sync.Once
	releaseClock := func() { releaseClockOnce.Do(func() { close(clockRelease) }) }
	releaseUpload := func() { releaseUploadOnce.Do(func() { close(uploadRelease) }) }
	// 止めたclockとuploadを解除してから閉じます。試験が失敗していれば、残った処理を待たずに
	// 中止し、上限付きで閉じます。
	t.Cleanup(func() {
		releaseClock()
		releaseUpload()
		closeWithinBoundForTest(t, boundedwait.HangTimeout, "logger Close", logger, false)
	})

	logger.Clock = &closeContractBlockingFirstClock{
		at:      rotationTime,
		entered: clockEntered,
		release: clockRelease,
	}
	archiveUploader := &blockingSuccessfulArchiveUploader{
		started: uploadStarted,
		release: uploadRelease,
	}
	logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploader)
	require.NoError(t, os.Truncate(
		logger.Fsstore.ActiveFilePath,
		int64(logger.config.Fsstore().Lifecycle().MaxFileSize()),
	))

	// OccurredAtを先に確定し、blocking clockの最初のNowをrotation判定内に限定します。
	entry := newTestApplicationLogEntry(
		context.Background(),
		pkglogger.LogLevelInfo,
		"write accepted before close must finish its rotation archive",
		nil,
		false,
	)
	entry.OccurredAt = rotationTime.UTC()
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- logger.HandleLogEntry(context.Background(), entry)
	}()

	receiveForTest(t, clockEntered, "accepted write did not reach the blocked rotation decision")
	// lockForWriteの受付とregistration予約が完了し、rotation判定内で停止しています。

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- logger.Close()
	}()
	require.Eventually(t, func() bool {
		logger.backgroundMu.Lock()
		defer logger.backgroundMu.Unlock()
		return logger.closeState == common.LoggerCloseStateStarted
	}, hangTimeoutForTest, time.Millisecond)

	// CloseがStartedになった後でも、すでに受理したwriteはarchive childを登録します。
	releaseClock()
	receiveForTest(t, uploadStarted, "rotation archive was skipped after Close started")

	writeErr := receiveForTest(t, writeDone, "accepted write did not complete after registering its rotation archive")
	require.NoError(t, writeErr)
	select {
	case closeErr := <-closeDone:
		t.Fatalf("Close returned before the accepted rotation archive completed: %v", closeErr)
	default:
	}

	releaseUpload()
	closeErr := receiveForTest(t, closeDone, "Close did not return after the accepted rotation archive completed")
	require.NoError(t, closeErr)
	require.EqualValues(t, 1, archiveUploader.calls.Load())
}

func TestFileLoggerInternalWriteHandlerCanRequestClose(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "handler-close", "handler-close")
	cfg.FsstoreConfig.Duration.WriteFileLatencyEnabled = true
	cfg.RuntimeConfig.IsDebug = true
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, false)

	closeResult := make(chan error, 1)
	var closeOnce sync.Once
	closeFromHandler := func() {
		closeOnce.Do(func() {
			logger.RequestClose()
			go func() { closeResult <- logger.Close() }()
		})
	}
	logger.OnError = func(
		context.Context,
		error,
		*logging.LogEntry,
		observer.Tracer,
		observer.Meter,
	) {
		closeFromHandler()
	}
	logger.OnWarn = func(
		context.Context,
		*logging.LogEntry,
		observer.Tracer,
		observer.Meter,
	) {
		closeFromHandler()
	}
	logger.OnDebug = func(
		context.Context,
		*logging.LogEntry,
		observer.Tracer,
		observer.Meter,
	) {
		closeFromHandler()
	}

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- logger.testInfo(context.Background(), "internal handler closes logger")
	}()
	writeErr := receiveForTest(t, writeDone, "internal write handler deadlocked while closing its logger")
	require.NoError(t, writeErr)
	closeErr := receiveForTest(t, closeResult, "write latency handler was not invoked")
	require.NoError(t, closeErr)
}

func TestFileLoggerCleanupErrorHandlerCanRequestClose(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 30, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "cleanup-handler-close", "cleanup-handler-close")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, false)

	cleanupErr := errors.New("cleanup failed before notification")
	handled := make(chan error, 1)
	closeResult := make(chan error, 1)
	logger.OnError = func(
		_ context.Context,
		err error,
		_ *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		handled <- err
		logger.RequestClose()
		go func() { closeResult <- logger.Close() }()
	}

	logger.triggerCleanupUsing(
		context.Background(),
		logger.Fsstore.FileName,
		func(
			context.Context,
			logpattern.FileNamePattern,
			inlinePurgePlan,
			taskNotificationDelivery,
		) error {
			return cleanupErr
		},
		true,
		"",
	)

	handledErr := receiveForTest(t, handled, "cleanup error was not dispatched")
	require.ErrorIs(t, handledErr, cleanupErr)
	closeErr := receiveForTest(t, closeResult, "cleanup OnError deadlocked while closing its own logger")
	require.NoError(t, closeErr)
}

func TestFileLoggerCleanupInternalErrorHandlersCanRequestClose(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 45, 0, 0, time.UTC)

	t.Run("scanner error", func(t *testing.T) {
		logger := newInlineUsageTestLogger(t, now, "cleanup-scanner-handler-close")
		scanErr := errors.New("cleanup scanner unavailable")
		logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
			logpattern.FileNamePattern,
			logpattern.GlobMode,
		) ([]infrafsstore.FileInfo, error) {
			return nil, scanErr
		}}
		handled, closeResult := closeFileLoggerOnFirstCleanupError(logger)

		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)

		requireCleanupErrorHandlerClosedLogger(t, handled, closeResult, requireHandledErrorIs(scanErr))
	})

	t.Run("raw recovery integrity verification error", func(t *testing.T) {
		codec := &inlineRecoveryCompressionCodec{}
		logger := newInlineCompressionRecoveryTestLogger(
			t,
			now,
			"cleanup-recovery-handler-close",
			codec,
		)
		rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
		require.NoError(t, os.WriteFile(
			rawPath+pkgfsstore.ExtensionCompressGzip,
			[]byte("invalid gzip completion proof"),
			0o600,
		))
		handled, closeResult := closeFileLoggerOnFirstCleanupError(logger)

		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)

		err := receiveForTest(t, handled, "cleanup integrity error was not dispatched")
		require.ErrorIs(t, err, infrafsstore.ErrInvalidCompressedFile)
		requireCleanupCloseResult(t, closeResult, "cleanup OnError")
	})

	t.Run("purge removal error", func(t *testing.T) {
		logger := newInlineUsageTestLogger(t, now, "cleanup-purge-handler-close")
		candidatePath := filepath.Join(
			logger.Fsstore.FileName.Dir,
			logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_20260820_1.log",
		)
		require.NoError(t, os.Mkdir(candidatePath, 0o700))
		require.NoError(t, os.WriteFile(
			filepath.Join(candidatePath, "prevents-directory-removal"),
			[]byte("retained child"),
			0o600,
		))
		logger.inlinePurge.fileScanner = &fakePurgeFileScanner{scan: func(
			logpattern.FileNamePattern,
			logpattern.GlobMode,
		) ([]infrafsstore.FileInfo, error) {
			return []infrafsstore.FileInfo{{
				Path:    candidatePath,
				Size:    1,
				ModTime: now.AddDate(0, 0, -10),
			}}, nil
		}}
		handled, closeResult := closeFileLoggerOnFirstCleanupError(logger)

		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)

		requireCleanupErrorHandlerClosedLogger(t, handled, closeResult, requireHandledPurgeRemovalFailure(candidatePath))
	})

	t.Run("pending upload error", func(t *testing.T) {
		codec := &inlineRecoveryCompressionCodec{}
		logger := newInlineCompressionRecoveryTestLogger(
			t,
			now,
			"cleanup-upload-handler-close",
			codec,
		)
		compressedPath := filepath.Join(
			logger.Fsstore.FileName.Dir,
			logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_20260829_1.log.gz",
		)
		writeGzipFile(t, compressedPath, "pending upload")
		uploadErr := errors.New("cleanup pending upload unavailable")
		logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploadErrorUploader{err: uploadErr})
		logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()
		handled, closeResult := closeFileLoggerOnFirstCleanupError(logger)

		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)

		requireCleanupErrorHandlerClosedLogger(t, handled, closeResult, requireHandledErrorIs(uploadErr))
	})

	t.Run("uploaded local removal error", func(t *testing.T) {
		codec := &inlineRecoveryCompressionCodec{}
		logger := newInlineCompressionRecoveryTestLogger(
			t,
			now,
			"cleanup-uploaded-removal-handler-close",
			codec,
		)
		compressedPath := filepath.Join(
			logger.Fsstore.FileName.Dir,
			logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_20260829_1.log.gz",
		)
		writeGzipFile(t, compressedPath, "uploaded archive")
		logger.ArchiveUploader = newSingleArchiveUploader(t, closeContractSuccessfulArchiveUploader{})
		logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()
		handled, closeResult := closeFileLoggerOnFirstCleanupError(logger)
		restoreRemoval := testutil.DenyFileRemoval(t, compressedPath)
		defer restoreRemoval()

		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)

		requireCleanupErrorHandlerClosedLogger(t, handled, closeResult, requireHandledFileRemovalError(compressedPath))
		require.FileExists(t, compressedPath)
	})
}

func TestFileLoggerCleanupWarnAndDebugHandlersCanRequestClose(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)

	t.Run("directory warning", func(t *testing.T) {
		logger := newInlineUsageTestLogger(t, now, "cleanup-warn-handler-close")
		require.NoError(t, os.Truncate(
			logger.Fsstore.ActiveFilePath,
			int64(logger.inlinePurge.config.MaxDirSize()+1),
		))
		handled := make(chan *logging.LogEntry, 1)
		closeResult := make(chan error, 1)
		var closeOnce sync.Once
		logger.OnWarn = func(
			_ context.Context,
			entry *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			closeOnce.Do(func() {
				handled <- entry
				logger.RequestClose()
				go func() { closeResult <- logger.Close() }()
			})
		}

		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)

		entry := receiveForTest(t, handled, "cleanup warning was not dispatched")
		require.Contains(t, entry.Message, WarnMsgActiveLogFilesExceedMaxDirSize)
		requireCleanupCloseResult(t, closeResult, "cleanup OnWarn")
	})

	t.Run("purge debug", func(t *testing.T) {
		logger := newInlineUsageDebugTestLogger(t, now, "cleanup-debug-handler-close")
		oldPath := filepath.Join(
			logger.Fsstore.FileName.Dir,
			logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_20260820_1.log",
		)
		require.NoError(t, os.WriteFile(oldPath, []byte("old log"), 0o600))
		oldTime := now.AddDate(0, 0, -10)
		require.NoError(t, os.Chtimes(oldPath, oldTime, oldTime))
		handled := make(chan *logging.LogEntry, 1)
		closeResult := make(chan error, 1)
		var closeOnce sync.Once
		logger.OnDebug = func(
			_ context.Context,
			entry *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			closeOnce.Do(func() {
				handled <- entry
				logger.RequestClose()
				go func() { closeResult <- logger.Close() }()
			})
		}

		logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)

		entry := receiveForTest(t, handled, "cleanup debug notification was not dispatched")
		require.Contains(t, entry.Message, DebugMsgPurgedFileAge)
		requireCleanupCloseResult(t, closeResult, "cleanup OnDebug")
	})
}

func closeFileLoggerOnFirstCleanupError(
	logger *FileLogger,
) (<-chan error, <-chan error) {
	handled := make(chan error, 1)
	closeResult := make(chan error, 1)
	var closeOnce sync.Once
	logger.OnError = func(
		_ context.Context,
		err error,
		_ *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		closeOnce.Do(func() {
			handled <- err
			logger.RequestClose()
			go func() { closeResult <- logger.Close() }()
		})
	}
	return handled, closeResult
}

// requireCleanupErrorHandlerClosedLoggerは、cleanupのエラーがOnErrorへ渡され、
// そのhandlerから要求したCloseが完了することを確認します。requireHandledErrは、
// 通知されたエラーの種類と原因を確認します。
func requireCleanupErrorHandlerClosedLogger(
	t *testing.T,
	handled <-chan error,
	closeResult <-chan error,
	requireHandledErr func(*testing.T, error),
) {
	t.Helper()
	handledErr := receiveForTest(t, handled, "cleanup error was not dispatched")
	requireHandledErr(t, handledErr)
	requireCleanupCloseResult(t, closeResult, "cleanup OnError")
}

// requireHandledErrorIsは、通知されたエラーがwantを原因として保持することを確認します。
func requireHandledErrorIs(want error) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		require.ErrorIs(t, err, want)
	}
}

// requireHandledPurgeRemovalFailureは、通知されたエラーがpurgeで削除できなかったpathの
// fsstore.FileRemovalFailureを含むことを確認します。
func requireHandledPurgeRemovalFailure(path string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		var failure *infrafsstore.FileRemovalFailure
		require.ErrorAs(t, err, &failure)
		require.Equal(t, path, failure.File.Path)
	}
}

// requireHandledFileRemovalErrorは、通知されたエラーがpathの削除段階の
// fsstore.FileRemovalErrorを含むことを確認します。
func requireHandledFileRemovalError(path string) func(*testing.T, error) {
	return func(t *testing.T, err error) {
		t.Helper()
		var removal *infrafsstore.FileRemovalError
		require.ErrorAs(t, err, &removal)
		require.Equal(t, infrafsstore.FileRemovalStepRemove, removal.Step)
		require.Equal(t, path, removal.Path)
	}
}

// requireRotationArchivePanicReportedは、通知されたエラーが、DefaultLoggerのrotation後の
// ログ処理で値wantPanicのpanicを回収した内部エラーであることを確認します。
func requireRotationArchivePanicReported(t *testing.T, err error, wantPanic any) {
	t.Helper()
	requireFileLoggerInternalError(t, err, DefaultLogger, OperationRotationArchive)
	var panicErr *RotationArchivePanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, wantPanic, panicErr.Value)
	require.NotEmpty(t, panicErr.Stack)
}

func requireCleanupCloseResult(
	t *testing.T,
	closeResult <-chan error,
	handlerName string,
) {
	t.Helper()
	require.NoError(t, receiveForTest(t, closeResult, handlerName+" deadlocked while closing its own logger"))
}

func TestFileLoggerRotationArchiveIgnoresCallerCancellation(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		rotationTime := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
		logger := newCloseContractRotationLogger(t, rotationTime, "detached-archive-context", nil)
		upload := newCancellationAwareArchiveUploader()
		releaseUpload := releaseOnCleanupForTest(t, upload.release)
		logger.ArchiveUploader = newSingleArchiveUploader(t, upload)
		forceCloseContractRotation(t, logger)

		callerCtx, cancelCaller := context.WithCancel(context.Background())
		defer cancelCaller()
		require.NoError(t, rotateFileLoggerForTest(callerCtx, logger))
		receiveForTest(t, upload.started, "rotation archive upload start")

		cancelCaller()
		// 取消がarchive taskへ伝わるなら、待機中のuploadはbubble内の処理が止まるまでに戻ります。
		synctest.Wait()
		require.Empty(t, upload.completed, "accepted archive task inherited caller cancellation")
		releaseUpload()
		require.NoError(t, receiveForTest(t, upload.completed, "detached archive task completion"))
		waitBackgroundForTest(t, logger)
	})
}

func TestFileLoggerRotationArchiveErrorHandlerCanRequestClose(t *testing.T) {
	compressionErr := errors.New("compression unavailable")
	uploadErr := errors.New("upload unavailable")
	tests := []struct {
		name          string
		compression   pkgfsstore.CompressionCodec
		archiveUpload uploader.Uploader
		wantErr       error
	}{
		{
			name:        "compression error",
			compression: failingUsageCompressionCodec{err: compressionErr},
			wantErr:     compressionErr,
		},
		{
			name:          "upload error",
			archiveUpload: archiveUploadErrorUploader{err: uploadErr},
			wantErr:       uploadErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rotationTime := time.Date(2026, time.August, 30, 14, 30, 0, 0, time.UTC)
			logger := newCloseContractRotationLogger(
				t,
				rotationTime,
				"archive-handler-close-"+strings.ReplaceAll(tt.name, " ", "-"),
				tt.compression,
			)
			logger.ArchiveUploader = newSingleArchiveUploader(t, tt.archiveUpload)
			forceCloseContractRotation(t, logger)

			handled := make(chan error, 1)
			closeResult := make(chan error, 1)
			var closeOnce sync.Once
			logger.OnError = func(
				_ context.Context,
				err error,
				_ *logging.LogEntry,
				_ observer.Tracer,
				_ observer.Meter,
			) {
				closeOnce.Do(func() {
					handled <- err
					logger.RequestClose()
					go func() { closeResult <- logger.Close() }()
				})
			}

			require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
			handledErr := receiveForTest(t, handled, "archive error was not dispatched")
			require.ErrorIs(t, handledErr, tt.wantErr)
			require.ErrorIs(t, logger.ForceClose(), shutdown.ErrForcedClose, "永続的な障害の終了待ちは明示的に中止する")
			closeErr := receiveForTest(t, closeResult, "OnError deadlocked while closing its own logger")
			require.ErrorIs(t, closeErr, shutdown.ErrForcedClose)
			receiveForTest(t, logger.closeController.Done(), "file logger shutdown worker completion")
		})
	}
}

func TestFileLoggerRotationArchivePanicIsRecoveredAndReported(t *testing.T) {
	rotationTime := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
	logger := newCloseContractRotationLogger(t, rotationTime, "archive-panic-recovery", nil)
	logger.ArchiveUploader = panickingDestinationArchiveUploader{}
	forceCloseContractRotation(t, logger)

	handled := make(chan error, 1)
	logger.OnError = func(
		_ context.Context,
		err error,
		_ *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		handled <- err
	}

	require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
	handledErr := receiveForTest(t, handled, "archive task panic was not recovered and reported")
	requireRotationArchivePanicReported(t, handledErr, "archive uploader panic")
	require.ErrorIs(t, logger.ForceClose(), shutdown.ErrForcedClose)
	receiveForTest(t, logger.ShutdownDone(), "file logger shutdown worker completion")
	require.ErrorIs(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger), shutdown.ErrForcedClose)
}

func TestFileLoggerAcceptedWritePanicReleasesLockAndCloseReservation(t *testing.T) {
	rotationTime := time.Date(2026, time.August, 30, 15, 30, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "write-panic-close", "write-panic-close")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	logger.Clock = closeContractPanickingClock{}

	entry := newTestApplicationLogEntry(
		context.Background(),
		pkglogger.LogLevelInfo,
		"accepted write panic must release close ownership",
		nil,
		false,
	)
	entry.OccurredAt = rotationTime
	require.PanicsWithValue(t, closeContractWritePanic, func() {
		_ = logger.HandleLogEntry(context.Background(), entry)
	})

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- logger.Close()
	}()
	closeErr := receiveForTest(t, closeDone, "Close remained blocked by the panicked write")
	require.NoError(t, closeErr)
}

func TestFileLoggerPostRotationCleanupPlanningPanicReleasesArchiveTask(t *testing.T) {
	rotationTime := time.Date(2026, time.August, 30, 16, 0, 0, 0, time.UTC)
	logger := newCloseContractRotationLogger(t, rotationTime, "post-cleanup-panic", nil)
	runtime := logger.inlinePurge
	require.True(t, runtime.directorySizeTracker.TryStartObservation(
		rotationTime,
		runtime.config,
		true,
	))
	runtime.directorySizeTracker.FinishObservation(0)
	logger.Clock = &closeContractPanicAfterFirstClock{
		at:         rotationTime,
		panicValue: closeContractPostCleanupClockPanic,
	}
	oldFilePath := logger.Fsstore.ActiveFilePath
	protectedPaths := logger.pathsToProtectForRotatedFile(oldFilePath)
	forceCloseContractRotation(t, logger)

	handled := make(chan error, 2)
	logger.OnError = func(
		_ context.Context,
		err error,
		_ *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		handled <- err
	}

	require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
	handledErr := receiveForTest(t, handled, "post-rotation cleanup panic was not reported")
	requireRotationArchivePanicReported(t, handledErr, closeContractPostCleanupClockPanic)
	requireRotationProtectionReleased(t, logger, protectedPaths)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(
		rotationTime,
		runtime.config,
		false,
	), "rotation mutation must invalidate the directory estimate before cleanup planning")
	runtime.directorySizeTracker.AbortObservation()
	requireFileLoggerCloseReturns(t, logger)
}

func TestFileLoggerRetryScheduleClockPanicStillReleasesRotationProtection(t *testing.T) {
	rotationTime := time.Date(2026, time.August, 30, 16, 30, 0, 0, time.UTC)
	logger := newCloseContractRotationLogger(t, rotationTime, "schedule-clock-panic", nil)
	runtime := logger.inlinePurge
	require.True(t, runtime.directorySizeTracker.TryStartObservation(
		rotationTime,
		runtime.config,
		true,
	))
	runtime.directorySizeTracker.FinishObservation(0)
	logger.Clock = &closeContractPanicAfterFirstClock{
		at:         rotationTime,
		panicValue: closeContractScheduleClockPanic,
	}
	logger.ArchiveUploader = panickingDestinationArchiveUploader{}
	oldFilePath := logger.Fsstore.ActiveFilePath
	protectedPaths := logger.pathsToProtectForRotatedFile(oldFilePath)
	forceCloseContractRotation(t, logger)

	handled := make(chan error, 3)
	logger.OnError = func(
		_ context.Context,
		err error,
		_ *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		handled <- err
	}

	require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
	handledErr := receiveForTest(t, handled, "retry schedule clock panic was not reported")
	requireRotationArchivePanicReported(t, handledErr, closeContractScheduleClockPanic)
	require.ErrorIs(t, logger.ForceClose(), shutdown.ErrForcedClose)
	receiveForTest(t, logger.ShutdownDone(), "file logger shutdown worker completion")
	requireRotationProtectionReleased(t, logger, protectedPaths)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(
		rotationTime,
		runtime.config,
		false,
	), "cache invalidation must run even when retry scheduling panics")
	runtime.directorySizeTracker.AbortObservation()
	require.ErrorIs(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger), shutdown.ErrForcedClose)
}

func requireRotationProtectionReleased(
	t *testing.T,
	logger *FileLogger,
	protectedPaths []string,
) {
	t.Helper()
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		for _, path := range protectedPaths {
			require.NotContains(t, protected, pathidentity.AbsoluteOrCleanPath(path))
		}
	})
}

func requireFileLoggerCloseReturns(t *testing.T, logger *FileLogger) {
	t.Helper()
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- logger.Close()
	}()
	closeErr := receiveForTest(t, closeDone, "Close remained blocked after rotation archive panic")
	require.NoError(t, closeErr)
}

func newCloseContractRotationLogger(
	t *testing.T,
	rotationTime time.Time,
	name string,
	compression pkgfsstore.CompressionCodec,
) *FileLogger {
	t.Helper()
	cfg := newTestFileLoggerConfig(t.TempDir(), name, name)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	cfg.FsstoreConfig.CompressionCodec = compression
	cfg.FsstoreConfig.Duration.CompressWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.CompressErrorThresholdDuration = 2 * time.Hour
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(rotationTime)
	// 失敗した試験は、本文で行うはずだったForceCloseを待たずに後片付けを終えます。
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, false)
	return logger
}

func forceCloseContractRotation(t *testing.T, logger *FileLogger) {
	t.Helper()
	require.NoError(t, os.Truncate(
		logger.Fsstore.ActiveFilePath,
		int64(logger.config.Fsstore().Lifecycle().MaxFileSize()),
	))
}

func TestLoggerSetClosedErrorUsesCommonLoggerClosedCause(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "file-set-close-contract")
	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)

	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
	require.ErrorIs(t, logger.testInfo(context.Background(), "after set close"), applogging.ErrLoggerClosed)

	_, err = loggerSet.NewFileLogger(context.Background())
	require.ErrorIs(t, err, ErrLoggerSetClosed)
	require.ErrorIs(t, err, applogging.ErrLoggerClosed)
}

func TestDeliveryFileLoggerRejectsWithReportOnceCloseStarts(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-file-close-contract")
	logger, err := NewDeliveryFileLogger(
		context.Background(),
		NewDeliveryFileLoggerConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	activePath := logger.Fsstore.ActiveFilePath

	logger.Fsstore.Mu.Lock()
	fileLocked := true
	defer func() {
		if fileLocked {
			logger.Fsstore.Mu.Unlock()
		}
	}()
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- logger.Close()
	}()

	require.Eventually(t, func() bool {
		logger.lifecycleMu.Lock()
		defer logger.lifecycleMu.Unlock()
		return logger.closeState == common.LoggerCloseStateStarted
	}, hangTimeoutForTest, time.Millisecond)
	report, deliveryErr := logger.testInfo(context.Background(), "during close")
	require.ErrorIs(t, deliveryErr, applogging.ErrLoggerClosed)
	requireDeliveryFileClosedRejection(t, report)

	logger.Fsstore.Mu.Unlock()
	fileLocked = false
	require.NoError(t, receiveForTest(t, closeDone, "Close after the file lock is released"))
	require.Nil(t, logger.Fsstore.File)
	logger.lifecycleMu.Lock()
	require.Equal(t, common.LoggerCloseStateCompleted, logger.closeState)
	logger.lifecycleMu.Unlock()

	report, deliveryErr = logger.testErrorWithStack(context.Background(), "after close", nil)
	require.ErrorIs(t, deliveryErr, applogging.ErrLoggerClosed)
	requireDeliveryFileClosedRejection(t, report)
	require.Nil(t, logger.Fsstore.File)
	require.Equal(t, activePath, logger.Fsstore.ActiveFilePath)
}

func TestDeliveryLoggerSetClosedResultIsRejected(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-file-set-close-contract")
	loggerSet, err := NewDeliveryLoggerSet(
		NewDeliveryFileLoggerConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	logicalLogger := loggerSet.LogicalLogger()
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
	deliveryLogger, err := multi.NewDeliveryMultiLoggerWithTargets(
		closeContractMultiLoggerConfig{},
		multi.DeliveryTarget{Backend: BackendServices, Logger: logicalLogger},
	)
	require.NoError(t, err)

	report, deliveryErr := deliveryLogger.Info(
		context.Background(),
		"after set close",
	)
	require.ErrorIs(t, deliveryErr, applogging.ErrLoggerClosed)
	requireDeliveryFileClosedRejection(t, report)
}

func requireDeliveryFileClosedRejection(t *testing.T, report pkglogger.DeliveryReport) {
	t.Helper()
	require.Len(t, report.Targets, 1)
	require.Equal(t, pkglogger.DeliveryRejected, report.Targets[0].Status)
	require.ErrorIs(t, report.Targets[0].Err, applogging.ErrLoggerClosed)
}

type closeContractBlockingFirstClock struct {
	at      time.Time
	entered chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (c *closeContractBlockingFirstClock) Now() time.Time {
	block := false
	c.once.Do(func() {
		block = true
		close(c.entered)
	})
	if block {
		<-c.release
	}
	return c.at
}

type cancellationAwareArchiveUploader struct {
	started   chan struct{}
	release   chan struct{}
	completed chan error
}

type closeContractSuccessfulArchiveUploader struct{}

func (closeContractSuccessfulArchiveUploader) Upload(
	context.Context,
	uploader.UploadRequest,
) error {
	return nil
}

func newCancellationAwareArchiveUploader() *cancellationAwareArchiveUploader {
	return &cancellationAwareArchiveUploader{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		completed: make(chan error, 1),
	}
}

func (u *cancellationAwareArchiveUploader) Upload(
	ctx context.Context,
	_ uploader.UploadRequest,
) error {
	close(u.started)
	select {
	case <-ctx.Done():
		err := ctx.Err()
		u.completed <- err
		return err
	case <-u.release:
		u.completed <- nil
		return nil
	}
}

const closeContractWritePanic = "accepted write panic"

type closeContractPanickingClock struct{}

func (closeContractPanickingClock) Now() time.Time {
	panic(closeContractWritePanic)
}

const (
	closeContractPostCleanupClockPanic = "post-rotation cleanup clock panic"
	closeContractScheduleClockPanic    = "rotation retry schedule clock panic"
)

type closeContractPanicAfterFirstClock struct {
	at         time.Time
	panicValue string
	calls      atomic.Int32
}

func (c *closeContractPanicAfterFirstClock) Now() time.Time {
	if c.calls.Add(1) > 1 {
		panic(c.panicValue)
	}
	return c.at
}
