package file

import (
	"context"
	"errors"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

type closeObservingArchiveUploader struct {
	recordingArchiveUploader
	closeCalls int
	closeErr   error
}

func (u *closeObservingArchiveUploader) Close() error {
	u.closeCalls++
	return u.closeErr
}

func TestFileLoggerCloseFileErrorReleasesGuardAndClosesUploader(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "file-close-error")
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	upload := &closeObservingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, upload)
	activePath := logger.Fsstore.ActiveFilePath
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Contains(t, protected, activePath)
	})
	require.NoError(t, logger.Fsstore.File.Close())

	require.ErrorIs(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger), os.ErrClosed)
	require.Nil(t, logger.Fsstore.File)
	require.Nil(t, logger.ArchiveUploader)
	require.Equal(t, common.LoggerCloseStateCompleted, logger.closeState)
	require.Equal(t, 1, upload.closeCalls)
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.NotContains(t, protected, activePath)
	})

	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
	require.Equal(t, 1, upload.closeCalls)
}

func TestDeliveryFileLoggerCloseFileErrorReleasesGuardAndDirectoryLease(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-file-close-error")
	adapter := NewDeliveryFileLoggerConfigAdapter(cfg)
	logger, err := NewDeliveryFileLogger(context.Background(), adapter, nil, nil)
	require.NoError(t, err)
	activePath := logger.Fsstore.ActiveFilePath
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Contains(t, protected, activePath)
	})
	require.NoError(t, logger.Fsstore.File.Close())

	require.ErrorIs(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger), os.ErrClosed)
	require.Nil(t, logger.Fsstore.File)
	require.Equal(t, common.LoggerCloseStateCompleted, logger.closeState)
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.NotContains(t, protected, activePath)
	})

	replacement, err := NewDeliveryFileLogger(context.Background(), adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "replacement Close", func() closerForTest { return replacement }, true)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
	replacement.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Contains(t, protected, replacement.Fsstore.ActiveFilePath)
	})
}

func TestLoggerSetCloseFileErrorRetriesOnlyUnfinishedUploader(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "uploader-close-retry")
		cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
		adapter := NewConfigAdapter(cfg)
		loggerSet, err := NewLoggerSet(adapter, nil, nil)
		require.NoError(t, err)
		defaultLogger, err := loggerSet.NewFileLogger(context.Background())
		require.NoError(t, err)
		errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
		require.NoError(t, err)
		defaultUpload := &closeObservingArchiveUploader{}
		defaultLogger.ArchiveUploader = newSingleArchiveUploader(t, defaultUpload)
		uploadErr := errors.New("uploader is still closing")
		errorUpload := &controlledCloseArchiveUploader{
			entered: make(chan int, 8), errors: []error{uploadErr},
		}
		errorLogger.ArchiveUploader = newSingleArchiveUploader(t, errorUpload)
		require.NoError(t, errorLogger.Fsstore.File.Close())

		closed := make(chan error, 1)
		go func() { closed <- loggerSet.Close() }()
		synctest.Wait()
		require.Empty(t, closed, "未完了のuploaderだけを同じCloseで再試行する")
		loggerSet.mu.Lock()
		require.Equal(t, common.LoggerCloseStateStarted, loggerSet.closeState)
		require.Len(t, loggerSet.closedLoggers, 1)
		_, defaultClosed := loggerSet.closedLoggers[defaultLogger]
		require.True(t, defaultClosed)
		loggerSet.mu.Unlock()
		require.Equal(t, 1, defaultUpload.closeCalls)
		require.EqualValues(t, 1, errorUpload.calls.Load())
		errorLogger.backgroundMu.Lock()
		require.Equal(t, common.LoggerCloseStateStarted, errorLogger.closeState)
		errorLogger.backgroundMu.Unlock()
		for _, logger := range []*FileLogger{defaultLogger, errorLogger} {
			// Closeの再試行もFileを書き換えるため、確認時も同じlockで読む。
			logger.Fsstore.Mu.Lock()
			file := logger.Fsstore.File
			activePath := logger.Fsstore.ActiveFilePath
			logger.Fsstore.Mu.Unlock()
			if logger == defaultLogger {
				require.Nil(t, file)
				logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
					require.NotContains(t, protected, activePath)
				})
			} else {
				require.NotNil(t, file, "uploaderと通知の終了前にwriterを解放しない")
				logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
					require.Contains(t, protected, activePath)
				})
			}
		}
		replacement, err := NewLoggerSet(adapter, nil, nil)
		require.Nil(t, replacement)
		require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

		// 未完了のuploaderが終了してからfileを閉じ、leaseを解放します。
		time.Sleep(shutdown.ReportInterval)
		require.ErrorIs(t, receiveForTest(t, closed, "Close result"), os.ErrClosed)
		require.Nil(t, errorLogger.Fsstore.File)
		require.Equal(t, 1, defaultUpload.closeCalls)
		require.EqualValues(t, 2, errorUpload.calls.Load())
		require.Nil(t, errorLogger.ArchiveUploader)
		require.Equal(t, common.LoggerCloseStateCompleted, errorLogger.closeState)
		require.Equal(t, common.LoggerCloseStateCompleted, loggerSet.closeState)
		require.Len(t, loggerSet.closedLoggers, 2)
		replacement, err = NewLoggerSet(adapter, nil, nil)
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "replacement Close", func() closerForTest { return replacement }, true)
		require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "loggerSet Close", loggerSet))
		require.Equal(t, 1, defaultUpload.closeCalls)
		require.EqualValues(t, 2, errorUpload.calls.Load())
	})
}
