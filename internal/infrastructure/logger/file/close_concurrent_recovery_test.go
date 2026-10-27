package file

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type controlledCloseArchiveUploader struct {
	recordingArchiveUploader
	calls    atomic.Int32
	entered  chan int
	releases []<-chan struct{}
	errors   []error
}

func (u *controlledCloseArchiveUploader) Close() error {
	attempt := int(u.calls.Add(1))
	u.entered <- attempt
	if attempt <= len(u.releases) {
		<-u.releases[attempt-1]
	}
	if attempt <= len(u.errors) {
		return u.errors[attempt-1]
	}
	return nil
}

func TestFileLoggerConcurrentCloseJoinsRecoveryNotificationClose(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "concurrent-recovery-close")
		logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "logger Close", func() closerForTest { return logger }, true)
		releaseUploader := make(chan struct{})
		unblockUploader := sync.OnceFunc(func() { close(releaseUploader) })
		defer unblockUploader()
		upload := &controlledCloseArchiveUploader{
			entered: make(chan int, 16), releases: []<-chan struct{}{releaseUploader},
		}
		logger.ArchiveUploader = newSingleArchiveUploader(t, upload)

		notificationStarted, allowNotification := make(chan struct{}), make(chan struct{})
		unblockNotification := sync.OnceFunc(func() { close(allowNotification) })
		defer unblockNotification()
		notificationClosed := make(chan error, 1)
		require.True(t, logger.archiveLogFileProcessingRecovery.Schedule(
			archiveLogFileProcessingRecoveryKey{File: checkpointstore.CheckpointKey{Generation: "20261007-1"}},
			nil,
			func(context.Context) (bool, error) { return true, nil },
			func(error) func() {
				return func() {
					close(notificationStarted)
					<-allowNotification
					logger.RequestClose()
					go func() { notificationClosed <- logger.Close() }()
				}
			},
			nil,
		))
		receiveForTest(t, notificationStarted, "notification start")

		closed := make(chan error, 8)
		go func() { closed <- logger.Close() }()
		require.Equal(t, 1, receiveForTest(t, upload.entered, "upload attempt"))
		for range cap(closed) - 1 {
			go func() { closed <- logger.Close() }()
		}
		unblockNotification()
		synctest.Wait()
		require.EqualValues(t, 1, upload.calls.Load(), "only one caller owns uploader cleanup")
		require.Empty(t, closed)
		require.Empty(t, notificationClosed, "the asynchronous request joins the same unfinished close attempt")
		require.ErrorIs(t, logger.rejectIfClosed(), pkglogger.ErrLoggerClosed)
		unblockUploader()
		for range cap(closed) {
			require.NoError(t, receiveForTest(t, closed, "Close result"))
		}
		require.NoError(t, receiveForTest(t, notificationClosed, "notification Close result"))
		receiveForTest(t, logger.archiveLogFileProcessingRecovery.NotificationsDone(), "notification drain")
		require.Nil(t, logger.ArchiveUploader)
		require.Nil(t, logger.Fsstore.File)
		require.Equal(t, loggercommon.LoggerCloseStateCompleted, logger.closeState)
		require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "logger Close", logger))
		require.EqualValues(t, 1, upload.calls.Load())
	})
}

func TestFileLoggerConcurrentCloseRetriesIncompleteCleanupUntilSuccess(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "concurrent-close-retry")
		logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "logger Close", func() closerForTest { return logger }, true)
		firstRelease, retryRelease := make(chan struct{}), make(chan struct{})
		unblockFirst := sync.OnceFunc(func() { close(firstRelease) })
		unblockRetry := sync.OnceFunc(func() { close(retryRelease) })
		defer unblockFirst()
		defer unblockRetry()
		uploadErr := errors.New("uploader close must be retried")
		upload := &controlledCloseArchiveUploader{
			entered:  make(chan int, 16),
			releases: []<-chan struct{}{firstRelease, retryRelease},
			errors:   []error{uploadErr},
		}
		logger.ArchiveUploader = newSingleArchiveUploader(t, upload)
		// uploader終了後にfileを閉じ、その結果を待機中の全callerへ返します。
		require.NoError(t, logger.Fsstore.File.Close())
		first := make(chan error, 1)
		go func() { first <- logger.Close() }()
		require.Equal(t, 1, receiveForTest(t, upload.entered, "upload attempt"))
		firstJoiners := make(chan error, 3)
		for range cap(firstJoiners) {
			go func() { firstJoiners <- logger.Close() }()
		}
		synctest.Wait()
		require.EqualValues(t, 1, upload.calls.Load())
		require.Empty(t, firstJoiners)
		unblockFirst()
		synctest.Wait()
		require.Empty(t, first, "未完了の終了処理を全callerが継続して待つ")
		require.Empty(t, firstJoiners)
		time.Sleep(shutdown.ReportInterval)
		synctest.Wait()
		retry := make(chan error, 2)
		go func() { retry <- logger.Close() }()
		require.Equal(t, 2, receiveForTest(t, upload.entered, "upload attempt"))
		go func() { retry <- logger.Close() }()
		synctest.Wait()
		require.Empty(t, retry)
		require.EqualValues(t, 2, upload.calls.Load())
		require.Equal(t, loggercommon.LoggerCloseStateStarted, logger.closeState)
		require.ErrorIs(t, logger.rejectIfClosed(), pkglogger.ErrLoggerClosed)
		unblockRetry()
		require.ErrorIs(t, receiveForTest(t, first, "first Close result"), os.ErrClosed)
		for range cap(firstJoiners) {
			require.ErrorIs(t, receiveForTest(t, firstJoiners, "joined Close result"), os.ErrClosed)
		}
		for range cap(retry) {
			require.ErrorIs(t, receiveForTest(t, retry, "retried Close result"), os.ErrClosed)
		}
		require.Nil(t, logger.ArchiveUploader)
		require.Equal(t, loggercommon.LoggerCloseStateCompleted, logger.closeState)
		require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "logger Close", logger))
		require.EqualValues(t, 2, upload.calls.Load())
	})
}
