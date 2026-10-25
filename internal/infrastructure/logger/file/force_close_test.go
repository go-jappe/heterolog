package file

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

func TestFileLoggerForceCloseReleasesWaitersButKeepsActiveResources(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "forced-inline")
		logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		entered, release := make(chan context.Context, 1), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		require.True(t, logger.archiveLogFileProcessingRecovery.Schedule(archiveLogFileProcessingRecoveryKey{PreparationPath: "accepted"}, nil,
			func(ctx context.Context) (bool, error) { entered <- ctx; <-release; return true, nil }, nil, nil))
		operationContext := receiveForTest(t, entered, "blocked operation entry")
		closed := make(chan error, 2)
		for range cap(closed) {
			go func() { closed <- logger.Close() }()
		}
		synctest.Wait()
		require.Empty(t, closed)
		require.NoError(t, operationContext.Err(), "normal Close must not cancel accepted recovery")
		require.ErrorIs(t, logger.ForceClose(), shutdown.ErrForcedClose)
		synctest.Wait()
		require.ErrorIs(t, operationContext.Err(), context.Canceled)
		require.ErrorIs(t, logger.rejectIfClosed(), applogging.ErrLoggerClosed)
		for range cap(closed) {
			require.ErrorIs(t, receiveForTest(t, closed, "Close result"), shutdown.ErrForcedClose)
		}
		require.NotNil(t, logger.Fsstore.File)
		logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
			require.Contains(t, protected, logger.Fsstore.ActiveFilePath)
		})
		select {
		case <-logger.ShutdownDone():
			t.Fatal("actual shutdown completed while accepted recovery was still running")
		default:
		}
		require.ErrorIs(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "logger Close", logger), shutdown.ErrForcedClose)
		unblock()
		receiveForTest(t, logger.closeController.Done(), "shutdown worker completion")
		require.Nil(t, logger.Fsstore.File)
	})
}

func TestLoggerSetForceCloseDoesNotCreateLazyRoles(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "forced-set")
		set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		logger, err := set.NewFileLogger(context.Background())
		require.NoError(t, err)
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		require.True(t, logger.archiveLogFileProcessingRecovery.Schedule(archiveLogFileProcessingRecoveryKey{PreparationPath: "accepted"}, nil,
			func(context.Context) (bool, error) { close(entered); <-release; return true, nil }, nil, nil))
		receiveForTest(t, entered, "blocked operation entry")
		closed := make(chan error, 1)
		go func() { closed <- set.Close() }()
		synctest.Wait()
		require.ErrorIs(t, set.ForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, receiveForTest(t, closed, "Close result"), shutdown.ErrForcedClose)
		_, err = set.NewDebugFileLogger(context.Background())
		require.ErrorIs(t, err, ErrLoggerSetClosed)
		require.Len(t, set.loggers, 1)
		require.NotNil(t, logger.Fsstore.File)
		unblock()
		receiveForTest(t, set.closeController.Done(), "shutdown worker completion")
		require.Nil(t, logger.Fsstore.File)
	})
}

func TestLoggerSetRequestForceCloseLeavesResourceDisposalToOwner(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "request-force-only")
		set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		logger, err := set.NewFileLogger(context.Background())
		require.NoError(t, err)
		require.ErrorIs(t, set.RequestForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, logger.rejectIfClosed(), applogging.ErrLoggerClosed)
		require.ErrorIs(t, logger.lockForWrite(), applogging.ErrLoggerClosed)
		synctest.Wait()
		require.NotNil(t, logger.Fsstore.File)
		require.False(t, set.ShutdownComplete())
		select {
		case <-set.ShutdownDone():
			t.Fatal("中止要求だけで終了workerを開始しない")
		default:
		}
		require.ErrorIs(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "set Close", set), shutdown.ErrForcedClose)
		receiveForTest(t, set.ShutdownDone(), "shutdown worker completion")
		require.Nil(t, logger.Fsstore.File)
		require.True(t, set.ShutdownComplete())
	})
}
