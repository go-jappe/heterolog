package fsarchivehandler

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

func TestFailureArchiveForceCloseReleasesWaitersButKeepsActiveResources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pipeline := newTestArchiveHandler(t, time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC), nil)
		entered, release := make(chan context.Context, 1), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		require.True(t, pipeline.handler.logFileProcessingRecovery.Schedule(archiveLogFileProcessingRecoveryKey{PreparationPath: "accepted"}, nil,
			func(ctx context.Context) (bool, error) { entered <- ctx; <-release; return true, nil }, nil, nil))
		operationContext := <-entered
		closed := make(chan error, 2)
		for range cap(closed) {
			go func() { closed <- pipeline.Shutdown(context.Background()) }()
		}
		synctest.Wait()
		require.Empty(t, closed)
		require.NoError(t, operationContext.Err(), "normal Shutdown must not cancel accepted recovery")
		require.ErrorIs(t, pipeline.ForceClose(), shutdown.ErrForcedClose)
		synctest.Wait()
		require.ErrorIs(t, operationContext.Err(), context.Canceled)
		require.False(t, pipeline.tryAcceptFailureRequest())
		for range cap(closed) {
			require.ErrorIs(t, <-closed, shutdown.ErrForcedClose)
		}
		require.NotNil(t, pipeline.handler.Fsstore.FileRetryable)
		require.NotNil(t, pipeline.archiveRootLease)
		select {
		case <-pipeline.ShutdownDone():
			t.Fatal("actual shutdown completed while accepted recovery was still running")
		default:
		}
		unblock()
		waitFailureArchiveClosedForTest(t, pipeline)
		require.Nil(t, pipeline.handler.Fsstore.FileRetryable)
		require.Nil(t, pipeline.archiveRootLease)
	})
}
