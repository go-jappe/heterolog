package fsarchivehandler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type closeObservingArchiveCheckpointStore struct {
	checkpointstore.Store
	closeCalls atomic.Int32
	closeErr   error
	onClose    func()
}

func (s *closeObservingArchiveCheckpointStore) Close() error {
	s.closeCalls.Add(1)
	if s.onClose != nil {
		s.onClose()
	}
	return errors.Join(s.Store.Close(), s.closeErr)
}

func TestFailureArchiveShutdownClosesBothCheckpointStoresAfterAcceptedTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		retryableErr := errors.New("retryable checkpoint save remains unconfirmed")
		permanentErr := errors.New("permanent checkpoint save remains unconfirmed")
		retryable := &closeObservingArchiveCheckpointStore{closeErr: retryableErr}
		permanent := &closeObservingArchiveCheckpointStore{closeErr: permanentErr}
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			for category, store := range map[failurearchive.FailureCategory]*closeObservingArchiveCheckpointStore{
				failurearchive.FailureCategoryRetryable: retryable,
				failurearchive.FailureCategoryPermanent: permanent,
			} {
				cfg.setArchiveUploader(category, newDestinationFailureArchiveUploader("archive"))
				cfg.embedArchiveCheckpointStore(category, &store.Store, store)
			}
		})
		handler := pipeline.handler
		var continuationStillAccepted atomic.Bool
		retryable.onClose = func() {
			continuationStillAccepted.Store(handler.logFileProcessingRecovery.Schedule(archiveLogFileProcessingRecoveryKey{File: checkpointstore.CheckpointKey{Generation: "too-late", Compressed: true}}, nil, func(context.Context) (bool, error) { return true, nil }, nil, nil))
		}
		require.True(t, handler.beginArchiveTask())
		acceptedFinished := false
		defer func() {
			if !acceptedFinished {
				handler.endArchiveTask()
			}
		}()
		closed := make(chan error, 1)
		go func() { closed <- pipeline.Shutdown(context.Background()) }()
		synctest.Wait()
		handler.archiveTaskMu.Lock()
		taskClosing := handler.archiveTaskClosing
		handler.archiveTaskMu.Unlock()
		require.True(t, taskClosing, "Shutdown must be waiting for the accepted task")
		require.Zero(t, retryable.closeCalls.Load())
		require.Zero(t, permanent.closeCalls.Load())
		_, err := retryable.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20260922-1", Compressed: true}, []objectstorage.DestinationID{"archive"})
		require.NoError(t, err, "accepted archive task must still be able to save its checkpoint")
		handler.endArchiveTask()
		acceptedFinished = true
		select {
		case err := <-closed:
			require.ErrorIs(t, err, retryableErr)
			require.ErrorIs(t, err, permanentErr)
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("Shutdown did not close checkpoint stores")
		}
		require.False(t, continuationStillAccepted.Load())
		require.EqualValues(t, 1, retryable.closeCalls.Load())
		require.EqualValues(t, 1, permanent.closeCalls.Load())
		require.Nil(t, handler.Fsstore.FileRetryable)
		require.Nil(t, handler.Fsstore.FilePermanent)
		require.NoError(t, pipeline.Shutdown(context.Background()))
		require.EqualValues(t, 1, retryable.closeCalls.Load())
		require.EqualValues(t, 1, permanent.closeCalls.Load())
	})
}

func TestFailureArchiveConstructionCleanupClosesPartialCheckpointStores(t *testing.T) {
	store := &closeObservingArchiveCheckpointStore{
		Store: newFailureArchiveCompressedLogCheckpointStoreForTest(t, t.TempDir(), "partial_construction"),
	}
	pipeline := &FailureArchivePipeline{handler: &FsArchiveHandler{retryableDirectory: archiveDirectoryRuntime{uploadCheckpoints: store}}}
	initErr := errors.New("permanent checkpoint construction failed")
	remaining, err := pipeline.cleanupFailedConstruction(initErr)
	require.Nil(t, remaining)
	require.ErrorIs(t, err, initErr)
	require.EqualValues(t, 1, store.closeCalls.Load())
}
