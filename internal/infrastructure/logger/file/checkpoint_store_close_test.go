package file

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type closeObservingInlineCheckpointStore struct {
	checkpointstore.Store
	closeCalls atomic.Int32
	closeErr   error
	onClose    func()
}

func (s *closeObservingInlineCheckpointStore) Close() error {
	s.closeCalls.Add(1)
	if s.onClose != nil {
		s.onClose()
	}
	return errors.Join(s.Store.Close(), s.closeErr)
}

func TestFileLoggerClosesCheckpointStoreAfterAcceptedWork(t *testing.T) {
	logger := newInlineUsageTestLogger(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), "checkpoint-store-close")
	closeErr := errors.New("checkpoint save remains unconfirmed")
	store := &closeObservingInlineCheckpointStore{
		Store:    newCompressedLogCheckpointTestStore(t, t.TempDir(), "close_order"),
		closeErr: closeErr,
	}
	var continuationStillAccepted atomic.Bool
	store.onClose = func() {
		continuationStillAccepted.Store(logger.archiveLogFileProcessingRecovery.Schedule(archiveLogFileProcessingRecoveryKey{File: checkpointstore.CheckpointKey{Generation: "too-late", Compressed: true}}, nil, func(context.Context) (bool, error) { return true, nil }, nil, nil))
	}
	logger.archiveUploadCheckpoints = store
	require.True(t, logger.beginBackgroundOperation())
	acceptedFinished := false
	defer func() {
		if !acceptedFinished {
			logger.endBackgroundOperation()
		}
	}()
	closed := make(chan error, 1)
	go func() { closed <- logger.Close() }()
	require.Eventually(t, func() bool {
		logger.backgroundMu.Lock()
		defer logger.backgroundMu.Unlock()
		return logger.closeState == loggercommon.LoggerCloseStateStarted
	}, hangTimeoutForTest, time.Millisecond)
	require.Zero(t, store.closeCalls.Load(), "Store must remain available to work accepted before Close")
	_, err := store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20260922-1", Compressed: true}, []objectstorage.DestinationID{"archive"})
	require.NoError(t, err)
	logger.endBackgroundOperation()
	acceptedFinished = true
	closedErr := receiveForTest(t, closed, "FileLogger did not finish closing its checkpoint Store")
	require.ErrorIs(t, closedErr, closeErr)
	require.EqualValues(t, 1, store.closeCalls.Load())
	require.False(t, continuationStillAccepted.Load(), "file continuation must stop before the Store closes")
	require.Nil(t, logger.Fsstore.File, "checkpoint error must not prevent other resource cleanup")
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
	require.EqualValues(t, 1, store.closeCalls.Load())
}
