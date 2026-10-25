package checkpoint

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreOnErrorReportsRecurringFailuresOutsideStoreLock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		key := CheckpointKey{Generation: "20260915-2", Compressed: true}
		type report struct {
			err        error
			checkpoint Checkpoint
			loadErr    error
		}
		reports := make(chan report, 4)
		var store *FilesystemStore
		var err error
		store, err = NewFilesystemStore(t.TempDir(), "on-error-recurring", WithOnError(func(err error) {
			loadCtx, cancel := context.WithTimeout(ctx, SaveRecoveryInterval/2)
			defer cancel()
			checkpoint, loadErr := store.Load(loadCtx, key)
			// 予定外の追加通知で再試行goroutineを止めないよう、受け取り手がなければ失敗として記録します。
			boundedwait.SendOrReport(t, reports, report{err: err, checkpoint: checkpoint, loadErr: loadErr}, "retry error report")
		}))
		require.NoError(t, err)
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "store.Close()", store.Close, store.ForceClose, nil)
		restore := blockCheckpointReplacement(t, store)
		accepted, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
		pending := pendingSaveForTest(t, err)
		synctest.Wait()
		require.Empty(t, reports, "the initial save failure is returned to the caller, not reported again")

		previous := errors.Unwrap(pending)
		for range 3 {
			advanceCheckpointRecoveryForTest()
			require.Len(t, reports, 1, "each failed retry reports once and can load without waiting for the store lock")
			got := boundedwait.ReceiveInBubble(t, reports, "receive from reports")
			require.Error(t, got.err)
			require.Same(t, checkpointSaveErrorForTest(store), got.err)
			require.NotSame(t, previous, got.err, "the callback receives the new retry error")
			require.NoError(t, got.loadErr)
			require.Equal(t, accepted, got.checkpoint)
			previous = got.err
		}

		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		time.Sleep(3 * SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, reports, "successful recovery stops error notifications")
		persisted, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, []DestinationCursor{{DestinationID: "primary", Generation: accepted.Key.Generation}}, persisted.Cursors)
	})
}

func TestFilesystemStoreOnErrorCallerQueueMayCloseStore(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		reports := make(chan error, 4)
		store, err := NewFilesystemStore(t.TempDir(), "on-error-queued-close", WithOnError(func(err error) {
			// 予定外の追加通知で再試行goroutineを止めないよう、受け取り手がなければ失敗として記録します。
			boundedwait.SendOrReport(t, reports, err, "retry error")
		}))
		require.NoError(t, err)
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "store.Close()", store.Close, store.ForceClose, nil)
		type closeResult struct {
			reported error
			closeErr error
		}
		closed := make(chan closeResult, 1)
		// 報告が届く前に試験が失敗しても、報告を待つgoroutineをbubbleに残しません。
		// Storeの終了処理より先に実行するよう、Storeのcleanupの後に登録します。
		stopWaiting := make(chan struct{})
		t.Cleanup(func() { close(stopWaiting) })
		go func() {
			select {
			case err := <-reports:
				closed <- closeResult{reported: err, closeErr: store.Close()}
			case <-stopWaiting:
			}
		}()
		restore := blockCheckpointReplacement(t, store)
		_, err = store.AdvanceCompletionCursor(context.Background(), CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		synctest.Wait()
		require.Empty(t, closed, "the caller's queue only receives background retry failures")
		advanceCheckpointRecoveryForTest()
		require.Empty(t, closed, "the error handler waits until accepted state is saved")
		for range 2 {
			advanceCheckpointRecoveryForTest()
		}
		require.Len(t, reports, 2, "retry errors continue to be registered while the error handler waits in Close")
		for range 2 {
			require.Error(t, boundedwait.ReceiveInBubble(t, reports, "receive from reports"))
		}

		restore()
		advanceCheckpointRecoveryForTest()
		require.Len(t, closed, 1, "a caller-owned error handler can finish closing the store after recovery")
		result := boundedwait.ReceiveInBubble(t, closed, "receive from closed")
		require.Error(t, result.reported)
		require.NoError(t, result.closeErr)
		time.Sleep(3 * SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, reports, "completed recovery stops further retry notifications")
		require.NoError(t, checkpointSaveErrorForTest(store))
		persisted, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, checkpointStateForTest(store), persisted)
	})
}

func TestFilesystemStoreOnErrorOptionalDuringRecovery(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		options []FilesystemStoreOption
	}{
		{name: "omitted"},
		{name: "explicit nil", options: []FilesystemStoreOption{WithOnError(nil)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, err := NewFilesystemStore(t.TempDir(), "on-error-optional", test.options...)
				require.NoError(t, err)
				boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "store.Close()", store.Close, store.ForceClose, nil)
				restore := blockCheckpointReplacement(t, store)
				accepted, err := store.AdvanceCompletionCursor(context.Background(), CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
				pendingSaveForTest(t, err)
				for range 2 {
					previous := checkpointSaveErrorForTest(store)
					advanceCheckpointRecoveryForTest()
					require.Error(t, checkpointSaveErrorForTest(store))
					require.NotSame(t, previous, checkpointSaveErrorForTest(store))
				}
				restore()
				advanceCheckpointRecoveryForTest()
				require.NoError(t, checkpointSaveErrorForTest(store))
				persisted, err := store.readDocument()
				require.NoError(t, err)
				require.Equal(t, []DestinationCursor{{DestinationID: "primary", Generation: accepted.Key.Generation}}, persisted.Cursors)
			})
		})
	}
}
