package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func checkpointSaveErrorForTest(store *FilesystemStore) error {
	store.stateOrSaveMu.Lock()
	defer store.stateOrSaveMu.Unlock()
	return store.lastSaveErr
}

func checkpointStateForTest(store *FilesystemStore) checkpointDocument {
	store.stateOrSaveMu.Lock()
	defer store.stateOrSaveMu.Unlock()
	return cloneDocument(store.state)
}

func pendingSaveForTest(t *testing.T, err error) *PendingSaveError {
	t.Helper()
	var pending *PendingSaveError
	require.ErrorAs(t, err, &pending)
	return pending
}

func advanceCheckpointRecoveryForTest() {
	// Start the worker's timer before advancing the bubble clock.
	synctest.Wait()
	time.Sleep(SaveRecoveryInterval)
	synctest.Wait()
}

// newCheckpointTestStoreは、試験終了時に閉じるStoreを作ります。保存待ちを残す試験もあるため
// 終了処理のerrorは確認しません。Closeは受理済みの保存の完了を期限なく待つため、共有fixtureとして
// bubble外の実時間でもbubble内の仮想時間でもboundedwait.HangTimeoutで待ち、期限を過ぎたら
// ForceCloseを通知して失敗させます。保存の再試行が終わらない回帰でも試験は停止しません。
func newCheckpointTestStore(t *testing.T, source string) *FilesystemStore {
	t.Helper()
	store, err := NewFilesystemStore(t.TempDir(), source)
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, store.ForceClose, nil)
	return store
}

// blockCheckpointReplacement makes the real atomic rename fail without replacing
// Store methods. Restore is called only while no save is in progress.
func blockCheckpointReplacement(t *testing.T, store *FilesystemStore) func() {
	t.Helper()
	path := store.documentPath()
	previous := filepath.Join(t.TempDir(), "previous.json")
	err := os.Rename(path, previous)
	if err != nil && !os.IsNotExist(err) {
		require.NoError(t, err)
	}
	hadPrevious := err == nil
	require.NoError(t, os.Mkdir(path, 0o700))
	marker := filepath.Join(path, "prevent-replacement")
	require.NoError(t, os.WriteFile(marker, []byte("preserve"), fileMode))
	var restoreOnce sync.Once
	restore := func() {
		restoreOnce.Do(func() {
			require.NoError(t, os.Remove(marker))
			require.NoError(t, os.Remove(path))
			if hadPrevious {
				require.NoError(t, os.Rename(previous, path))
			}
		})
	}
	t.Cleanup(restore)
	return restore
}

func TestFilesystemStoreRecoversEitherFileRepresentationWithoutCallerRetry(t *testing.T) {
	t.Parallel()
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				store := newCheckpointTestStore(t, "automatic-save-recovery")
				var api Store = store
				key := CheckpointKey{Generation: "20260915-1", Compressed: compressed}
				before := checkpointStateForTest(store)
				restore := blockCheckpointReplacement(t, store)
				snapshot, err := api.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
				pending := pendingSaveForTest(t, err)
				require.True(t, snapshot.DestinationComplete("primary"))
				require.Same(t, checkpointSaveErrorForTest(store), errors.Unwrap(pending))
				accepted := checkpointStateForTest(store)
				require.NotEqual(t, before, accepted)
				synctest.Wait()
				restore()
				time.Sleep(SaveRecoveryInterval - time.Nanosecond)
				synctest.Wait()
				persisted, err := store.readDocument()
				require.NoError(t, err)
				require.Equal(t, before, persisted, "recovery waits for its retry interval")
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				require.NoError(t, checkpointSaveErrorForTest(store))
				persisted, err = store.readDocument()
				require.NoError(t, err)
				require.Equal(t, accepted, persisted)
				loaded, err := api.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, snapshot, loaded)
			})
		})
	}
}

func TestFilesystemStoreOneRecoveryPersistsLatestAcceptedCursors(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "latest-state-save-recovery")
		var api Store = store
		_, err := api.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		before := checkpointStateForTest(store)
		restore := blockCheckpointReplacement(t, store)
		_, firstErr := api.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"secondary"})
		pendingSaveForTest(t, firstErr)
		synctest.Wait()
		restore()
		beforeInfo, err := os.Stat(store.documentPath())
		require.NoError(t, err)

		// Even after filesystem recovery, additional updates only advance memory.
		// A single scheduled retry saves the greatest cursor accepted per destination.
		const concurrentUpdates = 12
		results := make(chan error, concurrentUpdates)
		var callers sync.WaitGroup
		for index := range concurrentUpdates {
			callers.Add(1)
			go func() {
				defer callers.Done()
				key := CheckpointKey{Generation: fmt.Sprintf("20260915-%d", index+2), Compressed: true}
				snapshot, err := api.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
				if snapshot.Key != key || !snapshot.DestinationComplete("primary") {
					results <- fmt.Errorf("failed save did not return accepted checkpoint: %#v", snapshot)
					return
				}
				results <- err
			}()
		}
		callers.Wait()
		close(results)
		for err := range results {
			pendingSaveForTest(t, err)
		}
		_, err = api.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-10", Compressed: false}, []objectstorage.DestinationID{"secondary"})
		pendingSaveForTest(t, err)
		_, err = api.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: false}, []objectstorage.DestinationID{"secondary"})
		pendingSaveForTest(t, err)
		accepted := checkpointStateForTest(store)
		require.Equal(t, []DestinationCursor{
			{DestinationID: "primary", Generation: "20260915-13"},
			{DestinationID: "secondary", Generation: "20260915-10"},
		}, accepted.Cursors)
		persisted, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, before, persisted, "additional updates must not save while recovery is pending")
		afterInfo, err := os.Stat(store.documentPath())
		require.NoError(t, err)
		require.True(t, os.SameFile(beforeInfo, afterInfo))
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		persisted, err = store.readDocument()
		require.NoError(t, err)
		require.Equal(t, accepted, persisted)
	})
}

func TestFilesystemStoreFailedRecoveryReportsLatestErrorWithoutBlockingLoad(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		store := newCheckpointTestStore(t, "failed-save-recovery")
		restore := blockCheckpointReplacement(t, store)
		_, err := store.AdvanceCompletionCursor(context.Background(), CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pending := pendingSaveForTest(t, err)
		originalCause := errors.Unwrap(pending)
		for range 3 {
			previous := checkpointSaveErrorForTest(store)
			advanceCheckpointRecoveryForTest()
			latest := checkpointSaveErrorForTest(store)
			require.Error(t, latest)
			require.NotSame(t, previous, latest, "each failed recovery updates the last save error")
			checkpoint, loadErr := store.Load(context.Background(), CheckpointKey{Generation: "20260915-2", Compressed: true})
			require.NoError(t, loadErr)
			require.True(t, checkpoint.DestinationComplete("primary"))
			require.Same(t, latest, checkpointSaveErrorForTest(store), "Load does not clear persistence failure")
		}
		require.Same(t, originalCause, errors.Unwrap(pending), "previous callers retain their original failure")
		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
	})
}

func TestFilesystemStoreAdditionalUpdatesWaitForRecoveryThenResumeImmediateSave(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "separate-save-epochs")
		restore := blockCheckpointReplacement(t, store)
		_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		firstFailure := pendingSaveForTest(t, err)
		synctest.Wait()
		restore()
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"secondary"})
		pending := pendingSaveForTest(t, err)
		require.Same(t, errors.Unwrap(firstFailure), errors.Unwrap(pending))
		beforeRecovery, err := store.readDocument()
		require.NoError(t, err)
		require.Empty(t, beforeRecovery.Cursors, "a new update must not bypass pending save recovery")
		checkpoint, err := store.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, checkpoint.CompletedDestinationIDs, "primary", "secondary")
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		persisted, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, checkpointStateForTest(store), persisted)

		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-3", Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err, "new updates save immediately after recovery succeeds")
		persisted, err = store.readDocument()
		require.NoError(t, err)
		require.Equal(t, checkpointStateForTest(store), persisted)

		// Also start a fresh epoch after the previous worker has fully retired.
		restore = blockCheckpointReplacement(t, store)
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-4", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
	})
}

func TestFilesystemStoreCloseWaitsForRecoveryAndSavesLatestState(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "close-save-recovery")
		restore := blockCheckpointReplacement(t, store)
		_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		pendingSaveForTest(t, store.SaveUploadProgress(ctx, failedUploadProgressForTest("primary")))
		latest := checkpointStateForTest(store)
		closed := make(chan error, 2)
		for range 2 {
			go func() { closed <- store.Close() }()
		}
		for range 3 {
			advanceCheckpointRecoveryForTest()
			require.Empty(t, closed, "Close must wait while accepted state cannot be saved")
			require.Error(t, checkpointSaveErrorForTest(store))
			require.Equal(t, latest, checkpointStateForTest(store))
		}
		restore()
		advanceCheckpointRecoveryForTest()
		for range 2 {
			require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		}
		require.NoError(t, checkpointSaveErrorForTest(store))
		time.Sleep(time.Minute)
		synctest.Wait()
		persisted, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, latest, persisted, "Close must preserve the latest accepted state")
		require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose), "Close remains safe to call repeatedly")
	})
}
