package checkpoint

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreCursorAdvanceBeforeSaveRecoveryPersistsOnlyLatestState(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "delete-before-save-recovery")
		restore := blockCheckpointReplacement(t, store)
		checkpoint, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		require.True(t, checkpoint.Complete([]objectstorage.DestinationID{"primary"}))

		// A later accepted completion must replace, not accumulate, the cursor.
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260924-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		loaded, err := store.Load(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true})
		require.NoError(t, err)
		require.True(t, loaded.DestinationComplete("primary"))
		require.Equal(t, []DestinationCursor{{DestinationID: "primary", Generation: "20260924-2"}}, checkpointStateForTest(store).Cursors)
		synctest.Wait()
		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		persisted, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, checkpointStateForTest(store), persisted)
	})
}

func TestFilesystemStoreUnsavedResultsOwnTheirMemory(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "unsaved-result-copy")
		restore := blockCheckpointReplacement(t, store)
		defer restore()
		checkpoint, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		checkpoint.CompletedDestinationIDs[0] = "changed-result"
		loaded, err := store.Load(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, loaded.CompletedDestinationIDs, "primary")
		loaded.CompletedDestinationIDs[0] = "changed-load"
		unchanged, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		assertDestinationIDs(t, unchanged.CompletedDestinationIDs, "primary")
		missing, err := store.Load(ctx, CheckpointKey{Generation: "20260924-2", Compressed: true})
		require.NoError(t, err, "an unrelated file is not blocked by persistence failure")
		require.Empty(t, missing.CompletedDestinationIDs)
	})
}

func TestFilesystemStoreRestartRetainsLatestCursorAfterCloseDrainsSave(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const source = "restart-after-unsaved-delete"
		store := newCheckpointTestStore(t, source)
		_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		restore := blockCheckpointReplacement(t, store)
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260924-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		restore()
		require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose))

		// Closing waits until the latest accepted cursor is durable across restart.
		restarted, err := NewFilesystemStore(store.directory, source)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, boundedwait.CloseInBubble(t, "restarted.Close()", restarted.Close, restarted.ForceClose))
		}()
		loaded, err := restarted.Load(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, loaded.CompletedDestinationIDs, "primary")
		loaded, err = restarted.Load(ctx, CheckpointKey{Generation: "20260924-2", Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, loaded.CompletedDestinationIDs, "primary")
	})
}

func TestFilesystemStoreRestartKeepsCompletionForRemainingFileAfterClose(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const source = "restart-with-remaining-file"
		store := newCheckpointTestStore(t, source)
		restore := blockCheckpointReplacement(t, store)
		checkpoint, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		require.True(t, checkpoint.DestinationComplete("primary"))
		restore()
		require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose))
		restarted, err := NewFilesystemStore(store.directory, source)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, boundedwait.CloseInBubble(t, "restarted.Close()", restarted.Close, restarted.ForceClose))
		}()
		loaded, err := restarted.Load(ctx, CheckpointKey{Generation: "20260924-1", Compressed: true})
		require.NoError(t, err)
		require.True(t, loaded.DestinationComplete("primary"), "graceful shutdown must preserve accepted completion")
	})
}
