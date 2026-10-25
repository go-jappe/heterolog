package checkpoint

import (
	"context"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/stretchr/testify/require"
)

func TestFilesystemStoreManualSaveReturnsFailureWithoutRetryOrCloseWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := NewFilesystemStore(t.TempDir(), "manual-save", WithSaveRetryDisabled())
		require.NoError(t, err)
		ctx := context.Background()
		key := CheckpointKey{Generation: "20260915-1", Compressed: true}
		restore := blockCheckpointReplacement(t, store)
		_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
		pending := pendingSaveForTest(t, err)
		require.False(t, store.saveRecovery.running)
		loaded, err := store.Load(ctx, key)
		require.NoError(t, err)
		assertDestinationIDs(t, loaded.CompletedDestinationIDs, "primary")
		restore()
		before, err := os.ReadFile(store.documentPath())
		require.NoError(t, err)
		time.Sleep(2 * SaveRecoveryInterval)
		synctest.Wait()
		after, err := os.ReadFile(store.documentPath())
		require.NoError(t, err)
		require.Equal(t, before, after, "手動実行は待機後にも保存を再試行しない")
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose), pending.Err)
		boundedwait.ReceiveInBubble(t, store.Done(), "receive from store.Done()")
	})
}

func TestFilesystemStoreManualSavePersistsSuccessfulUpdates(t *testing.T) {
	store, err := NewFilesystemStore(t.TempDir(), "manual-save-success", WithSaveRetryDisabled())
	require.NoError(t, err)
	_, err = store.AdvanceCompletionCursor(context.Background(), CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	reopened, err := NewFilesystemStore(store.Directory(), store.SourceID(), WithSaveRetryDisabled())
	require.NoError(t, err)
	defer reopened.Close()
	loaded, err := reopened.Load(context.Background(), CheckpointKey{Generation: "20260915-1", Compressed: true})
	require.NoError(t, err)
	assertDestinationIDs(t, loaded.CompletedDestinationIDs, "primary")
}
