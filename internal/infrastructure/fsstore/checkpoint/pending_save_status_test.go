package checkpoint

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/stretchr/testify/require"
)

func TestFilesystemStorePendingSaveStatusDistinguishesMemoryFromPersistence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store, err := NewFilesystemStore(t.TempDir(), "save-status")
		require.NoError(t, err)
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "store.Close()", store.Close, store.ForceClose, boundedwait.NoError)
		require.NoError(t, store.PendingSaveError(ctx))
		restore := blockCheckpointReplacement(t, store)
		key := CheckpointKey{Generation: "20261011-1", Compressed: true}
		_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
		pending := pendingSaveForTest(t, err)
		require.ErrorIs(t, store.PendingSaveError(ctx), pending.Err)
		loaded, err := store.Load(ctx, key)
		require.NoError(t, err)
		assertDestinationIDs(t, loaded.CompletedDestinationIDs, "primary")
		restore()
		// 読み取りで保存は再試行せず、既存のSaveRecoveryが保存した後だけnilになります。
		require.ErrorIs(t, store.PendingSaveError(ctx), pending.Err)
		time.Sleep(SaveRecoveryInterval)
		synctest.Wait()
		require.NoError(t, store.PendingSaveError(ctx))
		require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose))
	})
}

func TestFilesystemStorePendingSaveStatusHonorsCancellation(t *testing.T) {
	store, err := NewFilesystemStore(t.TempDir(), "cancel-save-status")
	require.NoError(t, err)
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, store.PendingSaveError(ctx), context.Canceled)
	var nilStore *FilesystemStore
	require.ErrorIs(t, nilStore.PendingSaveError(context.Background()), ErrInvalidDirectory)
}
