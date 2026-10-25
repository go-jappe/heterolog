package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreRetainsAcceptedStateAcrossRenameFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "pending-state-source")
		_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		restore := blockCheckpointReplacement(t, store)
		destinations := []objectstorage.DestinationID{"secondary"}
		checkpoint, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, destinations)
		pendingSaveForTest(t, err)
		// 保存待ちの原因は、checkpoint文書の公開(rename)の失敗です。
		var publishErr *os.LinkError
		require.ErrorAs(t, err, &publishErr)
		require.Equal(t, store.documentPath(), publishErr.New)
		assertDestinationIDs(t, checkpoint.CompletedDestinationIDs, "primary", "secondary")
		accepted := checkpointStateForTest(store)
		require.True(t, checkpoint.Complete([]objectstorage.DestinationID{"primary", "secondary"}))
		wantCheckpoint := cloneCheckpoint(checkpoint)
		destinations[0] = "changed"
		require.Equal(t, accepted, checkpointStateForTest(store), "accepted state owns its destination slices")
		for range 3 {
			checkpoint, err = store.Load(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true})
			require.NoError(t, err)
			require.Equal(t, wantCheckpoint, checkpoint)
			require.Equal(t, accepted, checkpointStateForTest(store))
		}
		synctest.Wait()
		entries, err := os.ReadDir(store.directory)
		require.NoError(t, err)
		require.Len(t, entries, 2, "only the replacement blocker and destination registrations remain")
		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		require.Equal(t, accepted, checkpointStateForTest(store))
		restarted, err := NewFilesystemStore(store.directory, "pending-state-source")
		require.NoError(t, err)
		loaded, err := restarted.Load(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true})
		require.NoError(t, err)
		require.Equal(t, wantCheckpoint, loaded)
	})
}

func TestFilesystemStoreDoesNotRecreateRemovedDirectoryWhenSaving(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "directory-failure-source")
		require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{"original"}, nil))
		registrations, err := os.ReadFile(store.registrationsPath())
		require.NoError(t, err)
		require.NoError(t, os.Remove(store.documentPath()))
		require.NoError(t, os.Remove(store.registrationsPath()))
		require.NoError(t, os.Remove(store.directory))
		checkpoint, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"original"})
		pendingSaveForTest(t, err)
		require.ErrorIs(t, err, os.ErrNotExist)
		assertDestinationIDs(t, checkpoint.CompletedDestinationIDs, "original")
		accepted := checkpointStateForTest(store)
		require.Len(t, accepted.Cursors, 1)
		advanceCheckpointRecoveryForTest()
		require.NoDirExists(t, store.directory, "save recovery must not initialize directories at runtime")
		require.Error(t, checkpointSaveErrorForTest(store))
		checkpoint, err = store.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, checkpoint.CompletedDestinationIDs, "original")
		require.NoError(t, os.Mkdir(store.directory, 0o700))
		require.NoError(t, os.WriteFile(store.registrationsPath(), registrations, fileMode))
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		restarted, err := NewFilesystemStore(store.directory, "directory-failure-source")
		require.NoError(t, err)
		loaded, err := restarted.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
		require.NoError(t, err)
		require.Equal(t, checkpoint, loaded)
	})
}

func TestFilesystemStorePublishedContentsDoNotCompletePendingSave(t *testing.T) {
	t.Parallel()
	for _, published := range []bool{false, true} {
		name := "before rename"
		if published {
			name = "after rename before directory sync"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				store := newCheckpointTestStore(t, "pending-directory-sync")
				_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
				require.NoError(t, err)
				restore := blockCheckpointReplacement(t, store)
				_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"secondary"})
				pendingSaveForTest(t, err)
				latest := checkpointStateForTest(store)
				synctest.Wait()
				restore()
				if published {
					// Simulate successful rename followed by a failed directory sync.
					payload, err := json.Marshal(latest)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(store.documentPath(), payload, fileMode))
				}
				checkpoint, err := store.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
				require.NoError(t, err)
				require.True(t, checkpoint.Complete([]objectstorage.DestinationID{"primary", "secondary"}))
				require.Error(t, checkpointSaveErrorForTest(store), "file contents alone do not establish durable completion")
				advanceCheckpointRecoveryForTest()
				require.NoError(t, checkpointSaveErrorForTest(store))
				checkpoint, err = store.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
				require.NoError(t, err)
				require.True(t, checkpoint.Complete([]objectstorage.DestinationID{"primary", "secondary"}))
				persisted, err := store.readDocument()
				require.NoError(t, err)
				require.Equal(t, latest, persisted)
			})
		})
	}
}

func TestFilesystemStoreRejectsInvalidInputWithoutChangingCanonicalState(t *testing.T) {
	t.Parallel()
	for _, pending := range []bool{false, true} {
		name := "saved"
		if pending {
			name = "unsaved"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				store := newCheckpointTestStore(t, "invalid-state-source")
				_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-4", Compressed: true}, []objectstorage.DestinationID{"primary"})
				require.NoError(t, err)
				if pending {
					restore := blockCheckpointReplacement(t, store)
					_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-4", Compressed: true}, []objectstorage.DestinationID{"secondary"})
					pendingSaveForTest(t, err)
					synctest.Wait()
					restore()
				}
				before := checkpointStateForTest(store)
				saveErr := checkpointSaveErrorForTest(store)
				payload, err := os.ReadFile(store.documentPath())
				require.NoError(t, err)
				_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: " ", Compressed: true}, []objectstorage.DestinationID{"primary"})
				require.ErrorIs(t, err, ErrInvalidGroupKey)
				var accepted *PendingSaveError
				require.False(t, errors.As(err, &accepted), "invalid state was not accepted")
				require.Equal(t, before, checkpointStateForTest(store))
				require.Equal(t, saveErr, checkpointSaveErrorForTest(store))
				after, err := os.ReadFile(store.documentPath())
				require.NoError(t, err)
				require.Equal(t, payload, after)
			})
		})
	}
}
