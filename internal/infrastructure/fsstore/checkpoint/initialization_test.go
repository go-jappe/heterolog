package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreInitializationDistinguishesNewAndMissingState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, recoverMissing := range []bool{false, true} {
		name := "strict"
		if recoverMissing {
			name = "recover"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := NewFilesystemStore(directory, "source", WithMissingCheckpointRecovery(recoverMissing))
			require.NoError(t, err, "a first initialization is allowed in either mode")
			require.FileExists(t, store.documentPath())
			require.FileExists(t, store.registrationsPath())
			_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260926-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
			require.NoError(t, err)
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			require.NoError(t, os.Remove(store.documentPath()))

			restarted, err := NewFilesystemStore(directory, "source", WithMissingCheckpointRecovery(recoverMissing))
			if !recoverMissing {
				require.ErrorIs(t, err, ErrRegisteredCheckpointMissing)
				require.Nil(t, restarted)
				require.NoFileExists(t, store.documentPath())
				return
			}
			require.NoError(t, err)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted), boundedwait.NoError)
			checkpoint, err := restarted.Load(ctx, CheckpointKey{Generation: "20260926-1", Compressed: true})
			require.NoError(t, err)
			require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.PendingDestinationIDs([]objectstorage.DestinationID{"primary"}))
			strictRestart, err := NewFilesystemStore(directory, "source", WithMissingCheckpointRecovery(false))
			require.NoError(t, err, "recovery publishes a durable empty document")
			require.NoError(t, boundedwait.Close(t, "strictRestart.Close()", strictRestart.Close, boundedwait.ForceCloserOf(strictRestart)))
		})
	}
}

func TestFilesystemStoreDefaultRecoveryDoesNotChangeOtherSource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	first, err := NewFilesystemStore(directory, "first")
	require.NoError(t, err)
	second, err := NewFilesystemStore(directory, "second")
	require.NoError(t, err)
	_, err = second.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260926-1", Compressed: true}, []objectstorage.DestinationID{"secondary"})
	require.NoError(t, err)
	before, err := os.ReadFile(second.documentPath())
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "first.Close()", first.Close, boundedwait.ForceCloserOf(first)))
	require.NoError(t, boundedwait.Close(t, "second.Close()", second.Close, boundedwait.ForceCloserOf(second)))
	require.NoError(t, os.Remove(first.documentPath()))
	recovered, err := NewFilesystemStore(directory, "first")
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "recovered.Close()", recovered.Close, boundedwait.ForceCloserOf(recovered)))
	after, err := os.ReadFile(second.documentPath())
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestFilesystemStoreRejectsMissingRegistrationEvenWithEmptySuccesses(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	store, err := NewFilesystemStore(directory, "source")
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	// 空の完了cursorでも、登録済み宛先の開始境界が失われた可能性を否定できません。
	require.NoError(t, os.Remove(store.registrationsPath()))
	restarted, err := NewFilesystemStore(directory, "source", WithMissingCheckpointRecovery(false))
	require.ErrorIs(t, err, ErrRegisteredDestinationsMissing)
	require.Nil(t, restarted)
	require.NoFileExists(t, store.registrationsPath())
}

func TestFilesystemStoreNeverRecoversCorruptionOrInvalidPaths(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"document", "registrations"} {
		for _, kind := range []string{"corrupt", "directory", "dangling symlink"} {
			t.Run(target+"/"+kind, func(t *testing.T) {
				store, err := NewFilesystemStore(t.TempDir(), "source")
				require.NoError(t, err)
				require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
				path := store.documentPath()
				if target == "registrations" {
					path = store.registrationsPath()
				}
				require.NoError(t, os.Remove(path))
				switch kind {
				case "corrupt":
					require.NoError(t, os.WriteFile(path, []byte("{broken"), fileMode))
				case "directory":
					require.NoError(t, os.Mkdir(path, 0o700))
				case "dangling symlink":
					testutil.Symlink(t, filepath.Join(t.TempDir(), "missing"), path)
				}
				before, err := os.Lstat(path)
				require.NoError(t, err)
				restarted, err := NewFilesystemStore(store.directory, "source")
				require.ErrorIs(t, err, ErrCorruptCheckpoint)
				require.Nil(t, restarted)
				after, err := os.Lstat(path)
				require.NoError(t, err)
				require.True(t, os.SameFile(before, after), "invalid state must not be replaced")
			})
		}
	}
}

func TestFilesystemStoreRetainsGenerationCursorAfterFilesDisappear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "source")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
	const live = "20260926-1"
	const removed = "20260926-2"
	logDirectory := t.TempDir()
	for _, key := range []CheckpointKey{
		{Generation: live, Compressed: true}, {Generation: live, Compressed: false},
		{Generation: removed, Compressed: true}, {Generation: removed, Compressed: false},
	} {
		_, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
	}
	removedPath := filepath.Join(logDirectory, "app_20260926_2.log.gz")
	require.NoError(t, os.WriteFile(removedPath, []byte("uploaded content"), fileMode))
	require.NoError(t, os.Remove(removedPath))
	for _, key := range []CheckpointKey{{Generation: live, Compressed: true}, {Generation: live, Compressed: false}} {
		checkpoint, err := store.Load(ctx, key)
		require.NoError(t, err)
		require.True(t, checkpoint.DestinationComplete("primary"))
	}
	for _, key := range []CheckpointKey{{Generation: removed, Compressed: true}, {Generation: removed, Compressed: false}} {
		checkpoint, err := store.Load(ctx, key)
		require.NoError(t, err)
		require.True(t, checkpoint.DestinationComplete("primary"))
	}
	require.Len(t, store.state.Cursors, 1, "retention is bounded by destinations, not historical generations")
	_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260926-3", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	nextRaw, err := store.Load(ctx, CheckpointKey{Generation: "20260926-3", Compressed: false})
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, nextRaw.CompletedDestinationIDs, "raw and compressed share the same completed generation")
	raw, err := store.Load(ctx, CheckpointKey{Generation: live, Compressed: false})
	require.NoError(t, err)
	require.True(t, raw.DestinationComplete("primary"), "completed generations remain complete across representation changes")
}
