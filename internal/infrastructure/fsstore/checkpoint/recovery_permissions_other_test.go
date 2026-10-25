//go:build !windows

package checkpoint

import (
	"context"
	"io/fs"
	"os"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreRecoveryDoesNotReplaceUnreadableCheckpoint(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"successes", "registrations"} {
		t.Run(target, func(t *testing.T) {
			store, err := NewFilesystemStore(t.TempDir(), "source")
			require.NoError(t, err)
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			path := store.documentPath()
			if target == "registrations" {
				path = store.registrationsPath()
			}
			require.NoError(t, os.Chmod(path, 0))
			t.Cleanup(func() { require.NoError(t, os.Chmod(path, fileMode)) })
			if _, err := os.ReadFile(path); err == nil {
				t.Skip("process can bypass file permissions")
			}
			restarted, err := NewFilesystemStore(store.directory, "source")
			require.ErrorIs(t, err, fs.ErrPermission)
			require.Nil(t, restarted)
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.Zero(t, info.Mode().Perm(), "recovery must not replace an unreadable file")
		})
	}
}

func TestFilesystemStoreCloseWaitsForDirectoryPermissionsToRecover(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "close-permission-recovery")
		_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		require.NoError(t, os.Chmod(store.directory, 0o500))
		t.Cleanup(func() { require.NoError(t, os.Chmod(store.directory, 0o700)) })
		probe, probeErr := os.CreateTemp(store.directory, "permission-probe-")
		if probeErr == nil {
			require.NoError(t, probe.Close())
			require.NoError(t, os.Remove(probe.Name()))
			t.Skip("process can bypass directory permissions")
		}
		require.ErrorIs(t, probeErr, fs.ErrPermission)
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		require.ErrorIs(t, err, fs.ErrPermission)
		latest := checkpointStateForTest(store)
		closed := make(chan error, 1)
		go func() { closed <- store.Close() }()
		for range 3 {
			advanceCheckpointRecoveryForTest()
			require.Empty(t, closed, "Close waits while the filesystem remains unwritable")
			require.ErrorIs(t, checkpointSaveErrorForTest(store), fs.ErrPermission)
		}
		require.NoError(t, os.Chmod(store.directory, 0o700))
		advanceCheckpointRecoveryForTest()
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		persisted, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, latest, persisted)
	})
}
