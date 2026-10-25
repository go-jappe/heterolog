//go:build windows

package checkpoint

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreWindowsSharedACLHandleAllowsMultipleSources(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	const generation = "20260926-1"
	const destination = objectstorage.DestinationID("primary")
	for _, source := range []string{"default", "error"} {
		store, err := NewFilesystemStore(directory, source)
		require.NoError(t, err)
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: generation, Compressed: true}, []objectstorage.DestinationID{destination})
		require.NoError(t, err)
		require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	}
	// 別handleのaccess/shareを固定し、directoryを共有中の復元と新規作成を確認します。
	name, err := windows.UTF16PtrFromString(directory)
	require.NoError(t, err)
	handle, err := windows.CreateFile(name, windows.MAXIMUM_ALLOWED,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, windows.CloseHandle(handle)) })
	_, err = os.ReadDir(directory)
	require.ErrorIs(t, err, windows.ERROR_SHARING_VIOLATION)
	for _, source := range []string{"default", "error", "debug"} {
		store, err := NewFilesystemStore(directory, source)
		require.NoError(t, err)
		checkpoint, err := store.Load(ctx, CheckpointKey{Generation: generation, Compressed: true})
		require.NoError(t, err)
		require.Equal(t, source != "debug", checkpoint.DestinationComplete(destination))
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: generation, Compressed: false}, []objectstorage.DestinationID{destination})
		require.NoError(t, err)
		require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	}
}
