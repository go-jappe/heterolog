//go:build !windows

package fsstore_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// inlineとrunnerは同じStoreを使うため、保存先準備の契約は共通Storeで検証します。
func TestCheckpointCreationDoesNotOpenExistingExecuteOnlyAncestors(t *testing.T) {
	upper := filepath.Join(t.TempDir(), "execute-only")
	writable := filepath.Join(upper, "writable")
	require.NoError(t, os.MkdirAll(writable, 0o700))
	require.NoError(t, os.Chmod(upper, 0o111))
	t.Cleanup(func() { require.NoError(t, os.Chmod(upper, 0o700)) })
	// 上位はtraverseだけ可能です。保存先の作成・保存で既存の上位を同期しません。
	err := fsstore.SyncDirectory(upper)
	if err == nil {
		t.Skip("current user or filesystem bypasses directory read permissions")
	}
	require.ErrorIs(t, err, os.ErrPermission)
	directory := filepath.Join(writable, "state", "checkpoints")
	require.NoError(t, checkpointstore.PrepareDirectory(directory))
	store, err := checkpointstore.NewFilesystemStore(directory, "execute-only-parent")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, err = store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20260911-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}

func TestCheckpointPreparationCanBeRetriedAfterInitialParentSyncFailure(t *testing.T) {
	for _, restoreParent := range []bool{false, true} {
		name := "retries while parent remains unsyncable"
		if restoreParent {
			name = "parent permissions restored before retry"
		}
		t.Run(name, func(t *testing.T) {
			// 準備は解決済みのパスで同期失敗を返すため、一時ディレクトリの上位のリンクを解決します。
			base, resolveErr := filepath.EvalSymlinks(t.TempDir())
			require.NoError(t, resolveErr)
			parent := filepath.Join(base, "write-only-parent")
			require.NoError(t, os.Mkdir(parent, 0o700))
			require.NoError(t, os.Chmod(parent, 0o300))
			t.Cleanup(func() { require.NoError(t, os.Chmod(parent, 0o700)) })
			if err := fsstore.SyncDirectory(parent); err == nil {
				t.Skip("current user or filesystem bypasses directory read permissions")
			} else {
				require.ErrorIs(t, err, os.ErrPermission)
			}
			directory := filepath.Join(parent, "checkpoints")
			const sourceID = "parent-sync-failure"
			const destination objectstorage.DestinationID = "primary"
			// mkdirは成功しますが、親を開いて同期できないため準備は失敗します。
			err := checkpointstore.PrepareDirectory(directory)
			require.ErrorIs(t, err, os.ErrPermission)
			require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			require.DirExists(t, directory)
			entries, err := os.ReadDir(directory)
			require.NoError(t, err)
			require.Empty(t, entries, "Store構築前なのでcheckpointは存在しない")
			if !restoreParent {
				// 保存先が作成済みでも、記録した親を同期できない間は再準備も失敗し続けます。
				for attempt := 0; attempt < 2; attempt++ {
					err := checkpointstore.PrepareDirectory(directory)
					require.ErrorIs(t, err, os.ErrPermission)
					require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
					var syncErr *fsstore.DirectorySyncError
					require.ErrorAs(t, err, &syncErr)
					require.Equal(t, parent, syncErr.Directory)
				}
				entries, err := os.ReadDir(directory)
				require.NoError(t, err)
				require.Empty(t, entries, "失敗した準備はcheckpointを作らない")
				require.ErrorIs(t, fsstore.SyncDirectory(parent), os.ErrPermission, "親の権限を暗黙に変更していない")
			}
			require.NoError(t, os.Chmod(parent, 0o700))
			// 再準備は記録した親の同期を先に再試行し、成功してから保存先を使える状態にします。
			require.NoError(t, checkpointstore.PrepareDirectory(directory))
			store, err := checkpointstore.NewFilesystemStore(directory, sourceID)
			require.NoError(t, err)
			_, err = store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20260911-1", Compressed: true}, []objectstorage.DestinationID{destination})
			require.NoError(t, err)
			require.NoError(t, store.Close())
			// 元Storeを終了し、別Storeの初回読込で保存済み内容を確認します。
			reopened, err := checkpointstore.NewFilesystemStore(directory, sourceID)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			got, err := reopened.Load(context.Background(), checkpointstore.CheckpointKey{Generation: "20260911-1", Compressed: true})
			require.NoError(t, err)
			require.Equal(t, []objectstorage.DestinationID{destination}, got.CompletedDestinationIDs)
		})
	}
}

func TestCheckpointPreparationCanBeRetriedAfterParentCreationPermissionsRestored(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "read-only-parent")
	require.NoError(t, os.Mkdir(parent, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(parent, 0o700)) })
	probe := filepath.Join(parent, "permission-probe")
	if err := os.Mkdir(probe, 0o700); err == nil {
		require.NoError(t, os.Remove(probe))
		t.Skip("current user or filesystem bypasses directory write permissions")
	} else {
		require.ErrorIs(t, err, os.ErrPermission)
	}
	directory := filepath.Join(parent, "checkpoints")
	require.ErrorIs(t, checkpointstore.PrepareDirectory(directory), os.ErrPermission)
	_, err := os.Stat(directory)
	require.ErrorIs(t, err, os.ErrNotExist)
	info, err := os.Stat(parent)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o500), info.Mode().Perm())
	require.NoError(t, os.Chmod(parent, 0o700))
	require.NoError(t, checkpointstore.PrepareDirectory(directory))
	store, err := checkpointstore.NewFilesystemStore(directory, "parent-creation-failure")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, err = store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20260911-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}
