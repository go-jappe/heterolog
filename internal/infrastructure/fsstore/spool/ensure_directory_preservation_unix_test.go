//go:build unix

package spool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestDirectoryPreservationMovesFIFOWithoutOpeningIt(t *testing.T) {
	// 隔離処理は解決済みのrootを要求するため、一時ディレクトリの上位のリンクを解決します。
	root := resolvedTempDirForTest(t)
	path := filepath.Join(root, "active")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Skipf("FIFO unsupported: %v", err)
	}
	preserved, err := EnsureDirectoryWithPreservation(context.Background(), root, filepath.Join(path, "2026"))
	require.NoError(t, err)
	require.Len(t, preserved, 1)
	info, err := os.Lstat(preserved[0].RetainedPath)
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeNamedPipe)
	info, err = os.Lstat(path)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestDirectoryPreservationRetainsEntryWhenPermissionPreventsRename(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission restrictions")
	}
	root := resolvedTempDirForTest(t)
	parent := filepath.Join(root, "active")
	require.NoError(t, os.Mkdir(parent, 0o755))
	path := filepath.Join(parent, "2026")
	require.NoError(t, os.WriteFile(path, []byte("keep obstruction"), 0o644))
	require.NoError(t, os.Chmod(parent, 0o500))
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	preserved, err := EnsureDirectoryWithPreservation(context.Background(), root, filepath.Join(path, "10"))
	require.ErrorIs(t, err, os.ErrPermission)
	require.Empty(t, preserved)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "keep obstruction", string(body))
	entries, err := os.ReadDir(filepath.Join(root, ".preserved"))
	require.NoError(t, err)
	require.Empty(t, entries)
	// 権限を戻せば次の保存準備で復旧でき、元のエントリを失いません。
	require.NoError(t, os.Chmod(parent, 0o755))
	preserved, err = EnsureDirectoryWithPreservation(context.Background(), root, filepath.Join(path, "10"))
	require.NoError(t, err)
	require.Len(t, preserved, 1)
}
