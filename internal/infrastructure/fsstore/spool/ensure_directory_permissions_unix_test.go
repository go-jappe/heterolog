//go:build !windows

package spool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpoolDirectoryPreparationRestrictsExistingManagedDirectories(t *testing.T) {
	t.Parallel()
	base := resolvedTempDirForTest(t)
	root := filepath.Join(base, "spool-root")
	existing := filepath.Join(root, "active", "2026")
	require.NoError(t, os.MkdirAll(existing, 0o700))
	// 以前の版は0755で作成していました。umaskに依存しないよう、明示的に広げます。
	for _, path := range []string{base, root, filepath.Join(root, "active"), existing} {
		require.NoError(t, os.Chmod(path, 0o755))
	}
	partition := filepath.Join(existing, "10")
	require.NoError(t, EnsureDirectoryNoSymlinks(root, partition))
	// root自身から書込先までの既存directoryはgroup・otherの権限を外し、所有者の権限は維持します。
	for _, path := range []string{root, filepath.Join(root, "active"), existing} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, directoryMode, info.Mode().Perm(), "mode of %s", path)
	}
	requirePrivateSpoolDirectory(t, partition)
	// spool rootより上位は利用者の保存先のため変更しません。
	info, err := os.Stat(base)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}
