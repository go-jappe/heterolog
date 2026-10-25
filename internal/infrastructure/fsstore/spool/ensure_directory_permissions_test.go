package spool

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
)

// resolvedTempDirForTestは、macOSの/varなど一時ディレクトリの上位のリンクを解決したパスを返します。
// EnsureDirectoryWithPreservationは解決済みのrootを要求し、同期失敗の記録も解決済みのパスで返します。
func resolvedTempDirForTest(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return directory
}

// requirePrivateSpoolDirectoryは、spoolのdirectoryへ所有者以外がアクセスできないことを確認します。
// Unix系ではdirectoryModeを超える権限がないことを確かめ、umaskが追加で外したbitは許容します。
// WindowsのFileModeはACLを表さないため、実行ユーザー・SYSTEM・Administratorsだけを許可する
// 保護されたDACLであることを確認します。
func requirePrivateSpoolDirectory(t *testing.T, path string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		testutil.RequirePrivatePermissions(t, path, directoryMode)
		return
	}
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "%s is not a directory", path)
	require.Zero(t, info.Mode().Perm()&^directoryMode, "mode of %s = %o has bits beyond %o", path, info.Mode().Perm(), directoryMode)
}

func TestSpoolDirectoriesArePrivateFromCreation(t *testing.T) {
	t.Parallel()
	root := filepath.Join(resolvedTempDirForTest(t), "spool-root")
	partition := filepath.Join(root, "active", "2026", "10", "22", "13", "05")
	require.NoError(t, EnsureDirectoryNoSymlinks(root, partition))
	for path := partition; ; path = filepath.Dir(path) {
		requirePrivateSpoolDirectory(t, path)
		if path == root {
			break
		}
	}

	// 隔離先の.preservedも、spool rootの配下と同じく非公開で作成します。
	obstruction := filepath.Join(root, "done")
	require.NoError(t, os.WriteFile(obstruction, []byte("obstruction"), 0o600))
	preserved, err := EnsureDirectoryWithPreservation(context.Background(), root, filepath.Join(obstruction, "2026"))
	require.NoError(t, err)
	require.Len(t, preserved, 1)
	for _, path := range []string{
		filepath.Join(root, infrafsstore.PreservedDirectoryName),
		obstruction,
		filepath.Join(obstruction, "2026"),
	} {
		requirePrivateSpoolDirectory(t, path)
	}
}
