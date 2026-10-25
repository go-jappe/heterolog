//go:build !windows

package objectstoragespool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestStoreLayoutStaysGroupTraversableAfterLogSpoolCreatesSharedParent(t *testing.T) {
	// 一時ディレクトリ自身はos.MkdirTempの0700なので、その下にspoolが作る階層を比べます。
	base, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	const target = "app"
	// 通常ログのspoolが、object storageのspoolと共有する<Target>/spoolを先に作成します。
	logRoot, err := spool.NewRootDir(base, target, "opensearch")
	require.NoError(t, err)
	require.NoError(t, spool.EnsureDirectoryNoSymlinks(logRoot.String(), logRoot.ActiveDir().Path()))
	root, err := NewRoot(base, target, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.NoError(t, err)
	store, err := NewStore(root)
	require.NoError(t, err)
	require.NoError(t, store.EnsureLayout(context.Background()))

	// 共有する上位は以前と同じ0755、通常ログのspoolは0700、object storageのspoolは自身の
	// directoryModeで作成し、いずれもprocess umaskを適用します。
	sharedMode := createdDirectoryModeForTest(t, 0o755)
	storeMode := createdDirectoryModeForTest(t, directoryMode)
	shared := []string{filepath.Join(base, target), filepath.Join(base, target, spool.DirectoryName)}
	for _, path := range shared {
		requireDirectoryModeForTest(t, path, sharedMode)
	}
	for _, path := range []string{logRoot.String(), logRoot.ActiveDir().Path()} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Zero(t, info.Mode().Perm()&^0o700, "log spool directory %s mode %o", path, info.Mode().Perm())
	}
	objectStorageDirectory := filepath.Join(base, target, spool.DirectoryName, objectStorageDirectoryName)
	var storeDirectories []string
	for path := store.Root().ActiveDir(); ; path = filepath.Dir(path) {
		requireDirectoryModeForTest(t, path, storeMode)
		storeDirectories = append(storeDirectories, path)
		if path == objectStorageDirectory {
			break
		}
		require.NotEqual(t, path, filepath.Dir(path), "object storage spool is not below the shared spool directory")
	}
	// umaskがgroupの検索権限を残す場合、groupは共有の上位を通ってobject storageのspoolへ到達できます。
	if storeMode&0o010 != 0 {
		for _, path := range append(shared, storeDirectories...) {
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.NotZero(t, info.Mode().Perm()&0o010, "group cannot traverse %s (mode %o)", path, info.Mode().Perm())
		}
	}
}

func requireDirectoryModeForTest(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "%s is not a directory", path)
	require.Equal(t, want, info.Mode().Perm(), "mode of %s", path)
}
