package compressedlogarchive

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestStorePreservesExistingDirectoryPermissions(t *testing.T) {
	for _, tc := range []struct {
		name         string
		deniedAccess uint32
		writeDenied  bool
	}{
		{name: "ACL changes denied", deniedAccess: windows.WRITE_DAC},
		{name: "file creation denied", deniedAccess: windows.FILE_WRITE_DATA, writeDenied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			require.NoError(t, store.EnsureDirectories(context.Background()))
			directory := store.Root().RetryableDir()
			restore := testutil.DenyWindowsAccess(t, directory, tc.deniedAccess)
			before, err := windows.GetNamedSecurityInfo(directory, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
			require.NoError(t, err)
			require.NoError(t, store.EnsureDirectories(context.Background()))

			contents := []byte("source must survive")
			source := writeTestCompressedLog(t, "app_20260911_1.log.gz", contents)
			input := testSaveInput(source, uploader.FailureCategoryRetryable)
			finalPath := filepath.Join(store.Root().RetryableDir(), filepath.Base(source))
			published, publishErr := store.Save(context.Background(), input)
			after, err := windows.GetNamedSecurityInfo(directory, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
			require.NoError(t, err)
			require.Equal(t, before.String(), after.String(), "existing directory ACL must remain unchanged")
			if tc.writeDenied {
				require.ErrorIs(t, publishErr, os.ErrPermission)
				// 書き込みを拒否した保存先への一時file作成で失敗します。
				var pathErr *os.PathError
				require.ErrorAs(t, publishErr, &pathErr)
				require.Equal(t, directory, filepath.Dir(pathErr.Path))
				require.Empty(t, published.Path)
				_, err = os.Stat(finalPath)
				require.ErrorIs(t, err, os.ErrNotExist)
				assertNoArchiveTemporaryFiles(t, store)
				assertFileBytes(t, source, contents)

				restore()
				published, publishErr = store.Save(context.Background(), input)
			}
			require.NoError(t, publishErr)
			require.Equal(t, finalPath, published.Path)
			assertFileBytes(t, source, contents)
			assertFileBytes(t, published.Path, contents)
			testutil.RequirePrivatePermissions(t, published.Path, archivedFileMode)
			assertNoArchiveTemporaryFiles(t, store)
		})
	}
}

func TestStorePublishesOnlyAfterPreviousLayoutParentSyncSucceeds(t *testing.T) {
	for _, otherStore := range []bool{false, true} {
		name := "same store"
		if otherStore {
			name = "other store"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			parent := filepath.Dir(store.Root().Path())
			require.NoError(t, os.MkdirAll(parent, 0o700))
			// assertionで中断してもACL復旧後に残りのdirectoryを作成できることを確認します。
			t.Cleanup(func() {
				require.NoError(t, store.EnsureDirectories(context.Background()))
			})
			// Leave child-directory creation permitted while denying one right
			// required by the GENERIC_WRITE handle used for synchronization.
			restore := testutil.DenyWindowsAccess(t, parent, windows.FILE_WRITE_EA)
			probeErr := fsstore.SyncDirectory(parent)
			require.ErrorIs(t, probeErr, windows.ERROR_ACCESS_DENIED)
			require.ErrorIs(t, probeErr, pkgfsstore.ErrDurabilityUnconfirmed)

			contents := []byte("source must survive")
			source := writeTestCompressedLog(t, "app_20260911_1.log.gz", contents)
			input := testSaveInput(source, uploader.FailureCategoryRetryable)
			finalPath := filepath.Join(store.Root().RetryableDir(), filepath.Base(source))
			published, err := store.Save(context.Background(), input)
			require.ErrorIs(t, err, windows.ERROR_ACCESS_DENIED)
			require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			var parentSyncErr *fsstore.DirectorySyncError
			require.ErrorAs(t, err, &parentSyncErr)
			require.Equal(t, pathidentity.ComparisonKey(parent), pathidentity.ComparisonKey(parentSyncErr.Directory))
			require.Empty(t, published.Path)
			require.DirExists(t, store.Root().Path())
			require.DirExists(t, store.Root().RetryableDir())
			testutil.RequirePrivatePermissions(t, store.Root().RetryableDir(), directoryMode)
			assertFileBytes(t, source, contents)
			_, err = os.Stat(finalPath)
			require.ErrorIs(t, err, os.ErrNotExist)
			assertDirectoryEmpty(t, store.Root().RetryableDir())
			if otherStore {
				store, err = NewStore(store.Root())
				require.NoError(t, err)
			}
			// 親のACLを直さず再試行します。前回作成したrootの親を同期できない間は、
			// 作成済みのdirectoryでも永続化を確認できないため公開しません。
			for attempt := 0; attempt < 2; attempt++ {
				published, err = store.Save(context.Background(), input)
				require.ErrorIs(t, err, windows.ERROR_ACCESS_DENIED)
				require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
				var syncErr *fsstore.DirectorySyncError
				require.ErrorAs(t, err, &syncErr)
				require.Equal(t, pathidentity.ComparisonKey(parent), pathidentity.ComparisonKey(syncErr.Directory))
				require.Empty(t, published.Path)
				_, err = os.Stat(finalPath)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			assertDirectoryEmpty(t, store.Root().RetryableDir())
			assertFileBytes(t, source, contents)
			// 失敗が続いたのは親のACLを変更していないためです。
			require.ErrorIs(t, fsstore.SyncDirectory(parent), windows.ERROR_ACCESS_DENIED)

			// ACLを戻すと、記録した親の同期を先に再試行してから公開します。
			restore()
			published, err = store.Save(context.Background(), input)
			require.NoError(t, err)
			require.Equal(t, finalPath, published.Path)
			require.Equal(t, uploader.FailureCategoryRetryable, published.FailureCategory)
			assertFileBytes(t, source, contents)
			assertFileBytes(t, published.Path, contents)
			assertNoArchiveTemporaryFiles(t, store)
			require.NoError(t, store.EnsureDirectories(context.Background()))
			restarted, err := NewStore(store.Root())
			require.NoError(t, err)
			require.NoError(t, restarted.EnsureDirectories(context.Background()))
		})
	}
}
