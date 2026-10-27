package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
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
			store := newTestStore(t)
			require.NoError(t, store.EnsureLayout(context.Background()))
			staging := store.Root().stagingDir()
			restore := testutil.DenyWindowsAccess(t, staging, tc.deniedAccess)
			before, err := windows.GetNamedSecurityInfo(staging, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
			require.NoError(t, err)
			require.NoError(t, store.EnsureLayout(context.Background()))

			const contents = "source must survive"
			source := writeTestSource(t, t.TempDir(), "app_20260911_1.log.gz", contents)
			finalPath := filepath.Join(store.Root().ActiveDir(), filepath.Base(source))
			published, publishErr := store.SaveRetryableFailure(context.Background(), source)
			after, err := windows.GetNamedSecurityInfo(staging, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
			require.NoError(t, err)
			require.Equal(t, before.String(), after.String(), "existing directory ACL must remain unchanged")
			if tc.writeDenied {
				require.ErrorIs(t, publishErr, os.ErrPermission)
				// 書き込みを拒否したstagingへの一時file作成で失敗します。
				var pathErr *os.PathError
				require.ErrorAs(t, publishErr, &pathErr)
				require.Equal(t, staging, filepath.Dir(pathErr.Path))
				require.Empty(t, published.Path)
				_, err = os.Stat(finalPath)
				require.ErrorIs(t, err, os.ErrNotExist)
				assertDirectoryEmpty(t, staging)
				data, err := os.ReadFile(source)
				require.NoError(t, err)
				require.Equal(t, contents, string(data))

				restore()
				published, publishErr = store.SaveRetryableFailure(context.Background(), source)
			}
			require.NoError(t, publishErr)
			require.Equal(t, finalPath, published.Path)
			for _, path := range []string{source, published.Path} {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, contents, string(data))
			}
			testutil.RequirePrivatePermissions(t, published.Path, payloadMode)
			assertDirectoryEmpty(t, staging)
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
			store := newTestStore(t)
			parent := filepath.Dir(store.Root().Path())
			require.NoError(t, os.MkdirAll(parent, 0o700))
			// assertionで中断してもACL復旧後に残りのlayoutを作成できることを確認します。
			t.Cleanup(func() {
				require.NoError(t, store.EnsureLayout(context.Background()))
			})
			// GENERIC_WRITEはFILE_WRITE_EAを含みますが、子の作成に必要なのは
			// FILE_ADD_SUBDIRECTORYだけです。mkdirを妨げずに同期を拒否します。
			restore := testutil.DenyWindowsAccess(t, parent, windows.FILE_WRITE_EA)
			probeErr := fsstore.SyncDirectory(parent)
			require.ErrorIs(t, probeErr, windows.ERROR_ACCESS_DENIED)
			require.ErrorIs(t, probeErr, pkgfsstore.ErrDurabilityUnconfirmed)

			const contents = "source must survive"
			source := writeTestSource(t, t.TempDir(), "app_20260911_1.log.gz", contents)
			finalPath := filepath.Join(store.Root().ActiveDir(), filepath.Base(source))
			published, err := store.SaveRetryableFailure(context.Background(), source)
			require.ErrorIs(t, err, windows.ERROR_ACCESS_DENIED)
			require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			var parentSyncErr *fsstore.DirectorySyncError
			require.ErrorAs(t, err, &parentSyncErr)
			require.Equal(t, pathidentity.ComparisonKey(parent), pathidentity.ComparisonKey(parentSyncErr.Directory))
			require.Empty(t, published.Path)
			require.DirExists(t, store.Root().Path())
			require.DirExists(t, store.Root().ActiveDir())
			testutil.RequirePrivatePermissions(t, store.Root().ActiveDir(), directoryMode)
			data, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, contents, string(data))
			_, err = os.Stat(finalPath)
			require.ErrorIs(t, err, os.ErrNotExist)
			assertDirectoryEmpty(t, store.Root().ActiveDir())
			if _, err := os.Stat(store.Root().stagingDir()); !errors.Is(err, os.ErrNotExist) {
				require.NoError(t, err)
				assertDirectoryEmpty(t, store.Root().stagingDir())
			}
			if otherStore {
				store, err = NewStore(store.Root())
				require.NoError(t, err)
			}
			// 親のACLを直さず再試行します。前回作成したrootの親を同期できない間は、
			// 作成済みのlayoutでも永続化を確認できないため公開しません。
			for attempt := 0; attempt < 2; attempt++ {
				published, err = store.SaveRetryableFailure(context.Background(), source)
				require.ErrorIs(t, err, windows.ERROR_ACCESS_DENIED)
				require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
				var syncErr *fsstore.DirectorySyncError
				require.ErrorAs(t, err, &syncErr)
				require.Equal(t, pathidentity.ComparisonKey(parent), pathidentity.ComparisonKey(syncErr.Directory))
				require.Empty(t, published.Path)
				_, err = os.Stat(finalPath)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			assertDirectoryEmpty(t, store.Root().ActiveDir())
			data, err = os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, contents, string(data))
			// 失敗が続いたのは親のACLを変更していないためです。
			require.ErrorIs(t, fsstore.SyncDirectory(parent), windows.ERROR_ACCESS_DENIED)

			// ACLを戻すと、記録した親の同期を先に再試行してから公開します。
			restore()
			published, err = store.SaveRetryableFailure(context.Background(), source)
			require.NoError(t, err)
			require.Equal(t, finalPath, published.Path)
			for _, path := range []string{source, published.Path} {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, contents, string(data))
			}
			assertDirectoryEmpty(t, store.Root().stagingDir())
			require.NoError(t, store.EnsureLayout(context.Background()))
			restarted, err := NewStore(store.Root())
			require.NoError(t, err)
			require.NoError(t, restarted.EnsureLayout(context.Background()))
		})
	}
}
