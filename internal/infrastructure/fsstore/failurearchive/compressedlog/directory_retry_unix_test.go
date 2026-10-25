//go:build !windows

package compressedlogarchive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestStorePublishesAfterExistingDestinationPermissionsRestored(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	require.NoError(t, store.EnsureDirectories(context.Background()))
	directory := store.Root().RetryableDir()
	require.NoError(t, os.Chmod(directory, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(directory, directoryMode)) })
	probe, err := os.CreateTemp(directory, ".permission-probe-*")
	if err == nil {
		require.NoError(t, probe.Close())
		require.NoError(t, os.Remove(probe.Name()))
		t.Skip("current user or filesystem bypasses directory write permissions")
	}
	require.ErrorIs(t, err, os.ErrPermission)

	contents := []byte("source must survive")
	source := writeTestCompressedLog(t, "app_20260911_1.log.gz", contents)
	input := testSaveInput(source, uploader.FailureCategoryRetryable)
	finalPath := filepath.Join(store.Root().RetryableDir(), filepath.Base(source))
	require.NoError(t, store.EnsureDirectories(context.Background()))
	published, err := store.Save(context.Background(), input)
	require.ErrorIs(t, err, os.ErrPermission)
	require.Empty(t, published.Path)
	info, err := os.Stat(directory)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o500), info.Mode().Perm())
	_, err = os.Stat(finalPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	assertNoArchiveTemporaryFiles(t, store)
	assertFileBytes(t, source, contents)

	require.NoError(t, os.Chmod(directory, directoryMode))
	published, err = store.Save(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, finalPath, published.Path)
	assertFileBytes(t, published.Path, contents)
	assertNoArchiveTemporaryFiles(t, store)
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
			if err := os.MkdirAll(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(parent, 0o300); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.Chmod(parent, 0o700); err != nil {
					t.Error(err)
				}
				if err := store.EnsureDirectories(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if err := fsstore.SyncDirectory(parent); err == nil {
				t.Skip("current user or filesystem bypasses directory read permissions")
			} else if !errors.Is(err, os.ErrPermission) {
				t.Fatal(err)
			}
			source := writeTestCompressedLog(t, "app_20260911_1.log.gz", []byte("source must survive"))
			input := testSaveInput(source, uploader.FailureCategoryRetryable)
			finalPath := filepath.Join(store.Root().RetryableDir(), filepath.Base(source))
			published, err := store.Save(context.Background(), input)
			if !errors.Is(err, os.ErrPermission) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || published.Path != "" {
				t.Fatalf("initial publish = %+v, error = %v, want parent sync failure without publication", published, err)
			}
			if info, err := os.Stat(store.Root().Path()); err != nil || !info.IsDir() {
				t.Fatalf("failed initialization did not leave created root: %v", err)
			}
			assertFileBytes(t, source, []byte("source must survive"))
			if _, err := os.Stat(finalPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("file published despite initial layout sync failure: %v", err)
			}
			assertDirectoryEmpty(t, store.Root().RetryableDir())
			if otherStore {
				var err error
				store, err = NewStore(store.Root())
				if err != nil {
					t.Fatal(err)
				}
			}
			// 親の権限を直さず再試行します。前回作成したrootの親を同期できない間は、
			// 作成済みのdirectoryでも永続化を確認できないため公開しません。
			for attempt := 0; attempt < 2; attempt++ {
				published, err = store.Save(context.Background(), input)
				var syncErr *fsstore.DirectorySyncError
				if !errors.Is(err, os.ErrPermission) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || !errors.As(err, &syncErr) || published.Path != "" {
					t.Fatalf("retry %d while parent is unsyncable = %+v, error = %v, want unconfirmed parent sync without publication", attempt, published, err)
				}
				if syncErr.Directory != parent {
					t.Fatalf("retry %d unconfirmed parent = %q, want %q", attempt, syncErr.Directory, parent)
				}
				if _, err := os.Stat(finalPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("file published while parent sync is unconfirmed: %v", err)
				}
			}
			assertDirectoryEmpty(t, store.Root().RetryableDir())
			assertFileBytes(t, source, []byte("source must survive"))
			if err := fsstore.SyncDirectory(parent); !errors.Is(err, os.ErrPermission) {
				t.Fatalf("parent permissions changed during retry: %v", err)
			}
			// 親を同期できるようになれば、記録した同期を先に再試行してから公開します。
			if err := os.Chmod(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			published, err = store.Save(context.Background(), input)
			if err != nil || published.Path != finalPath || published.FailureCategory != uploader.FailureCategoryRetryable {
				t.Fatalf("publish after parent sync recovery = %+v, error = %v", published, err)
			}
			assertFileBytes(t, source, []byte("source must survive"))
			assertFileBytes(t, finalPath, []byte("source must survive"))
			assertNoArchiveTemporaryFiles(t, store)
			if err := store.EnsureDirectories(context.Background()); err != nil {
				t.Fatalf("completed layout after recovery: %v", err)
			}
			restarted, err := NewStore(store.Root())
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.EnsureDirectories(context.Background()); err != nil {
				t.Fatalf("restarted store directories after recovery: %v", err)
			}
		})
	}
}
