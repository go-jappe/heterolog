//go:build !windows

package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestStorePublishesAfterExistingStagingPermissionsRestored(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.EnsureLayout(context.Background()))
	staging := store.Root().stagingDir()
	require.NoError(t, os.Chmod(staging, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(staging, directoryMode)) })
	probe, err := os.CreateTemp(staging, ".permission-probe-*")
	if err == nil {
		require.NoError(t, probe.Close())
		require.NoError(t, os.Remove(probe.Name()))
		t.Skip("current user or filesystem bypasses directory write permissions")
	}
	require.ErrorIs(t, err, os.ErrPermission)

	const contents = "source must survive"
	source := writeTestSource(t, t.TempDir(), "app_20260911_1.log.gz", contents)
	finalPath := filepath.Join(store.Root().ActiveDir(), filepath.Base(source))
	require.NoError(t, store.EnsureLayout(context.Background()))
	published, err := store.SaveRetryableFailure(context.Background(), source)
	require.ErrorIs(t, err, os.ErrPermission)
	require.Empty(t, published.Path)
	info, err := os.Stat(staging)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o500), info.Mode().Perm())
	_, err = os.Stat(finalPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	assertDirectoryEmpty(t, staging)
	data, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, contents, string(data))

	require.NoError(t, os.Chmod(staging, directoryMode))
	published, err = store.SaveRetryableFailure(context.Background(), source)
	require.NoError(t, err)
	require.Equal(t, finalPath, published.Path)
	data, err = os.ReadFile(published.Path)
	require.NoError(t, err)
	require.Equal(t, contents, string(data))
	assertDirectoryEmpty(t, staging)
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
				if err := store.EnsureLayout(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if err := fsstore.SyncDirectory(parent); err == nil {
				t.Skip("current user or filesystem bypasses directory read permissions")
			} else if !errors.Is(err, os.ErrPermission) {
				t.Fatal(err)
			}
			source := writeTestSource(t, t.TempDir(), "app_20260911_1.log.gz", "source must survive")
			finalPath := filepath.Join(store.Root().ActiveDir(), filepath.Base(source))
			published, err := store.SaveRetryableFailure(context.Background(), source)
			if !errors.Is(err, os.ErrPermission) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || published.Path != "" {
				t.Fatalf("initial publish = %+v, error = %v, want parent sync failure without publication", published, err)
			}
			if info, err := os.Stat(store.Root().Path()); err != nil || !info.IsDir() {
				t.Fatalf("failed initialization did not leave created root: %v", err)
			}
			if data, err := os.ReadFile(source); err != nil || string(data) != "source must survive" {
				t.Fatalf("source after failed publish = %q, error = %v", data, err)
			}
			if _, err := os.Stat(finalPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("file published despite initial layout sync failure: %v", err)
			}
			assertDirectoryEmpty(t, store.Root().ActiveDir())
			if otherStore {
				var err error
				store, err = NewStore(store.Root())
				if err != nil {
					t.Fatal(err)
				}
			}
			// 親の権限を直さず再試行します。前回作成したrootの親を同期できない間は、
			// 作成済みのlayoutでも永続化を確認できないため公開しません。
			for attempt := 0; attempt < 2; attempt++ {
				published, err = store.SaveRetryableFailure(context.Background(), source)
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
			assertDirectoryEmpty(t, store.Root().ActiveDir())
			if _, err := os.Stat(store.Root().stagingDir()); !errors.Is(err, os.ErrNotExist) {
				if err != nil {
					t.Fatal(err)
				}
				assertDirectoryEmpty(t, store.Root().stagingDir())
			}
			if data, err := os.ReadFile(source); err != nil || string(data) != "source must survive" {
				t.Fatalf("source after unconfirmed retries = %q, error = %v", data, err)
			}
			if err := fsstore.SyncDirectory(parent); !errors.Is(err, os.ErrPermission) {
				t.Fatalf("parent permissions changed during retry: %v", err)
			}
			// 親を同期できるようになれば、記録した同期を先に再試行してから公開します。
			if err := os.Chmod(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			published, err = store.SaveRetryableFailure(context.Background(), source)
			if err != nil || published.Path != finalPath {
				t.Fatalf("publish after parent sync recovery = %+v, error = %v", published, err)
			}
			for _, path := range []string{source, finalPath} {
				if data, err := os.ReadFile(path); err != nil || string(data) != "source must survive" {
					t.Fatalf("saved bytes at %q = %q, error = %v", path, data, err)
				}
			}
			assertDirectoryEmpty(t, store.Root().stagingDir())
			if err := store.EnsureLayout(context.Background()); err != nil {
				t.Fatalf("completed layout after recovery: %v", err)
			}
			restarted, err := NewStore(store.Root())
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.EnsureLayout(context.Background()); err != nil {
				t.Fatalf("restarted store layout after recovery: %v", err)
			}
		})
	}
}
