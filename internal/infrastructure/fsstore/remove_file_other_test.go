//go:build !windows

package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestRemoveFileAndSyncDirectoryStopsAtRemovalFailure(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "app_20260919_1.log.gz")
	if err := os.WriteFile(path, []byte("keep this compressed log"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Error(err)
		}
	})
	if err := os.Chmod(directory, 0); err != nil {
		t.Fatal(err)
	}
	// 削除も親の同期も失敗する権限にして、削除エラーだけが返ることを確認します。
	if probe, err := os.Open(directory); err == nil {
		_ = probe.Close()
		t.Skip("current user or filesystem bypasses directory permissions")
	} else if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("permission fixture open error = %v, want permission error", err)
	}
	err := RemoveFileAndSyncDirectory(path)
	if !errors.Is(err, os.ErrPermission) || errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("RemoveFileAndSyncDirectory() error = %v, want removal error without sync error", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if contents, err := os.ReadFile(path); err != nil || string(contents) != "keep this compressed log" {
		t.Fatalf("source file contents = %q, error = %v", contents, err)
	}
}

func TestRemoveFileAndSyncDirectoryRetriesSyncAfterFileWasRemoved(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "app_20260919_1.log.gz")
	if err := os.WriteFile(path, []byte("compressed log"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Error(err)
		}
	})
	// 書込・検索権限で削除は可能ですが、読取権限がないため同期用openは失敗します。
	if err := os.Chmod(directory, 0o300); err != nil {
		t.Fatal(err)
	}
	if probe, err := os.Open(directory); err == nil {
		_ = probe.Close()
		t.Skip("current user or filesystem bypasses directory read permissions")
	} else if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("permission fixture open error = %v, want permission error", err)
	}
	err := RemoveFileAndSyncDirectory(path)
	if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("file was not removed before sync: error = %v", statErr)
	}
	if !errors.Is(err, os.ErrPermission) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("RemoveFileAndSyncDirectory() error = %v, want permission error and unconfirmed sync", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	// ファイルが既に消えていても同期を省略せず、権限復旧後の再試行を完了できます。
	if err := RemoveFileAndSyncDirectory(path); err != nil {
		t.Fatalf("retry after restoring permissions: %v", err)
	}
}
