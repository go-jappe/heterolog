package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestRemoveFileAndSyncDirectoryRemovesFileAndConfirmsAbsence(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "app_20260919_1.log.gz")
	otherPath := filepath.Join(directory, "app_20260919_2.log.gz")
	for _, filePath := range []string{path, otherPath} {
		if err := os.WriteFile(filePath, []byte("compressed log"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// 最初は削除と同期、以後は不存在を確認して親の同期を繰り返せます。
	for attempt := range 3 {
		if err := RemoveFileAndSyncDirectory(path); err != nil {
			t.Fatalf("attempt %d: RemoveFileAndSyncDirectory() error = %v", attempt, err)
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("attempt %d: removed path error = %v, want os.ErrNotExist", attempt, err)
		}
	}
	if contents, err := os.ReadFile(otherPath); err != nil || string(contents) != "compressed log" {
		t.Fatalf("unrelated file contents = %q, error = %v", contents, err)
	}
}

func TestRemoveFileAndSyncDirectoryAcceptsAlreadyMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.log.gz")
	for attempt := range 2 {
		if err := RemoveFileAndSyncDirectory(path); err != nil {
			t.Fatalf("attempt %d: RemoveFileAndSyncDirectory() error = %v", attempt, err)
		}
	}
}

func TestRemoveFileAndSyncDirectoryRejectsMissingParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "app_20260919_1.log.gz")
	err := RemoveFileAndSyncDirectory(path)
	if !errors.Is(err, os.ErrNotExist) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("RemoveFileAndSyncDirectory() error = %v, want missing parent and unconfirmed sync", err)
	}
	requireFileRemovalStep(t, err, FileRemovalStepSyncAfterRemove, path)
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing parent was created: error = %v", err)
	}
}

func TestRemoveFileAndSyncDirectoryPreservesFileOnRemovalFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260919_1.log.gz")
	if err := os.WriteFile(path, []byte("keep this compressed log"), 0o600); err != nil {
		t.Fatal(err)
	}
	restore := testutil.DenyFileRemoval(t, path)
	defer restore()

	// 権限拒否が働くことをOS操作で先に確認します。試験対象が誤って成功を
	// 返した場合を、環境依存のskipとして見逃さないためです。
	if err := os.Remove(path); err == nil {
		t.Skip("current user or filesystem bypasses file removal permissions")
	} else if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("permission fixture remove error = %v, want permission error", err)
	}
	err := RemoveFileAndSyncDirectory(path)
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("RemoveFileAndSyncDirectory() error = %v, want permission error", err)
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) || pathErr.Path != path {
		t.Fatalf("removal error lost its path error: %v", err)
	}
	requireFileRemovalStep(t, err, FileRemovalStepRemove, path)
	if errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("removal failure was classified as a sync failure: %v", err)
	}
	if contents, err := os.ReadFile(path); err != nil || string(contents) != "keep this compressed log" {
		t.Fatalf("source file contents = %q, error = %v", contents, err)
	}
}
