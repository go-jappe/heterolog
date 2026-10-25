package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestRemoveRegularFileAndSyncDirectoryRecordsRemovedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260924_1.log.gz")
	content := []byte("compressed file contents")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}

	result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
	if result.Err != nil || directorySyncErr != nil {
		t.Fatalf("removal/sync = (%+v, %v)", result, directorySyncErr)
	}
	if result.File.Path != path || result.File.Size != uint64(len(content)) || !result.File.ModTime.Equal(info.ModTime()) {
		t.Fatalf("removed file metadata = %+v", result.File)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed file still exists: %v", err)
	}

	result, directorySyncErr = RemoveRegularFileAndSyncDirectory(path)
	if result.Err != nil || directorySyncErr != nil {
		t.Fatalf("already missing file removal/sync = (%+v, %v)", result, directorySyncErr)
	}
}

func TestRemoveRegularFileAndSyncDirectoryConfirmsAbsenceDespiteDirectorySyncFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-parent", "archive.log.gz")
	result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
	if result.Err != nil {
		t.Fatalf("missing path was not recorded as successful removal: %+v", result)
	}
	if !errors.Is(directorySyncErr, os.ErrNotExist) || !errors.Is(directorySyncErr, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("missing parent directory sync error = %v", directorySyncErr)
	}
	requireFileRemovalStep(t, directorySyncErr, FileRemovalStepSyncAfterAbsent, path)
}

func TestRemoveRegularFileAndSyncDirectoryPreservesStatError(t *testing.T) {
	path, restore := makePurgeStatFailure(t)
	defer restore()
	_, cause := os.Lstat(path)
	var original *os.PathError
	if !errors.As(cause, &original) || errors.Is(cause, os.ErrNotExist) {
		t.Fatalf("stat failure fixture is invalid: %v", cause)
	}
	result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
	if directorySyncErr != nil {
		t.Fatalf("stat failure produced directory sync error: %v", directorySyncErr)
	}
	var reported *os.PathError
	if !errors.Is(result.Err, original.Err) || !errors.As(result.Err, &reported) || reported.Path != path {
		t.Fatalf("stat error lost its cause: %v", result.Err)
	}
	requireFileRemovalStep(t, result.Err, FileRemovalStepInspect, path)
}

func TestRemoveRegularFileAndSyncDirectoryRejectsDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.log.gz")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
	// 呼び出し元がerrors.Isで判別できるよう、通常ファイルでないことをErrNotRegularFileで示します。
	if !errors.Is(result.Err, ErrNotRegularFile) || directorySyncErr != nil {
		t.Fatalf("directory removal/sync = (%+v, %v), want rejection without syncing", result, directorySyncErr)
	}
	if info, err := os.Lstat(path); err != nil || !info.IsDir() {
		t.Fatalf("directory was not preserved: %v", err)
	}
}

func TestRemoveRegularFileAndSyncDirectoryRejectsSymlink(t *testing.T) {
	for _, dangling := range []bool{false, true} {
		name := "existing target"
		if dangling {
			name = "missing target"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			target := filepath.Join(directory, "target.log.gz")
			path := filepath.Join(directory, "archive.log.gz")
			if !dangling {
				if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			testutil.Symlink(t, target, path)
			result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
			if !errors.Is(result.Err, ErrNotRegularFile) || directorySyncErr != nil {
				t.Fatalf("symlink removal/sync = (%+v, %v), want rejection without syncing", result, directorySyncErr)
			}
			if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("symlink was not preserved: %v", err)
			}
			if !dangling {
				if data, err := os.ReadFile(target); err != nil || string(data) != "keep" {
					t.Fatalf("symlink target changed: %q, %v", data, err)
				}
			}
		})
	}
}
