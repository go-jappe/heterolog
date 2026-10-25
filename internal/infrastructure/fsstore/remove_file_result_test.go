package fsstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestRemoveFileSeparatesRemovalFromDirectorySyncAndCloseFailures(t *testing.T) {
	for _, test := range []struct {
		name                  string
		cause                 error
		durabilityUnconfirmed bool
	}{
		{name: "sync failure", cause: errors.New("directory sync failed"), durabilityUnconfirmed: true},
		{name: "close failure", cause: errors.New("directory close failed")},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, alreadyMissing := range []bool{false, true} {
				name := "existing file"
				if alreadyMissing {
					name = "already missing file"
				}
				t.Run(name, func(t *testing.T) {
					directory := cleanedTempDirForTest(t)
					path := filepath.Join(directory, "app_20260925_1.log.gz")
					if !alreadyMissing {
						if err := os.WriteFile(path, []byte("compressed log"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					syncCalls := 0
					removalErr, directorySyncErr := removeFileAndSyncDirectory(path, func(parent string) error {
						syncCalls++
						if parent != directory {
							t.Fatalf("sync directory = %q, want %q", parent, directory)
						}
						if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("sync began before file absence: %v", err)
						}
						if test.durabilityUnconfirmed {
							return errors.Join(pkgfsstore.ErrDurabilityUnconfirmed, test.cause)
						}
						// SyncDirectoryのSync成功後のClose失敗を再現します。
						// このエラーにはErrDurabilityUnconfirmedが付いていません。
						return fmt.Errorf("close directory %q after sync: %w", parent, test.cause)
					})
					if removalErr != nil || !errors.Is(directorySyncErr, test.cause) {
						t.Fatalf("removal/sync = (%v, %v), want successful removal and directory error", removalErr, directorySyncErr)
					}
					if got := errors.Is(directorySyncErr, pkgfsstore.ErrDurabilityUnconfirmed); got != test.durabilityUnconfirmed {
						t.Fatalf("unconfirmed durability = %v, want %v: %v", got, test.durabilityUnconfirmed, directorySyncErr)
					}
					if syncCalls != 1 {
						t.Fatalf("directory sync calls = %d, want one without retry", syncCalls)
					}
				})
			}
		})
	}
}

func TestRemoveRegularFileResultReportsAbsenceIndependentlyOfSync(t *testing.T) {
	t.Run("removed and already missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "app_20260925_1.log.gz")
		if err := os.WriteFile(path, []byte("compressed log"), 0o600); err != nil {
			t.Fatal(err)
		}
		for attempt := range 2 {
			result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
			if result.Err != nil || directorySyncErr != nil {
				t.Fatalf("attempt %d: removal/sync = (%+v, %v), want confirmed absence and successful sync", attempt, result, directorySyncErr)
			}
		}
	})
	t.Run("missing with sync failure", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing-parent", "app_20260925_1.log.gz")
		result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
		if result.Err != nil || !errors.Is(directorySyncErr, pkgfsstore.ErrDurabilityUnconfirmed) {
			t.Fatalf("removal/sync = (%+v, %v), want confirmed absence and sync failure", result, directorySyncErr)
		}
	})
	for _, symlink := range []bool{false, true} {
		name := "directory"
		if symlink {
			name = "symlink"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "app_20260925_1.log.gz")
			if symlink {
				testutil.Symlink(t, filepath.Join(directory, "missing-target"), path)
			} else if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
			if result.Err == nil || directorySyncErr != nil {
				t.Fatalf("removal/sync = (%+v, %v), want refusal without syncing", result, directorySyncErr)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("refused path was removed: %v", err)
			}
		})
	}
}

func TestRemoveFileSeparatesRemovalFailureWithoutSyncing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app_20260925_1.log.gz")
	contents := []byte("keep this compressed log")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	restore := testutil.DenyFileRemoval(t, path)
	defer restore()
	if err := os.Remove(path); err == nil {
		t.Skip("current user or filesystem bypasses file removal permissions")
	} else if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("permission fixture remove error = %v", err)
	}
	removalErr, directorySyncErr := removeFileAndSyncDirectory(path, func(string) error {
		t.Fatal("directory sync called after removal failure")
		return nil
	})
	if !errors.Is(removalErr, os.ErrPermission) || directorySyncErr != nil {
		t.Fatalf("removal/sync = (%v, %v), want permission error without syncing", removalErr, directorySyncErr)
	}
	result, directorySyncErr := RemoveRegularFileAndSyncDirectory(path)
	if !errors.Is(result.Err, os.ErrPermission) || directorySyncErr != nil {
		t.Fatalf("regular file removal/sync = (%+v, %v), want permission error without syncing", result, directorySyncErr)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != string(contents) {
		t.Fatalf("retained file = %q, %v", data, err)
	}
}
