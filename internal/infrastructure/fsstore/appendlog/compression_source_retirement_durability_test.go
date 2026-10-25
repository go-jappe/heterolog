package appendlog

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

func TestRemoveSourceAfterCompressionWithDirectorySyncOrdersDurableStateTransitions(t *testing.T) {
	directory := cleanedTempDirForTest(t)
	sourcePath := filepath.Join(directory, "ordered.log")
	compressedPath := sourcePath + ".gz"
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if err := os.WriteFile(sourcePath, []byte("source"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	var operations []string
	err := removeSourceAfterCompression(
		sourcePath,
		compressedPath,
		func(oldPath, newPath string) error {
			operations = append(operations, "rename")
			return os.Rename(oldPath, newPath)
		},
		func(path string) error {
			operations = append(operations, "remove")
			return os.Remove(path)
		},
		func(directoryPath string) error {
			if directoryPath != directory {
				t.Fatalf("sync directory = %q, want %q", directoryPath, directory)
			}
			operations = append(operations, "sync")
			return nil
		},
	)
	if err != nil {
		t.Fatalf("removeSourceAfterCompression() error = %v", err)
	}

	wantOperations := []string{"rename", "sync", "remove", "sync"}
	if !reflect.DeepEqual(operations, wantOperations) {
		t.Fatalf("operations = %v, want %v", operations, wantOperations)
	}
	assertRetirementPathDoesNotExist(t, sourcePath)
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func TestRemoveSourceAfterCompressionWithDirectorySyncKeepsPendingSourceWhenFirstSyncFails(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "first-sync-failure.log")
	compressedPath := sourcePath + ".gz"
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	wantContent := []byte("source remains pending until rename is durable")
	if err := os.WriteFile(sourcePath, wantContent, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	syncErr := errors.New("injected first directory sync failure")
	removeCalled := false
	err := removeSourceAfterCompression(
		sourcePath,
		compressedPath,
		os.Rename,
		func(string) error {
			removeCalled = true
			return nil
		},
		func(string) error { return syncErr },
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("removeSourceAfterCompression() error = %v, want %v", err, syncErr)
	}
	if errors.Is(err, ErrCompressionSourceCleanup) {
		t.Fatalf("error = %v, must not match cleanup sentinel", err)
	}
	if removeCalled {
		t.Fatal("remove called after the first directory sync failed")
	}
	assertRetirementPathDoesNotExist(t, sourcePath)
	gotContent, readErr := os.ReadFile(pendingPath)
	if readErr != nil {
		t.Fatalf("read pending source: %v", readErr)
	}
	if string(gotContent) != string(wantContent) {
		t.Fatalf("pending source content = %q, want %q", gotContent, wantContent)
	}
}

func TestRemoveSourceAfterCompressionWithDirectorySyncReturnsCleanupErrorWhenSyncAfterRemoveFails(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "post-remove-sync-failure.log")
	compressedPath := sourcePath + ".gz"
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if err := os.WriteFile(sourcePath, []byte("source"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	syncErr := errors.New("injected post-remove directory sync failure")
	syncCalls := 0
	err := removeSourceAfterCompression(
		sourcePath,
		compressedPath,
		os.Rename,
		os.Remove,
		func(string) error {
			syncCalls++
			if syncCalls == 2 {
				return syncErr
			}
			return nil
		},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("removeSourceAfterCompression() error = %v, want %v", err, syncErr)
	}
	if !errors.Is(err, ErrCompressionSourceCleanup) {
		t.Fatalf("error = %v, want ErrCompressionSourceCleanup", err)
	}
	requireCompressionSourceRetirementPaths(t, err, sourcePath, pendingPath, compressedPath)
	if syncCalls != 2 {
		t.Fatalf("directory sync calls = %d, want 2", syncCalls)
	}
	assertRetirementPathDoesNotExist(t, sourcePath)
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func TestRemoveSourceAfterCompressionWithDirectorySyncSyncsExistingPendingSourceBeforeRemove(t *testing.T) {
	directory := cleanedTempDirForTest(t)
	sourcePath := filepath.Join(directory, "existing-pending.log")
	compressedPath := sourcePath + ".gz"
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if err := os.WriteFile(pendingPath, []byte("already retired source"), 0o600); err != nil {
		t.Fatalf("write pending source: %v", err)
	}

	var operations []string
	err := removeSourceAfterCompression(
		sourcePath,
		compressedPath,
		func(string, string) error {
			t.Fatal("rename called for an existing pending source")
			return nil
		},
		func(path string) error {
			operations = append(operations, "remove")
			return os.Remove(path)
		},
		func(directoryPath string) error {
			if directoryPath != directory {
				t.Fatalf("sync directory = %q, want %q", directoryPath, directory)
			}
			operations = append(operations, "sync")
			return nil
		},
	)
	if err != nil {
		t.Fatalf("removeSourceAfterCompression() error = %v", err)
	}

	wantOperations := []string{"sync", "remove", "sync"}
	if !reflect.DeepEqual(operations, wantOperations) {
		t.Fatalf("operations = %v, want %v", operations, wantOperations)
	}
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func TestRemoveCompressionSourceDeletePendingPathRemovesFileAndSyncsRealDirectory(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "cleanup.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if err := os.WriteFile(pendingPath, []byte("retired source"), 0o600); err != nil {
		t.Fatalf("write pending source: %v", err)
	}

	preRemovalSyncCompleted, err := removeCompressionSourceDeletePendingPath(pendingPath, os.Remove, infrafsstore.SyncDirectory)
	if err != nil {
		t.Fatalf("removeCompressionSourceDeletePendingPath() error = %v", err)
	}
	if !preRemovalSyncCompleted {
		t.Fatal("pre-removal sync was not reported complete after successful removal")
	}
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func TestRemoveCompressionSourceDeletePendingPathSyncsBeforeAndAfterRemoval(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "ordered-cleanup.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if err := os.WriteFile(pendingPath, []byte("retired source"), 0o600); err != nil {
		t.Fatalf("write pending source: %v", err)
	}

	var operations []string
	preRemovalSyncCompleted, err := removeCompressionSourceDeletePendingPath(
		pendingPath,
		os.Remove,
		func(directoryPath string) error {
			if want := filepath.Dir(pendingPath); directoryPath != want {
				t.Fatalf("sync directory = %q, want %q", directoryPath, want)
			}
			if _, statErr := os.Lstat(pendingPath); statErr == nil {
				operations = append(operations, "sync-before-remove")
			} else if errors.Is(statErr, os.ErrNotExist) {
				operations = append(operations, "sync-after-remove")
			} else {
				t.Fatalf("lstat pending source during sync: %v", statErr)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("removeCompressionSourceDeletePendingPath() error = %v", err)
	}
	if !preRemovalSyncCompleted {
		t.Fatal("pre-removal sync was not reported complete after successful removal")
	}
	if want := []string{"sync-before-remove", "sync-after-remove"}; !reflect.DeepEqual(operations, want) {
		t.Fatalf("operations = %v, want %v", operations, want)
	}
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func TestRemoveCompressionSourceDeletePendingPathKeepsFileWhenPreRemovalSyncFails(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "pre-remove-sync-failure.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	wantContent := []byte("keep until retirement rename is durable")
	if err := os.WriteFile(pendingPath, wantContent, 0o600); err != nil {
		t.Fatalf("write pending source: %v", err)
	}
	syncErr := errors.New("injected pre-removal directory sync failure")

	preRemovalSyncCompleted, err := removeCompressionSourceDeletePendingPath(
		pendingPath,
		os.Remove,
		func(string) error { return syncErr },
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("removeCompressionSourceDeletePendingPath() error = %v, want %v", err, syncErr)
	}
	if preRemovalSyncCompleted {
		t.Fatal("failed pre-removal sync was reported complete")
	}
	if errors.Is(err, ErrCompressionSourceCleanup) {
		t.Fatalf("error = %v, must not report completion of source retirement", err)
	}
	got, readErr := os.ReadFile(pendingPath)
	if readErr != nil || string(got) != string(wantContent) {
		t.Fatalf("pending source after pre-removal sync failure = %q, %v; want %q", got, readErr, wantContent)
	}
}

func TestRemoveCompressionSourceDeletePendingPathReturnsPostRemovalSyncError(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "post-remove-sync-failure.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	wantContent := []byte("source remains for normal recovery")
	for _, path := range []string{sourcePath, pendingPath} {
		if err := os.WriteFile(path, wantContent, 0o600); err != nil {
			t.Fatalf("write source or pending file: %v", err)
		}
	}
	syncErr := errors.New("injected post-removal directory sync failure")
	syncCalls := 0
	preRemovalSyncCompleted, err := removeCompressionSourceDeletePendingPath(pendingPath, os.Remove, func(directoryPath string) error {
		if want := filepath.Dir(pendingPath); directoryPath != want {
			t.Fatalf("sync directory = %q, want %q", directoryPath, want)
		}
		syncCalls++
		if syncCalls == 2 {
			assertRetirementPathDoesNotExist(t, pendingPath)
			return syncErr
		}
		return nil
	})
	if !errors.Is(err, syncErr) {
		t.Fatalf("removeCompressionSourceDeletePendingPath() error = %v, want %v", err, syncErr)
	}
	if !preRemovalSyncCompleted {
		t.Fatal("post-removal sync failure lost the completed pre-removal sync")
	}
	if errors.Is(err, ErrCompressionSourceCleanup) {
		t.Fatalf("error = %v, must not report completion of source retirement", err)
	}
	if syncCalls != 2 {
		t.Fatalf("sync calls = %d, want 2", syncCalls)
	}
	assertRetirementPathDoesNotExist(t, pendingPath)
	if got, readErr := os.ReadFile(sourcePath); readErr != nil || string(got) != string(wantContent) {
		t.Fatalf("source after pending removal = %q, %v; want %q", got, readErr, wantContent)
	}
}

func TestRemoveCompressionSourceDeletePendingPathReturnsRemovalErrorAfterPreRemovalSync(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "remove-failure.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	wantContent := []byte("source and pending must remain")
	for _, path := range []string{sourcePath, pendingPath} {
		if err := os.WriteFile(path, wantContent, 0o600); err != nil {
			t.Fatalf("write source or pending file: %v", err)
		}
	}
	removeErr := errors.New("injected pending source removal failure")
	var operations []string
	preRemovalSyncCompleted, err := removeCompressionSourceDeletePendingPath(
		pendingPath,
		func(path string) error {
			if path != pendingPath {
				t.Fatalf("remove path = %q, want %q", path, pendingPath)
			}
			operations = append(operations, "remove")
			return removeErr
		},
		func(directoryPath string) error {
			if want := filepath.Dir(pendingPath); directoryPath != want {
				t.Fatalf("sync directory = %q, want %q", directoryPath, want)
			}
			operations = append(operations, "sync")
			return nil
		},
	)
	if !errors.Is(err, removeErr) {
		t.Fatalf("removeCompressionSourceDeletePendingPath() error = %v, want %v", err, removeErr)
	}
	if !preRemovalSyncCompleted {
		t.Fatal("removal failure lost the completed pre-removal sync")
	}
	if errors.Is(err, ErrCompressionSourceCleanup) {
		t.Fatalf("error = %v, must not report completion of source retirement", err)
	}
	if want := []string{"sync", "remove"}; !reflect.DeepEqual(operations, want) {
		t.Fatalf("operations = %v, want %v", operations, want)
	}
	for _, path := range []string{sourcePath, pendingPath} {
		if got, readErr := os.ReadFile(path); readErr != nil || string(got) != string(wantContent) {
			t.Fatalf("file after failed removal %q = %q, %v; want %q", path, got, readErr, wantContent)
		}
	}
}

func TestRemoveCompressionSourceDeletePendingPathSyncsBeforeAndAfterMissingFile(t *testing.T) {
	pendingPath := logpattern.CompressionSourceDeletePendingPath(filepath.Join(t.TempDir(), "already-removed.log"))
	var operations []string
	preRemovalSyncCompleted, err := removeCompressionSourceDeletePendingPath(
		pendingPath,
		func(path string) error {
			if path != pendingPath {
				t.Fatalf("remove path = %q, want %q", path, pendingPath)
			}
			operations = append(operations, "remove")
			return os.Remove(path)
		},
		func(directoryPath string) error {
			if want := filepath.Dir(pendingPath); directoryPath != want {
				t.Fatalf("sync directory = %q, want %q", directoryPath, want)
			}
			operations = append(operations, "sync")
			return nil
		},
	)
	if err != nil {
		t.Fatalf("removeCompressionSourceDeletePendingPath() error = %v", err)
	}
	if !preRemovalSyncCompleted {
		t.Fatal("pre-removal sync was not reported complete for an already-missing pending file")
	}
	if want := []string{"sync", "remove", "sync"}; !reflect.DeepEqual(operations, want) {
		t.Fatalf("operations = %v, want %v", operations, want)
	}
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func TestRemoveCompressionSourceDeletePendingPathRejectsOtherNamesBeforeOperations(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "ordinary.log")
	wantContent := []byte("ordinary source must not be removed")
	if err := os.WriteFile(sourcePath, wantContent, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	for _, path := range []string{"", sourcePath, sourcePath + ".gz"} {
		preRemovalSyncCompleted, err := removeCompressionSourceDeletePendingPath(
			path,
			func(string) error {
				t.Fatal("remove called for a path without the reserved pending suffix")
				return nil
			},
			func(string) error {
				t.Fatal("sync called for a path without the reserved pending suffix")
				return nil
			},
		)
		if !errors.Is(err, ErrNotCompressionSourceDeletePendingPath) {
			t.Fatalf("removeCompressionSourceDeletePendingPath(%q) error = %v, want path validation error", path, err)
		}
		if preRemovalSyncCompleted {
			t.Fatalf("invalid path %q was reported synced", path)
		}
		if errors.Is(err, ErrCompressionSourceCleanup) {
			t.Fatalf("invalid path %q error = %v, must not report completion of source retirement", path, err)
		}
	}
	if got, readErr := os.ReadFile(sourcePath); readErr != nil || string(got) != string(wantContent) {
		t.Fatalf("source after invalid pending removal = %q, %v; want %q", got, readErr, wantContent)
	}
}

func TestRecoverCompressionSourceDeletePendingPathRejectsEmptySourceAtEntry(t *testing.T) {
	for _, pendingPath := range []string{"", logpattern.CompressionSourceDeletePendingSuffix} {
		durable, err := RecoverCompressionSourceDeletePendingPath(pendingPath)
		if !errors.Is(err, ErrUnresolvableCompressionSourcePath) {
			t.Errorf("RecoverCompressionSourceDeletePendingPath(%q) error = %v, want source path validation error", pendingPath, err)
		}
		if durable {
			t.Errorf("RecoverCompressionSourceDeletePendingPath(%q) marked invalid source as retired", pendingPath)
		}
	}
}

func TestRecoverCompressionSourceDeletePendingPathCompletesExistingRetirement(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "pending-only.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if err := os.WriteFile(pendingPath, []byte("retired source"), 0o600); err != nil {
		t.Fatalf("write pending source: %v", err)
	}

	durable, err := RecoverCompressionSourceDeletePendingPath(pendingPath)
	if err != nil {
		t.Fatalf("RecoverCompressionSourceDeletePendingPath() error = %v", err)
	}
	if !durable {
		t.Fatal("sourceRetirementDurable = false, want true for pending-only recovery")
	}
	assertRetirementPathDoesNotExist(t, sourcePath)
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func TestRecoverCompressionSourceDeletePendingPathRemovesIdenticalPendingBeforeRawRecovery(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source-and-pending.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	wantSource := []byte("canonical raw remains for peer verification")
	if err := os.WriteFile(sourcePath, wantSource, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(pendingPath, wantSource, 0o600); err != nil {
		t.Fatalf("write pending source: %v", err)
	}

	durable, err := RecoverCompressionSourceDeletePendingPath(pendingPath)
	if err != nil {
		t.Fatalf("RecoverCompressionSourceDeletePendingPath() error = %v", err)
	}
	if durable {
		t.Fatal("sourceRetirementDurable = true, want false while canonical raw remains")
	}
	gotSource, readErr := os.ReadFile(sourcePath)
	if readErr != nil || string(gotSource) != string(wantSource) {
		t.Fatalf("canonical source after conflict cleanup = %q, %v; want %q", gotSource, readErr, wantSource)
	}
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func assertRetirementPathDoesNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("os.Lstat(%q) error = %v, want os.ErrNotExist", path, err)
	}
}

// requireCompressionSourceRetirementPathsは、errが元ログ・削除待ち・圧縮fileの各pathを持つ
// *CompressionSourceRetirementErrorであることを確認します。
func requireCompressionSourceRetirementPaths(t *testing.T, err error, sourcePath, pendingPath, compressedPath string) {
	t.Helper()
	var retirementErr *CompressionSourceRetirementError
	if !errors.As(err, &retirementErr) || retirementErr.SourcePath != sourcePath ||
		retirementErr.PendingPath != pendingPath || retirementErr.CompressedPath != compressedPath {
		t.Fatalf("cleanup error = %v, want retirement of source=%q pending=%q compressed=%q", err, sourcePath, pendingPath, compressedPath)
	}
}
