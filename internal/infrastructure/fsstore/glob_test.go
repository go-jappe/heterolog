package fsstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetFilesInfoReturnsObservedFiles(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	firstPath := filepath.Join(directory, "first.log")
	secondPath := filepath.Join(directory, "second.log")
	if err := os.WriteFile(firstPath, []byte("first"), 0o600); err != nil {
		t.Fatalf("WriteFile(first) error: %v", err)
	}
	if err := os.WriteFile(secondPath, []byte("second"), 0o600); err != nil {
		t.Fatalf("WriteFile(second) error: %v", err)
	}

	files := GetFilesInfo([]string{firstPath, secondPath})
	if len(files) != 2 {
		t.Fatalf("len(files) = %d, want 2", len(files))
	}
	if files[0].Size != uint64(len("first")) || files[1].Size != uint64(len("second")) {
		t.Fatalf("file sizes = [%d, %d], want [%d, %d]", files[0].Size, files[1].Size, len("first"), len("second"))
	}
}

func TestGetFilesInfoSkipsStatFailures(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	existingPath := filepath.Join(directory, "existing.log")
	// A missing file deterministically exercises the stat-error path on every OS.
	invalidPath := filepath.Join(directory, "missing.log")
	if err := os.WriteFile(existingPath, []byte("existing"), 0o600); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}
	files := GetFilesInfo([]string{existingPath, invalidPath})
	if len(files) != 1 || files[0].Path != existingPath {
		t.Fatalf("files = %#v, want only %q", files, existingPath)
	}
}
