//go:build !linux && !darwin && !windows

package fsstore

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestRenameFileNoReplaceUnsupported(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	destination := filepath.Join(directory, "destination")
	if err := os.WriteFile(source, []byte("unpublished contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := RenameFileNoReplace(source, destination)
	if !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("unsupported platform error = %v, want ErrUnsupported", err)
	}
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) || linkErr.Old != source || linkErr.New != destination {
		t.Fatalf("unsupported rename error lost paths: %v", err)
	}
	if contents, err := os.ReadFile(source); err != nil || string(contents) != "unpublished contents" {
		t.Fatalf("unsupported move changed source: contents = %q, error = %v", contents, err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("unsupported move created destination: %v", err)
	}
}
