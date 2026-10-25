//go:build windows

package fsstore

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenameFileNoReplaceWindowsLongPath(t *testing.T) {
	directory := filepath.Join(t.TempDir(), strings.Repeat("a", 100), strings.Repeat("b", 100), strings.Repeat("c", 100))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(directory, "source")
	destination := filepath.Join(directory, "destination")
	if err := os.WriteFile(source, []byte("long path contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameFileNoReplace(source, destination); err != nil {
		t.Fatal(err)
	}
	assertNoReplaceContents(t, destination, "long path contents")
	if _, err := os.Lstat(source); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("source after long-path move: %v, want ErrNotExist", err)
	}
	if err := os.WriteFile(source, []byte("next publisher"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameFileNoReplace(source, destination); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("long-path collision error = %v, want ErrExist", err)
	}
	assertNoReplaceContents(t, source, "next publisher")
	assertNoReplaceContents(t, destination, "long path contents")
}

func TestRenameFileNoReplaceWindowsCaseAliasCollision(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	destination := filepath.Join(directory, "Published.Payload")
	alias := filepath.Join(directory, "published.payload")
	if err := os.WriteFile(destination, []byte("existing contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	aliasInfo, err := os.Stat(alias)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skip("test directory is case-sensitive")
	}
	if err != nil || !os.SameFile(before, aliasInfo) {
		t.Fatalf("case alias fixture does not refer to destination: %v", err)
	}
	if err := os.WriteFile(source, []byte("unpublished contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameFileNoReplace(source, alias); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("case-alias collision error = %v, want ErrExist", err)
	}
	assertNoReplaceContents(t, source, "unpublished contents")
	assertNoReplaceContents(t, destination, "existing contents")
	assertNoReplaceContents(t, alias, "existing contents")
}
