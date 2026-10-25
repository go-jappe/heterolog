package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadDirectoryEntriesMatchesReadDir(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"z-checkpoint.json", "A-checkpoint.json", "日本語.json"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(directory, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadDirectoryEntries(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("entry count = %d, want %d", len(got), len(want))
	}
	for index, entry := range got {
		expected := want[index]
		if entry.Name() != expected.Name() || entry.IsDir() != expected.IsDir() || entry.Type() != expected.Type() {
			t.Fatalf("entry[%d] = %v, want %v", index, entry, expected)
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() != expected.Name() {
			t.Fatalf("info name = %q, want %q", info.Name(), expected.Name())
		}
		if !entry.IsDir() && info.Size() != int64(len(entry.Name())) {
			t.Fatalf("info size = %d, want %d", info.Size(), len(entry.Name()))
		}
	}
}

func TestReadDirectoryEntriesEmptyAndInvalidPaths(t *testing.T) {
	directory := t.TempDir()
	got, err := ReadDirectoryEntries(directory)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty directory = %v, %v; want non-nil empty slice and nil error", got, err)
	}
	missing := filepath.Join(directory, "missing")
	if _, err := ReadDirectoryEntries(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing directory error = %v, want ErrNotExist", err)
	}
	regular := filepath.Join(directory, "regular")
	if err := os.WriteFile(regular, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{regular, "", directory + "\x00invalid"} {
		if _, err := ReadDirectoryEntries(path); err == nil {
			t.Fatalf("invalid directory %q was accepted", path)
		}
	}
	// 通常fileは未作成のdirectoryではありません。WindowsのENOTDIRはERROR_PATH_NOT_FOUNDと
	// 同じ値のため、呼出側がos.ErrNotExistとして空の一覧へ読み替えないことも確認します。
	if _, err := ReadDirectoryEntries(regular); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("regular file error = %v, must not be reported as a missing directory", err)
	}
}

func TestReadDirectoryEntriesLongPath(t *testing.T) {
	directory := t.TempDir()
	for len(directory) < 280 {
		directory = filepath.Join(directory, strings.Repeat("directory", 4))
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "checkpoint.json"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := ReadDirectoryEntries(directory)
	if err != nil || len(entries) != 1 || entries[0].Name() != "checkpoint.json" {
		t.Fatalf("long path entries = %v, error = %v", entries, err)
	}
}
