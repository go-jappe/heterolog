package fsstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenDirectoryForReadBoundedBatches(t *testing.T) {
	const batchSize = 8
	for _, count := range []int{0, batchSize - 1, batchSize, batchSize*3 + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			directory := t.TempDir()
			want := make(map[string]bool, count)
			for index := range count {
				name := fmt.Sprintf("日本語-%03d.json", index)
				if err := os.WriteFile(filepath.Join(directory, name), nil, 0o600); err != nil {
					t.Fatal(err)
				}
				want[name] = true
			}
			file, err := OpenDirectoryForRead(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			for {
				entries, err := file.ReadDir(batchSize)
				if err != nil && !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
				if len(entries) > batchSize {
					t.Fatalf("batch size = %d, want at most %d", len(entries), batchSize)
				}
				for _, entry := range entries {
					if !want[entry.Name()] {
						t.Fatalf("unexpected or repeated entry %q", entry.Name())
					}
					delete(want, entry.Name())
				}
				if errors.Is(err, io.EOF) {
					break
				}
			}
			if len(want) != 0 {
				t.Fatalf("missing entries: %v", want)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := file.ReadDir(batchSize); err == nil {
				t.Fatal("read after close succeeded")
			}
		})
	}
}

func TestOpenDirectoryForReadMissingPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if file, err := OpenDirectoryForRead(missing); !errors.Is(err, os.ErrNotExist) {
		if file != nil {
			file.Close()
		}
		t.Fatalf("missing directory error = %v, want ErrNotExist", err)
	}
}
