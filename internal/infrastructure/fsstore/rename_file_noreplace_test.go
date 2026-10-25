//go:build linux || darwin || windows

package fsstore

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestRenameFileNoReplaceMovesSource(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	destination := filepath.Join(directory, "destination")
	if err := os.WriteFile(source, []byte("published contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RenameFileNoReplace(source, destination); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(source); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("source after successful move: %v, want ErrNotExist", err)
	}
	// FATでは名前変更でfile IDが変わるため、SameFileではなく内容と元パスの消失を確認します。
	assertNoReplaceContents(t, destination, "published contents")
}

func TestRenameFileNoReplacePreservesExistingDestination(t *testing.T) {
	for _, kind := range []string{"file", "empty directory", "nonempty directory"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			source := filepath.Join(directory, "source")
			destination := filepath.Join(directory, "destination")
			if err := os.WriteFile(source, []byte("unpublished contents"), 0o600); err != nil {
				t.Fatal(err)
			}
			keptFile := destination
			if kind != "file" {
				if err := os.Mkdir(destination, 0o700); err != nil {
					t.Fatal(err)
				}
				keptFile = filepath.Join(destination, "keep")
			}
			if kind != "empty directory" {
				if err := os.WriteFile(keptFile, []byte("existing contents"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Stat(destination)
			if err != nil {
				t.Fatal(err)
			}
			err = RenameFileNoReplace(source, destination)
			if !errors.Is(err, fs.ErrExist) {
				t.Fatalf("existing destination error = %v, want ErrExist", err)
			}
			assertNoReplaceErrorPaths(t, err, source, destination)
			assertNoReplaceContents(t, source, "unpublished contents")
			after, err := os.Stat(destination)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("existing destination changed: %v", err)
			}
			if kind != "empty directory" {
				assertNoReplaceContents(t, keptFile, "existing contents")
			} else if entries, err := os.ReadDir(destination); err != nil || len(entries) != 0 {
				t.Fatalf("empty directory changed: entries = %v, error = %v", entries, err)
			}
		})
	}
}

func TestRenameFileNoReplaceMissingSource(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "missing")
	destination := filepath.Join(directory, "destination")
	err := RenameFileNoReplace(source, destination)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing source error = %v, want ErrNotExist", err)
	}
	assertNoReplaceErrorPaths(t, err, source, destination)
	if _, err := os.Lstat(destination); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("destination created despite missing source: %v", err)
	}
}

func TestRenameFileNoReplaceConcurrentPublishers(t *testing.T) {
	const contenders = 32
	directory := t.TempDir()
	destination := filepath.Join(directory, "destination")
	sources := make([]string, contenders)
	contents := make([]string, contenders)
	for i := range sources {
		sources[i] = filepath.Join(directory, fmt.Sprintf("source-%d", i))
		contents[i] = fmt.Sprintf("contents from publisher %d", i)
		if err := os.WriteFile(sources[i], []byte(contents[i]), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	type result struct {
		index int
		err   error
	}
	results := make(chan result, contenders)
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(contenders)
	for i := range sources {
		go func() {
			ready.Done()
			<-start
			results <- result{i, RenameFileNoReplace(sources[i], destination)}
		}()
	}
	ready.Wait()
	close(start)
	winner := -1
	wins := 0
	for range contenders {
		result := <-results
		if result.err == nil {
			winner = result.index
			wins++
		} else if !errors.Is(result.err, fs.ErrExist) {
			t.Errorf("publisher %d: %v, want ErrExist", result.index, result.err)
		}
	}
	if wins != 1 {
		t.Fatalf("successful publishers = %d, want exactly one", wins)
	}
	assertNoReplaceContents(t, destination, contents[winner])
	for i, source := range sources {
		if i == winner {
			if _, err := os.Lstat(source); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("winner source after publication: %v, want ErrNotExist", err)
			}
			continue
		}
		assertNoReplaceContents(t, source, contents[i])
	}
}

func TestRenameFileNoReplacePreservesDestinationSymlink(t *testing.T) {
	for _, kind := range []string{"file", "directory", "dangling"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			source := filepath.Join(directory, "source")
			destination := filepath.Join(directory, "destination")
			target := filepath.Join(directory, "target")
			if err := os.WriteFile(source, []byte("unpublished contents"), 0o600); err != nil {
				t.Fatal(err)
			}
			keptFile := target
			if kind == "directory" {
				if err := os.Mkdir(target, 0o700); err != nil {
					t.Fatal(err)
				}
				keptFile = filepath.Join(target, "keep")
			}
			if kind != "dangling" {
				if err := os.WriteFile(keptFile, []byte("existing contents"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			testutil.Symlink(t, target, destination)
			if err := RenameFileNoReplace(source, destination); !errors.Is(err, fs.ErrExist) {
				t.Fatalf("existing symlink error = %v, want ErrExist", err)
			}
			assertNoReplaceContents(t, source, "unpublished contents")
			if got, err := os.Readlink(destination); err != nil || got != target {
				t.Fatalf("destination symlink changed: target = %q, error = %v", got, err)
			}
			if kind != "dangling" {
				assertNoReplaceContents(t, keptFile, "existing contents")
			} else if _, err := os.Lstat(target); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("dangling symlink target was created: %v", err)
			}
		})
	}
}

func assertNoReplaceContents(t *testing.T, path, want string) {
	t.Helper()
	if contents, err := os.ReadFile(path); err != nil || string(contents) != want {
		t.Fatalf("file %q contents = %q, error = %v, want %q", path, contents, err, want)
	}
}

func assertNoReplaceErrorPaths(t *testing.T, err error, source, destination string) {
	t.Helper()
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) || linkErr.Old != source || linkErr.New != destination {
		t.Fatalf("rename error lost source/destination paths: %v", err)
	}
}
