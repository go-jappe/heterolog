package failurearchive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestCompressOldFileDurabilityTemporaryFileSyncFailureKeepsSourceRetryable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "failed-docs.jsonl")
	wantContent := []byte("retry after temporary file sync failure\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	cfg := newValidTestFileArchiveConfig(t)
	cfg.enableFileCompress = true
	cfg.compressFormat = pkgfsstore.CompressGzip
	store := FileStore{Config: cfg}
	syncErr := errors.New("injected temporary file sync failure")
	directorySyncCalled := false

	returnedPath, err := store.compressOldFileWithResultAndDurability(
		context.Background(),
		sourcePath,
		func(*os.File) error {
			return syncErr
		},
		func(string) error {
			directorySyncCalled = true
			return nil
		},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("compressOldFileWithResultAndDurability() error = %v, want %v", err, syncErr)
	}
	if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("compress error = %v, want durability unconfirmed", err)
	}
	if returnedPath != "" {
		t.Fatalf("compressed path = %q, want empty", returnedPath)
	}
	if directorySyncCalled {
		t.Fatal("syncDirectory() called after temporary file sync failure")
	}

	gotContent, readErr := os.ReadFile(sourcePath)
	if readErr != nil {
		t.Fatalf("read source file: %v", readErr)
	}
	if string(gotContent) != string(wantContent) {
		t.Fatalf("source content = %q, want %q", gotContent, wantContent)
	}
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	assertCompressionPathDoesNotExist(t, compressedPath)
	assertCompressionPathDoesNotExist(t, compressedPath+".tmp")
	assertCompressionPathDoesNotExist(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
}

func TestCompressOldFileDurabilityPublishedFileDirectorySyncFailureKeepsSourceAndFinal(t *testing.T) {
	t.Parallel()

	dir := cleanedTempDirForTest(t)
	sourcePath := filepath.Join(dir, "failed-docs.jsonl")
	wantContent := []byte("published compressed content\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	cfg := newValidTestFileArchiveConfig(t)
	cfg.enableFileCompress = true
	cfg.compressFormat = pkgfsstore.CompressGzip
	store := FileStore{Config: cfg}
	syncErr := errors.New("injected published file directory sync failure")
	directorySyncCalls := 0

	returnedPath, err := store.compressOldFileWithResultAndDurability(
		context.Background(),
		sourcePath,
		(*os.File).Sync,
		func(directory string) error {
			directorySyncCalls++
			if directory != dir {
				t.Fatalf("syncDirectory(%q), want source parent directory %q", directory, dir)
			}
			return syncErr
		},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("compressOldFileWithResultAndDurability() error = %v, want %v", err, syncErr)
	}
	if directorySyncCalls != 1 {
		t.Fatalf("directory sync calls = %d, want 1", directorySyncCalls)
	}
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	if returnedPath != compressedPath {
		t.Fatalf("compressed path = %q, want %q", returnedPath, compressedPath)
	}

	gotSourceContent, readErr := os.ReadFile(sourcePath)
	if readErr != nil {
		t.Fatalf("read source file: %v", readErr)
	}
	if string(gotSourceContent) != string(wantContent) {
		t.Fatalf("source content = %q, want %q", gotSourceContent, wantContent)
	}
	if gotCompressedContent := readGzipFile(t, compressedPath); string(gotCompressedContent) != string(wantContent) {
		t.Fatalf("compressed content = %q, want %q", gotCompressedContent, wantContent)
	}
	assertCompressionPathDoesNotExist(t, compressedPath+".tmp")
	assertCompressionPathDoesNotExist(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
}

func TestCompressOldFileDurabilitySyncsTemporaryFileBeforePublishingAndRetiresSource(t *testing.T) {
	t.Parallel()

	dir := cleanedTempDirForTest(t)
	sourcePath := filepath.Join(dir, "failed-docs.jsonl")
	wantContent := []byte("durably compressed content\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o640); err != nil {
		t.Fatalf("write source file: %v", err)
	}

	cfg := newValidTestFileArchiveConfig(t)
	cfg.enableFileCompress = true
	cfg.compressFormat = pkgfsstore.CompressGzip
	store := FileStore{Config: cfg}
	var syncOrder []string

	returnedPath, err := store.compressOldFileWithResultAndDurability(
		context.Background(),
		sourcePath,
		func(file *os.File) error {
			syncOrder = append(syncOrder, "temporary-file")
			return file.Sync()
		},
		func(directory string) error {
			if directory != dir {
				t.Fatalf("syncDirectory(%q), want source parent directory %q", directory, dir)
			}
			syncOrder = append(syncOrder, "parent-directory")
			return infrafsstore.SyncDirectory(directory)
		},
	)
	if err != nil {
		t.Fatalf("compressOldFileWithResultAndDurability() error = %v", err)
	}
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	if returnedPath != compressedPath {
		t.Fatalf("compressed path = %q, want %q", returnedPath, compressedPath)
	}
	wantSyncOrder := []string{"temporary-file", "parent-directory"}
	if len(syncOrder) != len(wantSyncOrder) || syncOrder[0] != wantSyncOrder[0] || syncOrder[1] != wantSyncOrder[1] {
		t.Fatalf("sync order = %v, want %v", syncOrder, wantSyncOrder)
	}

	assertCompressionPathDoesNotExist(t, sourcePath)
	assertCompressionPathDoesNotExist(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
	assertCompressionPathDoesNotExist(t, compressedPath+".tmp")
	if gotCompressedContent := readGzipFile(t, compressedPath); string(gotCompressedContent) != string(wantContent) {
		t.Fatalf("compressed content = %q, want %q", gotCompressedContent, wantContent)
	}
}

func assertCompressionPathDoesNotExist(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("os.Lstat(%q) error = %v, want os.ErrNotExist", path, err)
	}
}
