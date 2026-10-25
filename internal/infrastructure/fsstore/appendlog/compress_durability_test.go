package appendlog

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

func TestCompressOldFileDurabilityOperationsRunInCommitOrder(t *testing.T) {
	directory := cleanedTempDirForTest(t)
	sourcePath := filepath.Join(directory, "durable-order.log")
	wantContent := []byte("durable compression order\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	store := FileStore{Config: newCompressionTestConfig()}
	events := make([]string, 0, 5)
	returnedPath, err := store.compressOldFile(
		context.Background(),
		sourcePath,
		nil,
		func(path string) error {
			events = append(events, "remove-pending")
			return os.Remove(path)
		},
		func(file *os.File) error {
			events = append(events, "sync-temp")
			return file.Sync()
		},
		func(directoryPath string) error {
			if directoryPath != directory {
				t.Fatalf("sync directory = %q, want %q", directoryPath, directory)
			}
			events = append(events, "sync-directory")
			return infrafsstore.SyncDirectory(directoryPath)
		},
	)
	if err != nil {
		t.Fatalf("compress durable file: %v", err)
	}

	wantEvents := []string{
		"sync-temp",
		"sync-directory", // final compressed pathの公開
		"sync-directory", // rawからpending pathへのrename
		"remove-pending",
		"sync-directory", // pending pathの削除
	}
	if len(events) != len(wantEvents) {
		t.Fatalf("durability events = %v, want %v", events, wantEvents)
	}
	for index := range wantEvents {
		if events[index] != wantEvents[index] {
			t.Fatalf("durability events = %v, want %v", events, wantEvents)
		}
	}

	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	if returnedPath != compressedPath {
		t.Fatalf("compressed path = %q, want compressed path %q", returnedPath, compressedPath)
	}
	assertFileDoesNotExist(t, sourcePath)
	assertFileDoesNotExist(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
	if got := readGzipContent(t, compressedPath); string(got) != string(wantContent) {
		t.Fatalf("compressed content = %q, want %q", got, wantContent)
	}
}

func TestCompressOldFileTempSyncFailureKeepsRawRetryable(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "temp-sync-failure.log")
	wantContent := []byte("raw remains retryable\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	syncErr := errors.New("temporary compressed file sync failed")
	store := FileStore{Config: newCompressionTestConfig()}
	returnedPath, err := store.compressOldFile(
		context.Background(),
		sourcePath,
		nil,
		func(string) error {
			t.Fatal("source cleanup ran after temporary file sync failure")
			return nil
		},
		func(*os.File) error { return syncErr },
		func(string) error {
			t.Fatal("directory sync ran after temporary file sync failure")
			return nil
		},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("compress error = %v, want %v", err, syncErr)
	}
	if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("compress error = %v, want durability unconfirmed", err)
	}
	if returnedPath != "" {
		t.Fatalf("compressed path = %q, want empty", returnedPath)
	}
	if got, readErr := os.ReadFile(sourcePath); readErr != nil || string(got) != string(wantContent) {
		t.Fatalf("source after sync failure = %q, %v; want %q", got, readErr, wantContent)
	}
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	assertFileDoesNotExist(t, compressedPath)
	assertFileDoesNotExist(t, compressedPath+".tmp")
	assertFileDoesNotExist(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
}

func TestCompressOldFileFinalDirectorySyncFailureKeepsRawAndPublishedPeer(t *testing.T) {
	directory := cleanedTempDirForTest(t)
	sourcePath := filepath.Join(directory, "final-directory-sync-failure.log")
	wantContent := []byte("published peer remains recoverable\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	syncErr := errors.New("final directory sync failed")
	store := FileStore{Config: newCompressionTestConfig()}
	returnedPath, err := store.compressOldFile(
		context.Background(),
		sourcePath,
		nil,
		func(string) error {
			t.Fatal("source cleanup ran before final directory sync succeeded")
			return nil
		},
		(*os.File).Sync,
		func(directoryPath string) error {
			if directoryPath != directory {
				t.Fatalf("sync directory = %q, want %q", directoryPath, directory)
			}
			return syncErr
		},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("compress error = %v, want %v", err, syncErr)
	}
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	if returnedPath != compressedPath {
		t.Fatalf("compressed path = %q, want published path %q with outer error", returnedPath, compressedPath)
	}
	if got, readErr := os.ReadFile(sourcePath); readErr != nil || string(got) != string(wantContent) {
		t.Fatalf("source after directory sync failure = %q, %v; want %q", got, readErr, wantContent)
	}
	if got := readGzipContent(t, compressedPath); string(got) != string(wantContent) {
		t.Fatalf("compressed content = %q, want %q", got, wantContent)
	}
	assertFileDoesNotExist(t, compressedPath+".tmp")
	assertFileDoesNotExist(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
}

func TestCompressOldFileRetirementDirectorySyncFailureKeepsPendingSource(t *testing.T) {
	directory := cleanedTempDirForTest(t)
	sourcePath := filepath.Join(directory, "retirement-directory-sync-failure.log")
	wantContent := []byte("pending source remains recoverable\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	syncErr := errors.New("retirement directory sync failed")
	directorySyncCalls := 0
	removeCalled := false
	store := FileStore{Config: newCompressionTestConfig()}
	returnedPath, err := store.compressOldFile(
		context.Background(),
		sourcePath,
		nil,
		func(string) error {
			removeCalled = true
			return nil
		},
		(*os.File).Sync,
		func(directoryPath string) error {
			if directoryPath != directory {
				t.Fatalf("sync directory = %q, want %q", directoryPath, directory)
			}
			directorySyncCalls++
			if directorySyncCalls == 2 {
				return syncErr
			}
			return infrafsstore.SyncDirectory(directoryPath)
		},
	)
	if !errors.Is(err, syncErr) {
		t.Fatalf("compress error = %v, want %v", err, syncErr)
	}
	if errors.Is(err, ErrCompressionSourceCleanup) {
		t.Fatalf("retirement sync error = %v, must not match cleanup sentinel", err)
	}
	if removeCalled {
		t.Fatal("pending source was removed after retirement directory sync failure")
	}
	if directorySyncCalls != 2 {
		t.Fatalf("directory sync calls = %d, want 2", directorySyncCalls)
	}

	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	if returnedPath != compressedPath {
		t.Fatalf("compressed path = %q, want published path %q with outer error", returnedPath, compressedPath)
	}
	assertFileDoesNotExist(t, sourcePath)
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	if got, readErr := os.ReadFile(pendingPath); readErr != nil || string(got) != string(wantContent) {
		t.Fatalf("pending source = %q, %v; want %q", got, readErr, wantContent)
	}
	if got := readGzipContent(t, compressedPath); string(got) != string(wantContent) {
		t.Fatalf("compressed content = %q, want %q", got, wantContent)
	}
}

func TestCompressOldFilePostRemovalDirectorySyncFailureAllowsPublishedArchive(t *testing.T) {
	directory := cleanedTempDirForTest(t)
	sourcePath := filepath.Join(directory, "post-removal-directory-sync-failure.log")
	wantContent := []byte("retirement already durable\n")
	if err := os.WriteFile(sourcePath, wantContent, 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}

	syncErr := errors.New("post-removal directory sync failed")
	directorySyncCalls := 0
	store := FileStore{Config: newCompressionTestConfig()}
	returnedPath, err := store.compressOldFile(
		context.Background(),
		sourcePath,
		nil,
		os.Remove,
		(*os.File).Sync,
		func(directoryPath string) error {
			if directoryPath != directory {
				t.Fatalf("sync directory = %q, want %q", directoryPath, directory)
			}
			directorySyncCalls++
			if directorySyncCalls == 3 {
				return syncErr
			}
			return infrafsstore.SyncDirectory(directoryPath)
		},
	)
	if !errors.Is(err, ErrCompressionSourceCleanup) || !errors.Is(err, syncErr) {
		t.Fatalf("error = %v, want cleanup sentinel and %v", err, syncErr)
	}
	if directorySyncCalls != 3 {
		t.Fatalf("directory sync calls = %d, want 3", directorySyncCalls)
	}
	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	if returnedPath != compressedPath {
		t.Fatalf("compressed path = %q, want %q", returnedPath, compressedPath)
	}
	assertFileDoesNotExist(t, sourcePath)
	assertFileDoesNotExist(t, logpattern.CompressionSourceDeletePendingPath(sourcePath))
	if got := readGzipContent(t, compressedPath); string(got) != string(wantContent) {
		t.Fatalf("compressed content = %q, want %q", got, wantContent)
	}
}
