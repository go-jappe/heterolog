package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestConfirmSavedFilePreservesDifferentBytesBeforeUpload(t *testing.T) {
	store := newTestStore(t)
	oldSource := writeTestSource(t, t.TempDir(), "app_20260929_1.log.gz", "old")
	old, err := store.SaveRetryableFailure(context.Background(), oldSource)
	if err != nil {
		t.Fatal(err)
	}
	newSource := writeTestSource(t, t.TempDir(), old.FileName, "new")
	result, found, err := store.ConfirmSavedFile(context.Background(), newSource)
	if err != nil || found || len(result.ConflictRenames) != 1 {
		t.Fatalf("ConfirmSavedFile() = %+v, %t, %v", result, found, err)
	}
	if _, err := os.Lstat(old.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old canonical name still exists: %v", err)
	}
	retained, err := os.ReadFile(result.ConflictRenames[0].RetainedPath)
	if err != nil || string(retained) != "old" {
		t.Fatalf("retained bytes = %q, %v", retained, err)
	}
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		files, err := store.ListFiles(context.Background(), state)
		if err != nil || len(files) != 0 {
			t.Fatalf("ListFiles(%s) = %+v, %v", state, files, err)
		}
	}
}

func TestConfirmSavedFileDoesNotMoveProcessingFile(t *testing.T) {
	store := newTestStore(t)
	oldSource := writeTestSource(t, t.TempDir(), "app_20260929_2.log.gz", "old")
	if _, err := store.SaveRetryableFailure(context.Background(), oldSource); err != nil {
		t.Fatal(err)
	}
	processing, claimed, err := store.ClaimOldestActive(context.Background())
	if err != nil || !claimed {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", processing, claimed, err)
	}
	newSource := writeTestSource(t, t.TempDir(), processing.FileName, "new")
	result, found, err := store.ConfirmSavedFile(context.Background(), newSource)
	if !errors.Is(err, ErrCompressedLogAlreadyExists) || found || len(result.ConflictRenames) != 0 {
		t.Fatalf("ConfirmSavedFile(processing) = %+v, %t, %v", result, found, err)
	}
	oldBytes, err := os.ReadFile(processing.Path)
	if err != nil || string(oldBytes) != "old" {
		t.Fatalf("processing bytes = %q, %v", oldBytes, err)
	}
}

func TestSavePreservesOtherStateDuplicateAndAdoptsEqualFile(t *testing.T) {
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "app_20260929_3.log.gz", "current")
	active, err := store.SaveRetryableFailure(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	deadPath := filepath.Join(store.Root().DeadDir(), active.FileName)
	if err := os.WriteFile(deadPath, []byte("stale"), 0o640); err != nil {
		t.Fatal(err)
	}
	adopted, found, err := store.ConfirmSavedFile(context.Background(), source)
	if err != nil || !found || adopted.Path != active.Path || len(adopted.ConflictRenames) != 1 {
		t.Fatalf("ConfirmSavedFile(duplicate) = %+v, %t, %v", adopted, found, err)
	}
	if _, err := os.Lstat(deadPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("duplicate dead file still exists: %v", err)
	}
	retained, err := os.ReadFile(adopted.ConflictRenames[0].RetainedPath)
	if err != nil || string(retained) != "stale" {
		t.Fatalf("retained dead duplicate = %q, %v", retained, err)
	}
}

func TestSaveCollisionRenameFailureRetainsSourceAndOldFile(t *testing.T) {
	store := newTestStore(t)
	oldSource := writeTestSource(t, t.TempDir(), "app_20260929_4.log.gz", "old")
	old, err := store.SaveRetryableFailure(context.Background(), oldSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.Root().preservedDir()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.Root().preservedDir(), []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	newSource := writeTestSource(t, t.TempDir(), old.FileName, "new")
	result, err := store.SaveRetryableFailure(context.Background(), newSource)
	if err == nil || len(result.ConflictRenames) != 0 {
		t.Fatalf("SaveRetryableFailure() = %+v, %v, want collision rename error", result, err)
	}
	for path, want := range map[string]string{old.Path: "old", newSource: "new"} {
		body, readErr := os.ReadFile(path)
		if readErr != nil || string(body) != want {
			t.Fatalf("%s bytes = %q, %v; want %q", path, body, readErr, want)
		}
	}
}

func TestSaveReportsPreservedConflictWhenStagingFailsLater(t *testing.T) {
	store := newTestStore(t)
	oldSource := writeTestSource(t, t.TempDir(), "app_20260929_6.log.gz", "old")
	if _, err := store.SaveRetryableFailure(context.Background(), oldSource); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(store.Root().stagingDir()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.Root().stagingDir(), []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	newSource := writeTestSource(t, t.TempDir(), filepath.Base(oldSource), "new")
	result, err := store.SaveRetryableFailure(context.Background(), newSource)
	if err == nil || len(result.ConflictRenames) != 1 {
		t.Fatalf("SaveRetryableFailure(staging failure) = %+v, %v", result, err)
	}
	oldBody, readErr := os.ReadFile(result.ConflictRenames[0].RetainedPath)
	if readErr != nil || string(oldBody) != "old" {
		t.Fatalf("retained old bytes = %q, %v", oldBody, readErr)
	}
	newBody, readErr := os.ReadFile(newSource)
	if readErr != nil || string(newBody) != "new" {
		t.Fatalf("source bytes = %q, %v", newBody, readErr)
	}
}

func TestCountPreservedFilesIncludesRenamesCopiesAndIsolatedNonregularEntries(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	oldSource := writeTestSource(t, t.TempDir(), "app_20260929_5.log.gz", "old")
	if _, err := store.SaveRetryableFailure(context.Background(), oldSource); err != nil {
		t.Fatal(err)
	}
	newSource := writeTestSource(t, t.TempDir(), filepath.Base(oldSource), "new")
	terminal, err := store.SavePermanentFailure(context.Background(), newSource)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PreserveFile(context.Background(), newSource); err != nil {
		t.Fatal(err)
	}
	count, err := infrafsstore.CountPreservedFiles(context.Background(), store.Root().Path())
	if err != nil || count != 2 {
		t.Fatalf("CountPreservedFiles() = %d, %v, want rename and source copy", count, err)
	}
	old := time.Now().AddDate(0, 0, -10)
	if err := os.Chtimes(terminal.Path, old, old); err != nil {
		t.Fatal(err)
	}
	report, err := newTestTerminalPurger(t, stores, enabledPurgeConfig(1, 1<<20)).Run(context.Background())
	if err != nil || report.ByState[pkgspool.StateDead].DeletedFileCount != 1 {
		t.Fatalf("terminal purge = %+v, %v, want the normal dead file removed", report, err)
	}
	count, err = infrafsstore.CountPreservedFiles(context.Background(), store.Root().Path())
	if err != nil || count != 2 {
		t.Fatalf("CountPreservedFiles(after purge) = %d, %v, want both preservation artifacts", count, err)
	}
	retainedDirectory := filepath.Join(store.Root().preservedDir(), "retained-directory")
	if err := os.Mkdir(retainedDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	retainedChild := filepath.Join(retainedDirectory, "child")
	if err := os.WriteFile(retainedChild, []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	count, err = infrafsstore.CountPreservedFiles(context.Background(), store.Root().Path())
	if count != 3 || err != nil {
		t.Fatalf("CountPreservedFiles(with directory) = %d, %v, want directory counted once", count, err)
	}
	if body, readErr := os.ReadFile(retainedChild); readErr != nil || string(body) != "retained" {
		t.Fatalf("retained child = %q, %v", body, readErr)
	}
	t.Run("FIFO", func(t *testing.T) {
		fifoPath := filepath.Join(store.Root().preservedDir(), ".preserve-fifo.tmp")
		createTestFIFO(t, fifoPath)
		before, err := os.Lstat(fifoPath)
		if err != nil {
			t.Fatal(err)
		}
		count, err := infrafsstore.CountPreservedFiles(context.Background(), store.Root().Path())
		if count != 4 || err != nil {
			t.Fatalf("CountPreservedFiles(with FIFO) = %d, %v, want FIFO counted once", count, err)
		}
		after, err := os.Lstat(fifoPath)
		if err != nil || !os.SameFile(before, after) || after.Mode()&os.ModeNamedPipe == 0 {
			t.Fatalf("preserved FIFO changed: %v, %v", after, err)
		}
	})
}

func TestRetainedConflictNameStaysWithinPortableComponentLength(t *testing.T) {
	name := strings.Repeat("あ", 80) + "_20260929_1.log.gz"
	retained := retainedConflictName(pkgspool.StateQuarantine, name, [16]byte{1})
	if len(retained) >= 240 {
		t.Fatalf("retained conflict name has %d bytes", len(retained))
	}
}
