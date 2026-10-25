package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestStorePublishesOriginalFileDirectlyBelowState(t *testing.T) {
	store := newTestStore(t)
	sourcePath := writeTestSource(t, t.TempDir(), "app_20260906_1.log.gz", "compressed-log")

	published, err := store.SaveRetryableFailure(context.Background(), sourcePath)
	if err != nil {
		t.Fatalf("SaveRetryableFailure() error = %v", err)
	}
	wantPath := filepath.Join(store.Root().ActiveDir(), filepath.Base(sourcePath))
	if published.FileName != filepath.Base(sourcePath) ||
		published.State != pkgspool.StateActive ||
		published.Path != wantPath {
		t.Fatalf("SaveRetryableFailure() = %+v, want active file %q", published, wantPath)
	}
	body, err := os.ReadFile(wantPath)
	if err != nil || string(body) != "compressed-log" {
		t.Fatalf("published body = %q, %v", body, err)
	}
	info, err := os.Stat(wantPath)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("published path is not a regular file: %v", err)
	}
	if entries, err := os.ReadDir(wantPath); err == nil || len(entries) != 0 {
		t.Fatal("published path unexpectedly is a directory")
	}
	if _, err := os.Stat(filepath.Join(store.Root().ActiveDir(), "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest exists beside flat file: %v", err)
	}
	assertDirectoryEmpty(t, store.Root().stagingDir())
}

func TestStorePublishesPermanentFailureToDead(t *testing.T) {
	store := newTestStore(t)
	sourcePath := writeTestSource(t, t.TempDir(), "app_20260906_2.log.gz", "permanent")

	published, err := store.SavePermanentFailure(context.Background(), sourcePath)
	if err != nil {
		t.Fatalf("SavePermanentFailure() error = %v", err)
	}
	if published.State != pkgspool.StateDead || filepath.Dir(published.Path) != store.Root().DeadDir() {
		t.Fatalf("SavePermanentFailure() = %+v, want dead", published)
	}
}

func TestStoreCompressedBackendArchiveCanBeClaimedReusedAndRemovedAfterCompletion(t *testing.T) {
	for _, compressionExtension := range []string{"gz", "custom_format-v2"} {
		t.Run(compressionExtension, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			fileName := "app_20261008_1.jsonl." + compressionExtension
			const content = "compressed backend failure records"
			source := writeTestSource(t, t.TempDir(), fileName, content)
			active, err := store.SaveRetryableFailure(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			processing, claimed, err := store.ClaimOldestActive(ctx)
			if err != nil || !claimed || processing.FileName != fileName {
				t.Fatalf("ClaimOldestActive() = %#v, %t, %v", processing, claimed, err)
			}
			if _, err := os.Lstat(active.Path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("claimed file remains in active: %v", err)
			}
			done, err := store.TransitionProcessing(ctx, processing, pkgspool.StateDone)
			if err != nil {
				t.Fatal(err)
			}
			reused, found, err := store.ConfirmSavedFile(ctx, source)
			if err != nil || !found || !reflect.DeepEqual(reused, done) {
				t.Fatalf("ConfirmSavedFile() = %#v, %t, %v, want %#v", reused, found, err, done)
			}
			files, err := store.ListFiles(ctx, pkgspool.StateDone)
			if err != nil || len(files) != 1 || files[0].Path != done.Path {
				t.Fatalf("ListFiles(done) = %#v, %v", files, err)
			}
			body, err := os.ReadFile(done.Path)
			if err != nil || string(body) != content {
				t.Fatalf("done content = %q, %v", body, err)
			}
			if err := store.RemoveTerminal(ctx, done); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(done.Path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("removed terminal file still exists: %v", err)
			}
			if _, claimed, err := store.ClaimOldestActive(ctx); err != nil || claimed {
				t.Fatalf("ClaimOldestActive() after completion = %t, %v", claimed, err)
			}
			body, err = os.ReadFile(source)
			if err != nil || string(body) != content {
				t.Fatalf("source changed by spool processing: %q, %v", body, err)
			}
		})
	}
}

func TestStorePublishSameBasenameAndContentIsIdempotentAcrossStates(t *testing.T) {
	store := newTestStore(t)
	directory := t.TempDir()
	firstSource := writeTestSource(t, directory, "app_20260906_3.log.gz", "same")
	first, err := store.SaveRetryableFailure(context.Background(), firstSource)
	if err != nil {
		t.Fatalf("first SaveRetryableFailure() error = %v", err)
	}
	claimed, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", claimed, ok, err)
	}
	done, err := store.TransitionProcessing(context.Background(), claimed, pkgspool.StateDone)
	if err != nil {
		t.Fatalf("TransitionProcessing(done) error = %v", err)
	}
	secondSource := writeTestSource(t, t.TempDir(), filepath.Base(firstSource), "same")
	second, err := store.SavePermanentFailure(context.Background(), secondSource)
	if err != nil {
		t.Fatalf("idempotent SavePermanentFailure() error = %v", err)
	}
	if !reflect.DeepEqual(second, done) || reflect.DeepEqual(second, first) {
		t.Fatalf("idempotent publish = %+v, want existing done %+v", second, done)
	}
}

func TestStoreEquivalentTerminalHandoffRefreshesModificationTime(t *testing.T) {
	store := newTestStore(t)
	fileName := "app_20260906_34.log.gz"
	firstSource := writeTestSource(t, t.TempDir(), fileName, "same terminal bytes")
	dead, err := store.SavePermanentFailure(context.Background(), firstSource)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-48 * time.Hour)
	if err := os.Chtimes(dead.Path, old, old); err != nil {
		t.Fatal(err)
	}
	secondSource := writeTestSource(t, t.TempDir(), fileName, "same terminal bytes")
	beforeHandoff := time.Now().UTC().Add(-time.Second)

	adopted, err := store.SavePermanentFailure(context.Background(), secondSource)
	if err != nil {
		t.Fatalf("SavePermanentFailure(equivalent) error = %v", err)
	}
	if !reflect.DeepEqual(adopted, dead) {
		t.Fatalf("adopted file = %+v, want existing dead %+v", adopted, dead)
	}
	info, err := os.Lstat(dead.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().Before(beforeHandoff) {
		t.Fatalf("equivalent handoff mtime = %s, want refreshed after %s", info.ModTime(), beforeHandoff)
	}
}

func TestStorePublishSameBasenameWithDifferentContentPreservesOldFile(t *testing.T) {
	store := newTestStore(t)
	fileName := "app_20260906_4.log.gz"
	firstSource := writeTestSource(t, t.TempDir(), fileName, "first")
	first, err := store.SaveRetryableFailure(context.Background(), firstSource)
	if err != nil {
		t.Fatalf("first SaveRetryableFailure() error = %v", err)
	}
	secondSource := writeTestSource(t, t.TempDir(), fileName, "second")
	second, err := store.SaveRetryableFailure(context.Background(), secondSource)
	if err != nil {
		t.Fatalf("SaveRetryableFailure(different content) error = %v", err)
	}
	body, err := os.ReadFile(first.Path)
	if err != nil || string(body) != "second" {
		t.Fatalf("canonical file body=%q err=%v, want second", body, err)
	}
	if len(second.ConflictRenames) != 1 || second.ConflictRenames[0].OriginalPath != first.Path ||
		filepath.Dir(second.ConflictRenames[0].RetainedPath) != store.Root().preservedDir() {
		t.Fatalf("preserved collisions = %+v", second.ConflictRenames)
	}
	oldBody, err := os.ReadFile(second.ConflictRenames[0].RetainedPath)
	if err != nil || string(oldBody) != "first" {
		t.Fatalf("retained old body=%q err=%v", oldBody, err)
	}
	active, err := store.ListFiles(context.Background(), pkgspool.StateActive)
	if err != nil || len(active) != 1 || active[0].Path != first.Path {
		t.Fatalf("ListFiles(active) = %+v, %v", active, err)
	}
}

func TestStoreConcurrentPublishNeverOverwritesSameBasename(t *testing.T) {
	store := newTestStore(t)
	sourcePath := writeTestSource(t, t.TempDir(), "app_20260906_5.log.gz", "same")
	const workers = 8
	results := make(chan CompressedLogFile, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			file, err := store.SaveRetryableFailure(context.Background(), sourcePath)
			results <- file
			errs <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent SaveRetryableFailure() error = %v", err)
		}
	}
	for file := range results {
		if file.State != pkgspool.StateActive || file.FileName != filepath.Base(sourcePath) {
			t.Fatalf("concurrent result = %+v", file)
		}
	}
	files, err := store.ListFiles(context.Background(), pkgspool.StateActive)
	if err != nil || len(files) != 1 {
		t.Fatalf("ListFiles(active) = %+v, %v, want one", files, err)
	}
}

func TestStoreConcurrentRetryableAndPermanentPublishCreatesOneStateOnly(t *testing.T) {
	store := newTestStore(t)
	fileName := "app_20260906_6.log.gz"
	retryableSource := writeTestSource(t, t.TempDir(), fileName, "same")
	permanentSource := writeTestSource(t, t.TempDir(), fileName, "same")

	type publishResult struct {
		file CompressedLogFile
		err  error
	}
	start := make(chan struct{})
	results := make(chan publishResult, 2)
	go func() {
		<-start
		file, err := store.SaveRetryableFailure(context.Background(), retryableSource)
		results <- publishResult{file: file, err: err}
	}()
	go func() {
		<-start
		file, err := store.SavePermanentFailure(context.Background(), permanentSource)
		results <- publishResult{file: file, err: err}
	}()
	close(start)

	first := <-results
	second := <-results
	for _, result := range []publishResult{first, second} {
		if result.err != nil {
			t.Fatalf("concurrent cross-state publish error = %v", result.err)
		}
	}
	if !reflect.DeepEqual(first.file, second.file) {
		t.Fatalf("concurrent publishes returned different files: first=%+v second=%+v", first.file, second.file)
	}
	active, err := store.ListFiles(context.Background(), pkgspool.StateActive)
	if err != nil {
		t.Fatal(err)
	}
	dead, err := store.ListFiles(context.Background(), pkgspool.StateDead)
	if err != nil {
		t.Fatal(err)
	}
	if len(active)+len(dead) != 1 {
		t.Fatalf("published files across active/dead = %d, want 1", len(active)+len(dead))
	}
}

func TestStoreRemovesAbandonedPrivateStagingFilesAndReportsUnknownEntries(t *testing.T) {
	store := newTestStore(t)
	if err := store.EnsureLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	abandoned := filepath.Join(store.Root().stagingDir(), stagingFilePrefix+"crash"+stagingFileSuffix)
	unrelated := filepath.Join(store.Root().stagingDir(), "operator-note")
	if err := os.WriteFile(abandoned, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unrelated, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	removed, err := store.RemoveAbandonedStagingFiles(context.Background())
	if err == nil {
		t.Fatal("RemoveAbandonedStagingFiles() error = nil, want unknown entry error")
	}
	var unexpected *UnexpectedStagingEntryError
	if !errors.As(err, &unexpected) || unexpected.Path != unrelated {
		t.Fatalf("RemoveAbandonedStagingFiles() error = %v, want unknown entry %q", err, unrelated)
	}
	if removed != 1 {
		t.Fatalf("RemoveAbandonedStagingFiles() removed = %d, want 1", removed)
	}
	if _, err := os.Stat(abandoned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned staging file remains: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated staging entry was removed: %v", err)
	}
}

func TestStoreClaimRequeueAndRecovery(t *testing.T) {
	store := newTestStore(t)
	first, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_6.log.gz", "first"),
	)
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	second, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_7.log.gz", "second"),
	)
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(first.Path, old, old); err != nil {
		t.Fatalf("set first time: %v", err)
	}

	claimed, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok || claimed.FileName != first.FileName || claimed.State != pkgspool.StateProcessing {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", claimed, ok, err)
	}
	if _, err := store.RequeueProcessing(context.Background(), claimed); err != nil {
		t.Fatalf("RequeueProcessing(): %v", err)
	}

	claimed, ok, err = store.ClaimOldestActive(context.Background())
	if err != nil || !ok || claimed.FileName != second.FileName {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", claimed, ok, err)
	}
	if _, err := store.TransitionProcessing(
		context.Background(),
		claimed,
		pkgspool.StateActive,
	); !errors.Is(err, ErrInvalidStateTransition) {
		t.Fatalf("TransitionProcessing(active) error = %v, want invalid transition", err)
	}
	recovered, err := store.RecoverProcessing(context.Background())
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverProcessing() = %d, %v, want 1", recovered, err)
	}
}

func TestStoreRequeueProcessingRefreshesModificationTimeWithoutChangingContent(t *testing.T) {
	store := newTestStore(t)
	first, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_40.log.gz", "first compressed bytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_41.log.gz", "second compressed bytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	oldest := time.Now().UTC().Add(-2 * time.Hour)
	older := oldest.Add(time.Hour)
	if err := os.Chtimes(first.Path, oldest, oldest); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(second.Path, older, older); err != nil {
		t.Fatal(err)
	}

	claimed, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok || claimed.FileName != first.FileName {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v, want first", claimed, ok, err)
	}
	before, err := os.ReadFile(claimed.Path)
	if err != nil {
		t.Fatal(err)
	}
	refreshStartedAt := time.Now().UTC().Add(-time.Second)
	requeued, err := store.RequeueProcessing(context.Background(), claimed)
	if err != nil {
		t.Fatalf("RequeueProcessing() error = %v", err)
	}
	if requeued.State != pkgspool.StateActive || requeued.Path != first.Path {
		t.Fatalf("RequeueProcessing() = %+v, want active path %q", requeued, first.Path)
	}
	after, err := os.ReadFile(requeued.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("compressed bytes changed: before=%q after=%q", before, after)
	}
	info, err := os.Lstat(requeued.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().Before(refreshStartedAt) {
		t.Fatalf("requeued mtime = %s, want after %s", info.ModTime(), refreshStartedAt)
	}

	next, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok || next.FileName != second.FileName {
		t.Fatalf("next ClaimOldestActive() = %+v, %t, %v, want second", next, ok, err)
	}
}

func TestStoreRequeueProcessingRenameFailureLeavesRefreshedFileInProcessing(t *testing.T) {
	store := newTestStore(t)
	published, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_42.log.gz", "processing bytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", claimed, ok, err)
	}
	old := time.Now().UTC().Add(-2 * time.Hour)
	if err := os.Chtimes(claimed.Path, old, old); err != nil {
		t.Fatal(err)
	}
	// Simulate an external conflicting active entry so the no-replace rename
	// fails after the processing file's refreshed mtime has been synced.
	if err := os.WriteFile(published.Path, []byte("conflicting active bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	refreshStartedAt := time.Now().UTC().Add(-time.Second)

	if requeued, err := store.RequeueProcessing(context.Background(), claimed); err == nil || !reflect.DeepEqual(requeued, CompressedLogFile{}) {
		t.Fatalf("RequeueProcessing() = %+v, %v, want zero file and rename error", requeued, err)
	}
	processingBody, err := os.ReadFile(claimed.Path)
	if err != nil {
		t.Fatalf("processing file was not retained: %v", err)
	}
	if string(processingBody) != "processing bytes" {
		t.Fatalf("processing bytes = %q, want preserved", processingBody)
	}
	processingInfo, err := os.Lstat(claimed.Path)
	if err != nil {
		t.Fatal(err)
	}
	if processingInfo.ModTime().Before(refreshStartedAt) {
		t.Fatalf("processing mtime = %s, want refreshed after %s", processingInfo.ModTime(), refreshStartedAt)
	}
	activeBody, err := os.ReadFile(published.Path)
	if err != nil || string(activeBody) != "conflicting active bytes" {
		t.Fatalf("conflicting active file changed: body=%q err=%v", activeBody, err)
	}
}

func TestStoreRecoverProcessingRefreshesModificationTimeAndPreservesQueueOrder(t *testing.T) {
	store := newTestStore(t)
	recoveredCandidate, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_43.log.gz", "recovered compressed bytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_44.log.gz", "waiting compressed bytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	oldest := time.Now().UTC().Add(-2 * time.Hour)
	older := oldest.Add(time.Hour)
	if err := os.Chtimes(recoveredCandidate.Path, oldest, oldest); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(waiting.Path, older, older); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok || claimed.FileName != recoveredCandidate.FileName {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v, want recovery candidate", claimed, ok, err)
	}
	refreshStartedAt := time.Now().UTC().Add(-time.Second)

	recovered, err := store.RecoverProcessing(context.Background())
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverProcessing() = %d, %v, want 1", recovered, err)
	}
	recoveredBody, err := os.ReadFile(recoveredCandidate.Path)
	if err != nil || string(recoveredBody) != "recovered compressed bytes" {
		t.Fatalf("recovered bytes = %q, %v", recoveredBody, err)
	}
	recoveredInfo, err := os.Lstat(recoveredCandidate.Path)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredInfo.ModTime().Before(refreshStartedAt) {
		t.Fatalf("recovered mtime = %s, want after %s", recoveredInfo.ModTime(), refreshStartedAt)
	}
	next, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok || next.FileName != waiting.FileName {
		t.Fatalf("next ClaimOldestActive() = %+v, %t, %v, want waiting file", next, ok, err)
	}
}

func TestStoreRecoverProcessingRenameFailureLeavesRefreshedFileInProcessing(t *testing.T) {
	store := newTestStore(t)
	published, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_45.log.gz", "processing recovery bytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", claimed, ok, err)
	}
	old := time.Now().UTC().Add(-2 * time.Hour)
	if err := os.Chtimes(claimed.Path, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(published.Path, []byte("conflicting recovery bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	refreshStartedAt := time.Now().UTC().Add(-time.Second)

	recovered, err := store.RecoverProcessing(context.Background())
	if err == nil || recovered != 0 {
		t.Fatalf("RecoverProcessing() = %d, %v, want zero and rename error", recovered, err)
	}
	processingBody, readErr := os.ReadFile(claimed.Path)
	if readErr != nil || string(processingBody) != "processing recovery bytes" {
		t.Fatalf("processing recovery file changed: body=%q err=%v", processingBody, readErr)
	}
	processingInfo, statErr := os.Lstat(claimed.Path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if processingInfo.ModTime().Before(refreshStartedAt) {
		t.Fatalf("processing recovery mtime = %s, want refreshed after %s", processingInfo.ModTime(), refreshStartedAt)
	}
}

func TestStoreConcurrentClaimHasSingleOwner(t *testing.T) {
	store := newTestStore(t)
	_, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_8.log.gz", "payload"),
	)
	if err != nil {
		t.Fatalf("SaveRetryableFailure() error = %v", err)
	}

	type result struct {
		claimed bool
		err     error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, claimed, claimErr := store.ClaimOldestActive(context.Background())
			results <- result{claimed: claimed, err: claimErr}
		}()
	}
	wait.Wait()
	close(results)
	claimedCount := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("ClaimOldestActive() error = %v", result.err)
		}
		if result.claimed {
			claimedCount++
		}
	}
	if claimedCount != 1 {
		t.Fatalf("claimed count = %d, want 1", claimedCount)
	}
}

func TestStoreRemoveTerminalRejectsActiveAndRemovesDone(t *testing.T) {
	store := newTestStore(t)
	active, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_9.log.gz", "payload"),
	)
	if err != nil {
		t.Fatalf("SaveRetryableFailure() error = %v", err)
	}
	if err := store.RemoveTerminal(context.Background(), active); !errors.Is(err, ErrInvalidStateTransition) {
		t.Fatalf("RemoveTerminal(active) error = %v", err)
	}
	claimed, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok {
		t.Fatalf("ClaimOldestActive() = %+v, %t, %v", claimed, ok, err)
	}
	done, err := store.TransitionProcessing(context.Background(), claimed, pkgspool.StateDone)
	if err != nil {
		t.Fatalf("TransitionProcessing(done) error = %v", err)
	}
	if err := store.RemoveTerminal(context.Background(), done); err != nil {
		t.Fatalf("RemoveTerminal(done) error = %v", err)
	}
	if _, err := os.Lstat(done.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("done file remains: %v", err)
	}
}

func TestStoreRejectsSymlinkSource(t *testing.T) {
	store := newTestStore(t)
	directory := t.TempDir()
	target := writeTestSource(t, directory, "target.log.gz", "payload")
	symlink := filepath.Join(directory, "app_20260906_10.log.gz")
	testutil.Symlink(t, target, symlink)
	if _, err := store.SaveRetryableFailure(context.Background(), symlink); !errors.Is(err, ErrInvalidCompressedLogPath) {
		t.Fatalf("SaveRetryableFailure(symlink) error = %v, want ErrInvalidCompressedLogPath", err)
	}
}

func TestStoreStateListingPreservesNonRegularEntriesAndContinues(t *testing.T) {
	testCases := []struct {
		name  string
		setup func(*testing.T, string) string
	}{
		{
			name: "directory",
			setup: func(t *testing.T, activeDirectory string) string {
				path := filepath.Join(activeDirectory, "legacy-bundle")
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "keep.txt"), []byte("nested data"), 0o600); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
		{
			name: "symbolic link",
			setup: func(t *testing.T, activeDirectory string) string {
				target := writeTestSource(t, t.TempDir(), "target.log.gz", "outside")
				path := filepath.Join(activeDirectory, "app_20260906_90.log.gz")
				testutil.Symlink(t, target, path)
				return path
			},
		},
		{
			name: "non-regular file",
			setup: func(t *testing.T, activeDirectory string) string {
				path := filepath.Join(activeDirectory, "app_20260906_91.log.gz")
				createTestFIFO(t, path)
				return path
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store := newTestStore(t)
			valid, err := store.SaveRetryableFailure(
				context.Background(),
				writeTestSource(t, t.TempDir(), "app_20260906_89.log.gz", "valid"),
			)
			if err != nil {
				t.Fatal(err)
			}
			invalidPath := testCase.setup(t, store.Root().ActiveDir())

			files, err := store.ListFiles(context.Background(), pkgspool.StateActive)
			if err != nil || len(files) != 1 || files[0].Path != valid.Path {
				t.Fatalf("ListFiles(active) = %+v, %v, want the valid file", files, err)
			}
			if _, err := os.Lstat(invalidPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid state entry was not moved: %v", err)
			}
			entries, err := os.ReadDir(store.root.preservedDir())
			if err != nil || len(entries) != 1 {
				t.Fatalf("preserved entries = %v, %v, want one", entries, err)
			}
			preservedPath := filepath.Join(store.root.preservedDir(), entries[0].Name())
			switch testCase.name {
			case "directory":
				if body, err := os.ReadFile(filepath.Join(preservedPath, "keep.txt")); err != nil || string(body) != "nested data" {
					t.Fatalf("preserved directory contents = %q, %v", body, err)
				}
			case "symbolic link":
				target, err := os.Readlink(preservedPath)
				if err != nil {
					t.Fatal(err)
				}
				if body, err := os.ReadFile(target); err != nil || string(body) != "outside" {
					t.Fatalf("symlink target changed: %q, %v", body, err)
				}
			case "non-regular file":
				if info, err := os.Lstat(preservedPath); err != nil || info.Mode()&os.ModeNamedPipe == 0 {
					t.Fatalf("preserved FIFO changed: %v", err)
				}
			}
			warnings, err := store.takePreservationWarnings(context.Background())
			var preservation *InvalidEntryPreservationWarning
			if err != nil || len(warnings) != 1 || !errors.Is(warnings[0], ErrInvalidCompressedLogPath) ||
				!errors.As(warnings[0], &preservation) ||
				preservation.SourcePath != invalidPath || preservation.PreservedPath != preservedPath {
				t.Fatalf("preservation warnings = %v, %v", warnings, err)
			}
			if count, err := infrafsstore.CountPreservedFiles(context.Background(), store.Root().Path()); err != nil || count != 1 {
				t.Fatalf("preserved count = %d, %v, want one", count, err)
			}
			claimed, found, err := store.ClaimOldestActive(context.Background())
			if err != nil || !found || claimed.FileName != valid.FileName {
				t.Fatalf("ClaimOldestActive() = %+v, %t, %v", claimed, found, err)
			}
		})
	}
}

func TestStoreUsesResolvedRootAfterAncestorAliasChanges(t *testing.T) {
	base := t.TempDir()
	outside := resolvedTempDirForTest(t)
	linkedRoot := filepath.Join(base, "spool-link")
	testutil.Symlink(t, outside, linkedRoot)
	root, err := NewRoot(
		filepath.Join(linkedRoot, "configured"),
		"target",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantRoot := filepath.Join(outside, "configured", "target", "spool", "objectstorage", "s3", "logger")
	if root.Path() != wantRoot {
		t.Fatalf("root = %q, want physical path %q", root.Path(), wantRoot)
	}
	assertDirectoryEmpty(t, outside)
	replacement := t.TempDir()
	if err := os.Remove(linkedRoot); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, replacement, linkedRoot)
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	source := writeTestSource(t, t.TempDir(), "app_20260913_1.log.gz", "fixed root")
	published, err := store.SaveRetryableFailure(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if published.Path != filepath.Join(wantRoot, "active", filepath.Base(source)) {
		t.Fatalf("published path followed changed alias: %q", published.Path)
	}
	assertDirectoryEmpty(t, replacement)
}

func TestStoreEnsureLayoutRejectsSymlinkStateDirectory(t *testing.T) {
	store := newTestStore(t)
	if err := os.MkdirAll(store.Root().Path(), directoryMode); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	testutil.Symlink(t, outside, store.Root().ActiveDir())
	if err := store.EnsureLayout(context.Background()); err == nil {
		t.Fatal("EnsureLayout() accepted a symlink state directory")
	}
	source := writeTestSource(t, t.TempDir(), "app_20260906_92.log.gz", "preserve")
	if _, err := store.SaveRetryableFailure(context.Background(), source); err == nil {
		t.Fatal("SaveRetryableFailure() accepted a symlink state directory")
	}
	assertDirectoryEmpty(t, outside)
}

// resolvedTempDirForTestは、macOSの/varなど一時ディレクトリの上位のリンクを解決したパスを返します。
// rootは解決済みのパスを返し、管理下の作成ではリンクを拒否するため、比べる側も解決しておきます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func newTestStore(t testing.TB) *Store {
	t.Helper()
	root, err := NewRoot(
		t.TempDir(),
		"target",
		objectstorage.UploadTargetS3,
		pkglogger.LoggerContractSync,
	)
	if err != nil {
		t.Fatalf("NewRoot() error = %v", err)
	}
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store
}

func writeTestSource(t *testing.T, directory string, fileName string, body string) string {
	t.Helper()
	path := filepath.Join(directory, fileName)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write source %q: %v", path, err)
	}
	return path
}

func assertDirectoryEmpty(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", path, err)
	}
	if len(entries) != 0 {
		t.Fatalf("directory %q entries = %v, want empty", path, entries)
	}
}
