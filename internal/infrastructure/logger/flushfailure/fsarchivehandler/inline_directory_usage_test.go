package fsarchivehandler

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestInlineDirectorySizeIsolatedByFailureCategoryAndSkipsScansBelowLimit(t *testing.T) {
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)

	handler := pipeline.handler
	retryTracker := handler.directorySizeTracker(failurearchive.FailureCategoryRetryable)

	var mu sync.Mutex
	scans := map[failurearchive.FailureCategory]int{}
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		mu.Lock()
		switch pattern.Dir {
		case handler.Fsstore.FileNameRetryable.Dir:
			scans[failurearchive.FailureCategoryRetryable]++
		case handler.Fsstore.FileNamePermanent.Dir:
			scans[failurearchive.FailureCategoryPermanent]++
		}
		mu.Unlock()
		return logpattern.GlobFileNames(pattern, mode)
	}

	result := retryableArchiveResult(now, "below-limit")
	for range 3 {
		if err := pipeline.HandleFailure(context.Background(), result); err != nil {
			t.Fatalf("HandleFailure below limit: %v", err)
		}
	}

	mu.Lock()
	retryScans := scans[failurearchive.FailureCategoryRetryable]
	permanentScans := scans[failurearchive.FailureCategoryPermanent]
	mu.Unlock()
	if retryScans != 0 || permanentScans != 0 {
		t.Fatalf("full scans below limit = retryable:%d permanent:%d, want 0:0", retryScans, permanentScans)
	}

	// 概算値が上限を超えた次の要求で、retryableだけがfilesystemを
	// 待機せず一度観測する。
	retryTracker.RecordGrowth(handler.Config.MaxDirSize())
	thresholdAt := now
	mutableClock.Set(thresholdAt)
	if err := pipeline.HandleFailure(context.Background(), retryableArchiveResult(thresholdAt, "threshold")); err != nil {
		t.Fatalf("HandleFailure at estimated threshold: %v", err)
	}

	mu.Lock()
	retryScans = scans[failurearchive.FailureCategoryRetryable]
	permanentScans = scans[failurearchive.FailureCategoryPermanent]
	mu.Unlock()
	if retryScans != 1 || permanentScans != 0 {
		t.Fatalf("threshold full scans = retryable:%d permanent:%d, want 1:0", retryScans, permanentScans)
	}
}

func TestInlineDirectorySizeObservesBelowLimitWhenUsageCacheTTLExpires(t *testing.T) {
	now := time.Date(2026, 8, 24, 10, 30, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)

	handler := pipeline.handler
	var mu sync.Mutex
	scans := map[failurearchive.FailureCategory]int{}
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		mu.Lock()
		switch pattern.Dir {
		case handler.Fsstore.FileNameRetryable.Dir:
			scans[failurearchive.FailureCategoryRetryable]++
		case handler.Fsstore.FileNamePermanent.Dir:
			scans[failurearchive.FailureCategoryPermanent]++
		}
		mu.Unlock()
		return logpattern.GlobFileNames(pattern, mode)
	}

	beforeUsageCacheExpiry := now.Add(testUsageCacheTTL - time.Nanosecond)
	mutableClock.Set(beforeUsageCacheExpiry)
	if err := pipeline.HandleFailure(context.Background(), retryableArchiveResult(beforeUsageCacheExpiry, "before-usage-cache-expiry")); err != nil {
		t.Fatalf("HandleFailure before usage cache expiry: %v", err)
	}
	mu.Lock()
	retryableScansBefore := scans[failurearchive.FailureCategoryRetryable]
	mu.Unlock()
	if retryableScansBefore != 0 {
		t.Fatalf("retryable scans before usage cache expiry = %d, want 0", retryableScansBefore)
	}

	atUsageCacheExpiry := now.Add(testUsageCacheTTL)
	mutableClock.Set(atUsageCacheExpiry)
	if err := pipeline.HandleFailure(context.Background(), retryableArchiveResult(atUsageCacheExpiry, "at-usage-cache-expiry")); err != nil {
		t.Fatalf("HandleFailure at usage cache expiry: %v", err)
	}

	mu.Lock()
	retryableScans := scans[failurearchive.FailureCategoryRetryable]
	permanentScans := scans[failurearchive.FailureCategoryPermanent]
	mu.Unlock()
	if retryableScans != 1 || permanentScans != 1 {
		t.Fatalf("usage cache expiry scans = retryable:%d permanent:%d, want 1:1", retryableScans, permanentScans)
	}
}

func TestInlineDirectorySizeRunsAgeScanOnNextDayBeforeEstimateThreshold(t *testing.T) {
	initial := time.Date(2026, 8, 24, 23, 59, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, initial, nil)
	mutableClock := &archivePurgeClock{now: initial}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)

	handler := pipeline.handler
	pattern := handler.Fsstore.FileNameRetryable
	oldModTime := initial.AddDate(0, 0, -3)
	oldPath := filepath.Join(pattern.Dir, pattern.Prefix+pattern.Target+"_"+oldModTime.Format("20060102")+"_1.jsonl")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatalf("write old archive: %v", err)
	}
	if err := os.Chtimes(oldPath, oldModTime, oldModTime); err != nil {
		t.Fatalf("set old archive modtime: %v", err)
	}

	var mu sync.Mutex
	retryScans := 0
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		if pattern.Dir == handler.Fsstore.FileNameRetryable.Dir {
			mu.Lock()
			retryScans++
			mu.Unlock()
		}
		return logpattern.GlobFileNames(pattern, mode)
	}

	nextDay := initial.Add(2 * time.Minute)
	mutableClock.Set(nextDay)
	if err := pipeline.HandleFailure(context.Background(), retryableArchiveResult(nextDay, "next-day")); err != nil {
		t.Fatalf("HandleFailure next day: %v", err)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old archive stat error = %v, want not exist after next-day age purge", err)
	}

	mu.Lock()
	firstDayScans := retryScans
	mu.Unlock()
	if firstDayScans != 1 {
		t.Fatalf("next-day retryable scans = %d, want 1", firstDayScans)
	}
	if err := pipeline.HandleFailure(context.Background(), retryableArchiveResult(nextDay, "same-day")); err != nil {
		t.Fatalf("HandleFailure same day: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if retryScans != firstDayScans {
		t.Fatalf("same-day age scan repeated: before=%d after=%d", firstDayScans, retryScans)
	}
}

func TestInlineDirectorySizeCountsProtectedFilesButDoesNotDeleteThem(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)

	handler := pipeline.handler
	dir := handler.Fsstore.FileNameRetryable.Dir
	protectedPath := filepath.Join(dir, "other-protected_20260824_1.jsonl")
	deletablePath := filepath.Join(dir, "other-deletable_20260824_1.jsonl")
	writeArchiveSizedFile(t, protectedPath, 8*1024*1024, now.Add(-time.Hour))
	writeArchiveSizedFile(t, deletablePath, 4*1024*1024, now.Add(-2*time.Hour))

	protectedPaths := handler.protectRotatedFileBeforeTransition(
		failurearchive.FailureCategoryRetryable,
		protectedPath,
	)
	defer handler.releaseArchivePathProtection(
		failurearchive.FailureCategoryRetryable,
		protectedPaths,
	)
	tracker := handler.directorySizeTracker(failurearchive.FailureCategoryRetryable)
	tracker.RecordGrowth(handler.Config.MaxDirSize() + 1)
	mutableClock.Set(now)

	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("trigger retryable cleanup: %v", err)
	}
	if _, err := os.Stat(protectedPath); err != nil {
		t.Fatalf("protected archive was deleted: %v", err)
	}
	if _, err := os.Stat(deletablePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unprotected archive stat error = %v, want not exist", err)
	}

}

func TestWriteArchiveJSONLReportsPartialWriteBytesOnError(t *testing.T) {
	sentinel := errors.New("partial write")
	partial := &partialArchiveWriter{limit: 7, err: sentinel}
	written, err := writeArchiveJSONL(partial, map[string]any{"payload": "long-enough"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("writeArchiveJSONL error = %v, want %v", err, sentinel)
	}
	if written != 7 {
		t.Fatalf("writeArchiveJSONL written = %d, want 7", written)
	}
}

func TestRotationStartsDirectoryObservationWhenCompressionAndUploadAreDisabled(t *testing.T) {
	now := time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.maxFileBackups = 2
	})
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	scans := 0
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		scans++
		return logpattern.GlobFileNames(pattern, mode)
	}

	handler.Fsstore.Mu.Lock()
	_, err := handler.rotateRetryable(context.Background(), now.AddDate(0, 0, 1))
	handler.Fsstore.Mu.Unlock()
	if err != nil {
		t.Fatalf("rotate retryable archive: %v", err)
	}
	if scans != 0 {
		t.Fatalf("rotation must not scan while the file-store lock is held: scans = %d", scans)
	}

	mutableClock.Set(now)
	if err = handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("cleanup after rotation: %v", err)
	}
	if scans != 1 {
		t.Fatalf("filesystem scans after rotation = %d, want 1", scans)
	}
}

func TestInlineGenerationLimitUsesNaturalDirectoryObservation(t *testing.T) {
	now := time.Date(2026, 8, 24, 14, 30, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.maxFileBackups = 2
	})
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	handler.retryableDirectory.lastAgePurgeAt = now

	pattern := handler.Fsstore.FileNameRetryable
	baseName := pattern.Prefix + pattern.Target
	paths := []string{
		filepath.Join(pattern.Dir, baseName+"_20260820_1."+pattern.Extension),
		filepath.Join(pattern.Dir, baseName+"_20260821_1."+pattern.Extension),
		filepath.Join(pattern.Dir, baseName+"_20260822_1."+pattern.Extension),
	}
	for index, path := range paths {
		writeArchiveSizedFile(t, path, 1, now.Add(time.Duration(index-3)*time.Hour))
	}

	scans := 0
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		scans++
		return logpattern.GlobFileNames(pattern, mode)
	}

	firstAttempt := now
	mutableClock.Set(firstAttempt)
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("cleanup before directory observation TTL: %v", err)
	}
	if scans != 0 {
		t.Fatalf("filesystem scans before directory observation TTL = %d, want 0", scans)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("generation removed before natural observation %q: %v", path, err)
		}
	}

	mutableClock.Set(firstAttempt.Add(testUsageCacheTTL))
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("generation cleanup at directory observation TTL: %v", err)
	}
	if scans != 1 {
		t.Fatalf("generation-limit filesystem scans at TTL = %d, want 1", scans)
	}
	if _, err := os.Stat(paths[0]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest generation stat error = %v, want not exist", err)
	}
	for _, path := range paths[1:] {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("retained generation %q: %v", path, err)
		}
	}

	mutableClock.Set(firstAttempt.Add(testUsageCacheTTL))
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("post-generation cleanup: %v", err)
	}
	if scans != 1 {
		t.Fatalf("filesystem scans after observed generation count = %d, want 1", scans)
	}
}

func TestInlineDirectorySizeObservationFailureRetriesImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC)
		pipeline := newTestArchiveHandler(t, now, nil)
		mutableClock := &archivePurgeClock{now: now}
		pipeline.handler.Clock = mutableClock
		initializeInlineArchiveTrackers(t, pipeline)
		handler := pipeline.handler
		tracker := handler.directorySizeTracker(failurearchive.FailureCategoryRetryable)
		sentinel := errors.New("directory listing unavailable")
		handler.retryableDirectory.lastAgePurgeAt = time.Time{}

		listCalls := 0
		started := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseScan := func() { releaseOnce.Do(func() { close(release) }) }
		// 試験が途中で失敗しても、止めた走査を再開してから片付けます。
		t.Cleanup(releaseScan)
		handler.globArchiveFiles = func(
			logpattern.FileNamePattern,
			logpattern.GlobMode,
		) ([]string, error) {
			listCalls++
			if listCalls == 1 {
				close(started)
				<-release
			}
			return nil, sentinel
		}
		tracker.RecordGrowth(handler.Config.MaxDirSize() + 1)
		attemptAt := now
		mutableClock.Set(attemptAt)
		result := make(chan error, 1)
		go func() {
			result <- handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
		}()
		<-started
		// 実行中のcleanup要求は待機・pending化せず破棄する。
		for request := 0; request < 2; request++ {
			if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
				t.Fatalf("concurrent cleanup request %d: %v", request, err)
			}
		}
		releaseScan()
		if err := <-result; !errors.Is(err, sentinel) {
			t.Fatalf("first observation error = %v, want %v", err, sentinel)
		}
		if listCalls != 1 {
			t.Fatalf("failed owner calls = %d, want 1", listCalls)
		}

		// 観測失敗後も概算値は上限超過のままなので、失敗完了後の次の外部要求で
		// 直ちに再試行する。
		tracker.RecordGrowth(handler.Config.MaxDirSize() + 1)
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); !errors.Is(err, sentinel) {
			t.Fatalf("immediate retry observation error = %v, want %v", err, sentinel)
		}
		if listCalls != 2 {
			t.Fatalf("failed listing calls after immediate retry = %d, want 2", listCalls)
		}
		if got := handler.retryableDirectory.lastAgePurgeAt; !got.IsZero() {
			t.Fatalf("retryable lastAgePurgeAt after failed observations = %s, want zero", got)
		}
	})
}

func TestInlineDirectorySizeProductionObservationIgnoresIndividualStatFailures(t *testing.T) {
	now := time.Date(2026, 8, 24, 15, 30, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	tracker := handler.directorySizeTracker(failurearchive.FailureCategoryRetryable)

	danglingPath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "dangling.jsonl")
	testutil.Symlink(t, filepath.Join(t.TempDir(), "missing"), danglingPath)
	tracker.RecordGrowth(handler.Config.MaxDirSize() + 1)
	attemptAt := now
	mutableClock.Set(attemptAt)
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("best-effort observation with individual stat failure: %v", err)
	}
	if _, err := os.Lstat(danglingPath); err != nil {
		t.Fatalf("best-effort observation removed dangling symlink: %v", err)
	}
	if _, err := os.Stat(handler.Fsstore.ActiveFilePathRetryable); err != nil {
		t.Fatalf("best-effort observation damaged active archive: %v", err)
	}
}

func TestConcurrentHandleFailureWritesAllRecords(t *testing.T) {
	now := time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler

	const writers = 32
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			errs <- pipeline.HandleFailure(
				context.Background(),
				retryableArchiveResult(now, string(rune('a'+index%26))),
			)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent HandleFailure: %v", err)
		}
	}

	contents, err := os.ReadFile(handler.Fsstore.ActiveFilePathRetryable)
	if err != nil {
		t.Fatalf("read active retryable archive: %v", err)
	}
	if got := bytes.Count(contents, []byte{'\n'}); got != writers {
		t.Fatalf("concurrent archive records = %d, want %d", got, writers)
	}
}

func TestConcurrentCleanupDropsRequestsDuringObservationAndChecksGrowthOnNextRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 24, 17, 0, 0, 0, time.UTC)
		pipeline := newTestArchiveHandler(t, now, nil)
		mutableClock := &archivePurgeClock{now: now}
		pipeline.handler.Clock = mutableClock
		initializeInlineArchiveTrackers(t, pipeline)
		handler := pipeline.handler
		tracker := handler.directorySizeTracker(failurearchive.FailureCategoryRetryable)
		tracker.RecordGrowth(handler.Config.MaxDirSize() + 1)
		observationAt := now
		mutableClock.Set(observationAt)

		started := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseScan := func() { releaseOnce.Do(func() { close(release) }) }
		// 試験が途中で失敗しても、止めた走査を再開してから片付けます。
		t.Cleanup(releaseScan)
		var mu sync.Mutex
		scans := 0
		handler.globArchiveFiles = func(
			pattern logpattern.FileNamePattern,
			mode logpattern.GlobMode,
		) ([]string, error) {
			mu.Lock()
			scans++
			current := scans
			mu.Unlock()
			if current == 1 {
				close(started)
				<-release
			}
			return logpattern.GlobFileNames(pattern, mode)
		}

		done := make(chan error, 1)
		go func() {
			done <- handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
		}()
		select {
		case <-started:
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("timed out waiting for first cleanup scan")
		}

		// 観測中の概算signalはtrackerへ反映するが、実行中のcleanup要求は破棄する。
		tracker.RecordGrowth(handler.Config.MaxDirSize() + 1)
		for request := 0; request < 3; request++ {
			if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
				t.Fatalf("concurrent cleanup request %d: %v", request, err)
			}
		}
		releaseScan()
		if err := <-done; err != nil {
			t.Fatalf("cleanup owner: %v", err)
		}

		mu.Lock()
		gotScans := scans
		mu.Unlock()
		if gotScans != 1 {
			t.Fatalf("concurrent observation scans = %d, want exactly 1", gotScans)
		}

		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("cleanup after completed observation: %v", err)
		}
		mu.Lock()
		gotScans = scans
		mu.Unlock()
		if gotScans != 2 {
			t.Fatalf("observation scans after the next request = %d, want 2", gotScans)
		}
	})
}

func TestConcurrentCleanupDropsMidnightRequestAndChecksDateOnNextRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		initial := time.Date(2026, 8, 24, 23, 53, 0, 0, time.UTC)
		pipeline := newTestArchiveHandler(t, initial, nil)
		mutableClock := &archivePurgeClock{now: initial}
		pipeline.handler.Clock = mutableClock
		initializeInlineArchiveTrackers(t, pipeline)
		handler := pipeline.handler
		handler.directorySizeTracker(failurearchive.FailureCategoryRetryable).RecordGrowth(
			handler.Config.MaxDirSize() + 1,
		)
		preMidnight := initial.Add(6 * time.Minute)
		mutableClock.Set(preMidnight)

		started := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseScan := func() { releaseOnce.Do(func() { close(release) }) }
		// 試験が途中で失敗しても、止めた走査を再開してから片付けます。
		t.Cleanup(releaseScan)
		var mu sync.Mutex
		scans := 0
		handler.globArchiveFiles = func(
			pattern logpattern.FileNamePattern,
			mode logpattern.GlobMode,
		) ([]string, error) {
			mu.Lock()
			scans++
			current := scans
			mu.Unlock()
			if current == 1 {
				close(started)
				<-release
			}
			return logpattern.GlobFileNames(pattern, mode)
		}

		done := make(chan error, 1)
		go func() {
			done <- handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
		}()
		select {
		case <-started:
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("timed out waiting for pre-midnight cleanup scan")
		}

		nextDay := preMidnight.Add(2 * time.Minute)
		mutableClock.Set(nextDay)
		for request := 0; request < 3; request++ {
			if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
				t.Fatalf("concurrent next-day cleanup request %d: %v", request, err)
			}
		}
		releaseScan()
		if err := <-done; err != nil {
			t.Fatalf("cleanup owner: %v", err)
		}

		mu.Lock()
		gotScans := scans
		mu.Unlock()
		if gotScans != 1 {
			t.Fatalf("concurrent midnight scans = %d, want one in-flight observation", gotScans)
		}
		if got := handler.retryableDirectory.lastAgePurgeAt; !got.Equal(initial) {
			t.Fatalf("retryable lastAgePurgeAt = %s, want previous age-purge time %s", got, initial)
		}

		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("cleanup after midnight observation completed: %v", err)
		}
		mu.Lock()
		gotScans = scans
		mu.Unlock()
		if gotScans != 2 {
			t.Fatalf("midnight scans after the next request = %d, want 2", gotScans)
		}
		if got := handler.retryableDirectory.lastAgePurgeAt; !got.Equal(nextDay) {
			t.Fatalf("retryable lastAgePurgeAt = %s, want next request time %s", got, nextDay)
		}
	})
}

func TestBackgroundArchiveUnprotectTriggersInlineCleanup(t *testing.T) {
	now := time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)
	recorder := newRecordingFailureArchiveUploader(nil, nil)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, recorder)
	})
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	// 構築時のtarget-only予約を消費し、directory全体の観測で概算を初期化します。
	if !pipeline.handler.retryableDirectory.uploadRetry.claimForAttempt(now, testUsageCacheTTL) {
		t.Fatal("failed to consume construction bootstrap schedule")
	}
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler

	oldPath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260823_1.jsonl")
	if err := os.WriteFile(oldPath, []byte("rotated"), 0o600); err != nil {
		t.Fatalf("write rotated archive: %v", err)
	}
	var mu sync.Mutex
	scans := 0
	scanCompleted := make(chan struct{}, 1)
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		mu.Lock()
		scans++
		mu.Unlock()
		paths, err := logpattern.GlobFileNames(pattern, mode)
		select {
		case scanCompleted <- struct{}{}:
		default:
		}
		return paths, err
	}
	handler.directorySizeTracker(failurearchive.FailureCategoryRetryable).RecordGrowth(
		handler.Config.MaxDirSize() + 1,
	)
	mutableClock.Set(now)

	handler.dispatchArchiveRotatedFile(
		context.Background(),
		handler.Fsstore.ActiveFilePathRetryable,
		oldPath,
		failurearchive.FailureCategoryRetryable,
	)
	_ = receiveArchiveUploadInput(t, recorder)

	select {
	case <-scanCompleted:
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("timed out waiting for background unprotect scan")
	}
	mu.Lock()
	defer mu.Unlock()
	if scans != 1 {
		t.Fatalf("background unprotect scans = %d, want 1", scans)
	}
}

func TestAgeDeleteFailureStopsBeforeDirectorySizePurge(t *testing.T) {
	now := time.Date(2026, 8, 24, 19, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	dir := handler.Fsstore.FileNameRetryable.Dir

	failingPath := filepath.Join(dir, "test-app_20260821_1.jsonl")
	if err := os.WriteFile(failingPath, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write failing archive: %v", err)
	}
	old := now.AddDate(0, 0, -3)
	if err := os.Chtimes(failingPath, old, old); err != nil {
		t.Fatalf("set failing archive modtime: %v", err)
	}
	oversizedPath := filepath.Join(dir, "other_20260824_1.jsonl")
	writeArchiveSizedFile(t, oversizedPath, 11*1024*1024, now.Add(-time.Hour))
	restoreRemoval := testutil.DenyFileRemoval(t, failingPath)
	defer restoreRemoval()
	// Unixの削除拒否はdirectory全体に効くため、file残存だけでなく
	// 後続の容量purge自体が開始されていないことも確認します。
	purgeStages := &inlineArchivePurgeStageRecorder{}
	handler.Tracer = purgeStages

	handler.retryableDirectory.lastAgePurgeAt = time.Time{}
	attemptAt := now
	mutableClock.Set(attemptAt)
	err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
	assertInlineArchiveRemovalDenied(t, err, failingPath)
	purgeStages.assertStages(t, SpanFilePurgeByAge)
	if _, err := os.Stat(failingPath); err != nil {
		t.Fatalf("failed age-purge target disappeared: %v", err)
	}
	if _, err := os.Stat(oversizedPath); err != nil {
		t.Fatalf("directory-size purge ran after age failure: %v", err)
	}
	if got := handler.retryableDirectory.lastAgePurgeAt; !got.Equal(attemptAt) {
		t.Fatalf("retryable lastAgePurgeAt = %s, want attempted age purge time %s", got, attemptAt)
	}
}

func TestGenerationDeleteFailureStopsBeforeDirectorySizePurge(t *testing.T) {
	now := time.Date(2026, 8, 24, 20, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	dir := handler.Fsstore.FileNameRetryable.Dir

	failingPath := filepath.Join(dir, "test-app_20260823_1.jsonl")
	if err := os.WriteFile(failingPath, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write failing archive: %v", err)
	}
	if err := os.Chtimes(failingPath, now.Add(-3*time.Hour), now.Add(-3*time.Hour)); err != nil {
		t.Fatalf("set failing archive modtime: %v", err)
	}
	writeArchiveSizedFile(
		t,
		filepath.Join(dir, "test-app_20260823_2.jsonl"),
		1,
		now.Add(-2*time.Hour),
	)
	writeArchiveSizedFile(
		t,
		filepath.Join(dir, "test-app_20260823_3.jsonl"),
		1,
		now.Add(-time.Hour),
	)
	oversizedPath := filepath.Join(dir, "other_20260824_1.jsonl")
	writeArchiveSizedFile(t, oversizedPath, 11*1024*1024, now.Add(-30*time.Minute))
	restoreRemoval := testutil.DenyFileRemoval(t, failingPath)
	defer restoreRemoval()
	purgeStages := &inlineArchivePurgeStageRecorder{}
	handler.Tracer = purgeStages

	// 当日分のage purgeは完了済みとして、generationの削除失敗を検証します。
	lastAgePurgeAt := now.Add(-time.Hour)
	handler.retryableDirectory.lastAgePurgeAt = lastAgePurgeAt
	handler.directorySizeTracker(failurearchive.FailureCategoryRetryable).RecordGrowth(
		handler.Config.MaxDirSize() + 1,
	)
	attemptAt := now
	mutableClock.Set(attemptAt)
	err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
	assertInlineArchiveRemovalDenied(t, err, failingPath)
	purgeStages.assertStages(t, SpanFilePurgeByGeneration)
	if _, err := os.Stat(failingPath); err != nil {
		t.Fatalf("failed generation-purge target disappeared: %v", err)
	}
	if _, err := os.Stat(oversizedPath); err != nil {
		t.Fatalf("directory-size purge ran after generation failure: %v", err)
	}
	if got := handler.retryableDirectory.lastAgePurgeAt; !got.Equal(lastAgePurgeAt) {
		t.Fatalf("retryable lastAgePurgeAt = %s, want previous age purge time %s", got, lastAgePurgeAt)
	}
}

func TestInlineGenerationPurgeFailureRetriesAtNextNaturalObservation(t *testing.T) {
	now := time.Date(2026, 8, 24, 20, 30, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	mutableClock := &archivePurgeClock{now: now}
	pipeline.handler.Clock = mutableClock
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	dir := handler.Fsstore.FileNameRetryable.Dir

	failingPath := filepath.Join(dir, "test-app_20260823_1.jsonl")
	if err := os.WriteFile(failingPath, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write failing archive: %v", err)
	}
	if err := os.Chtimes(failingPath, now.Add(-3*time.Hour), now.Add(-3*time.Hour)); err != nil {
		t.Fatalf("set failing archive modtime: %v", err)
	}
	writeArchiveSizedFile(
		t,
		filepath.Join(dir, "test-app_20260823_2.jsonl"),
		1,
		now.Add(-2*time.Hour),
	)
	writeArchiveSizedFile(
		t,
		filepath.Join(dir, "test-app_20260823_3.jsonl"),
		1,
		now.Add(-time.Hour),
	)
	restoreRemoval := testutil.DenyFileRemoval(t, failingPath)
	defer restoreRemoval()
	// age purgeを省略し、generation失敗後の観測cacheを検証します。
	handler.retryableDirectory.lastAgePurgeAt = now
	purgeStages := &inlineArchivePurgeStageRecorder{}
	handler.Tracer = purgeStages

	scans := 0
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		scans++
		return logpattern.GlobFileNames(pattern, mode)
	}
	tracker := handler.directorySizeTracker(failurearchive.FailureCategoryRetryable)
	tracker.RecordGrowth(handler.Config.MaxDirSize() + 1)
	firstAttempt := now
	mutableClock.Set(firstAttempt)
	err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
	assertInlineArchiveRemovalDenied(t, err, failingPath)
	purgeStages.assertStages(t, SpanFilePurgeByGeneration)
	if scans != 1 {
		t.Fatalf("filesystem observations after purge failure = %d, want 1", scans)
	}

	// directory使用量の観測自体は完了しているため、generation削除失敗だけを理由に
	// 追加走査は開始しません。
	mutableClock.Set(firstAttempt)
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("cleanup before next natural observation: %v", err)
	}
	if scans != 1 {
		t.Fatalf("filesystem observations before TTL = %d, want 1", scans)
	}
	purgeStages.assertStages(t, SpanFilePurgeByGeneration)

	// TTL満了後の次の自然なdirectory観測で実fileから再評価し、削除を再試行します。
	mutableClock.Set(firstAttempt.Add(testUsageCacheTTL))
	err = handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
	assertInlineArchiveRemovalDenied(t, err, failingPath)
	purgeStages.assertStages(t, SpanFilePurgeByGeneration, SpanFilePurgeByGeneration)
	if scans != 2 {
		t.Fatalf("filesystem observations after TTL retry = %d, want 2", scans)
	}
	if _, err := os.Stat(failingPath); err != nil {
		t.Fatalf("failed generation-purge target disappeared: %v", err)
	}
}

func TestArchiveDirectoryBytesIncludesAllObservedFiles(t *testing.T) {
	pattern := logpattern.FileNamePattern{
		Dir:       "/archive",
		Prefix:    "failure_",
		Target:    "test-app",
		Extension: "jsonl",
	}
	activePath := filepath.Join(pattern.Dir, "failure_test-app_20260824_1.jsonl")
	backupPath := filepath.Join(pattern.Dir, "failure_test-app_20260823_1.jsonl.gz")
	otherPath := filepath.Join(pattern.Dir, "other_20260823_1.jsonl")
	files := []fsstore.FileInfo{
		{Path: activePath, Size: 2},
		{Path: backupPath, Size: 3},
		{Path: otherPath, Size: 5},
	}

	if got := archiveDirectoryBytes(files); got != 10 {
		t.Fatalf("archive directory bytes = %d, want 10", got)
	}
}

// 保持数は圧縮形式ごとではなく、日付・番号で決まる世代単位で数えます。
func TestExecuteArchiveGenerationPurgeRemovesRawAndCompressedFilesOfExpiredGeneration(t *testing.T) {
	now := time.Date(2026, 8, 24, 20, 30, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.maxFileBackups = 2
	})
	directory := pipeline.handler.Fsstore.FileNameRetryable.Dir
	files := []fsstore.FileInfo{
		{Path: filepath.Join(directory, "test-app_20260823_1.jsonl"), ModTime: now.Add(-6 * time.Hour)},
		{Path: filepath.Join(directory, "test-app_20260823_2.jsonl"), ModTime: now.Add(-5 * time.Hour)},
		{Path: filepath.Join(directory, "test-app_20260823_3.jsonl"), ModTime: now.Add(-4 * time.Hour)},
		{Path: filepath.Join(directory, "test-app_20260823_1.jsonl.gz"), ModTime: now.Add(-3 * time.Hour)},
		{Path: filepath.Join(directory, "test-app_20260823_2.jsonl.gz"), ModTime: now.Add(-2 * time.Hour)},
	}
	for _, file := range files {
		writeArchiveSizedFile(t, file.Path, 1, file.ModTime)
	}

	var result fsstore.PurgeResult
	guard := pipeline.handler.retryableDirectory.guard
	if err := guard.RunWithLifecycleLock(func() error {
		guard.RunPurgeWithProtection(files, func(map[string]struct{}) map[string]struct{} {
			return nil
		}, func(purge *appendlog.Purge) {
			result = pipeline.handler.executeArchiveGenerationPurge(
				context.Background(),
				purge,
				files,
				"jsonl",
			)
		})
		return nil
	}); err != nil {
		t.Fatalf("purge archive generations: %v", err)
	}
	if !result.Ran {
		t.Fatal("generation purge did not run for generations over limit")
	}
	// 3世代のうち最も古い第1世代の未圧縮・圧縮fileをまとめて削除します。
	for _, expired := range []fsstore.FileInfo{files[0], files[3]} {
		if _, err := os.Stat(expired.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("oldest generation archive %q stat error = %v, want not exist", expired.Path, err)
		}
	}
	for _, file := range []fsstore.FileInfo{files[1], files[2], files[4]} {
		if _, err := os.Stat(file.Path); err != nil {
			t.Fatalf("retained archive %q: %v", file.Path, err)
		}
	}
}

func TestArchivePurgeWithoutRuntimeGuardUsesSharedProtection(t *testing.T) {
	for _, phase := range []string{"age", "generation", "directory_size"} {
		t.Run(phase, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
			cfg := newValidArchiveHandlerConfig(t)
			cfg.maxFileBackups = 2
			if phase == "directory_size" {
				cfg.maxFileBackups = 3
				cfg.maxDirSize = 5
			}
			fileName := logpattern.FileNamePattern{
				Dir:       t.TempDir(),
				Target:    "test-app",
				Extension: "jsonl",
			}
			// Direct construction deliberately has no directory runtime registration.
			handler := &FsArchiveHandler{
				Config: cfg,
				Clock:  archiveFixedClock{now: now},
				Tracer: observer.NopTracer{},
				Meter:  observer.NopMeter{},
			}
			var files []fsstore.FileInfo
			for index, name := range []string{
				"test-app_20260901_1.jsonl",    // another writer's active file
				"test-app_20260901_2.jsonl.gz", // another owner's retained upload
				"test-app_20260901_2.jsonl",    // its raw generation peer
				"test-app_20260901_3.jsonl.gz", // this task's pending upload
				"test-app_20260901_3.jsonl",    // its raw generation peer
				"test-app_20260901_4.jsonl",
				"test-app_20260901_5.jsonl",
				"test-app_20260901_6.jsonl",
			} {
				file := fsstore.FileInfo{
					Path:    filepath.Join(fileName.Dir, name),
					Size:    1,
					ModTime: now.AddDate(0, 0, -30).Add(time.Duration(index) * time.Second),
				}
				writeArchiveSizedFile(t, file.Path, 1, file.ModTime)
				files = append(files, file)
			}
			guard := appendlog.DirectoryGuardForPath(fileName.Dir)
			owner := appendlog.NewDirectoryGuardToken()
			if err := guard.TransitionActive(owner, func() (string, error) {
				return files[0].Path, nil
			}); err != nil {
				t.Fatalf("register another writer: %v", err)
			}
			guard.ProtectPaths(owner, files[1].Path)
			t.Cleanup(func() { guard.RemoveToken(owner) })

			purgeResult := handler.purgeObservedArchiveFiles(
				context.Background(),
				failurearchive.FailureCategoryRetryable,
				time.Time{},
				now,
				phase == "age",
				newArchiveDirectoryInspection(fsstore.FileInspection{RegularFiles: files, Paths: archiveInfoPathsForTest(files)}, fileName),
				[]string{files[3].Path},
			)
			err := handler.reportArchivePurge(context.Background(), purgeResult, now)
			if err != nil {
				t.Fatalf("purge without runtime guard: %v", err)
			}
			if purgeResult.ageResult.Ran != (phase == "age") {
				t.Fatalf("age ran = %v, phase = %s", purgeResult.ageResult.Ran, phase)
			}
			wantRemainingBytes := uint64(5)
			if phase == "generation" {
				wantRemainingBytes = 7
			}
			if purgeResult.observedDirectoryBytes != wantRemainingBytes {
				t.Fatalf("remaining bytes = %d, want %d including protected files", purgeResult.observedDirectoryBytes, wantRemainingBytes)
			}
			for index, file := range files {
				_, err := os.Stat(file.Path)
				wantPresent := index < 5 || (phase == "generation" && index > 5)
				if wantPresent && err != nil {
					t.Errorf("retained archive %q: %v", file.Path, err)
				} else if !wantPresent && !errors.Is(err, os.ErrNotExist) {
					t.Errorf("purged archive %q stat = %v, want absent", file.Path, err)
				}
				assertArchivePathProtected(t, guard, file.Path, index < 2)
			}
		})
	}
}

func TestInlineDirectorySizeWarnsWhenActiveArchivesAloneExceedLimit(t *testing.T) {
	now := time.Date(2026, 8, 24, 22, 0, 0, 0, time.UTC)
	var warnings []*logging.LogEntry
	pipeline := newTestArchiveHandler(
		t,
		now,
		nil,
		InternalLogHandlers{OnWarn: func(
			_ context.Context,
			entry *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			warnings = append(warnings, entry)
		}},
	)
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	handler.retryableDirectory.lastAgePurgeAt = now

	activePath := handler.Fsstore.ActiveFilePathRetryable
	activeBytes := handler.Config.MaxDirSize() + 1
	if err := os.Truncate(activePath, int64(activeBytes)); err != nil {
		t.Fatalf("enlarge active retryable archive: %v", err)
	}
	handler.directorySizeTracker(failurearchive.FailureCategoryRetryable).RecordGrowth(activeBytes)

	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("cleanup active-only oversized directory: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("OnWarn calls = %d, want 1", len(warnings))
	}
	warning := warnings[0]
	if warning.Level != pkglogger.LogLevelWarn ||
		warning.Message != WarnMsgActiveArchiveFilesExceedMaxDirSize {
		t.Fatalf("warning = %#v, want active archive size warning", warning)
	}
	if got := warning.Fields["failureCategory"]; got != failurearchive.FailureCategoryRetryable {
		t.Fatalf("failureCategory = %v, want retryable", got)
	}
	if got := warning.Fields["activeFileSize"]; got != activeBytes {
		t.Fatalf("activeFileSize = %v, want %d", got, activeBytes)
	}
	if got := warning.Fields["directorySize"]; got != activeBytes {
		t.Fatalf("directorySize = %v, want %d", got, activeBytes)
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("active retryable archive was removed: %v", err)
	}
}

func TestInlineDirectorySizeWarnsWhenNoArchivesAreEligibleForPurge(t *testing.T) {
	now := time.Date(2026, 8, 24, 22, 30, 0, 0, time.UTC)
	var warnings []*logging.LogEntry
	pipeline := newTestArchiveHandler(
		t,
		now,
		nil,
		InternalLogHandlers{OnWarn: func(
			_ context.Context,
			entry *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			warnings = append(warnings, entry)
		}},
	)
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	handler.retryableDirectory.lastAgePurgeAt = now

	protectedPath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"other-retained_20260824_1.jsonl",
	)
	protectedBytes := int64(handler.Config.MaxDirSize() + 1)
	writeArchiveSizedFile(t, protectedPath, protectedBytes, now.Add(-time.Hour))
	state := handler.directoryForFailureCategory(failurearchive.FailureCategoryRetryable)
	state.guard.ProtectPaths(state.token, protectedPath)
	defer state.guard.UnprotectPaths(state.token, protectedPath)
	handler.directorySizeTracker(failurearchive.FailureCategoryRetryable).RecordGrowth(
		uint64(protectedBytes),
	)

	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("cleanup protected-only oversized directory: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("OnWarn calls = %d, want 1", len(warnings))
	}
	warning := warnings[0]
	if warning.Level != pkglogger.LogLevelWarn ||
		warning.Message != WarnMsgNoArchivesEligibleForDirectorySizePurge {
		t.Fatalf("warning = %#v, want no eligible archive warning", warning)
	}
	if got := warning.Fields["failureCategory"]; got != failurearchive.FailureCategoryRetryable {
		t.Fatalf("failureCategory = %v, want retryable", got)
	}
	if got := warning.Fields["remainingFiles"]; got != 2 {
		t.Fatalf("remainingFiles = %v, want 2", got)
	}
	if _, err := os.Stat(protectedPath); err != nil {
		t.Fatalf("retained archive was removed: %v", err)
	}
}

func TestArchiveDirectorySizeRemovalFailureDelegatesOnErrorOnce(t *testing.T) {
	now := time.Date(2026, 8, 24, 23, 0, 0, 0, time.UTC)
	sentinel := errors.New("remove failure")
	var (
		delegatedErrors  []error
		delegatedEntries []*logging.LogEntry
	)
	pipeline := newTestArchiveHandler(
		t,
		now,
		nil,
		InternalLogHandlers{OnError: func(
			_ context.Context,
			err error,
			entry *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			delegatedErrors = append(delegatedErrors, err)
			delegatedEntries = append(delegatedEntries, entry)
		}},
	)

	result := fsstore.PurgeResult{
		Ran: true,
		RemovalResults: []fsstore.FileRemovalResult{{
			File: fsstore.FileInfo{
				Path: filepath.Join(t.TempDir(), "failed.jsonl"),
			},

			Err: sentinel,
		}},
	}
	err := pipeline.handler.reportArchiveDirectorySizePurge(
		context.Background(),
		result,
		archiveDirectorySizePurgeDiagnostic{},
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("report error = %v, want %v", err, sentinel)
	}
	if len(delegatedErrors) != 1 || !errors.Is(delegatedErrors[0], sentinel) {
		t.Fatalf("OnError calls = %v, want one error containing %v", delegatedErrors, sentinel)
	}
	if len(delegatedEntries) != 1 ||
		delegatedEntries[0].Level != pkglogger.LogLevelError ||
		delegatedEntries[0].Message != "failed to purge logs by directory size" {
		t.Fatalf("OnError entries = %#v, want one directory-size removal failure", delegatedEntries)
	}
}

func TestArchiveDirectorySizeRemovalFailureThroughCleanupDelegatesOnErrorOnce(t *testing.T) {
	now := time.Date(2026, 8, 24, 23, 30, 0, 0, time.UTC)
	var delegatedErrors []error
	pipeline := newTestArchiveHandler(
		t,
		now,
		nil,
		InternalLogHandlers{OnError: func(
			_ context.Context,
			err error,
			_ *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			delegatedErrors = append(delegatedErrors, err)
		}},
	)
	initializeInlineArchiveTrackers(t, pipeline)
	handler := pipeline.handler
	handler.retryableDirectory.lastAgePurgeAt = now

	// non-empty directoryをfileとして観測させ、Stat成功後のRemoveだけを
	// 確実に失敗させます。cleanup outerと詳細reporterが同じ失敗を
	// 二重通知しないことを実経路で確認します。
	failingPath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260823_1.jsonl",
	)
	if err := os.Mkdir(failingPath, 0o700); err != nil {
		t.Fatalf("create non-removable observed path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(failingPath, "child"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write child in non-removable observed path: %v", err)
	}
	handler.globArchiveFiles = func(
		logpattern.FileNamePattern,
		logpattern.GlobMode,
	) ([]string, error) {
		return []string{failingPath}, nil
	}
	handler.inspectArchiveFiles = func(paths []string) fsstore.FileInspection {
		return fsstore.FileInspection{Paths: paths, RegularFiles: []fsstore.FileInfo{{
			Path:    failingPath,
			Size:    handler.Config.MaxDirSize() + 1,
			ModTime: now.Add(-time.Hour),
		}}}
	}
	handler.directorySizeTracker(failurearchive.FailureCategoryRetryable).RecordGrowth(
		handler.Config.MaxDirSize() + 1,
	)

	err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
	if err == nil {
		t.Fatal("cleanup error = nil, want directory removal failure")
	}
	if len(delegatedErrors) != 1 || !errors.Is(err, delegatedErrors[0]) {
		t.Fatalf("OnError calls = %v, want the removal failure exactly once", delegatedErrors)
	}
}

type archivePurgeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (c *archivePurgeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *archivePurgeClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

type partialArchiveWriter struct {
	writer io.Writer
	limit  int
	err    error
}

type blockingArchivePurgeTracer struct {
	spanName string
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (t *blockingArchivePurgeTracer) StartSpan(
	ctx context.Context,
	name string,
	_ ...observer.Attr,
) (context.Context, observer.Span) {
	if name == t.spanName {
		t.once.Do(func() { close(t.entered) })
		<-t.release
	}
	return ctx, observer.NopSpan{}
}

func (w *partialArchiveWriter) Write(p []byte) (int, error) {
	limit := w.limit
	if limit > len(p) {
		limit = len(p)
	}
	if limit < 0 {
		limit = 0
	}
	if w.writer == nil {
		return limit, w.err
	}
	n, err := w.writer.Write(p[:limit])
	if err != nil {
		return n, err
	}
	return n, w.err
}

func initializeInlineArchiveTrackers(t *testing.T, pipeline *FailureArchivePipeline) {
	t.Helper()
	if err := pipeline.HandleFailure(context.Background(), loggerfailure.SendResult{}); err != nil {
		t.Fatalf("initialize inline directory usage: %v", err)
	}
}

func retryableArchiveResult(now time.Time, id string) loggerfailure.SendResult {
	return loggerfailure.SendResult{
		Meta: loggerfailure.Meta{AttemptedAt: now},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord(id, now, 429, "retryable"),
		},
	}
}

func writeArchiveSizedFile(t *testing.T, path string, size int64, modTime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create archive %q: %v", path, err)
	}
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("truncate archive %q: %v", path, err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("set archive %q modtime: %v", path, err)
	}
}

// inlineArchivePurgeStageRecorderは、削除失敗が意図したpurge段階で発生し、
// 後続段階へ進まなかったことを確認します。
type inlineArchivePurgeStageRecorder struct {
	mu     sync.Mutex
	stages []string
}

func (r *inlineArchivePurgeStageRecorder) StartSpan(
	ctx context.Context,
	name string,
	_ ...observer.Attr,
) (context.Context, observer.Span) {
	switch name {
	case SpanFilePurgeByAge, SpanFilePurgeByGeneration, SpanFilePurgeByDirectorySize:
		r.mu.Lock()
		r.stages = append(r.stages, name)
		r.mu.Unlock()
	}
	return ctx, observer.NopSpan{}
}

func (r *inlineArchivePurgeStageRecorder) assertStages(t *testing.T, want ...string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.stages) != len(want) {
		t.Fatalf("purge stages = %v, want %v", r.stages, want)
	}
	for index := range want {
		if r.stages[index] != want[index] {
			t.Fatalf("purge stages = %v, want %v", r.stages, want)
		}
	}
}

func assertInlineArchiveRemovalDenied(t *testing.T, err error, path string) {
	t.Helper()
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("cleanup error = %v, want permission denial removing %q", err, path)
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) || pathErr.Op != "remove" || pathErr.Path != path {
		t.Fatalf("cleanup error = %v, want remove error for %q", err, path)
	}
}
