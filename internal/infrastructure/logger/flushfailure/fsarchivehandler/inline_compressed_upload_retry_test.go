package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	commoncheckpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestInlineObservationUploadsExistingCompressedArchiveOutsideLifecycleLock(t *testing.T) {
	now := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
	archiveUploader := &lifecycleReentrantFailureArchiveUploader{}
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, archiveUploader)
	})
	handler := pipeline.handler
	// upload中に確認するlockは構築時に決まる系列のGuardです。uploadの開始前に渡します。
	archiveUploader.guard = handler.retryableDirectory.guard

	compressedPath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260828_1.jsonl.gz",
	)
	if err := os.WriteFile(compressedPath, []byte("compressed"), 0o600); err != nil {
		t.Fatalf("write startup compressed archive: %v", err)
	}

	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("first inline observation: %v", err)
	}
	if got := archiveUploader.pathsSnapshot(); len(got) != 1 || got[0] != compressedPath {
		t.Fatalf("uploaded paths = %v, want [%s]", got, compressedPath)
	}
	if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("uploaded startup compressed archive stat error = %v, want not exist", err)
	}
}

func TestTargetOnlyUploadRetryObservesGlobbedPathsOnce(t *testing.T) {
	now := time.Date(2026, 8, 29, 10, 30, 0, 0, time.UTC)
	upload := &sequencedFailureArchiveUploader{}
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
	})
	handler := pipeline.handler
	failureCategory := failurearchive.FailureCategoryRetryable
	compressedPath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260828_1.jsonl.gz",
	)
	if err := os.WriteFile(compressedPath, []byte("compressed"), 0o600); err != nil {
		t.Fatalf("write pending compressed archive: %v", err)
	}

	statCalls := 0
	handler.inspectArchiveFiles = func(paths []string) fsstore.FileInspection {
		statCalls++
		return fsstore.InspectFilePaths(paths)
	}
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("target-only upload retry: %v", err)
	}
	if statCalls != 1 {
		t.Fatalf("target-only observation calls = %d, want 1", statCalls)
	}
	if got := upload.attemptCounts()[compressedPath]; got != 1 {
		t.Fatalf("upload attempts = %d, want 1", got)
	}
	if scheduled, _ := archiveUploadRetryScheduleSnapshot(
		&handler.directoryForFailureCategory(failureCategory).uploadRetry,
	); scheduled {
		t.Fatal("successful target-only upload left retry schedule armed")
	}
	if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("uploaded compressed archive stat error = %v, want not exist", err)
	}
}

func TestTargetOnlyUploadRetryIgnoresNonCanonicalCompressedFiles(t *testing.T) {
	now := time.Date(2026, 8, 29, 10, 45, 0, 0, time.UTC)
	upload := &sequencedFailureArchiveUploader{}
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
	})
	handler := pipeline.handler
	directory := handler.Fsstore.FileNameRetryable.Dir
	canonicalPath := filepath.Join(directory, "test-app_20260828_1.jsonl.gz")
	nonCanonicalPaths := []string{
		filepath.Join(directory, "test-app_memo_1.jsonl.gz"),
		filepath.Join(directory, "test-app_20260828_01.jsonl.gz"),
	}
	for _, path := range append([]string{canonicalPath}, nonCanonicalPaths...) {
		if err := os.WriteFile(path, []byte("compressed"), 0o600); err != nil {
			t.Fatalf("write compressed file %q: %v", path, err)
		}
	}

	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("target-only upload retry: %v", err)
	}
	if got := upload.attemptCounts(); len(got) != 1 || got[canonicalPath] != 1 {
		t.Fatalf("upload attempts = %v, want only canonical path %q", got, canonicalPath)
	}
	if _, err := os.Stat(canonicalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical compressed archive stat error = %v, want not exist", err)
	}
	for _, path := range nonCanonicalPaths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("non-canonical compressed file %q was removed: %v", path, err)
		}
	}
}

// 送信に失敗した圧縮済みfileは容量超過でもlocalへ残し、受理済みの再開処理が古い世代から順に再送します。
// 後続の世代は同じ宛先で失敗した世代を追い越さず、次の自然な観測も再開待ちの世代を重ねて送信しません。
func TestInlineObservationRetainsFailedCompressedUploadsAndRetriesInGenerationOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 11, 0, 0, 0, time.UTC)
		archiveUploader := &sequencedFailureArchiveUploader{failuresRemaining: 1}
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, archiveUploader)
		})
		handler := pipeline.handler
		mutableClock := &archivePurgeClock{now: now}
		handler.Clock = mutableClock

		// 書き込み中の20260829_1より古い世代だけを置きます。active世代は送信完了しないため、
		// それより新しい番号の圧縮済みfileは本番のrotationでは生じず、世代順の待機対象になります。
		paths := []string{
			filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260820_1.jsonl.gz"),
			filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260827_1.jsonl.gz"),
			filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260828_1.jsonl.gz"),
		}
		writeArchiveSizedFile(
			t,
			paths[0],
			int64(handler.Config.MaxDirSize())+1,
			now.AddDate(0, 0, -2),
		)
		writeArchiveSizedFile(t, paths[1], 1, now)
		writeArchiveSizedFile(t, paths[2], 1, now.Add(time.Minute))

		requireRetained := func(stage string) {
			t.Helper()
			for _, path := range paths {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("%s: failed or pending upload was not retained at %q: %v", stage, path, err)
				}
			}
		}
		requireAttempts := func(stage string, want map[string]int) {
			t.Helper()
			got := archiveUploader.attemptCounts()
			if len(got) != len(want) {
				t.Fatalf("%s: upload attempts = %v, want %v", stage, got, want)
			}
			for path, count := range want {
				if got[path] != count {
					t.Fatalf("%s: upload attempts = %v, want %v", stage, got, want)
				}
			}
		}

		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("first inline observation: %v", err)
		}
		requireRetained("first observation")
		requireAttempts("first observation", map[string]int{paths[0]: 1})

		mutableClock.Set(now.Add(testUsageCacheTTL))
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("next natural inline observation: %v", err)
		}
		requireRetained("next natural observation")
		requireAttempts("next natural observation", map[string]int{paths[0]: 1})

		drainArchiveRecoveryForTest(t, handler)
		for _, path := range paths {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("successfully retried compressed archive %q stat error = %v, want not exist", path, err)
			}
		}
		requireAttempts("accepted recovery", map[string]int{paths[0]: 2, paths[1]: 1, paths[2]: 1})
		if got := archiveUploader.attemptedPathOrder(); len(got) != 4 || got[1] != paths[0] || got[2] != paths[1] || got[3] != paths[2] {
			t.Fatalf("upload order = %v, want oldest generation retried before newer generations", got)
		}
	})
}

func TestInlineObservationDoesNotDuplicateDirectRotationCompressedUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
		release := make(chan struct{})
		done := make(chan struct{})
		archiveUploader := newRecordingFailureArchiveUploader(release, done)
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableFileCompress = true
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, archiveUploader)
		})
		// 試験が途中で失敗しても、止めたuploadを再開してからPipelineを閉じます。
		t.Cleanup(func() { closeArchiveTestChannel(release) })
		handler := pipeline.handler

		rawPath := filepath.Join(
			handler.Fsstore.FileNameRetryable.Dir,
			"test-app_20260828_1.jsonl",
		)
		if err := os.WriteFile(rawPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write rotated archive: %v", err)
		}
		handler.dispatchArchiveRotatedFile(
			context.Background(),
			handler.Fsstore.ActiveFilePathRetryable,
			rawPath,
			failurearchive.FailureCategoryRetryable,
		)

		compressedPath := rawPath + ".gz"
		if got := receiveArchiveUploadInput(t, archiveUploader).FilePath(); got != compressedPath {
			t.Fatalf("direct rotation upload path = %q, want %q", got, compressedPath)
		}
		cleanupDone := make(chan error, 1)
		go func() {
			cleanupDone <- handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
		}()
		// 観測が重複uploadを始めた場合も、その送信がreleaseを待つ所まで進めてから確認します。
		synctest.Wait()
		select {
		case duplicate := <-archiveUploader.inputs:
			t.Fatalf("duplicate upload while direct rotation owned path: %+v", duplicate)
		default:
		}
		select {
		case err := <-cleanupDone:
			if err != nil {
				t.Fatalf("inline observation during direct upload: %v", err)
			}
		default:
			t.Fatal("inline observation waited for the direct rotation upload")
		}

		closeArchiveTestChannel(release)
		select {
		case <-done:
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("timed out waiting for direct compressed upload")
		}
	})
}

func TestRotationTaskCoalescesInvalidationAndDefersFailedUploadRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 12, 30, 0, 0, time.UTC)
		uploadRelease := make(chan struct{})
		uploadStarted := make(chan string, 2)
		archiveUploader := &blockingFirstFailureArchiveUploader{
			release: uploadRelease,
			started: uploadStarted,
		}
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableFileCompress = true
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, archiveUploader)
		})
		// 試験が途中で失敗しても、止めたuploadを再開してからPipelineを閉じます。
		t.Cleanup(func() { closeArchiveTestChannel(uploadRelease) })
		handler := pipeline.handler
		failureCategory := failurearchive.FailureCategoryRetryable
		mutableClock := &archivePurgeClock{now: now}
		handler.Clock = mutableClock
		handler.retryableDirectory.lastAgePurgeAt = now

		tracker := handler.directorySizeTracker(failureCategory)
		if !tracker.TryStartObservation(now, handler.Config, true) {
			t.Fatal("failed to initialize retryable directory estimate")
		}
		tracker.FinishObservation(0)

		rawPath := filepath.Join(
			handler.Fsstore.FileNameRetryable.Dir,
			"test-app_20260828_1.jsonl",
		)
		if err := os.WriteFile(rawPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write rotated archive: %v", err)
		}
		compressedPath := rawPath + ".gz"

		var (
			scanMu                  sync.Mutex
			scans                   int
			rotationProtectedAtScan bool
		)
		handler.globArchiveFiles = func(
			pattern logpattern.FileNamePattern,
			mode logpattern.GlobMode,
		) ([]string, error) {
			protected := false
			handler.retryableDirectory.guard.WithProtectedPaths(func(paths map[string]struct{}) {
				_, protected = paths[pathidentity.AbsoluteOrCleanPath(compressedPath)]
			})
			scanMu.Lock()
			scans++
			rotationProtectedAtScan = protected
			scanMu.Unlock()
			return logpattern.GlobFileNames(pattern, mode)
		}

		handler.dispatchArchiveRotatedFile(
			context.Background(),
			handler.Fsstore.ActiveFilePathRetryable,
			rawPath,
			failureCategory,
		)
		select {
		case got := <-uploadStarted:
			if got != compressedPath {
				t.Fatalf("direct rotation upload path = %q, want %q", got, compressedPath)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for direct rotation upload")
		}

		// compressionは完了していますが、rotation taskがpathをretainしている間は
		// 下位操作からcacheを無効化せず、task完了時の一回へまとめます。
		if tracker.TryStartObservation(now, handler.Config, false) {
			tracker.AbortObservation()
			t.Fatal("rotation task invalidated directory estimate before releasing retained paths")
		}
		scanMu.Lock()
		if scans != 0 {
			t.Fatalf("filesystem scans during retained rotation task = %d, want 0", scans)
		}
		scanMu.Unlock()

		handler.archiveTaskMu.Lock()
		taskDone := handler.archiveTaskGeneration
		handler.archiveTaskMu.Unlock()
		closeArchiveTestChannel(uploadRelease)
		select {
		case <-taskDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for rotation task completion")
		}

		scanMu.Lock()
		completedScans := scans
		protectedAtScan := rotationProtectedAtScan
		scanMu.Unlock()
		if completedScans != 1 {
			t.Fatalf("post-rotation filesystem scans = %d, want 1", completedScans)
		}
		if !protectedAtScan {
			t.Fatal("post-rotation observation lost protection for the accepted recovery")
		}
		if got := archiveUploader.attemptCount(); got != 1 {
			t.Fatalf("upload attempts after forced post-rotation observation = %d, want 1", got)
		}
		if _, err := os.Stat(compressedPath); err != nil {
			t.Fatalf("failed direct upload was not kept locally: %v", err)
		}

		// 観測は受理済み復旧と同じpathへ再送せず、復旧queueが独立に完了します。
		mutableClock.Set(now.Add(testUsageCacheTTL))
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("next natural inline observation: %v", err)
		}
		if got := archiveUploader.attemptCount(); got != 1 {
			t.Fatalf("natural observation overlapped accepted recovery: attempts = %d", got)
		}
		drainArchiveRecoveryForTest(t, handler)
		if got := archiveUploader.attemptCount(); got != 2 {
			t.Fatalf("upload attempts after accepted recovery = %d, want 2", got)
		}
		if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retried compressed archive stat error = %v, want not exist", err)
		}
	})
}

func TestDroppedPostRotationObservationDefersUploadUntilFollowingNaturalObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 12, 40, 0, 0, time.UTC)
		uploadRelease := make(chan struct{})
		uploadStarted := make(chan string, 2)
		archiveUploader := &blockingFirstFailureArchiveUploader{
			release: uploadRelease,
			started: uploadStarted,
		}
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableFileCompress = true
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, archiveUploader)
		})
		handler := pipeline.handler
		failureCategory := failurearchive.FailureCategoryRetryable
		mutableClock := &archivePurgeClock{now: now}
		handler.Clock = mutableClock
		handler.retryableDirectory.lastAgePurgeAt = now

		tracker := handler.directorySizeTracker(failureCategory)
		if !tracker.TryStartObservation(now, handler.Config, true) {
			t.Fatal("failed to initialize retryable directory estimate")
		}
		tracker.FinishObservation(0)

		rawPath := filepath.Join(
			handler.Fsstore.FileNameRetryable.Dir,
			"test-app_20260828_1.jsonl",
		)
		if err := os.WriteFile(rawPath, []byte("retryable"), 0o600); err != nil {
			t.Fatalf("write rotated archive: %v", err)
		}
		compressedPath := rawPath + ".gz"

		var (
			scanMu sync.Mutex
			scans  int
		)
		handler.globArchiveFiles = func(
			pattern logpattern.FileNamePattern,
			mode logpattern.GlobMode,
		) ([]string, error) {
			scanMu.Lock()
			scans++
			scanMu.Unlock()
			return logpattern.GlobFileNames(pattern, mode)
		}

		handler.dispatchArchiveRotatedFile(
			context.Background(),
			handler.Fsstore.ActiveFilePathRetryable,
			rawPath,
			failureCategory,
		)
		select {
		case got := <-uploadStarted:
			if got != compressedPath {
				t.Fatalf("direct rotation upload path = %q, want %q", got, compressedPath)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for direct rotation upload")
		}

		state := handler.directoryForFailureCategory(failureCategory)
		// 別cleanupが実行中の状態を固定し、rotation completionが要求するpost観測を
		// cleanupRunningのCAS失敗で確実に破棄させます。
		state.cleanupRunning.Store(true)
		t.Cleanup(func() {
			state.cleanupRunning.Store(false)
			select {
			case <-uploadRelease:
			default:
				close(uploadRelease)
			}
		})

		handler.archiveTaskMu.Lock()
		taskDone := handler.archiveTaskGeneration
		handler.archiveTaskMu.Unlock()
		close(uploadRelease)
		select {
		case <-taskDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for rotation task completion")
		}

		scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
		if !scheduled {
			t.Fatal("dropped post-rotation observation did not preserve upload retry schedule")
		}
		if want := now.Add(testUsageCacheTTL); !retryAt.Equal(want) {
			t.Fatalf("upload retry time = %v, want %v", retryAt, want)
		}
		scanMu.Lock()
		postRotationScans := scans
		scanMu.Unlock()
		if postRotationScans != 0 {
			t.Fatalf("filesystem scans while post-rotation observation was dropped = %d, want 0", postRotationScans)
		}
		if got := archiveUploader.attemptCount(); got != 1 {
			t.Fatalf("upload attempts after dropped post-rotation observation = %d, want 1", got)
		}

		state.cleanupRunning.Store(false)
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("first normal observation after rotation: %v", err)
		}
		scheduled, retryAt = archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
		if !scheduled || !retryAt.Equal(now.Add(testUsageCacheTTL)) {
			t.Fatalf(
				"upload retry schedule after early natural observation = (%t, %v), want (true, %v)",
				scheduled,
				retryAt,
				now.Add(testUsageCacheTTL),
			)
		}
		if got := archiveUploader.attemptCount(); got != 1 {
			t.Fatalf("upload attempts in first normal observation after rotation = %d, want 1", got)
		}
		if _, err := os.Stat(compressedPath); err != nil {
			t.Fatalf("deferred compressed archive was not kept locally: %v", err)
		}

		mutableClock.Set(now.Add(testUsageCacheTTL))
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("following natural TTL observation: %v", err)
		}
		drainArchiveRecoveryForTest(t, handler)
		if got := archiveUploader.attemptCount(); got != 2 {
			t.Fatalf("upload attempts after following natural observation = %d, want 2", got)
		}
		if scheduled, _ := archiveUploadRetryScheduleSnapshot(&state.uploadRetry); scheduled {
			t.Fatal("successful target-only retry did not clear claimed schedule")
		}
		if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retried compressed archive stat error = %v, want not exist", err)
		}
	})
}

func TestRotationCompletionSerializesRetryScheduleWithProtectedSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 12, 42, 0, 0, time.UTC)
		directRelease := make(chan struct{})
		directReturned := make(chan struct{})
		upload := &snapshotOrderingFailureArchiveUploader{
			directStarted:  make(chan struct{}),
			directRelease:  directRelease,
			directReturned: directReturned,
			attemptsByPath: make(map[string]int),
		}
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableFileCompress = true
			cfg.maxFileBackups = 2
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
		})
		handler := pipeline.handler
		failureCategory := failurearchive.FailureCategoryRetryable
		mutableClock := &archivePurgeClock{now: now}
		handler.Clock = mutableClock
		handler.retryableDirectory.lastAgePurgeAt = now
		directory := handler.Fsstore.FileNameRetryable.Dir

		directRawPath := filepath.Join(directory, "test-app_20260828_1.jsonl")
		directCompressedPath := directRawPath + ".gz"
		if err := os.WriteFile(directRawPath, []byte("direct"), 0o600); err != nil {
			t.Fatalf("write direct rotated archive: %v", err)
		}
		writeArchiveSizedFile(
			t,
			filepath.Join(directory, "test-app_snapshot-series_20260829_1.jsonl"),
			1,
			now.Add(-4*time.Hour),
		)
		writeArchiveSizedFile(
			t,
			filepath.Join(directory, "test-app_snapshot-series_20260829_2.jsonl"),
			1,
			now.Add(-3*time.Hour),
		)
		writeArchiveSizedFile(
			t,
			filepath.Join(directory, "test-app_snapshot-series_20260829_3.jsonl"),
			1,
			now.Add(-2*time.Hour),
		)

		// 系列directoryは構築時に決まります。uploadの開始前に対象pathを渡します。
		upload.directPath = directCompressedPath
		t.Cleanup(func() { closeArchiveTestChannel(directRelease) })
		state := handler.directoryForFailureCategory(failureCategory)
		// 構築時に予約されるrestart bootstrapを消費し、このtestではdirect
		// rotation失敗が新しいscheduleを公開する瞬間だけを検証します。
		if !state.uploadRetry.claimForAttempt(now, testUsageCacheTTL) {
			t.Fatal("failed to consume test bootstrap upload retry schedule")
		}

		purgeEntered := make(chan struct{})
		purgeRelease := make(chan struct{})
		handler.Tracer = &blockingArchivePurgeTracer{
			spanName: SpanFilePurgeByGeneration,
			entered:  purgeEntered,
			release:  purgeRelease,
		}
		t.Cleanup(func() { closeArchiveTestChannel(purgeRelease) })

		handler.dispatchArchiveRotatedFile(
			context.Background(),
			handler.Fsstore.ActiveFilePathRetryable,
			directRawPath,
			failureCategory,
		)
		select {
		case <-upload.directStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for direct rotation upload")
		}
		handler.archiveTaskMu.Lock()
		taskDone := handler.archiveTaskGeneration
		handler.archiveTaskMu.Unlock()

		cleanupDone := make(chan error, 1)
		go func() {
			cleanupDone <- handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
		}()
		select {
		case <-purgeEntered:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for cleanup protected snapshot")
		}

		close(directRelease)
		select {
		case <-directReturned:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for direct upload failure")
		}
		// cleanupはprotected snapshotのfile操作中でlifecycle lockを保持しています。
		// rotation completionが同lockを使う限り、pathを観測していないcleanupが
		// 完了する前に新しいscheduleと保護解除が別々に公開されることはありません。
		// rotation completionがlifecycle lockを待って止まるまで進めてから確認します。
		synctest.Wait()
		if scheduled, _ := archiveUploadRetryScheduleSnapshot(&state.uploadRetry); scheduled {
			t.Fatal("rotation published retry schedule before protected snapshot completed")
		}

		close(purgeRelease)
		select {
		case err := <-cleanupDone:
			if err != nil {
				t.Fatalf("cleanup protected snapshot: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for cleanup completion")
		}
		select {
		case <-taskDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for serialized rotation completion")
		}
		scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
		if !scheduled || !retryAt.Equal(now.Add(testUsageCacheTTL)) {
			t.Fatalf(
				"rotation retry schedule after protected snapshot = (%t, %v), want (true, %v)",
				scheduled,
				retryAt,
				now.Add(testUsageCacheTTL),
			)
		}
		if got := upload.attemptCount(directCompressedPath); got != 1 {
			t.Fatalf("direct compressed upload attempts = %d, want 1", got)
		}

		// rotationが無効化したcacheによる全体観測はupload scheduleを消費せず、
		// 直接upload失敗pathをTTL前に再送しません。
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("first observation after unprotect: %v", err)
		}
		scheduled, retryAt = archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
		if !scheduled || !retryAt.Equal(now.Add(testUsageCacheTTL)) {
			t.Fatalf(
				"full observation changed upload retry schedule = (%t, %v), want (true, %v)",
				scheduled,
				retryAt,
				now.Add(testUsageCacheTTL),
			)
		}
		if got := upload.attemptCount(directCompressedPath); got != 1 {
			t.Fatalf("direct upload attempts before scheduled deadline = %d, want 1", got)
		}

		mutableClock.Set(now.Add(testUsageCacheTTL))
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("following natural observation: %v", err)
		}
		drainArchiveRecoveryForTest(t, handler)
		if got := upload.attemptCount(directCompressedPath); got != 2 {
			t.Fatalf("direct upload attempts after natural observation = %d, want 2", got)
		}
		if scheduled, _ := archiveUploadRetryScheduleSnapshot(&state.uploadRetry); scheduled {
			t.Fatal("successful target-only retry did not clear claimed schedule")
		}
		if _, err := os.Stat(directCompressedPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retried direct compressed archive stat error = %v, want not exist", err)
		}
	})
}

func TestDirectoryPurgeFailureDoesNotConsumeUploadRetrySchedule(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 44, 0, 0, time.UTC)
	upload := &sequencedFailureArchiveUploader{}
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.maxFileBackups = 2
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
	})
	handler := pipeline.handler
	failureCategory := failurearchive.FailureCategoryRetryable
	mutableClock := &archivePurgeClock{now: now}
	handler.Clock = mutableClock
	handler.retryableDirectory.lastAgePurgeAt = now
	directory := handler.Fsstore.FileNameRetryable.Dir

	failingPath := filepath.Join(directory, "test-app_20260828_1.jsonl")
	if err := os.WriteFile(failingPath, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write purge target: %v", err)
	}
	if err := os.Chtimes(failingPath, now.Add(-4*time.Hour), now.Add(-4*time.Hour)); err != nil {
		t.Fatalf("set purge target modtime: %v", err)
	}
	writeArchiveSizedFile(
		t,
		filepath.Join(directory, "test-app_20260828_2.jsonl"),
		1,
		now.Add(-3*time.Hour),
	)
	writeArchiveSizedFile(
		t,
		filepath.Join(directory, "test-app_20260828_3.jsonl"),
		1,
		now.Add(-2*time.Hour),
	)
	pendingCompressedPath := filepath.Join(directory, "test-app_20260828_4.jsonl.gz")
	writeArchiveSizedFile(t, pendingCompressedPath, 1, now.Add(-time.Hour))
	// 未圧縮fileは送信後も通常の保持・purgeへ残ります。送信済みの世代として記録し、
	// 送信待ちの保護ではなく世代数purgeの対象にします。
	advanceArchiveUploadCursorForTest(t, handler, failureCategory, filepath.Join(directory, "test-app_20260828_3.jsonl"))
	state := handler.directoryForFailureCategory(failureCategory)
	restoreRemoval := testutil.DenyFileRemoval(t, failingPath)
	defer restoreRemoval()
	purgeStages := &inlineArchivePurgeStageRecorder{}
	handler.Tracer = purgeStages
	// post-rotation/full観測はupload scheduleと独立してpurgeします。
	err := handler.triggerPostRotationArchiveCleanup(context.Background(), failureCategory, "")
	assertInlineArchiveRemovalDenied(t, err, failingPath)
	purgeStages.assertStages(t, SpanFilePurgeByGeneration)
	if _, err := os.Stat(failingPath); err != nil {
		t.Fatalf("failed generation-purge target disappeared: %v", err)
	}
	if scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry); !scheduled || !retryAt.IsZero() {
		t.Fatalf(
			"purge failure changed bootstrap upload schedule = (%t, %v), want (true, zero)",
			scheduled,
			retryAt,
		)
	}
	if got := upload.attemptCounts()[pendingCompressedPath]; got != 0 {
		t.Fatalf("pending upload attempts during failed purge = %d, want 0", got)
	}

	restoreRemoval()
	if err := os.Remove(failingPath); err != nil {
		t.Fatalf("remove failed purge target: %v", err)
	}
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("target-only upload retry after purge failure: %v", err)
	}
	if got := upload.attemptCounts()[pendingCompressedPath]; got != 1 {
		t.Fatalf("pending upload attempts after purge failure = %d, want 1", got)
	}
	if scheduled, _ := archiveUploadRetryScheduleSnapshot(&state.uploadRetry); scheduled {
		t.Fatal("successful target-only retry did not clear bootstrap schedule")
	}
	if _, err := os.Stat(pendingCompressedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("uploaded compressed archive stat error = %v, want not exist", err)
	}
}

// 重なったrotation taskでは、新しい世代が送信中の古い世代を追い越さず、受理済みの再開処理へ渡ります。
// 両taskの完了後の強制観測は、どちらの送信待ちpathも重ねて送信しません。
func TestOverlappingRotationTasksSkipAllPendingUploadsInForcedObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 12, 45, 0, 0, time.UTC)
		uploadRelease := make(chan struct{})
		uploadStarted := make(chan string, 2)
		archiveUploader := &overlappingFirstFailureArchiveUploader{
			release:  uploadRelease,
			started:  uploadStarted,
			attempts: make(map[string]int),
		}
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableFileCompress = true
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, archiveUploader)
		})
		handler := pipeline.handler
		failureCategory := failurearchive.FailureCategoryRetryable
		mutableClock := &archivePurgeClock{now: now}
		handler.Clock = mutableClock
		handler.retryableDirectory.lastAgePurgeAt = now
		t.Cleanup(func() {
			select {
			case <-uploadRelease:
			default:
				close(uploadRelease)
			}
		})

		tracker := handler.directorySizeTracker(failureCategory)
		if !tracker.TryStartObservation(now, handler.Config, true) {
			t.Fatal("failed to initialize retryable directory estimate")
		}
		tracker.FinishObservation(0)

		state := handler.directoryForFailureCategory(failureCategory)
		// 両taskが要求するpost-rotation cleanupをいったん破棄し、どちらの
		// compressed pathも保護解除後に一回のcoalesced scanへ渡します。
		// rotation completion自体がlifecycle lockを取得するため、lockを外から保持して
		// task完了を待つことはできません。
		state.cleanupRunning.Store(true)
		t.Cleanup(func() { state.cleanupRunning.Store(false) })

		rawPaths := []string{
			filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260827_1.jsonl"),
			filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260828_1.jsonl"),
		}
		compressedPaths := make([]string, len(rawPaths))
		for index, rawPath := range rawPaths {
			if err := os.WriteFile(rawPath, []byte("retryable"), 0o600); err != nil {
				t.Fatalf("write rotated archive %q: %v", rawPath, err)
			}
			compressedPaths[index] = rawPath + ".gz"
			handler.dispatchArchiveRotatedFile(
				context.Background(),
				handler.Fsstore.ActiveFilePathRetryable,
				rawPath,
				failureCategory,
			)
		}

		select {
		case path := <-uploadStarted:
			if path != compressedPaths[0] {
				t.Fatalf("first direct upload path = %q, want older generation %q", path, compressedPaths[0])
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for older direct upload")
		}
		// 新しい世代のtaskは、送信中の古い世代を追い越さずに受理済みの再開処理へ渡して終わります。
		synctest.Wait()
		if got := archiveUploader.attemptCount(compressedPaths[1]); got != 0 {
			t.Fatalf("newer generation upload attempts while older upload is in flight = %d, want 0", got)
		}
		if _, err := os.Stat(compressedPaths[1]); err != nil {
			t.Fatalf("newer compressed archive was not kept for the deferred upload: %v", err)
		}

		var scanMu sync.Mutex
		scans := 0
		handler.globArchiveFiles = func(
			pattern logpattern.FileNamePattern,
			mode logpattern.GlobMode,
		) ([]string, error) {
			scanMu.Lock()
			scans++
			scanMu.Unlock()
			return logpattern.GlobFileNames(pattern, mode)
		}

		handler.archiveTaskMu.Lock()
		taskDone := handler.archiveTaskGeneration
		handler.archiveTaskMu.Unlock()
		close(uploadRelease)

		select {
		case <-taskDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for overlapping rotation tasks")
		}
		state.cleanupRunning.Store(false)
		if err := handler.triggerPostRotationArchiveCleanup(context.Background(), failureCategory, ""); err != nil {
			t.Fatalf("coalesced post-rotation observation: %v", err)
		}
		scanMu.Lock()
		forcedScans := scans
		scanMu.Unlock()
		if forcedScans != 1 {
			t.Fatalf("coalesced post-rotation scans = %d, want 1", forcedScans)
		}
		for index, want := range []int{1, 0} {
			path := compressedPaths[index]
			if got := archiveUploader.attemptCount(path); got != want {
				t.Fatalf("forced observation upload attempts for %q = %d, want %d", path, got, want)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("pending compressed archive was not kept at %q: %v", path, err)
			}
		}

		mutableClock.Set(now.Add(testUsageCacheTTL))
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("next natural inline observation: %v", err)
		}
		for index, want := range []int{1, 0} {
			if got := archiveUploader.attemptCount(compressedPaths[index]); got != want {
				t.Fatalf("natural observation overlapped accepted recovery for %q: attempts = %d, want %d", compressedPaths[index], got, want)
			}
		}
		drainArchiveRecoveryForTest(t, handler)
		// 試験用uploaderは各pathの初回だけ失敗するため、どちらも再開処理の二回目で完了します。
		for _, path := range compressedPaths {
			if got := archiveUploader.attemptCount(path); got != 2 {
				t.Fatalf("accepted recovery upload attempts for %q = %d, want 2", path, got)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retried compressed archive %q stat error = %v, want not exist", path, err)
			}
		}
	})
}

// 送信に失敗した世代は受理済みの再開処理が再送し、directory全体を走査しません。
// 失敗後に予約した系列の再試行もtarget-only走査だけを使い、再開処理と重ねて送信しません。
func TestFailedUploadRetriesUseOnlyTheArchiveSeriesTargetScan(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 12, 50, 0, 0, time.UTC)
		upload := &sequencedFailureArchiveUploader{failuresRemaining: 2}
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
		})
		handler := pipeline.handler
		failureCategory := failurearchive.FailureCategoryRetryable
		clock := &archivePurgeClock{now: now}
		handler.Clock = clock
		handler.retryableDirectory.lastAgePurgeAt = now

		compressedPath := filepath.Join(
			handler.Fsstore.FileNameRetryable.Dir,
			"test-app_20260828_1.jsonl.gz",
		)
		writeArchiveSizedFile(t, compressedPath, 1, now)
		tracker := handler.directorySizeTracker(failureCategory)
		if !tracker.TryStartObservation(now, handler.Config, true) {
			t.Fatal("failed to initialize retryable directory estimate")
		}
		tracker.FinishObservation(1)

		var (
			scanMu    sync.Mutex
			scanModes []logpattern.GlobMode
		)
		handler.globArchiveFiles = func(
			pattern logpattern.FileNamePattern,
			mode logpattern.GlobMode,
		) ([]string, error) {
			scanMu.Lock()
			scanModes = append(scanModes, mode)
			scanMu.Unlock()
			return logpattern.GlobFileNames(pattern, mode)
		}

		// 構築時に予約されたbootstrapのtarget-only走査で初回の送信に失敗します。
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("bootstrap series upload: %v", err)
		}
		if got := upload.attemptCounts()[compressedPath]; got != 1 {
			t.Fatalf("upload attempts after bootstrap scan = %d, want 1", got)
		}
		state := handler.directoryForFailureCategory(failureCategory)
		if scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry); !scheduled || !retryAt.Equal(now.Add(testUsageCacheTTL)) {
			t.Fatalf("schedule after failed upload = (%t, %v), want (true, %v)", scheduled, retryAt, now.Add(testUsageCacheTTL))
		}

		clock.Set(now.Add(testUsageCacheTTL - time.Nanosecond))
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("cleanup before retry deadline: %v", err)
		}
		clock.Set(now.Add(testUsageCacheTTL))
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("series retry at deadline: %v", err)
		}
		if got := upload.attemptCounts()[compressedPath]; got != 1 {
			t.Fatalf("series retry overlapped accepted recovery: attempts = %d, want 1", got)
		}

		drainArchiveRecoveryForTest(t, handler)
		if got := upload.attemptCounts()[compressedPath]; got != 3 {
			t.Fatalf("upload attempts after accepted recovery = %d, want 3", got)
		}
		if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retried compressed archive stat error = %v, want not exist", err)
		}

		scanMu.Lock()
		gotModes := append([]logpattern.GlobMode(nil), scanModes...)
		scanMu.Unlock()
		if len(gotModes) != 2 {
			t.Fatalf("filesystem scan modes = %v, want bootstrap and deadline target-only scans", gotModes)
		}
		for index, mode := range gotModes {
			if mode != logpattern.GlobByTarget {
				t.Fatalf("filesystem scan mode[%d] = %v, want GlobByTarget", index, mode)
			}
		}
		if scheduled, _ := archiveUploadRetryScheduleSnapshot(&handler.retryableDirectory.uploadRetry); scheduled {
			t.Fatal("series retry without pending target did not clear claimed schedule")
		}
		if scheduled, _ := archiveUploadRetryScheduleSnapshot(&handler.permanentDirectory.uploadRetry); scheduled {
			t.Fatal("retryable series upload scheduled the permanent series")
		}
	})
}

func TestFailedArchiveSeriesTargetScanBacksOffUntilTTL(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 52, 0, 0, time.UTC)
	failureAt := now.Add(2 * testUsageCacheTTL)
	upload := &sequencedFailureArchiveUploader{}
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
	})
	handler := pipeline.handler
	failureCategory := failurearchive.FailureCategoryRetryable
	clock := &archivePurgeClock{now: now}
	handler.Clock = clock
	handler.retryableDirectory.lastAgePurgeAt = now
	compressedPath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260828_1.jsonl.gz",
	)
	writeArchiveSizedFile(t, compressedPath, 1, now)

	scanErr := errors.New("target archive glob failed")
	var (
		scanMu         sync.Mutex
		scanModes      []logpattern.GlobMode
		failTargetScan = true
	)
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		scanMu.Lock()
		scanModes = append(scanModes, mode)
		shouldFail := mode == logpattern.GlobByTarget && failTargetScan
		if shouldFail {
			failTargetScan = false
		}
		scanMu.Unlock()
		if shouldFail {
			// filesystem走査がTTLより長くかかった場合も、開始時刻ではなく
			// 実際に失敗した時刻をbackoffの起点にします。
			clock.Set(failureAt)
			return nil, scanErr
		}
		return logpattern.GlobFileNames(pattern, mode)
	}

	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); !errors.Is(err, scanErr) {
		t.Fatalf("first target-only scan error = %v, want %v", err, scanErr)
	}
	state := handler.directoryForFailureCategory(failureCategory)
	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
	if !scheduled || !retryAt.Equal(failureAt.Add(testUsageCacheTTL)) {
		t.Fatalf(
			"schedule after target scan error = (%t, %v), want (true, %v)",
			scheduled,
			retryAt,
			failureAt.Add(testUsageCacheTTL),
		)
	}

	// schedule期限前のwriteはtarget scanを繰り返しません。directory Trackerが
	// 未観測なので独立した全体baselineだけを一回取得します。
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("cleanup before target retry deadline: %v", err)
	}
	if got := upload.attemptCounts()[compressedPath]; got != 0 {
		t.Fatalf("upload attempts before target retry deadline = %d, want 0", got)
	}

	clock.Set(failureAt.Add(testUsageCacheTTL))
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("target scan at retry deadline: %v", err)
	}
	if got := upload.attemptCounts()[compressedPath]; got != 1 {
		t.Fatalf("upload attempts at target retry deadline = %d, want 1", got)
	}
	if scheduled, _ := archiveUploadRetryScheduleSnapshot(&state.uploadRetry); scheduled {
		t.Fatal("successful target retry did not clear claimed schedule")
	}

	scanMu.Lock()
	gotModes := append([]logpattern.GlobMode(nil), scanModes...)
	scanMu.Unlock()
	wantModes := []logpattern.GlobMode{
		logpattern.GlobByTarget,
		logpattern.GlobAllFiles,
		logpattern.GlobByTarget,
	}
	if len(gotModes) != len(wantModes) {
		t.Fatalf("filesystem scan modes = %v, want %v", gotModes, wantModes)
	}
	for index := range wantModes {
		if gotModes[index] != wantModes[index] {
			t.Fatalf("filesystem scan modes = %v, want %v", gotModes, wantModes)
		}
	}
}

func TestSuccessfulRetryDoesNotClearRotationFailureScheduledDuringRemoteIO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 29, 12, 55, 0, 0, time.UTC)
		oldRelease := make(chan struct{})
		upload := &concurrentRetryRotationFailureUploader{
			oldStarted: make(chan struct{}),
			oldRelease: oldRelease,
			newFailed:  make(chan struct{}),
			attempts:   make(map[string]int),
		}
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.enableFileCompress = true
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, upload)
		})
		handler := pipeline.handler
		failureCategory := failurearchive.FailureCategoryRetryable
		clock := &archivePurgeClock{now: now}
		handler.Clock = clock
		handler.retryableDirectory.lastAgePurgeAt = now
		directory := handler.Fsstore.FileNameRetryable.Dir
		oldPendingPath := filepath.Join(directory, "test-app_20260827_1.jsonl.gz")
		newRawPath := filepath.Join(directory, "test-app_20260828_1.jsonl")
		newPendingPath := newRawPath + ".gz"
		writeArchiveSizedFile(t, oldPendingPath, 1, now.Add(-time.Hour))
		// 系列directoryは構築時に決まります。uploadの開始前に対象pathを渡します。
		upload.oldPendingPath = oldPendingPath
		upload.newPendingPath = newPendingPath
		t.Cleanup(func() { closeArchiveTestChannel(oldRelease) })

		var (
			scanMu    sync.Mutex
			scanModes []logpattern.GlobMode
		)
		handler.globArchiveFiles = func(
			pattern logpattern.FileNamePattern,
			mode logpattern.GlobMode,
		) ([]string, error) {
			scanMu.Lock()
			scanModes = append(scanModes, mode)
			scanMu.Unlock()
			return logpattern.GlobFileNames(pattern, mode)
		}

		cleanupDone := make(chan error, 1)
		go func() {
			cleanupDone <- handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable)
		}()
		select {
		case <-upload.oldStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for old target-only upload retry")
		}

		if err := os.WriteFile(newRawPath, []byte("new rotation"), 0o600); err != nil {
			t.Fatalf("write new rotated archive: %v", err)
		}
		handler.dispatchArchiveRotatedFile(
			context.Background(),
			handler.Fsstore.ActiveFilePathRetryable,
			newRawPath,
			failureCategory,
		)
		handler.archiveTaskMu.Lock()
		taskDone := handler.archiveTaskGeneration
		handler.archiveTaskMu.Unlock()
		// 新しい世代は送信中の古い世代を追い越さず、未完了として再開処理と再試行の予約へ渡します。
		select {
		case <-taskDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for concurrent rotation completion")
		}
		// 世代順で待つ処理は再開処理が直ちに一度確認します。古い世代の送信中に確認を終えさせます。
		synctest.Wait()
		if got := upload.attemptCount(newPendingPath); got != 0 {
			t.Fatalf("new compressed upload attempts while old upload is in flight = %d, want 0", got)
		}

		state := handler.directoryForFailureCategory(failureCategory)
		scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
		if !scheduled || !retryAt.Equal(now.Add(testUsageCacheTTL)) {
			t.Fatalf(
				"concurrent rotation retry schedule = (%t, %v), want (true, %v)",
				scheduled,
				retryAt,
				now.Add(testUsageCacheTTL),
			)
		}
		close(oldRelease)
		select {
		case err := <-cleanupDone:
			if err != nil {
				t.Fatalf("old target-only upload retry: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for old upload retry completion")
		}

		// old uploadの成功経路はclaim後にscheduleへ触らないため、remote I/O中に
		// rotationが予約した新しい失敗をclearしません。
		scheduled, retryAt = archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
		if !scheduled || !retryAt.Equal(now.Add(testUsageCacheTTL)) {
			t.Fatalf(
				"schedule after old retry success = (%t, %v), want (true, %v)",
				scheduled,
				retryAt,
				now.Add(testUsageCacheTTL),
			)
		}
		if _, err := os.Stat(oldPendingPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("uploaded old compressed archive stat error = %v, want not exist", err)
		}
		// 古い世代の完了記録が進むと、待っていた新しい世代を再開間隔を待たずに再試行します。
		// 試験用uploaderは新しい世代の初回だけ失敗するため、その再送は受理済みの再開処理が行います。
		synctest.Wait()
		select {
		case <-upload.newFailed:
		default:
			t.Fatal("deferred new compressed upload did not resume after the old generation completed")
		}
		if got := upload.attemptCount(newPendingPath); got != 1 {
			t.Fatalf("new compressed upload attempts after old completion = %d, want 1", got)
		}

		clock.Set(now.Add(testUsageCacheTTL))
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("new target-only upload retry: %v", err)
		}
		if got := upload.attemptCount(newPendingPath); got != 1 {
			t.Fatalf("target-only retry overlapped accepted recovery: attempts = %d, want 1", got)
		}
		drainArchiveRecoveryForTest(t, handler)
		if got := upload.attemptCount(newPendingPath); got != 2 {
			t.Fatalf("new compressed upload attempts after accepted recovery = %d, want 2", got)
		}
		if scheduled, _ := archiveUploadRetryScheduleSnapshot(&state.uploadRetry); scheduled {
			t.Fatal("successful new target-only retry did not clear claimed schedule")
		}
		if _, err := os.Stat(newPendingPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("new compressed archive stat error = %v, want not exist", err)
		}

		scanMu.Lock()
		gotModes := append([]logpattern.GlobMode(nil), scanModes...)
		scanMu.Unlock()
		if len(gotModes) != 2 ||
			gotModes[0] != logpattern.GlobByTarget ||
			gotModes[1] != logpattern.GlobByTarget {
			t.Fatalf("filesystem scan modes = %v, want two target-only scans", gotModes)
		}
	})
}

// 送信後のlocal削除に失敗した圧縮済みfileは保持し、受理済みの再開処理が完了cursorを読み直して
// 削除だけを再試行します。送信済みの宛先へ同じfileを再送しません。
func TestInlineObservationRetriesCompressedArchiveAfterLocalDeleteFailure(t *testing.T) {
	now := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
	archiveUploader := &firstDeleteBlockingFailureArchiveUploader{}
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, archiveUploader)
	})
	handler := pipeline.handler
	// 削除の再試行は再開処理の間隔で行います。実時間で拒否を解除するため、短い間隔にします。
	setArchiveLogFileProcessingRecoveryIntervalForTest(t, handler, 10*time.Millisecond)
	mutableClock := &archivePurgeClock{now: now}
	handler.Clock = mutableClock
	directory := handler.Fsstore.FileNameRetryable.Dir
	compressedPath := filepath.Join(directory, "test-app_20260828_1.jsonl.gz")
	if err := os.WriteFile(compressedPath, []byte("compressed"), 0o600); err != nil {
		t.Fatalf("write compressed archive: %v", err)
	}
	removalDenied := false
	if !t.Run("local removal denied", func(t *testing.T) {
		archiveUploader.blockFirstRemoval = func() {
			// Windowsでも実Removeを拒否し、subtest終了時に権限とfixture handleを解放します。
			// 権限確認を迂回する実行環境では拒否を再現できないため、subtestをskipします。
			denyArchiveFileRemoval(t, compressedPath)
			removalDenied = true
		}
		if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
			t.Fatalf("first inline observation: %v", err)
		}
		if content, err := os.ReadFile(compressedPath); err != nil || string(content) != "compressed" {
			t.Fatalf("compressed archive was not retained after local delete failure: %v", err)
		}
		if got := archiveUploader.attemptCount(); got != 1 {
			t.Fatalf("first upload attempts = %d, want 1", got)
		}
	}) {
		return
	}
	if !removalDenied {
		t.Skip("local archive removal could not be denied for the retry precondition")
	}

	// 障害注入用のhandleまで閉じた後、次の自然な観測は再開処理と重ねて送信しません。
	mutableClock.Set(now.Add(testUsageCacheTTL))
	if err := handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable); err != nil {
		t.Fatalf("natural observation after local delete failure: %v", err)
	}
	drainArchiveRecoveryForTest(t, handler)
	if _, err := os.Stat(compressedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retried compressed archive stat error = %v, want not exist", err)
	}
	if got := archiveUploader.attemptCount(); got != 1 {
		t.Fatalf("upload attempts after local delete retry = %d, want 1", got)
	}
}

// 送信のpanicでも、今回の再試行が保護した全pathを解放してからpanicを返します。
// panicした世代は受理済みの再開処理が自身の保護で引き継ぎ、未着手の世代は保護しません。
func TestRetryRetainedCompressedArchiveUploadsReleasesEveryPathAfterPanic(t *testing.T) {
	now := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, panicFailureArchiveUploader{})
	})
	handler := pipeline.handler

	paths := []string{
		filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260828_1.jsonl.gz"),
		filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260828_2.jsonl.gz"),
	}
	for _, path := range paths {
		writeArchiveSizedFile(t, path, 1, now)
	}
	state := handler.directoryForFailureCategory(failurearchive.FailureCategoryRetryable)
	retryToken := appendlog.NewDirectoryGuardToken()
	state.guard.ProtectPaths(retryToken, paths...)
	for _, path := range paths {
		assertArchivePathProtected(t, state.guard, path, true)
	}

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		handler.retryRetainedArchiveUploads(
			context.Background(),
			failurearchive.FailureCategoryRetryable,
			handler.Fsstore.FileNameRetryable,
			archivePathProtection{token: retryToken, paths: paths},
		)
	}()
	if recovered == nil {
		t.Fatal("retryRetainedArchiveUploads did not propagate uploader panic")
	}
	// 古い世代から処理するため、panicした先頭の世代だけを再開処理が保護します。
	assertArchivePathProtected(t, state.guard, paths[0], true)
	assertArchivePathProtected(t, state.guard, paths[1], false)
	key := commoncheckpoint.CheckpointKey{Generation: commoncheckpoint.ArchiveCheckpointGenerationKey("20260828", 1), Compressed: true}
	if !handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(failurearchive.FailureCategoryRetryable, key)) {
		t.Fatal("panicked upload was not accepted by log file processing recovery")
	}
	// 再開処理を破棄すると、その保護も解放されます。
	restartArchiveRecoveryForTest(t, handler)
	for _, path := range paths {
		assertArchivePathProtected(t, state.guard, path, false)
	}
}

func TestInlinePurgePanicInvalidatesObservationAndReleasesRetainedUploads(t *testing.T) {
	now := time.Date(2026, 8, 30, 2, 0, 0, 0, time.UTC)
	const panicValue = "failure archive purge report panic"
	pipeline := newTestArchiveHandler(
		t,
		now,
		func(cfg *testArchiveConfig) {
			cfg.isDebug = true
			cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, &sequencedFailureArchiveUploader{})
		},
		InternalLogHandlers{OnDebug: func(
			context.Context,
			*logging.LogEntry,
			pkgobserver.Tracer,
			pkgobserver.Meter,
		) {
			panic(panicValue)
		}},
	)
	handler := pipeline.handler
	// Shutdown cleanupがこのtestで注入したpanic handlerを再度呼び出さないよう、
	// assertion後のCleanupより先に元へ戻します。
	t.Cleanup(func() { handler.OnDebug = nil })

	failureCategory := failurearchive.FailureCategoryRetryable
	directory := handler.Fsstore.FileNameRetryable.Dir
	oldPath := filepath.Join(directory, "test-app_20260820_1.jsonl")
	compressedPath := filepath.Join(directory, "test-app_20260829_2.jsonl.gz")
	writeArchiveSizedFile(
		t,
		oldPath,
		1,
		now.AddDate(0, 0, -handler.Config.MaxFileAge()-1),
	)
	writeArchiveSizedFile(t, compressedPath, 1, now)
	// 未圧縮fileは送信後も通常の保持・purgeへ残ります。送信済みの世代として記録し、
	// 送信待ちの保護ではなく保持期限のpurge対象にします。
	advanceArchiveUploadCursorForTest(t, handler, failureCategory, oldPath)

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		// 構築時に予約されるbootstrap target scanではなく、post-rotationと同じ
		// directory全体観測を明示し、purge/report panic時の所有権解放を検証します。
		_ = handler.runInlineArchivePurgeWithOptions(
			context.Background(),
			failureCategory,
			false,
			"",
		)
	}()
	if recovered != panicValue {
		t.Fatalf("recovered panic = %#v, want %q", recovered, panicValue)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("purged archive stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(compressedPath); err != nil {
		t.Fatalf("retained compressed archive stat error = %v", err)
	}
	assertArchivePathProtected(
		t,
		handler.retryableDirectory.guard,
		compressedPath,
		false,
	)

	tracker := handler.directorySizeTracker(failureCategory)
	if !tracker.TryStartObservation(now.Add(time.Minute), handler.Config, false) {
		t.Fatal("panic after purge left a valid directory observation cache")
	}
	tracker.AbortObservation()
}

type lifecycleReentrantFailureArchiveUploader struct {
	guard *appendlog.DirectoryGuard
	mu    sync.Mutex
	paths []string
}

func (u *lifecycleReentrantFailureArchiveUploader) Upload(
	_ context.Context,
	input uploader.UploadRequest,
) error {
	return u.guard.RunWithLifecycleLock(func() error {
		u.mu.Lock()
		defer u.mu.Unlock()
		u.paths = append(u.paths, input.FilePath())
		return nil
	})
}

func (u *lifecycleReentrantFailureArchiveUploader) pathsSnapshot() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.paths...)
}

type sequencedFailureArchiveUploader struct {
	mu                sync.Mutex
	failuresRemaining int
	attemptedPaths    []string
}

type firstDeleteBlockingFailureArchiveUploader struct {
	mu                sync.Mutex
	blockFirstRemoval func()
	attempts          int
}

type panicFailureArchiveUploader struct{}

type blockingFirstFailureArchiveUploader struct {
	mu       sync.Mutex
	release  <-chan struct{}
	started  chan<- string
	attempts int
}

type snapshotOrderingFailureArchiveUploader struct {
	mu               sync.Mutex
	directPath       string
	pendingPath      string
	directStarted    chan struct{}
	directRelease    <-chan struct{}
	directReturned   chan struct{}
	pendingStarted   chan struct{}
	pendingRelease   <-chan struct{}
	attemptsByPath   map[string]int
	directStartOnce  sync.Once
	directReturnOnce sync.Once
	pendingStartOnce sync.Once
}

type overlappingFirstFailureArchiveUploader struct {
	mu       sync.Mutex
	release  <-chan struct{}
	started  chan<- string
	attempts map[string]int
}

type concurrentRetryRotationFailureUploader struct {
	mu             sync.Mutex
	oldPendingPath string
	newPendingPath string
	oldStarted     chan struct{}
	oldRelease     <-chan struct{}
	newFailed      chan struct{}
	attempts       map[string]int
	oldStartedOnce sync.Once
	newFailedOnce  sync.Once
}

func (panicFailureArchiveUploader) Upload(context.Context, uploader.UploadRequest) error {
	panic("failure archive upload panic")
}

func (u *blockingFirstFailureArchiveUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	u.mu.Lock()
	u.attempts++
	attempt := u.attempts
	u.mu.Unlock()
	u.started <- input.FilePath()
	if attempt != 1 {
		return nil
	}
	select {
	case <-u.release:
		return errors.New("object storage unavailable")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (u *blockingFirstFailureArchiveUploader) attemptCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attempts
}

func (u *snapshotOrderingFailureArchiveUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	u.mu.Lock()
	u.attemptsByPath[input.FilePath()]++
	attempt := u.attemptsByPath[input.FilePath()]
	u.mu.Unlock()

	switch input.FilePath() {
	case u.directPath:
		if attempt != 1 {
			return nil
		}
		u.directStartOnce.Do(func() { close(u.directStarted) })
		select {
		case <-u.directRelease:
			u.directReturnOnce.Do(func() { close(u.directReturned) })
			return errors.New("object storage unavailable")
		case <-ctx.Done():
			return ctx.Err()
		}
	case u.pendingPath:
		u.pendingStartOnce.Do(func() { close(u.pendingStarted) })
		select {
		case <-u.pendingRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	default:
		return nil
	}
}

func (u *snapshotOrderingFailureArchiveUploader) attemptCount(path string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attemptsByPath[path]
}

func closeArchiveTestChannel(channel chan struct{}) {
	select {
	case <-channel:
	default:
		close(channel)
	}
}

func (u *overlappingFirstFailureArchiveUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	u.mu.Lock()
	u.attempts[input.FilePath()]++
	attempt := u.attempts[input.FilePath()]
	u.mu.Unlock()
	if attempt != 1 {
		return nil
	}
	u.started <- input.FilePath()
	select {
	case <-u.release:
		return errors.New("object storage unavailable")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (u *overlappingFirstFailureArchiveUploader) attemptCount(path string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attempts[path]
}

func (u *concurrentRetryRotationFailureUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	u.mu.Lock()
	u.attempts[input.FilePath()]++
	attempt := u.attempts[input.FilePath()]
	u.mu.Unlock()

	switch input.FilePath() {
	case u.oldPendingPath:
		if attempt != 1 {
			return nil
		}
		u.oldStartedOnce.Do(func() { close(u.oldStarted) })
		select {
		case <-u.oldRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case u.newPendingPath:
		if attempt != 1 {
			return nil
		}
		u.newFailedOnce.Do(func() { close(u.newFailed) })
		return errors.New("object storage unavailable for new rotation")
	default:
		return nil
	}
}

func (u *concurrentRetryRotationFailureUploader) attemptCount(path string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attempts[path]
}

func (u *firstDeleteBlockingFailureArchiveUploader) Upload(
	_ context.Context,
	_ uploader.UploadRequest,
) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.attempts++
	if u.attempts == 1 {
		u.blockFirstRemoval()
	}
	return nil
}

func (u *firstDeleteBlockingFailureArchiveUploader) attemptCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attempts
}

func (u *sequencedFailureArchiveUploader) Upload(
	_ context.Context,
	input uploader.UploadRequest,
) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.attemptedPaths = append(u.attemptedPaths, input.FilePath())
	if u.failuresRemaining > 0 {
		u.failuresRemaining--
		return errors.New("object storage unavailable")
	}
	return nil
}

func (u *sequencedFailureArchiveUploader) attemptCounts() map[string]int {
	u.mu.Lock()
	defer u.mu.Unlock()
	counts := make(map[string]int, len(u.attemptedPaths))
	for _, path := range u.attemptedPaths {
		counts[path]++
	}
	return counts
}

func (u *sequencedFailureArchiveUploader) attemptedPathOrder() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.attemptedPaths...)
}

func archiveUploadRetryScheduleSnapshot(
	schedule *archiveUploadRetrySchedule,
) (bool, time.Time) {
	schedule.mu.Lock()
	defer schedule.mu.Unlock()
	return schedule.scheduled, schedule.retryAt
}
