package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunOnceSpoolFileProcessReachesHealthyTailThroughReadFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		failures     int
		cacheEntries int
	}{
		{name: "shared expiry during 400 persistent failures", failures: 400},
		{name: "overflow of two entry cache", failures: 8, cacheEntries: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				paths := make([]string, 0, tc.failures+1)
				for i := 0; i < tc.failures; i++ {
					paths = append(paths, fmt.Sprintf("processing/2026/10/21/%04d-failing.jsonl", i))
				}
				healthy := "processing/2026/10/21/9999-healthy.jsonl"
				paths = append(paths, healthy)
				worker, store, sender := newSelectionTestWorker(t, paths...)
				worker.ProcessingPathBackoff = NewProcessingPathBackoff(ProcessingPathBackoffConfig{
					MaxPathCacheEntries: tc.cacheEntries,
				})
				readErr := errors.New("persistent read failure")
				for _, path := range paths[:tc.failures] {
					store.readErr[path] = readErr
				}

				startedAt := time.Now()
				for _, path := range paths[:tc.failures] {
					report, err := worker.RunOnceSpoolFileProcess(context.Background())
					require.ErrorIs(t, err, readErr)
					require.Equal(t, path, report.ProcessingPath, "the cursor must advance despite read failure and cache expiry")
					require.Equal(t, ProcessStatusFailed, report.Status)
					require.Zero(t, sender.calls)
					if tc.cacheEntries > 0 {
						require.LessOrEqual(t, worker.ProcessingPathBackoff.PathCacheCount(), tc.cacheEntries)
					}
					time.Sleep(time.Second)
				}
				if tc.failures == 400 {
					require.Greater(t, time.Since(startedAt), defaultProcessingPathSkipHoldDuration)
				}

				report, err := worker.RunOnceSpoolFileProcess(context.Background())
				require.NoError(t, err)
				assert.Equal(t, ProcessStatusCompleted, report.Status)
				assert.Equal(t, healthy, report.ProcessingPath)
				assert.Equal(t, 1, sender.calls)
				assertRecordIDs(t, sender.records, []string{healthy})
			})
		})
	}
}

func TestRunOnceSpoolFileProcessRevisitsExpiredEarlierFailureWhileLaterTransitionIsPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		earlier, later := "processing/a", "processing/b"
		worker, store, sender := newSelectionTestWorker(t, earlier, later)
		readErr := errors.New("read temporarily failed")
		store.readErr[earlier] = readErr
		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.ErrorIs(t, err, readErr)
		require.Equal(t, earlier, report.ProcessingPath)

		transitionErr := errors.New("store done failed")
		store.storeDoneErr = transitionErr
		report, err = worker.RunOnceSpoolFileProcess(context.Background())
		require.ErrorIs(t, err, transitionErr)
		require.Equal(t, later, report.ProcessingPath)
		require.True(t, worker.processingFileTransitionRetry().Contains(later))
		require.Equal(t, 1, sender.calls)

		delete(store.readErr, earlier)
		store.storeDoneErr = nil
		time.Sleep(defaultProcessingPathSkipHoldDuration)
		report, err = worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, report.Status)
		assert.Equal(t, earlier, report.ProcessingPath)
		assert.Equal(t, 2, sender.calls)
		assertRecordIDs(t, sender.records, []string{earlier})
		assert.True(t, worker.processingFileTransitionRetry().Contains(later))
		assert.Equal(t, 1, countWorkerStoreCalls(store, "read:"+later), "pending transition must not resend")
	})
}

func TestRunOnceSpoolFileProcessKeepsProgressWhenCursorAndEarlierFilesChange(t *testing.T) {
	t.Parallel()
	worker, store, sender := newSelectionTestWorker(t, "processing/b", "processing/d", "processing/f")
	for _, path := range []string{"processing/b", "processing/d"} {
		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.Equal(t, path, report.ProcessingPath, "advance only through checked candidates, not the whole fetched page")
	}

	// 最後に確認したファイルとその前のファイルが削除され、より古い名前が追加されても、
	// 先頭へ折り返す前に、保存済みの境界より後にあるファイルを確認します。
	store.processingPaths = []string{"processing/a", "processing/f"}
	store.readRecords["processing/a"] = []spool.SpoolRecord{testRecord("processing/a")}
	report, err := worker.RunOnceSpoolFileProcess(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "processing/f", report.ProcessingPath)
	assertRecordIDs(t, sender.records, []string{"processing/f"})

	store.processingPaths = []string{"processing/a"}
	report, err = worker.RunOnceSpoolFileProcess(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "processing/a", report.ProcessingPath)
	assertRecordIDs(t, sender.records, []string{"processing/a"})
	assert.Equal(t, 4, sender.calls)
}

func TestRunOnceSpoolFileProcessWrapsToRecoveredOlderFile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		older, newer := "processing/a", "processing/z"
		worker, store, sender := newSelectionTestWorker(t, older, newer)
		readErr := errors.New("temporary read failure")
		store.readErr[older] = readErr
		_, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.ErrorIs(t, err, readErr)
		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.Equal(t, newer, report.ProcessingPath)

		store.processingPaths = []string{older}
		delete(store.readErr, older)
		time.Sleep(defaultProcessingPathSkipHoldDuration)
		report, err = worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, report.Status)
		assert.Equal(t, older, report.ProcessingPath)
		assert.Equal(t, 2, sender.calls)
		assertRecordIDs(t, sender.records, []string{older})
	})
}

func TestRunOnceSpoolFileProcessBoundsBlockedScanAndContinuesNextCycle(t *testing.T) {
	t.Parallel()
	blockedCount := defaultProcessingCandidatesPerSelection + defaultProcessingCandidatePageSize + 3
	paths := make([]string, 0, blockedCount+1)
	for i := 0; i < blockedCount; i++ {
		paths = append(paths, fmt.Sprintf("processing/%06d-pending", i))
	}
	healthy := "processing/999999-ready"
	paths = append(paths, healthy)
	worker, store, sender := newSelectionTestWorker(t, paths...)
	for _, path := range paths[:blockedCount] {
		worker.processingFileTransitionRetry().Upsert(path, ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution, nil, time.Now())
	}
	listed := 0
	store.listCandidatesFn = func(ctx context.Context, after string, limit int) ([]spool.FileCandidate, error) {
		require.Positive(t, limit)
		require.LessOrEqual(t, limit, defaultProcessingCandidatePageSize)
		candidates := firstProcessingCandidatesAfter(store.processingPaths, after, limit)
		listed += len(candidates)
		return candidates, nil
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())
	require.NoError(t, err)
	assert.Equal(t, ProcessStatusNoTarget, report.Status)
	assert.Zero(t, sender.calls)
	require.Positive(t, listed)
	assert.LessOrEqual(t, listed, defaultProcessingCandidatesPerSelection)
	previousCursor := worker.processingScanAfter
	require.NotEmpty(t, previousCursor)
	previousCalls := len(store.listCandidateCalls)

	for attempts := 0; attempts < blockedCount+1 && sender.calls == 0; attempts++ {
		listed = 0
		report, err = worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.LessOrEqual(t, listed, defaultProcessingCandidatesPerSelection)
	}
	require.Greater(t, len(store.listCandidateCalls), previousCalls)
	assert.Equal(t, previousCursor, store.listCandidateCalls[previousCalls].after)
	assert.Equal(t, ProcessStatusCompleted, report.Status)
	assert.Equal(t, healthy, report.ProcessingPath)
	assert.Equal(t, 1, sender.calls)
	assertRecordIDs(t, sender.records, []string{healthy})
}

func TestSelectProcessingPathRetainsCheckedProgressAfterListFailureOrCancellation(t *testing.T) {
	for _, cancelDuringList := range []bool{false, true} {
		name := "listing error"
		if cancelDuringList {
			name = "context cancellation"
		}
		t.Run(name, func(t *testing.T) {
			paths := make([]string, defaultProcessingCandidatePageSize+1)
			for i := range paths {
				paths[i] = fmt.Sprintf("processing/%06d", i)
			}
			worker, store, sender := newSelectionTestWorker(t, paths...)
			for _, path := range paths[:len(paths)-1] {
				worker.processingFileTransitionRetry().Upsert(path, ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution, nil, time.Now())
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			listErr := errors.New("candidate listing failed")
			wantErr := listErr
			if cancelDuringList {
				wantErr = context.Canceled
			}
			store.listCandidatesFn = func(ctx context.Context, after string, limit int) ([]spool.FileCandidate, error) {
				if after != "" {
					if cancelDuringList {
						cancel()
						return nil, ctx.Err()
					}
					return nil, listErr
				}
				return firstProcessingCandidatesAfter(store.processingPaths, after, limit), nil
			}

			report, err := worker.RunOnceSpoolFileProcess(ctx)
			require.ErrorIs(t, err, wantErr)
			assert.Equal(t, ProcessStatusFailed, report.Status)
			assert.Zero(t, sender.calls)
			wantCursor := strings.TrimPrefix(paths[len(paths)-2], "processing/")
			require.Equal(t, wantCursor, worker.processingScanAfter)
			previousCalls := len(store.listCandidateCalls)

			store.listCandidatesFn = nil
			report, err = worker.RunOnceSpoolFileProcess(context.Background())
			require.NoError(t, err)
			assert.Equal(t, wantCursor, store.listCandidateCalls[previousCalls].after)
			assert.Equal(t, paths[len(paths)-1], report.ProcessingPath)
			assert.Equal(t, ProcessStatusCompleted, report.Status)
			assert.Equal(t, 1, sender.calls)
		})
	}
}

func TestSelectProcessingPathUsesActualPathForMembershipAndRelativePathForProgress(t *testing.T) {
	t.Parallel()
	candidates := []spool.FileCandidate{
		{Path: "/spool/processing/read-failure", RelativePath: "2026/01"},
		{Path: "/spool/processing/transition-pending", RelativePath: "2026/02"},
		{Path: "/spool/processing/healthy", RelativePath: "2026/03"},
	}
	worker, store, _ := newSelectionTestWorker(t)
	worker.processingPathBackoff().BlockPath(candidates[0].Path, ProcessingPathBackoffOpReadProcessingRecords, nil, time.Now())
	worker.processingFileTransitionRetry().Upsert(candidates[1].Path, ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution, nil, time.Now())
	store.listCandidatesFn = func(ctx context.Context, after string, limit int) ([]spool.FileCandidate, error) {
		require.Empty(t, after)
		return candidates, nil
	}

	path, ok, err := worker.selectProcessingPath(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, candidates[2].Path, path)
	assert.Equal(t, candidates[2].RelativePath, worker.processingScanAfter)
}

func TestSelectProcessingPathDoesNotSkipHealthyPrefixForUnrelatedMembership(t *testing.T) {
	t.Parallel()
	worker, _, _ := newSelectionTestWorker(t, "processing/a-healthy", "processing/z-pending")
	worker.processingPathBackoff().BlockPath("processing/deleted", ProcessingPathBackoffOpReadProcessingRecords, nil, time.Now())
	worker.processingFileTransitionRetry().Upsert("processing/z-pending", ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution, nil, time.Now())

	path, ok, err := worker.selectProcessingPath(context.Background())
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "processing/a-healthy", path)
}

func TestSelectProcessingPathWrapsOnlyOnceWhenEveryPathIsBlocked(t *testing.T) {
	t.Parallel()
	worker, store, _ := newSelectionTestWorker(t, "processing/a", "processing/b", "processing/c")
	worker.processingScanAfter = "b"
	for _, path := range store.processingPaths {
		worker.processingFileTransitionRetry().Upsert(path, ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution, nil, time.Now())
	}

	path, ok, err := worker.selectProcessingPath(context.Background())
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Empty(t, path)
	require.NotEmpty(t, store.listCandidateCalls)
	assert.Equal(t, "b", store.listCandidateCalls[0].after)
	assert.LessOrEqual(t, len(store.listCandidateCalls), 4, "one pass before and after wrapping a short blocked set must terminate")
	wraps := 0
	for _, call := range store.listCandidateCalls {
		if call.after == "" {
			wraps++
		}
	}
	assert.Equal(t, 1, wraps)
}

func newSelectionTestWorker(t *testing.T, paths ...string) (*Worker, *workerStoreStub, *senderStub) {
	t.Helper()
	store := &workerStoreStub{
		processingPaths: append([]string(nil), paths...),
		readRecords:     make(map[string][]spool.SpoolRecord, len(paths)),
		readErr:         make(map[string]error),
	}
	for _, path := range paths {
		store.readRecords[path] = []spool.SpoolRecord{testRecord(path)}
	}
	sender := &senderStub{report: mustExportReportForStatuses(t, spool.RecordOutcomeStatusSuccess)}
	worker, err := New(store, &activeStoreStub{}, sender)
	require.NoError(t, err)
	return worker, store, sender
}

func countWorkerStoreCalls(store *workerStoreStub, want string) int {
	count := 0
	for _, call := range store.calls {
		if call == want {
			count++
		}
	}
	return count
}
