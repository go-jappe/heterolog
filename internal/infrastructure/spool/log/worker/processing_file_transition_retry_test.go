package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProcessingFileTransitionRetryRejectsNonPositiveRetryDelay(t *testing.T) {
	for _, retryDelay := range []time.Duration{0, -time.Nanosecond} {
		t.Run(retryDelay.String(), func(t *testing.T) {
			retry, err := NewProcessingFileTransitionRetry(
				NewProcessingFileTransitionRetryConfigAdapter(ProcessingFileTransitionRetryConfig{RetryDelay: retryDelay}),
			)
			require.ErrorIs(t, err, ErrInvalidProcessingFileTransitionRetryDelay)
			assert.Nil(t, retry)
		})
	}
}

func TestNewProcessingFileTransitionRetryRejectsNonPositiveMaxEntries(t *testing.T) {
	for _, maxEntries := range []int{0, -1} {
		t.Run(fmt.Sprintf("%d", maxEntries), func(t *testing.T) {
			retry, err := NewProcessingFileTransitionRetry(
				NewProcessingFileTransitionRetryConfigAdapter(ProcessingFileTransitionRetryConfig{
					RetryDelay: time.Second,
					MaxEntries: maxEntries,
				}),
			)
			require.ErrorIs(t, err, ErrInvalidProcessingFileTransitionRetryMaxEntries)
			assert.Nil(t, retry)
		})
	}
}

func TestProcessingFileTransitionRetryUpsertFindReadyAndRemove(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Minute,
		MaxEntries: 100,
	})
	registerErr := errors.New("store done failed")

	retry.Upsert("processing/a", ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone, registerErr, now)

	assert.True(t, retry.Contains("processing/a"))
	assert.Equal(t, 1, retry.Count())

	_, _, found := retry.FindReady(now.Add(59 * time.Second))
	assert.False(t, found)

	path, entry, found := retry.FindReady(now.Add(time.Minute))
	require.True(t, found)
	assert.Equal(t, "processing/a", path)
	assert.Equal(t, ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone, entry.Operation)
	assert.Equal(t, "store done failed", entry.ErrorText)
	assert.True(t, entry.NextRetryAt.Equal(now.Add(time.Minute)))

	retry.Remove("processing/a")

	assert.False(t, retry.Contains("processing/a"))
	assert.Zero(t, retry.Count())
}

func TestProcessingFileTransitionRetryRequeueKeepsOperationAndPlan(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 9, 12, 30, 0, 0, time.UTC)
	plan := &ProcessingRedistributionPlan{
		ToDoneRecords: []spool.SpoolRecord{testRecord("success")},
	}
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: 2 * time.Minute,
		MaxEntries: 100,
	})
	retry.UpsertRedistribution(
		"processing/a",
		ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone,
		plan,
		errors.New("first failure"),
		now,
	)

	entry := retry.items["processing/a"]
	retry.Requeue("processing/a", entry, errors.New("second failure"), now.Add(time.Minute))

	actual := retry.items["processing/a"]
	assert.Equal(t, ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone, actual.Operation)
	assert.Same(t, plan, actual.Redistribution)
	assert.Equal(t, "second failure", actual.ErrorText)
	assert.True(t, actual.NextRetryAt.Equal(now.Add(3*time.Minute)))
}

func TestProcessingFileTransitionRetryIsFull(t *testing.T) {
	t.Parallel()

	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Second,
		MaxEntries: 1,
	})

	assert.False(t, retry.IsFull())

	retry.items["processing/a"] = ProcessingFileTransitionRetryEntry{}

	assert.True(t, retry.IsFull())

	retry.Upsert("processing/a", ProcessingFileTransitionRetryOpMoveProcessingToDone, nil, time.Now())

	assert.True(t, retry.IsFull())
}

func TestWorkerRunOnceProcessingFileTransitionRetryMovesDoneAndRemovesEntry(t *testing.T) {
	t.Parallel()

	store := &workerStoreStub{exists: map[string]bool{"processing/a": true}}
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Second,
		MaxEntries: 100,
	})
	retry.items["processing/a"] = ProcessingFileTransitionRetryEntry{
		Operation: ProcessingFileTransitionRetryOpMoveProcessingToDone,
	}
	worker := &Worker{
		TargetBackendService:          "opensearch",
		Store:                         store,
		ProcessingFileTransitionRetry: retry,
		ProcessingPathBackoff:         NewProcessingPathBackoff(ProcessingPathBackoffConfig{}),
	}

	report, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())

	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, ProcessingFileTransitionRetryStatusSucceeded, report.Status)
	assert.Equal(t, "opensearch", report.TargetBackendService)
	assert.Equal(t, "processing/a", report.ProcessingPath)
	assert.Equal(t, ProcessingFileTransitionRetryOpMoveProcessingToDone, report.Operation)
	assert.Equal(t, []string{"exists:processing/a", "move_done:processing/a"}, store.calls)
	assert.False(t, retry.Contains("processing/a"))
}

func TestWorkerRunOnceProcessingFileTransitionRetryRequeuesOnMoveFailure(t *testing.T) {
	t.Parallel()

	moveErr := errors.New("move dead failed")
	store := &workerStoreStub{
		exists:      map[string]bool{"processing/a": true},
		moveDeadErr: moveErr,
	}
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Minute,
		MaxEntries: 100,
	})
	retry.items["processing/a"] = ProcessingFileTransitionRetryEntry{
		Operation: ProcessingFileTransitionRetryOpMoveProcessingToDead,
	}
	worker := &Worker{
		Store:                         store,
		ProcessingFileTransitionRetry: retry,
	}

	report, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())

	require.ErrorIs(t, err, moveErr)
	assert.True(t, handled)
	assert.Equal(t, ProcessingFileTransitionRetryStatusFailed, report.Status)
	assert.ErrorIs(t, report.Err, moveErr)
	assert.True(t, retry.Contains("processing/a"))
	assert.Equal(t, ProcessingFileTransitionRetryOpMoveProcessingToDead, retry.items["processing/a"].Operation)
	assert.Equal(t, "move dead failed", retry.items["processing/a"].ErrorText)
	assert.False(t, retry.items["processing/a"].NextRetryAt.IsZero())
}

func TestWorkerRunOnceProcessingFileTransitionRetryMissingPlanQuarantinesPath(t *testing.T) {
	t.Parallel()

	store := &workerStoreStub{exists: map[string]bool{"processing/a": true}}
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Second,
		MaxEntries: 100,
	})
	retry.items["processing/a"] = ProcessingFileTransitionRetryEntry{
		Operation: ProcessingFileTransitionRetryOpStoreRecordsToActive,
	}
	worker := &Worker{
		Store:                         store,
		ProcessingFileTransitionRetry: retry,
	}

	report, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())

	require.ErrorIs(t, err, ErrMissingProcessingRedistributionPlan)
	assert.True(t, handled)
	assert.Equal(t, ProcessingFileTransitionRetryStatusFailed, report.Status)
	assert.ErrorIs(t, report.Err, ErrMissingProcessingRedistributionPlan)
	assert.Equal(t, []string{"exists:processing/a", "move_quarantine:processing/a"}, store.calls)
	assert.Equal(t, []spool.QuarantineReason{
		spool.QuarantineReasonInvalidTransitionState,
	}, store.quarantineReasons)
	assert.False(t, retry.Contains("processing/a"))
}

func TestWorkerRunOnceProcessingFileTransitionRetryRedistributionResumesAndKeepsFailedStep(t *testing.T) {
	t.Parallel()

	quarantineErr := errors.New("store quarantine failed")
	store := &workerStoreStub{
		exists:             map[string]bool{"processing/a": true},
		storeQuarantineErr: quarantineErr,
	}
	activeStore := &activeStoreStub{}
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Minute,
		MaxEntries: 100,
	})
	retry.items["processing/a"] = ProcessingFileTransitionRetryEntry{
		Operation: ProcessingFileTransitionRetryOpStoreRecordsToActive,
		Redistribution: &ProcessingRedistributionPlan{
			ToActiveRecords:     []spool.SpoolRecord{testRecord("retryable")},
			ToDeadRecords:       []spool.SpoolRecord{testRecord("permanent")},
			ToQuarantineRecords: []spool.SpoolRecord{testRecord("invalid")},
			ToDoneRecords:       []spool.SpoolRecord{testRecord("success")},
		},
	}
	worker := &Worker{
		Store:                         store,
		ActiveSpoolWriter:             activeStore,
		ProcessingFileTransitionRetry: retry,
	}

	report, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())

	require.ErrorIs(t, err, quarantineErr)
	assert.True(t, handled)
	assert.Equal(t, ProcessingFileTransitionRetryStatusFailed, report.Status)
	assert.Equal(t, ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine, report.Operation)
	assert.Equal(t, []string{
		"exists:processing/a",
		"store_dead:processing/a",
		"store_quarantine:processing/a",
	}, store.calls)
	require.Len(t, activeStore.records, 1)
	assert.Equal(t, []spool.SpoolRecord{testRecord("retryable")}, activeStore.records[0])

	entry := retry.items["processing/a"]
	assert.Equal(t, ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine, entry.Operation)
	require.NotNil(t, entry.Redistribution)
	assert.Empty(t, entry.Redistribution.ToActiveRecords)
	assert.Empty(t, entry.Redistribution.ToDeadRecords)
	assert.Equal(t, []spool.SpoolRecord{testRecord("invalid")}, entry.Redistribution.ToQuarantineRecords)
	assert.Equal(t, []spool.SpoolRecord{testRecord("success")}, entry.Redistribution.ToDoneRecords)
}

func TestWorkerRunOnceProcessingFileTransitionRetryRedistributesPlanWhenProcessingFileIsMissing(t *testing.T) {
	t.Parallel()

	store := &workerStoreStub{exists: map[string]bool{"processing/a": false}}
	activeStore := &activeStoreStub{}
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Second,
		MaxEntries: 100,
	})
	retry.items["processing/a"] = ProcessingFileTransitionRetryEntry{
		Operation: ProcessingFileTransitionRetryOpStoreRecordsToActive,
		Redistribution: &ProcessingRedistributionPlan{
			ToActiveRecords:     []spool.SpoolRecord{testRecord("retryable")},
			ToDeadRecords:       []spool.SpoolRecord{testRecord("permanent")},
			ToQuarantineRecords: []spool.SpoolRecord{testRecord("invalid")},
			ToDoneRecords:       []spool.SpoolRecord{testRecord("success")},
		},
	}
	worker := &Worker{
		Store:                         store,
		ActiveSpoolWriter:             activeStore,
		ProcessingFileTransitionRetry: retry,
	}

	report, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())

	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, ProcessingFileTransitionRetryStatusSucceeded, report.Status)
	assert.Equal(t, ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution, report.Operation)
	assert.Equal(t, []string{
		"exists:processing/a",
		"store_dead:processing/a",
		"store_quarantine:processing/a",
		"store_done:processing/a",
		"remove:processing/a",
	}, store.calls)
	require.Len(t, activeStore.records, 1)
	assertRecordIDs(t, activeStore.records[0], []string{"retryable"})
	require.Len(t, store.storedDead, 1)
	assertRecordIDs(t, store.storedDead[0].records, []string{"permanent"})
	require.Len(t, store.storedQuarantine, 1)
	assertRecordIDs(t, store.storedQuarantine[0].records, []string{"invalid"})
	require.Len(t, store.storedDone, 1)
	assertRecordIDs(t, store.storedDone[0].records, []string{"success"})
	assert.False(t, retry.Contains("processing/a"))
}

func TestRemoveProcessingRetryConfirmsDurabilityEvenWhenFileIsAlreadyMissing(t *testing.T) {
	syncErr := errors.New("processing directory sync failed")
	store := &workerStoreStub{exists: map[string]bool{"processing/a": false}, removeErr: syncErr}
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{RetryDelay: time.Second, MaxEntries: 10})
	retry.items["processing/a"] = ProcessingFileTransitionRetryEntry{Operation: ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution}
	w := &Worker{Store: store, ProcessingFileTransitionRetry: retry}
	report, handled, err := w.RunOnceProcessingFileTransitionRetry(context.Background())
	require.True(t, handled)
	require.ErrorIs(t, err, syncErr)
	require.Equal(t, ProcessingFileTransitionRetryStatusFailed, report.Status)
	require.True(t, retry.Contains("processing/a"), "absence does not prove the earlier unlink was synced")
	require.Equal(t, []string{"exists:processing/a", "remove:processing/a"}, store.calls)
	store.removeErr = nil
	entry := retry.items["processing/a"]
	entry.NextRetryAt = time.Time{}
	retry.items["processing/a"] = entry
	report, handled, err = w.RunOnceProcessingFileTransitionRetry(context.Background())
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, ProcessingFileTransitionRetryStatusSucceeded, report.Status)
	require.False(t, retry.Contains("processing/a"))
	require.Equal(t, []string{"exists:processing/a", "remove:processing/a", "exists:processing/a", "remove:processing/a"}, store.calls)
}
