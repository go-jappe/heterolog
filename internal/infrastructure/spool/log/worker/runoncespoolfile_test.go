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

func TestNewRejectsMissingDependencies(t *testing.T) {
	t.Parallel()

	store := &workerStoreStub{}
	activeSpoolWriter := &activeStoreStub{}
	sender := &senderStub{}

	tests := []struct {
		name              string
		store             WorkFileStore
		activeSpoolWriter ActiveSpoolWriter
		sender            Sender
		wantErr           error
	}{
		{
			name:              "nil store",
			activeSpoolWriter: activeSpoolWriter,
			sender:            sender,
			wantErr:           ErrWorkerStoreNil,
		},
		{
			name:    "nil active spool writer",
			store:   store,
			sender:  sender,
			wantErr: ErrWorkerActiveSpoolWriterNil,
		},
		{
			name:              "nil sender",
			store:             store,
			activeSpoolWriter: activeSpoolWriter,
			wantErr:           ErrWorkerSenderNil,
		},
		{
			name:              "valid",
			store:             store,
			activeSpoolWriter: activeSpoolWriter,
			sender:            sender,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			worker, err := New(tt.store, tt.activeSpoolWriter, tt.sender)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, worker)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, worker)
			assert.Same(t, store, worker.Store)
			assert.Same(t, activeSpoolWriter, worker.ActiveSpoolWriter)
			assert.Same(t, sender, worker.Sender)
			assert.NotNil(t, worker.ProcessingPathBackoff)
			assert.NotNil(t, worker.ProcessingFileTransitionRetry)
		})
	}
}

func TestNewUsesConfiguredTransitionRetryOrCreatesDefault(t *testing.T) {
	t.Parallel()
	configured, err := NewProcessingFileTransitionRetry(NewProcessingFileTransitionRetryConfigAdapter(
		ProcessingFileTransitionRetryConfig{RetryDelay: 7 * time.Second, MaxEntries: 3},
	))
	require.NoError(t, err)
	for _, retry := range []*ProcessingFileTransitionRetry{configured, nil} {
		created, err := New(&workerStoreStub{}, &activeStoreStub{}, &senderStub{},
			WithTargetBackendService("opensearch"), WithProcessingFileTransitionRetry(retry))
		require.NoError(t, err)
		require.Equal(t, "opensearch", created.TargetBackendService)
		if retry != nil {
			require.Same(t, retry, created.ProcessingFileTransitionRetry)
			require.Equal(t, 7*time.Second, created.ProcessingFileTransitionRetry.cfg.RetryDelay())
			require.Equal(t, 3, created.ProcessingFileTransitionRetry.cfg.MaxEntries())
		} else {
			require.NotNil(t, created.ProcessingFileTransitionRetry)
			require.Equal(t, defaultProcessingFileTransitionRetryDelay, created.ProcessingFileTransitionRetry.cfg.RetryDelay())
			require.Equal(t, defaultProcessingFileTransitionRetryMaxEntries, created.ProcessingFileTransitionRetry.cfg.MaxEntries())
		}
	}
}

func TestRunOnceSpoolFileProcessRejectsOutcomeCountMismatchBeforeFileTransition(t *testing.T) {
	t.Parallel()

	path := "processing/incomplete-report"
	records := []spool.SpoolRecord{testRecord("first"), testRecord("second")}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
	}
	worker := &Worker{
		Store: store,
		Sender: &senderStub{report: mustExportReportForStatuses(
			t,
			spool.RecordOutcomeStatusSuccess,
		)},
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.ErrorIs(t, err, spool.ErrInvalidExportReport)
	assert.Equal(t, ProcessStatusQuarantined, report.Status)
	assert.ErrorIs(t, report.Err, spool.ErrInvalidExportReport)
	assert.Equal(t, len(records), report.QuarantineRecordCount)
	assert.Equal(t, spool.QuarantineReasonInvalidExportReport, report.QuarantineReason)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"move_quarantine:" + path,
	}, store.calls)
	assert.Empty(t, store.storedDone)
	assert.Empty(t, store.storedDead)
	assert.Empty(t, store.storedQuarantine)
	assert.Empty(t, worker.processingFileTransitionRetry().items)
}

func TestRunOnceSpoolFileProcessRejectsEmptyExportReportReturnedWithSendError(t *testing.T) {
	t.Parallel()

	path := "processing/empty-error-report"
	sendErr := errors.New("sender stopped before reporting records")
	records := []spool.SpoolRecord{testRecord("unreported")}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
	}
	worker := &Worker{
		Store:  store,
		Sender: &senderStub{err: sendErr},
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.ErrorIs(t, err, sendErr)
	require.ErrorIs(t, err, spool.ErrInvalidExportReport)
	assert.Equal(t, ProcessStatusQuarantined, report.Status)
	assert.ErrorIs(t, report.Err, sendErr)
	assert.ErrorIs(t, report.Err, spool.ErrInvalidExportReport)
	assert.Equal(t, len(records), report.QuarantineRecordCount)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"move_quarantine:" + path,
	}, store.calls)
	assert.Empty(t, store.storedDone)
	assert.Empty(t, store.storedDead)
	assert.Empty(t, store.storedQuarantine)
	assert.Empty(t, worker.processingFileTransitionRetry().items)
}

func TestRunOnceSpoolFileProcessRejectsDuplicateAndMissingInputIndexBeforeFileTransition(t *testing.T) {
	t.Parallel()

	path := "processing/duplicate-input-index-report"
	records := []spool.SpoolRecord{testRecord("first"), testRecord("second")}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
	}
	incompleteReport := spool.ExportReport{
		{InputIndex: 0, Status: spool.RecordOutcomeStatusRetryable, Err: errors.New("retry later")},
		{InputIndex: 0, Status: spool.RecordOutcomeStatusRetryable, Err: errors.New("retry later")},
	}
	worker := &Worker{
		Store:  store,
		Sender: &senderStub{report: incompleteReport},
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.ErrorIs(t, err, spool.ErrInvalidExportReport)
	assert.Equal(t, ProcessStatusQuarantined, report.Status)
	assert.ErrorIs(t, report.Err, spool.ErrInvalidExportReport)
	assert.Equal(t, len(records), report.QuarantineRecordCount)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"move_quarantine:" + path,
	}, store.calls)
}

func TestProcessingRedistributionPlanFromOutcomesUsesInputIndexAndAppliesAttemptMetadata(t *testing.T) {
	t.Parallel()

	occurredAt := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	previousAttemptedAt := occurredAt.Add(time.Minute)
	attemptedAt := occurredAt.Add(2 * time.Minute)
	duplicate := spool.SpoolRecord{
		OccurredAt: occurredAt,
		RequestID:  "same-request",
		Payload:    []byte(`{"message":"same"}`),
	}
	previouslyAttemptedDuplicate := duplicate
	previouslyAttemptedDuplicate.FirstResendAttemptedAt = previousAttemptedAt
	previouslyAttemptedDuplicate.LastResendAttemptedAt = previousAttemptedAt
	previouslyAttemptedDuplicate.ResendAttemptCount = 2
	unattemptedRetryable := testRecord("unattempted-retryable")
	invalid := testRecord("invalid")

	report := spool.ExportReport{
		{
			InputIndex: 3,
			Status:     spool.RecordOutcomeStatusInvalid,
			Err:        errors.New("invalid payload"),
		},
		{
			InputIndex:  1,
			Status:      spool.RecordOutcomeStatusRetryable,
			AttemptedAt: attemptedAt,
			Err:         errors.New("retry after attempt"),
		},
		{
			InputIndex:  0,
			Status:      spool.RecordOutcomeStatusSuccess,
			AttemptedAt: attemptedAt,
		},
		{InputIndex: 2, Status: spool.RecordOutcomeStatusRetryable, Err: errors.New("not attempted")},
	}
	require.NoError(t, report.Validate(4))

	plan := processingRedistributionPlanFromOutcomes(
		[]spool.SpoolRecord{
			duplicate,
			previouslyAttemptedDuplicate,
			unattemptedRetryable,
			invalid,
		},
		report,
	)

	require.Len(t, plan.ToDoneRecords, 1)
	assert.Equal(t, duplicate.RequestID, plan.ToDoneRecords[0].RequestID)
	assert.Equal(t, attemptedAt, plan.ToDoneRecords[0].FirstResendAttemptedAt)
	assert.Equal(t, attemptedAt, plan.ToDoneRecords[0].LastResendAttemptedAt)
	assert.Equal(t, 1, plan.ToDoneRecords[0].ResendAttemptCount)

	require.Len(t, plan.ToActiveRecords, 2)
	assert.Equal(t, previousAttemptedAt, plan.ToActiveRecords[0].FirstResendAttemptedAt)
	assert.Equal(t, attemptedAt, plan.ToActiveRecords[0].LastResendAttemptedAt)
	assert.Equal(t, 3, plan.ToActiveRecords[0].ResendAttemptCount)
	assert.Equal(t, "not attempted", plan.ToActiveRecords[1].LastFailureReason)
	unattemptedRetryable.LastFailureReason = "not attempted"
	assert.Equal(t, unattemptedRetryable, plan.ToActiveRecords[1])

	assert.Empty(t, plan.ToDeadRecords)
	invalid.LastFailureReason = "invalid payload"
	assert.Equal(t, []spool.SpoolRecord{invalid}, plan.ToQuarantineRecords)
}

func TestRunOnceSpoolFileProcessFinalizesCompleteReportWithRetryableRecord(t *testing.T) {
	t.Parallel()

	path := "processing/retryable-without-error"
	records := []spool.SpoolRecord{testRecord("success"), testRecord("retryable")}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
	}
	activeStore := &activeStoreStub{}
	sender := &senderStub{report: mustExportReportForStatuses(
		t,
		spool.RecordOutcomeStatusSuccess,
		spool.RecordOutcomeStatusRetryable,
	)}
	worker := &Worker{
		Store:             store,
		ActiveSpoolWriter: activeStore,
		Sender:            sender,
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.NoError(t, err)
	assert.Equal(t, ProcessStatusCompleted, report.Status)
	assert.NoError(t, report.Err)
	assert.Equal(t, 1, report.SuccessRecordCount)
	assert.Equal(t, 1, report.RetryRecordCount)
	assert.Equal(t, 1, sender.calls)
	require.Len(t, activeStore.records, 1)
	assertRecordIDs(t, activeStore.records[0], []string{"retryable"})
	require.Len(t, store.storedDone, 1)
	assertRecordIDs(t, store.storedDone[0].records, []string{"success"})
	assertAllFinalized(t, store.storedDone[0].records)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"store_done:" + path,
		"remove:" + path,
	}, store.calls)
	assert.Empty(t, store.storedDead)
	assert.Empty(t, store.storedQuarantine)
	assert.Empty(t, worker.processingFileTransitionRetry().items)
}

func TestRunOnceSpoolFileProcessReturnsAllRetryableToActiveWithoutSendError(t *testing.T) {
	t.Parallel()

	path := "processing/all-retryable-without-error"
	records := []spool.SpoolRecord{testRecord("retryable-1"), testRecord("retryable-2")}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
	}
	activeStore := &activeStoreStub{}
	sender := &senderStub{report: mustExportReportForStatuses(
		t,
		spool.RecordOutcomeStatusRetryable,
		spool.RecordOutcomeStatusRetryable,
	)}
	worker := &Worker{
		Store:             store,
		ActiveSpoolWriter: activeStore,
		Sender:            sender,
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.NoError(t, err)
	assert.Equal(t, ProcessStatusCompleted, report.Status)
	assert.NoError(t, report.Err)
	assert.Equal(t, len(records), report.RetryRecordCount)
	assert.Zero(t, report.SuccessRecordCount)
	assert.Equal(t, 1, sender.calls)
	require.Len(t, activeStore.records, 1)
	assertRecordIDs(t, activeStore.records[0], []string{"retryable-1", "retryable-2"})
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"remove:" + path,
	}, store.calls)
	assert.Empty(t, store.storedDone)
	assert.Empty(t, store.storedDead)
	assert.Empty(t, store.storedQuarantine)
	assert.Empty(t, worker.processingFileTransitionRetry().items)
}

func TestRunOnceSpoolFileProcessRetriesInvalidReportQuarantineWithoutResending(t *testing.T) {
	t.Parallel()

	path := "processing/invalid-report-quarantine-retry"
	moveErr := errors.New("move invalid report to quarantine failed")
	records := []spool.SpoolRecord{testRecord("reported"), testRecord("missing")}
	store := &workerStoreStub{
		processingPaths:   []string{path},
		readRecords:       map[string][]spool.SpoolRecord{path: records},
		moveQuarantineErr: moveErr,
	}
	sender := &senderStub{report: mustExportReportForStatuses(
		t,
		spool.RecordOutcomeStatusSuccess,
	)}
	worker := &Worker{
		Store:  store,
		Sender: sender,
		ProcessingFileTransitionRetry: newTestProcessingFileTransitionRetry(t,
			ProcessingFileTransitionRetryConfig{
				RetryDelay: time.Minute,
				MaxEntries: 100,
			},
		),
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.ErrorIs(t, err, spool.ErrInvalidExportReport)
	require.ErrorIs(t, err, moveErr)
	assert.Equal(t, ProcessStatusFailed, report.Status)
	assert.ErrorIs(t, report.Err, spool.ErrInvalidExportReport)
	assert.ErrorIs(t, report.Err, moveErr)
	assert.Equal(t, len(records), report.QuarantineRecordCount)
	assert.Equal(t, 1, sender.calls)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"move_quarantine:" + path,
	}, store.calls)
	entry, ok := worker.ProcessingFileTransitionRetry.items[path]
	require.True(t, ok)
	assert.Equal(t, ProcessingFileTransitionRetryOpMoveProcessingToQuarantine, entry.Operation)
	assert.Equal(t, spool.QuarantineReasonInvalidExportReport, entry.QuarantineReason)
	assert.Nil(t, entry.Redistribution)
	assert.Equal(t, []spool.QuarantineReason{
		spool.QuarantineReasonInvalidExportReport,
	}, store.quarantineReasons)

	store.moveQuarantineErr = nil
	entry.NextRetryAt = time.Time{}
	worker.ProcessingFileTransitionRetry.items[path] = entry

	retryReport, handled, retryErr := worker.RunOnceProcessingFileTransitionRetry(context.Background())

	require.NoError(t, retryErr)
	assert.True(t, handled)
	assert.Equal(t, ProcessingFileTransitionRetryStatusSucceeded, retryReport.Status)
	assert.Equal(t, ProcessingFileTransitionRetryOpMoveProcessingToQuarantine, retryReport.Operation)
	assert.Equal(t, 1, sender.calls)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"move_quarantine:" + path,
		"exists:" + path,
		"move_quarantine:" + path,
	}, store.calls)
	assert.Equal(t, []spool.QuarantineReason{
		spool.QuarantineReasonInvalidExportReport,
		spool.QuarantineReasonInvalidExportReport,
	}, store.quarantineReasons)
	assert.False(t, worker.ProcessingFileTransitionRetry.Contains(path))
}

func TestRunOnceSpoolFileProcessFinalizesPartialResultAndReturnsRetryableToActive(t *testing.T) {
	t.Parallel()

	path := "processing/partial-send-error"
	sendErr := errors.New("later backend request result is unknown")
	records := []spool.SpoolRecord{
		testRecord("success"),
		testRecord("retryable"),
		testRecord("invalid"),
	}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
	}
	activeStore := &activeStoreStub{}
	sender := &senderStub{
		report: mustExportReportForStatuses(
			t,
			spool.RecordOutcomeStatusSuccess,
			spool.RecordOutcomeStatusRetryable,
			spool.RecordOutcomeStatusInvalid,
		),
		err: sendErr,
	}
	worker := &Worker{
		Store:             store,
		ActiveSpoolWriter: activeStore,
		Sender:            sender,
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.ErrorIs(t, err, sendErr)
	assert.Equal(t, ProcessStatusCompleted, report.Status)
	assert.ErrorIs(t, report.Err, sendErr)
	assert.Equal(t, 1, report.SuccessRecordCount)
	assert.Equal(t, 1, report.RetryRecordCount)
	assert.Equal(t, 1, report.QuarantineRecordCount)
	assert.Equal(t, 1, sender.calls)
	require.Len(t, activeStore.records, 1)
	assertRecordIDs(t, activeStore.records[0], []string{"retryable"})
	require.Len(t, store.storedDone, 1)
	assertRecordIDs(t, store.storedDone[0].records, []string{"success"})
	require.Len(t, store.storedQuarantine, 1)
	assertRecordIDs(t, store.storedQuarantine[0].records, []string{"invalid"})
	assertAllFinalized(t, store.storedDone[0].records)
	assertAllFinalized(t, store.storedQuarantine[0].records)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"store_quarantine:" + path,
		"store_done:" + path,
		"remove:" + path,
	}, store.calls)
	assert.Empty(t, worker.processingFileTransitionRetry().items)
}

func TestRunOnceSpoolFileProcessPartialSendErrorRetriesFileTransitionWithoutResending(t *testing.T) {
	t.Parallel()

	path := "processing/partial-send-file-transition-retry"
	sendErr := errors.New("later backend request result is unknown")
	storeErr := errors.New("store done failed")
	records := []spool.SpoolRecord{testRecord("success"), testRecord("retryable")}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
		storeDoneErr:    storeErr,
	}
	activeStore := &activeStoreStub{}
	sender := &senderStub{
		report: mustExportReportForStatuses(
			t,
			spool.RecordOutcomeStatusSuccess,
			spool.RecordOutcomeStatusRetryable,
		),
		err: sendErr,
	}
	worker := &Worker{
		Store:             store,
		ActiveSpoolWriter: activeStore,
		Sender:            sender,
		ProcessingFileTransitionRetry: newTestProcessingFileTransitionRetry(t,
			ProcessingFileTransitionRetryConfig{
				RetryDelay: time.Minute,
				MaxEntries: 100,
			},
		),
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.ErrorIs(t, err, sendErr)
	require.ErrorIs(t, err, storeErr)
	assert.Equal(t, ProcessStatusFailed, report.Status)
	assert.ErrorIs(t, report.Err, sendErr)
	assert.ErrorIs(t, report.Err, storeErr)
	assert.Equal(t, 1, sender.calls)
	require.Len(t, activeStore.records, 1)
	assertRecordIDs(t, activeStore.records[0], []string{"retryable"})
	entry, ok := worker.ProcessingFileTransitionRetry.items[path]
	require.True(t, ok)
	assert.Equal(t, ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone, entry.Operation)
	require.NotNil(t, entry.Redistribution)
	assertRecordIDs(t, entry.Redistribution.ToDoneRecords, []string{"success"})
	assert.Empty(t, entry.Redistribution.ToActiveRecords)

	store.storeDoneErr = nil
	entry.NextRetryAt = time.Time{}
	worker.ProcessingFileTransitionRetry.items[path] = entry

	retryReport, handled, retryErr := worker.RunOnceProcessingFileTransitionRetry(context.Background())

	require.NoError(t, retryErr)
	assert.True(t, handled)
	assert.Equal(t, ProcessingFileTransitionRetryStatusSucceeded, retryReport.Status)
	assert.Equal(t, 1, sender.calls)
	require.Len(t, activeStore.records, 1)
	require.Len(t, store.storedDone, 1)
	assertRecordIDs(t, store.storedDone[0].records, []string{"success"})
	assertAllFinalized(t, store.storedDone[0].records)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"store_done:" + path,
		"exists:" + path,
		"store_done:" + path,
		"remove:" + path,
	}, store.calls)
	assert.False(t, worker.ProcessingFileTransitionRetry.Contains(path))
}

func TestRunOnceSpoolFileProcessClaimsActiveAndStoresAllSucceededToDone(t *testing.T) {
	t.Parallel()

	path := "processing/claimed"
	records := []spool.SpoolRecord{testRecord("success-1"), testRecord("success-2")}
	store := &workerStoreStub{
		claimPath:   path,
		claimOK:     true,
		readRecords: map[string][]spool.SpoolRecord{path: records},
	}
	sender := &senderStub{
		report: mustExportReportForStatuses(
			t,
			spool.RecordOutcomeStatusSuccess,
			spool.RecordOutcomeStatusSuccess,
		),
	}
	worker := &Worker{
		TargetBackendService: "opensearch",
		Store:                store,
		Sender:               sender,
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.NoError(t, err)
	assert.Equal(t, ProcessStatusCompleted, report.Status)
	assert.Equal(t, "opensearch", report.TargetBackendService)
	assert.Equal(t, path, report.ProcessingPath)
	assert.Equal(t, len(records), report.RecordCount)
	assert.Equal(t, len(records), report.SuccessRecordCount)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"claim_active",
		"read:" + path,
		"store_done:" + path,
		"remove:" + path,
	}, store.calls)
	assert.Equal(t, 1, sender.calls)
	assert.Equal(t, records, sender.records)
	require.Len(t, store.storedDone, 1)
	assert.Equal(t, path, store.storedDone[0].path)
	assertRecordIDs(t, store.storedDone[0].records, []string{"success-1", "success-2"})
	assertAllFinalized(t, store.storedDone[0].records)
}

func TestRunOnceSpoolFileProcessStoresMixedResultAndRemovesProcessingFile(t *testing.T) {
	t.Parallel()

	path := "processing/mixed"
	records := []spool.SpoolRecord{
		testRecord("success"),
		testRecord("retryable"),
		testRecord("permanent"),
		testRecord("invalid"),
	}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
	}
	activeStore := &activeStoreStub{}
	sender := &senderStub{
		report: mustExportReportForStatuses(
			t,
			spool.RecordOutcomeStatusSuccess,
			spool.RecordOutcomeStatusRetryable,
			spool.RecordOutcomeStatusPermanent,
			spool.RecordOutcomeStatusInvalid,
		),
	}
	worker := &Worker{
		Store:             store,
		ActiveSpoolWriter: activeStore,
		Sender:            sender,
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.NoError(t, err)
	assert.Equal(t, ProcessStatusCompleted, report.Status)
	assert.Equal(t, path, report.ProcessingPath)
	assert.Equal(t, len(records), report.RecordCount)
	assert.Equal(t, 1, report.SuccessRecordCount)
	assert.Equal(t, 1, report.RetryRecordCount)
	assert.Equal(t, 1, report.DeadRecordCount)
	assert.Equal(t, 1, report.QuarantineRecordCount)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"store_dead:" + path,
		"store_quarantine:" + path,
		"store_done:" + path,
		"remove:" + path,
	}, store.calls)
	require.Len(t, activeStore.records, 1)
	assertRecordIDs(t, activeStore.records[0], []string{"retryable"})
	assert.Equal(t, "test retryable outcome", activeStore.records[0][0].LastFailureReason)
	require.Len(t, store.storedDead, 1)
	require.Len(t, store.storedQuarantine, 1)
	require.Len(t, store.storedDone, 1)
	assertRecordIDs(t, store.storedDead[0].records, []string{"permanent"})
	assertRecordIDs(t, store.storedQuarantine[0].records, []string{"invalid"})
	assertRecordIDs(t, store.storedDone[0].records, []string{"success"})
	assert.Equal(t, "test permanent outcome", store.storedDead[0].records[0].LastFailureReason)
	assert.Equal(t, "test invalid outcome", store.storedQuarantine[0].records[0].LastFailureReason)
	assertAllFinalized(t, store.storedDead[0].records)
	assertAllFinalized(t, store.storedQuarantine[0].records)
	assertAllFinalized(t, store.storedDone[0].records)
}

func TestRunOnceSpoolFileProcessQuarantinesUnreadableProcessingFile(t *testing.T) {
	t.Parallel()

	path := "processing/broken"
	decodeErr := fmt.Errorf("%w: line 1", spool.ErrDecodeProcessingSpoolFile)
	store := &workerStoreStub{
		processingPaths: []string{path},
		readErr:         map[string]error{path: decodeErr},
	}
	sender := &senderStub{}
	worker := &Worker{
		Store:  store,
		Sender: sender,
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.NoError(t, err)
	assert.Equal(t, ProcessStatusQuarantined, report.Status)
	assert.Equal(t, path, report.ProcessingPath)
	assert.ErrorIs(t, report.Err, spool.ErrDecodeProcessingSpoolFile)
	assert.Equal(t, spool.QuarantineReasonInvalidSpoolFile, report.QuarantineReason)
	assert.Equal(t, []spool.QuarantineReason{
		spool.QuarantineReasonInvalidSpoolFile,
	}, store.quarantineReasons)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"move_quarantine:" + path,
	}, store.calls)
	assert.Zero(t, sender.calls)
	assert.False(t, worker.processingPathBackoff().ShouldSkipPath(path, time.Now()))
}

func TestRunOnceSpoolFileProcessTerminalStoreFailureQueuesFileTransitionRetry(t *testing.T) {
	t.Parallel()

	path := "processing/store-fails"
	storeErr := errors.New("store done failed")
	records := []spool.SpoolRecord{testRecord("success")}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: records},
		storeDoneErr:    storeErr,
	}
	sender := &senderStub{
		report: mustExportReportForStatuses(
			t,
			spool.RecordOutcomeStatusSuccess,
		),
	}
	worker := &Worker{
		Store:  store,
		Sender: sender,
		ProcessingFileTransitionRetry: newTestProcessingFileTransitionRetry(t,
			ProcessingFileTransitionRetryConfig{
				RetryDelay: time.Minute,
				MaxEntries: 100,
			},
		),
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.ErrorIs(t, err, storeErr)
	assert.Equal(t, ProcessStatusFailed, report.Status)
	assert.ErrorIs(t, report.Err, storeErr)
	assert.Equal(t, []string{
		"list_processing_candidates_after",
		"read:" + path,
		"store_done:" + path,
	}, store.calls)

	entry := worker.ProcessingFileTransitionRetry.items[path]
	assert.Equal(t, ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone, entry.Operation)
	require.NotNil(t, entry.Redistribution)
	assertRecordIDs(t, entry.Redistribution.ToDoneRecords, []string{"success"})
	assertAllFinalized(t, entry.Redistribution.ToDoneRecords)
	assert.False(t, entry.NextRetryAt.IsZero())
}

func TestRunOnceSpoolFileProcessSkipsWhenFileTransitionRetryIsFull(t *testing.T) {
	t.Parallel()

	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Second,
		MaxEntries: 1,
	})
	retry.items["processing/pending"] = ProcessingFileTransitionRetryEntry{}
	store := &workerStoreStub{}
	worker := &Worker{
		TargetBackendService:          "opensearch",
		Store:                         store,
		Sender:                        &senderStub{},
		ProcessingFileTransitionRetry: retry,
	}

	report, err := worker.RunOnceSpoolFileProcess(context.Background())

	require.NoError(t, err)
	assert.Equal(t, ProcessStatusSkipped, report.Status)
	assert.Equal(t, ProcessSkipReasonProcessingFileTransitionRetryFull, report.SkipReason)
	assert.Equal(t, "opensearch", report.TargetBackendService)
	assert.Empty(t, store.calls)
}

func TestSelectProcessingPathSkipsBackoffAndFileTransitionRetryPaths(t *testing.T) {
	t.Parallel()

	now := time.Now()
	backoff := NewProcessingPathBackoff(ProcessingPathBackoffConfig{
		MaxPathCacheEntries:  4,
		PathSkipHoldDuration: time.Minute,
	})
	backoff.BlockPath("processing/backoff", ProcessingPathBackoffOpReadProcessingRecords, nil, now)
	retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Second,
		MaxEntries: 100,
	})
	retry.items["processing/file-transition"] = ProcessingFileTransitionRetryEntry{}
	store := &workerStoreStub{
		processingPaths: []string{
			"processing/backoff",
			"processing/file-transition",
			"processing/ready",
		},
	}
	worker := &Worker{
		Store:                         store,
		ProcessingPathBackoff:         backoff,
		ProcessingFileTransitionRetry: retry,
	}

	path, ok, err := worker.selectProcessingPath(context.Background())

	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "processing/ready", path)
	require.Len(t, store.listCandidateCalls, 1)
	assert.Equal(t, listCandidateCall{after: "", limit: defaultProcessingCandidatePageSize}, store.listCandidateCalls[0])
}

func assertRecordIDs(t *testing.T, records []spool.SpoolRecord, want []string) {
	t.Helper()

	got := make([]string, 0, len(records))
	for _, record := range records {
		got = append(got, record.RequestID)
	}

	assert.Equal(t, want, got)
}

func assertAllFinalized(t *testing.T, records []spool.SpoolRecord) {
	t.Helper()

	for i, record := range records {
		assert.False(t, record.FinalizedAt.IsZero(), "records[%d] FinalizedAt is zero", i)
	}
}
