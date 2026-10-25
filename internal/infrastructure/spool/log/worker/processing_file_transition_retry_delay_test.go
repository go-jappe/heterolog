package worker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestWorkerTransitionRetryWaitsAfterSlowFailure(t *testing.T) {
	tests := []struct {
		name         string
		failure      string
		operation    ProcessingFileTransitionRetryOp
		redistribute bool
		recoveryErr  error
	}{
		{name: "exists", failure: "exists", operation: ProcessingFileTransitionRetryOpMoveProcessingToDone},
		{name: "move_done", failure: "move_done", operation: ProcessingFileTransitionRetryOpMoveProcessingToDone},
		{name: "move_dead", failure: "move_dead", operation: ProcessingFileTransitionRetryOpMoveProcessingToDead},
		{name: "move_quarantine", failure: "move_quarantine", operation: ProcessingFileTransitionRetryOpMoveProcessingToQuarantine},
		{
			name: "missing_plan_quarantine", failure: "move_quarantine",
			operation:   ProcessingFileTransitionRetryOpStoreRecordsToActive,
			recoveryErr: ErrMissingProcessingRedistributionPlan,
		},
		{name: "remove_retry", failure: "remove", operation: ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution},
		{name: "store_active", failure: "store_active", operation: ProcessingFileTransitionRetryOpStoreRecordsToActive, redistribute: true},
		{name: "store_dead", failure: "store_dead", operation: ProcessingFileTransitionRetryOpStoreRecordsToActive, redistribute: true},
		{name: "store_quarantine", failure: "store_quarantine", operation: ProcessingFileTransitionRetryOpStoreRecordsToActive, redistribute: true},
		{name: "store_done", failure: "store_done", operation: ProcessingFileTransitionRetryOpStoreRecordsToActive, redistribute: true},
		{name: "remove_after_redistribution", failure: "remove", operation: ProcessingFileTransitionRetryOpStoreRecordsToActive, redistribute: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const path = "processing/a"
				const retryDelay = time.Minute
				failureErr := errors.New("slow storage failure")
				store := &slowTransitionStore{
					workerStoreStub: &workerStoreStub{},
					activeStore:     &activeStoreStub{},
					failure:         tt.failure,
					failureErr:      failureErr,
					failuresLeft:    2,
					latency:         2 * retryDelay,
				}
				retry := newTestProcessingFileTransitionRetry(t, ProcessingFileTransitionRetryConfig{
					RetryDelay: retryDelay,
					MaxEntries: 1,
				})
				if tt.redistribute {
					retry.UpsertRedistribution(path, tt.operation, &ProcessingRedistributionPlan{
						ToActiveRecords:     []spool.SpoolRecord{testRecord("active")},
						ToDeadRecords:       []spool.SpoolRecord{testRecord("dead")},
						ToQuarantineRecords: []spool.SpoolRecord{testRecord("quarantine")},
						ToDoneRecords:       []spool.SpoolRecord{testRecord("done")},
					}, nil, time.Now().Add(-retryDelay))
				} else if tt.operation == ProcessingFileTransitionRetryOpMoveProcessingToQuarantine {
					retry.UpsertQuarantine(path, spool.QuarantineReasonInvalidTransitionState, nil, time.Now().Add(-retryDelay))
				} else {
					retry.Upsert(path, tt.operation, nil, time.Now().Add(-retryDelay))
				}
				sender := &senderStub{}
				worker := &Worker{
					Store:                         store,
					ActiveSpoolWriter:             store,
					Sender:                        sender,
					ProcessingFileTransitionRetry: retry,
				}

				// Each failed attempt takes longer than RetryDelay. The next delay
				// must still begin when that failure returns, on every attempt.
				for attempt := 0; attempt < 2; attempt++ {
					report, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())
					require.True(t, handled)
					require.ErrorIs(t, err, failureErr)
					require.ErrorIs(t, report.Err, failureErr)
					require.Equal(t, ProcessingFileTransitionRetryStatusFailed, report.Status)
					require.Equal(t, path, report.ProcessingPath)
					require.Equal(t, 1, retry.Count(), "failed finalization must remain pending")

					callsAfterFailure := len(store.attempts)
					entryAfterFailure := retry.items[path]
					assertWaiting := func() {
						t.Helper()
						report, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())
						require.NoError(t, err)
						require.False(t, handled, "retry delay must start after the slow failure returns")
						require.Equal(t, ProcessingFileTransitionRetryStatusNoTarget, report.Status)
						require.Len(t, store.attempts, callsAfterFailure, "waiting must perform no storage I/O")
						require.Equal(t, entryAfterFailure, retry.items[path], "waiting must retain unfinished work")
					}
					assertWaiting()
					time.Sleep(retryDelay - time.Nanosecond)
					assertWaiting()
					time.Sleep(time.Nanosecond)
				}

				// The third attempt is allowed exactly at the deadline and succeeds
				// after storage recovers. Previously completed stores are not repeated.
				report, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())
				require.True(t, handled)
				if tt.recoveryErr == nil {
					require.NoError(t, err)
					require.Equal(t, ProcessingFileTransitionRetryStatusSucceeded, report.Status)
				} else {
					require.ErrorIs(t, err, tt.recoveryErr)
					require.Equal(t, ProcessingFileTransitionRetryStatusFailed, report.Status)
				}
				require.Zero(t, retry.Count())
				require.Zero(t, sender.calls, "finalization retries must not resend to the backend")
				if tt.redistribute {
					require.Equal(t, [][]spool.SpoolRecord{{testRecord("active")}}, store.activeStore.records)
					require.Equal(t, []storeRecordsCall{{path: path, records: []spool.SpoolRecord{testRecord("dead")}}}, store.storedDead)
					require.Equal(t, []storeRecordsCall{{path: path, records: []spool.SpoolRecord{testRecord("quarantine")}}}, store.storedQuarantine)
					require.Equal(t, []storeRecordsCall{{path: path, records: []spool.SpoolRecord{testRecord("done")}}}, store.storedDone)
				}
			})
		})
	}
}

// slowTransitionStore adds deterministic I/O latency to the ordinary in-memory
// store. Failed calls have no storage side effects; all successful calls retain
// the records in workerStoreStub / activeStoreStub for content assertions.
type slowTransitionStore struct {
	*workerStoreStub
	activeStore  *activeStoreStub
	failure      string
	failureErr   error
	failuresLeft int
	latency      time.Duration
	attempts     []string
}

func (s *slowTransitionStore) before(operation string) error {
	s.attempts = append(s.attempts, operation)
	if operation == s.failure && s.failuresLeft > 0 {
		s.failuresLeft--
		time.Sleep(s.latency)
		return s.failureErr
	}
	return nil
}

func (s *slowTransitionStore) ProcessingFileExists(ctx context.Context, path string) (bool, error) {
	if err := s.before("exists"); err != nil {
		return false, err
	}
	return s.workerStoreStub.ProcessingFileExists(ctx, path)
}

func (s *slowTransitionStore) MoveProcessingToDone(ctx context.Context, path string) error {
	if err := s.before("move_done"); err != nil {
		return err
	}
	return s.workerStoreStub.MoveProcessingToDone(ctx, path)
}

func (s *slowTransitionStore) MoveProcessingToDead(ctx context.Context, path string) error {
	if err := s.before("move_dead"); err != nil {
		return err
	}
	return s.workerStoreStub.MoveProcessingToDead(ctx, path)
}

func (s *slowTransitionStore) MoveProcessingToQuarantine(ctx context.Context, path string, reason spool.QuarantineReason) error {
	if err := s.before("move_quarantine"); err != nil {
		return err
	}
	return s.workerStoreStub.MoveProcessingToQuarantine(ctx, path, reason)
}

func (s *slowTransitionStore) RemoveProcessingFile(ctx context.Context, path string) error {
	if err := s.before("remove"); err != nil {
		return err
	}
	return s.workerStoreStub.RemoveProcessingFile(ctx, path)
}

func (s *slowTransitionStore) StoreActive(ctx context.Context, records []spool.SpoolRecord) error {
	if err := s.before("store_active"); err != nil {
		return err
	}
	return s.activeStore.StoreActive(ctx, records)
}

func (s *slowTransitionStore) StoreProcessingRecordsToDead(ctx context.Context, path string, records []spool.SpoolRecord) error {
	if err := s.before("store_dead"); err != nil {
		return err
	}
	return s.workerStoreStub.StoreProcessingRecordsToDead(ctx, path, records)
}

func (s *slowTransitionStore) StoreProcessingRecordsToQuarantine(ctx context.Context, path string, records []spool.SpoolRecord) error {
	if err := s.before("store_quarantine"); err != nil {
		return err
	}
	return s.workerStoreStub.StoreProcessingRecordsToQuarantine(ctx, path, records)
}

func (s *slowTransitionStore) StoreProcessingRecordsToDone(ctx context.Context, path string, records []spool.SpoolRecord) error {
	if err := s.before("store_done"); err != nil {
		return err
	}
	return s.workerStoreStub.StoreProcessingRecordsToDone(ctx, path, records)
}
