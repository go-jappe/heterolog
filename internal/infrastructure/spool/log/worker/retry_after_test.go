package worker

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retryAfterSenderStubは、応答までの時間を経過させてからreportを返します。
// Retry-Afterの期限が応答受信後の時刻から数えられることを確認するために使います。
type retryAfterSenderStub struct {
	latency time.Duration
	reports []spool.ExportReport

	calls   int
	records [][]spool.SpoolRecord
}

var _ Sender = (*retryAfterSenderStub)(nil)

func (s *retryAfterSenderStub) Send(
	ctx context.Context,
	records []spool.SpoolRecord,
) (spool.ExportReport, error) {
	_ = ctx

	time.Sleep(s.latency)
	s.records = append(s.records, cloneSpoolRecords(records))
	report := s.reports[min(s.calls, len(s.reports)-1)]
	s.calls++
	return report, nil
}

func throttledRetryableOutcome(attemptedAt time.Time, retryAfter time.Duration) spool.RecordOutcome {
	return spool.RecordOutcome{
		Status:      spool.RecordOutcomeStatusRetryable,
		AttemptedAt: attemptedAt,
		Err:         errors.New("backend throttled"),
		RetryAfter:  retryAfter,
	}
}

func successOutcome(attemptedAt time.Time) spool.RecordOutcome {
	return spool.RecordOutcome{
		Status:      spool.RecordOutcomeStatusSuccess,
		AttemptedAt: attemptedAt,
	}
}

func TestRunOnceSpoolFileProcessDefersDestinationUntilRetryAfter(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/throttled"
		records := []spool.SpoolRecord{testRecord("throttled"), testRecord("unsent")}
		store := &workerStoreStub{
			claimPath:   path,
			claimOK:     true,
			readRecords: map[string][]spool.SpoolRecord{path: records},
		}
		activeStore := &activeStoreStub{}
		start := time.Now()
		throttled := throttledRetryableOutcome(start, 30*time.Second)
		sender := &retryAfterSenderStub{
			latency: time.Second,
			reports: []spool.ExportReport{
				mustExportReport(t, throttled, spool.RecordOutcome{
					Status: spool.RecordOutcomeStatusRetryable,
					Err:    spool.DeferredByRetryAfter(throttled),
				}),
				mustExportReport(t, successOutcome(start), successOutcome(start)),
			},
		}
		worker := &Worker{
			Store:             store,
			ActiveSpoolWriter: activeStore,
			Sender:            sender,
		}

		first, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.Equal(t, ProcessStatusCompleted, first.Status)
		require.Equal(t, 2, first.RetryRecordCount)
		wantNotBefore := start.Add(time.Second + 30*time.Second)
		require.True(t, first.NotBefore.Equal(wantNotBefore), "NotBefore = %s, want %s", first.NotBefore, wantNotBefore)

		// Retry-Afterは送信先への指定なので、未送信のrecordにも同じ期限を保存する。
		require.Len(t, activeStore.records, 1)
		require.Len(t, activeStore.records[0], 2)
		for _, record := range activeStore.records[0] {
			assert.True(t, record.NotBefore.Equal(wantNotBefore), "%s NotBefore = %s", record.RequestID, record.NotBefore)
		}
		assert.Equal(t, 1, activeStore.records[0][0].ResendAttemptCount)
		assert.Zero(t, activeStore.records[0][1].ResendAttemptCount)
		callsAfterFirstSend := append([]string(nil), store.calls...)

		time.Sleep(wantNotBefore.Sub(time.Now()) - time.Nanosecond)
		deferred, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusSkipped, deferred.Status)
		assert.Equal(t, ProcessSkipReasonRetryAfter, deferred.SkipReason)
		assert.True(t, deferred.NotBefore.Equal(wantNotBefore))
		assert.Empty(t, deferred.ProcessingPath)
		assert.Equal(t, 1, sender.calls)
		assert.Equal(t, callsAfterFirstSend, store.calls, "deferred destination must not list, claim, or read files")

		time.Sleep(time.Nanosecond)
		resumed, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status)
		assert.Equal(t, 2, sender.calls)
		assert.Zero(t, resumed.NotBefore)
	})
}

// 再起動後のWorkerは期限をmemoryに持たないため、保存済みのNotBeforeから期限を復元する。
// fileはprocessingに残し、期限後にprocessingの候補として送信する。
func TestRunOnceSpoolFileProcessRestoresPersistedNotBeforeAfterRestart(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/persisted-retry-after"
		start := time.Now()
		notBefore := start.Add(time.Minute).Round(0)
		records := []spool.SpoolRecord{testRecord("deferred"), testRecord("same-file")}
		records[0].NotBefore = notBefore
		store := &workerStoreStub{
			processingPaths: []string{path},
			readRecords:     map[string][]spool.SpoolRecord{path: records},
			// 期限中はactiveの別fileもclaimしてはいけない。
			claimPath: "processing/claimed-active",
			claimOK:   true,
		}
		activeStore := &activeStoreStub{}
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{
			mustExportReport(t, successOutcome(start), successOutcome(start)),
		}}
		worker := &Worker{
			Store:             store,
			ActiveSpoolWriter: activeStore,
			Sender:            sender,
		}

		restored, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusSkipped, restored.Status)
		assert.Equal(t, ProcessSkipReasonRetryAfter, restored.SkipReason)
		assert.True(t, restored.NotBefore.Equal(notBefore))
		assert.Equal(t, path, restored.ProcessingPath)
		assert.Equal(t, 2, restored.RecordCount)
		assert.Zero(t, sender.calls)
		assert.Equal(t, []string{"list_processing_candidates_after", "read:" + path}, store.calls,
			"the deferred file stays in processing without transition")
		assert.Empty(t, activeStore.calls)

		time.Sleep(30 * time.Second)
		stillDeferred, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, stillDeferred.SkipReason)
		assert.Len(t, store.calls, 2, "restored destination wait must not claim other files")

		time.Sleep(notBefore.Sub(time.Now()))
		resumed, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status)
		assert.Equal(t, path, resumed.ProcessingPath)
		require.Equal(t, 1, sender.calls)
		assert.Equal(t, records, sender.records[0])
		require.Len(t, store.storedDone, 1)
		assert.True(t, store.storedDone[0].records[0].NotBefore.Equal(notBefore),
			"terminal records keep the stored wait unchanged")
	})
}

// done・dead・quarantineへの保存内容は再起動時の照合に使うため、Retry-Afterを受け取っても
// NotBeforeを変更しない。再送待ちへ戻すrecordだけ期限を延ばす。
func TestRunOnceSpoolFileProcessDefersOnlyRecordsReturnedToActive(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/mixed"
		start := time.Now()
		previousNotBefore := start.Add(-time.Minute).Round(0)
		records := []spool.SpoolRecord{
			testRecord("success"),
			testRecord("dead"),
			testRecord("quarantine"),
			testRecord("throttled"),
		}
		for i := range records {
			records[i].NotBefore = previousNotBefore
		}
		store := &workerStoreStub{
			processingPaths: []string{path},
			readRecords:     map[string][]spool.SpoolRecord{path: records},
		}
		activeStore := &activeStoreStub{}
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{mustExportReport(t,
			successOutcome(start),
			spool.RecordOutcome{Status: spool.RecordOutcomeStatusPermanent, AttemptedAt: start, Err: errors.New("forbidden")},
			spool.RecordOutcome{Status: spool.RecordOutcomeStatusInvalid, AttemptedAt: start, Err: errors.New("rejected")},
			throttledRetryableOutcome(start, 10*time.Second),
		)}}
		worker := &Worker{
			Store:             store,
			ActiveSpoolWriter: activeStore,
			Sender:            sender,
		}

		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.Equal(t, ProcessStatusCompleted, report.Status)
		wantNotBefore := start.Add(10 * time.Second)
		require.True(t, report.NotBefore.Equal(wantNotBefore))

		for _, stored := range [][]storeRecordsCall{store.storedDone, store.storedDead, store.storedQuarantine} {
			require.Len(t, stored, 1)
			require.Len(t, stored[0].records, 1)
			assert.True(t, stored[0].records[0].NotBefore.Equal(previousNotBefore), "%s", stored[0].records[0].RequestID)
		}
		require.Len(t, activeStore.records, 1)
		require.Len(t, activeStore.records[0], 1)
		assert.Equal(t, "throttled", activeStore.records[0][0].RequestID)
		assert.True(t, activeStore.records[0][0].NotBefore.Equal(wantNotBefore))
	})
}

// active保存の失敗後も、再試行計画は期限付きのrecordを保持する。状態遷移retryは
// backendへ送信しないため、Retry-Afterの期限中でも実行する。
func TestTransitionRetryPersistsNotBeforeWhileDestinationIsDeferred(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/store-active-failure"
		start := time.Now()
		store := &workerStoreStub{
			processingPaths: []string{path},
			readRecords:     map[string][]spool.SpoolRecord{path: {testRecord("throttled")}},
		}
		activeStore := &activeStoreStub{err: errors.New("active store unavailable")}
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{
			mustExportReport(t, throttledRetryableOutcome(start, time.Minute)),
		}}
		retry, err := NewProcessingFileTransitionRetry(NewProcessingFileTransitionRetryConfigAdapter(ProcessingFileTransitionRetryConfig{
			RetryDelay: time.Second,
			MaxEntries: 10,
		}))
		require.NoError(t, err)
		worker, err := New(store, activeStore, sender, WithProcessingFileTransitionRetry(retry))
		require.NoError(t, err)

		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.ErrorIs(t, err, activeStore.err)
		require.Equal(t, ProcessStatusFailed, report.Status)
		wantNotBefore := start.Add(time.Minute)
		require.True(t, report.NotBefore.Equal(wantNotBefore))

		activeStore.err = nil
		time.Sleep(time.Second)
		retryReport, handled, err := worker.RunOnceProcessingFileTransitionRetry(context.Background())
		require.NoError(t, err)
		require.True(t, handled)
		assert.Equal(t, ProcessingFileTransitionRetryStatusSucceeded, retryReport.Status)
		require.Len(t, activeStore.records, 1)
		assert.True(t, activeStore.records[0][0].NotBefore.Equal(wantNotBefore))
		assert.Equal(t, 1, sender.calls)

		deferred, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, deferred.SkipReason)
		assert.Equal(t, 1, sender.calls)
	})
}
