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

// backendが上限を超えるRetry-Afterを指定した場合は上限まで短縮して待ち、短縮した内容を
// reportへ残す。上限以下の指定はそのまま待つ。
func TestRunOnceSpoolFileProcessCapsRetryAfterAtConfiguredMaximum(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		retryAfter time.Duration
		wantWait   time.Duration
		clamped    bool
	}{
		{name: "above maximum", retryAfter: 3 * time.Hour, wantWait: time.Hour, clamped: true},
		{name: "at maximum", retryAfter: time.Hour, wantWait: time.Hour},
		{name: "below maximum", retryAfter: 30 * time.Minute, wantWait: 30 * time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				const path = "processing/throttled"
				store := &workerStoreStub{
					claimPath:   path,
					claimOK:     true,
					readRecords: map[string][]spool.SpoolRecord{path: {testRecord("throttled")}},
				}
				activeStore := &activeStoreStub{}
				start := time.Now()
				sender := &retryAfterSenderStub{reports: []spool.ExportReport{
					mustExportReport(t, throttledRetryableOutcome(start, tt.retryAfter)),
					mustExportReport(t, successOutcome(start)),
				}}
				worker, err := New(store, activeStore, sender, WithMaxRetryAfter(time.Hour))
				require.NoError(t, err)

				report, err := worker.RunOnceSpoolFileProcess(context.Background())
				require.NoError(t, err)
				wantNotBefore := start.Add(tt.wantWait)
				require.True(t, report.NotBefore.Equal(wantNotBefore), "NotBefore = %s, want %s", report.NotBefore, wantNotBefore)
				require.Len(t, activeStore.records, 1)
				assert.True(t, activeStore.records[0][0].NotBefore.Equal(wantNotBefore))
				if tt.clamped {
					require.NotNil(t, report.RetryAfterClamp)
					assert.Equal(t, tt.retryAfter, report.RetryAfterClamp.Requested)
					assert.Equal(t, time.Hour, report.RetryAfterClamp.MaxRetryAfter)
					assert.True(t, report.RetryAfterClamp.NotBefore.Equal(wantNotBefore))
					assert.False(t, report.RetryAfterClamp.Stored)
				} else {
					assert.Nil(t, report.RetryAfterClamp)
				}

				time.Sleep(tt.wantWait - time.Nanosecond)
				deferred, err := worker.RunOnceSpoolFileProcess(context.Background())
				require.NoError(t, err)
				assert.Equal(t, ProcessSkipReasonRetryAfter, deferred.SkipReason)
				assert.Nil(t, deferred.RetryAfterClamp, "a clamp is reported once per occurrence")
				assert.Equal(t, 1, sender.calls)

				time.Sleep(time.Nanosecond)
				resumed, err := worker.RunOnceSpoolFileProcess(context.Background())
				require.NoError(t, err)
				assert.Equal(t, ProcessStatusCompleted, resumed.Status)
				assert.Equal(t, 2, sender.calls)
			})
		})
	}
}

// 上限を設ける前に保存した期限や時計のずれにより、保存済みのNotBeforeが現在時刻から上限より
// 先にある場合は、初めて読み込んだ時点から上限までに短縮して一度だけ通知する。短縮後の期限を
// 過ぎたら保存済みの時刻にかかわらず送信し、再送待ちへ戻すrecordには新しい期限だけを残す。
func TestRunOnceSpoolFileProcessClampsStoredNotBeforeBeyondMaximum(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/stored-far-future"
		start := time.Now()
		farFuture := start.Add(10 * 24 * time.Hour).Round(0)
		records := []spool.SpoolRecord{testRecord("far-future")}
		records[0].NotBefore = farFuture
		store := &workerStoreStub{
			processingPaths: []string{path},
			readRecords:     map[string][]spool.SpoolRecord{path: records},
		}
		activeStore := &activeStoreStub{}
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{mustExportReport(t, spool.RecordOutcome{
			Status:      spool.RecordOutcomeStatusRetryable,
			AttemptedAt: start,
			Err:         errors.New("backend unavailable without retry-after"),
		})}}
		worker, err := New(store, activeStore, sender, WithMaxRetryAfter(time.Hour))
		require.NoError(t, err)

		clamped, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.Equal(t, ProcessSkipReasonRetryAfter, clamped.SkipReason)
		wantNotBefore := start.Add(time.Hour)
		require.True(t, clamped.NotBefore.Equal(wantNotBefore), "NotBefore = %s, want %s", clamped.NotBefore, wantNotBefore)
		require.NotNil(t, clamped.RetryAfterClamp)
		assert.Equal(t, farFuture.Sub(start), clamped.RetryAfterClamp.Requested)
		assert.Equal(t, time.Hour, clamped.RetryAfterClamp.MaxRetryAfter)
		assert.True(t, clamped.RetryAfterClamp.NotBefore.Equal(wantNotBefore))
		assert.True(t, clamped.RetryAfterClamp.Stored)
		assert.Zero(t, sender.calls)
		callsAfterClamp := append([]string(nil), store.calls...)

		time.Sleep(30 * time.Minute)
		waiting, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, waiting.SkipReason)
		assert.Nil(t, waiting.RetryAfterClamp, "a clamp is reported once per occurrence")
		assert.Equal(t, callsAfterClamp, store.calls)

		time.Sleep(time.Until(wantNotBefore))
		resumed, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status)
		require.Equal(t, 1, sender.calls, "the clamped deadline releases the file")
		require.Len(t, activeStore.records, 1)
		assert.Zero(t, activeStore.records[0][0].NotBefore, "an obsolete stored deadline is not carried forward")
	})
}

func TestNewRejectsNegativeMaxRetryAfterAndUsesDefaultForZero(t *testing.T) {
	t.Parallel()

	_, err := New(&workerStoreStub{}, &activeStoreStub{}, &senderStub{}, WithMaxRetryAfter(-time.Second))
	require.ErrorIs(t, err, ErrInvalidMaxRetryAfter)

	for _, option := range []Option{WithMaxRetryAfter(0), func(*Worker) {}} {
		worker, err := New(&workerStoreStub{}, &activeStoreStub{}, &senderStub{}, option)
		require.NoError(t, err)
		assert.Equal(t, time.Hour, worker.retryAfterLimit())
	}
}
