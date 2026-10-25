package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// retryAfterStoreStubは、Workerの再起動後も残る保存先を表します。同じstubを新しいWorkerへ
// 渡すことで、process再起動後の読み込みを再現します。
type retryAfterStoreStub struct {
	state   RetryAfterState
	saved   bool
	loadErr error
	// saveErrは全ての保存を失敗させます。transientSaveErrはtransientSaveFailures回だけ失敗させます。
	saveErr               error
	transientSaveErr      error
	transientSaveFailures int

	calls []string
	saves []RetryAfterState
	// saveAttemptsは、失敗した保存も含めた保存の試行時刻です。
	saveAttempts []time.Time
}

var _ RetryAfterStore = (*retryAfterStoreStub)(nil)

func (s *retryAfterStoreStub) Load(now time.Time, maxWait time.Duration) (RetryAfterState, *spool.RetryAfterClamp, error) {
	s.calls = append(s.calls, "load")
	if s.loadErr != nil {
		return RetryAfterState{}, nil, s.loadErr
	}
	return s.state, nil, nil
}

func (s *retryAfterStoreStub) Save(state RetryAfterState) error {
	s.calls = append(s.calls, "save")
	s.saveAttempts = append(s.saveAttempts, time.Now())
	if s.saveErr != nil {
		return s.saveErr
	}
	if s.transientSaveFailures > 0 {
		s.transientSaveFailures--
		return s.transientSaveErr
	}
	s.state, s.saved = state, true
	s.saves = append(s.saves, state)
	return nil
}

func (s *retryAfterStoreStub) Remove() error {
	s.calls = append(s.calls, "remove")
	s.state, s.saved = RetryAfterState{}, false
	return nil
}

func newRetryAfterStoreTestWorker(
	t *testing.T,
	store *workerStoreStub,
	activeStore *activeStoreStub,
	sender Sender,
	retryAfterStore RetryAfterStore,
	options ...Option,
) *Worker {
	t.Helper()
	options = append([]Option{WithRetryAfterStore(retryAfterStore)}, options...)
	worker, err := New(store, activeStore, sender, options...)
	require.NoError(t, err)
	return worker
}

// 再起動前に保存した送信先の期限は、最初の送信より前に読み込む。期限を保存していない古い
// recordが先に読まれる順序でも、読み込み・送信を始めず、期限後に送信する。
func TestRunOnceSpoolFileProcessLoadsPersistedDeadlineBeforeFirstSend(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/older-without-not-before"
		start := time.Now()
		notBefore := start.Add(time.Minute).Round(0)
		persisted := &retryAfterStoreStub{state: RetryAfterState{NotBefore: notBefore, AppliedThrough: notBefore}}
		store := &workerStoreStub{
			processingPaths: []string{path},
			readRecords:     map[string][]spool.SpoolRecord{path: {testRecord("older")}},
			claimPath:       "processing/claimed",
			claimOK:         true,
		}
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{mustExportReport(t, successOutcome(start))}}
		worker := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)

		restored, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusSkipped, restored.Status)
		assert.Equal(t, ProcessSkipReasonRetryAfter, restored.SkipReason)
		assert.True(t, restored.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", restored.NotBefore, notBefore)
		assert.Empty(t, store.calls, "no file is listed, claimed, or read before the persisted deadline")
		assert.Zero(t, sender.calls)

		time.Sleep(time.Until(notBefore) - time.Nanosecond)
		stillDeferred, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, stillDeferred.SkipReason)
		assert.Empty(t, store.calls)

		time.Sleep(time.Nanosecond)
		resumed, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status)
		assert.Equal(t, path, resumed.ProcessingPath)
		require.Equal(t, 1, sender.calls)
		assert.Equal(t, []string{"load", "remove"}, persisted.calls, "the deadline is loaded once and removed after expiry")
	})
}

// Retry-Afterの期限は後処理より先に保存し、再起動したWorkerは保存した期限まで送信しない。
func TestRunOnceSpoolFileProcessPersistsRetryAfterAcrossRestart(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/throttled"
		persisted := &retryAfterStoreStub{}
		store := &workerStoreStub{
			claimPath:   path,
			claimOK:     true,
			readRecords: map[string][]spool.SpoolRecord{path: {testRecord("throttled")}},
		}
		start := time.Now()
		sender := &retryAfterSenderStub{
			latency: time.Second,
			reports: []spool.ExportReport{
				mustExportReport(t, throttledRetryableOutcome(start, 30*time.Second)),
				mustExportReport(t, successOutcome(start)),
			},
		}
		first := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)
		throttled, err := first.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.Equal(t, ProcessStatusCompleted, throttled.Status)
		wantNotBefore := start.Add(31 * time.Second)
		require.True(t, throttled.NotBefore.Equal(wantNotBefore))
		require.NoError(t, throttled.RetryAfterMarkerErr)
		require.Len(t, persisted.saves, 1)
		assert.True(t, persisted.saves[0].NotBefore.Equal(wantNotBefore), "saved NotBefore = %s", persisted.saves[0].NotBefore)
		assert.True(t, persisted.saves[0].AppliedThrough.Equal(wantNotBefore), "saved AppliedThrough = %s", persisted.saves[0].AppliedThrough)

		time.Sleep(10 * time.Second)
		store.calls = nil
		restarted := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)
		restored, err := restarted.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, restored.SkipReason)
		assert.True(t, restored.NotBefore.Equal(wantNotBefore))
		assert.Empty(t, store.calls)
		assert.Equal(t, 1, sender.calls)

		time.Sleep(time.Until(wantNotBefore))
		resumed, err := restarted.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status)
		assert.Equal(t, 2, sender.calls)
		assert.False(t, persisted.saved, "the expired deadline is removed")
	})
}

// 上限を超えるrecordの期限を短縮した場合は、反映済みの期限も保存する。再起動後は短縮後の
// 期限を待ち、同じrecordの期限を読み込んだ時点から数え直さない。
func TestRunOnceSpoolFileProcessPersistsClampedStoredNotBefore(t *testing.T) {
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
		persisted := &retryAfterStoreStub{}
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{mustExportReport(t, successOutcome(start))}}
		first := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted, WithMaxRetryAfter(time.Hour))

		clamped, err := first.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.Equal(t, ProcessSkipReasonRetryAfter, clamped.SkipReason)
		wantNotBefore := start.Add(time.Hour)
		require.True(t, clamped.NotBefore.Equal(wantNotBefore))
		require.NotNil(t, clamped.RetryAfterClamp)
		assert.True(t, clamped.RetryAfterClamp.Stored)
		require.Len(t, persisted.saves, 1)
		assert.True(t, persisted.saves[0].NotBefore.Equal(wantNotBefore))
		assert.True(t, persisted.saves[0].AppliedThrough.Equal(farFuture))

		time.Sleep(30 * time.Minute)
		store.calls = nil
		restarted := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted, WithMaxRetryAfter(time.Hour))
		waiting, err := restarted.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, waiting.SkipReason)
		assert.True(t, waiting.NotBefore.Equal(wantNotBefore), "NotBefore after restart = %s, want %s", waiting.NotBefore, wantNotBefore)
		assert.Nil(t, waiting.RetryAfterClamp, "the persisted clamp is not reported again")

		time.Sleep(time.Until(wantNotBefore))
		resumed, err := restarted.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status, "the clamped deadline releases the far-future record")
		assert.Equal(t, 1, sender.calls)
	})
}

// 期限を保存できなくても、process内では期限まで送信せず、失敗を警告としてreportへ残す。
// 周期のerrorにはしないため、StopOnErrorのrunnerも再送を止めない。
func TestRunOnceSpoolFileProcessHonorsDeadlineWhenPersistenceFails(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/throttled"
		injected := errors.New("injected marker save failure")
		persisted := &retryAfterStoreStub{saveErr: injected}
		store := &workerStoreStub{
			claimPath:   path,
			claimOK:     true,
			readRecords: map[string][]spool.SpoolRecord{path: {testRecord("throttled")}},
		}
		start := time.Now()
		activeStore := &activeStoreStub{}
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{
			mustExportReport(t, throttledRetryableOutcome(start, time.Minute)),
			mustExportReport(t, successOutcome(start)),
		}}
		worker := newRetryAfterStoreTestWorker(t, store, activeStore, sender, persisted)

		throttled, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.NoError(t, throttled.Err)
		require.Equal(t, ProcessStatusCompleted, throttled.Status)
		require.ErrorIs(t, throttled.RetryAfterMarkerErr, injected)
		wantNotBefore := start.Add(time.Minute)
		require.Len(t, activeStore.records, 1)
		assert.True(t, activeStore.records[0][0].NotBefore.Equal(wantNotBefore), "the record keeps the deadline as a fallback")

		time.Sleep(time.Until(wantNotBefore) - time.Nanosecond)
		deferred, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, deferred.SkipReason)
		assert.True(t, deferred.NotBefore.Equal(wantNotBefore))
		assert.NoError(t, deferred.RetryAfterMarkerErr, "a save failure is reported once")
		assert.Equal(t, 1, sender.calls)

		time.Sleep(time.Nanosecond)
		resumed, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status)
		assert.Equal(t, 2, sender.calls)
	})
}

// 読めない保存内容で送信を止め続けない。警告を一度だけ残し、期限がないものとして送信する。
func TestRunOnceSpoolFileProcessContinuesWhenPersistedDeadlineIsUnreadable(t *testing.T) {
	t.Parallel()

	const path = "processing/ready"
	injected := errors.New("injected unreadable marker")
	persisted := &retryAfterStoreStub{loadErr: injected}
	store := &workerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]spool.SpoolRecord{path: {testRecord("ready")}},
	}
	sender := &senderStub{report: mustExportReport(t, successOutcome(time.Now()))}
	worker := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)

	report, err := worker.RunOnceSpoolFileProcess(context.Background())
	require.NoError(t, err)
	assert.Equal(t, ProcessStatusCompleted, report.Status)
	assert.ErrorIs(t, report.RetryAfterMarkerErr, injected)
	assert.Equal(t, 1, sender.calls)

	next, err := worker.RunOnceSpoolFileProcess(context.Background())
	require.NoError(t, err)
	assert.NoError(t, next.RetryAfterMarkerErr)
	assert.Equal(t, []string{"load"}, persisted.calls, "the unreadable deadline is not reloaded")
}

// 読み込み前にctxが終了した場合は、保存した期限を読まないまま送信へ進まない。
func TestRunOnceSpoolFileProcessDoesNotSendBeforeLoadingDeadline(t *testing.T) {
	t.Parallel()

	persisted := &retryAfterStoreStub{}
	store := &workerStoreStub{claimPath: "processing/claimed", claimOK: true}
	sender := &senderStub{}
	worker := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	report, err := worker.RunOnceSpoolFileProcess(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, ProcessStatusFailed, report.Status)
	assert.Empty(t, persisted.calls)
	assert.Empty(t, store.calls)
	assert.Zero(t, sender.calls)
}

// 通常送信の保存処理が記録した期限は、再送中のWorkerが次の周期から送信先のすべてのrecordに適用する。
// 期限を持たない古いrecordも読み込まず、再起動後も保存した期限まで送信しない。
func TestRetryAfterDeadlineRecordAppliesNormalSendDeadlineToAllRecords(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/older-without-not-before"
		persisted := &retryAfterStoreStub{}
		store := &workerStoreStub{
			processingPaths: []string{path},
			readRecords:     map[string][]spool.SpoolRecord{path: {testRecord("older")}},
			claimPath:       "processing/claimed",
			claimOK:         true,
		}
		start := time.Now()
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{mustExportReport(t, successOutcome(start))}}
		running := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)
		notBefore := start.Add(30 * time.Second)

		running.RetryAfterDeadline().RecordRetryAfter(notBefore)
		require.Len(t, persisted.saves, 1, "the normal-send deadline is saved when it is recorded")
		assert.True(t, persisted.saves[0].NotBefore.Equal(notBefore))
		deferred, err := running.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, deferred.SkipReason)
		assert.True(t, deferred.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", deferred.NotBefore, notBefore)
		assert.Empty(t, store.calls, "the running worker reads no record before the normal-send deadline")

		restarted := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)
		restored, err := restarted.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, restored.SkipReason)
		assert.True(t, restored.NotBefore.Equal(notBefore))
		assert.Empty(t, store.calls)
		assert.Zero(t, sender.calls)

		time.Sleep(time.Until(notBefore))
		resumed, err := restarted.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status)
		assert.Equal(t, 1, sender.calls)
	})
}

// 通常送信の期限は、保存済みのより後の期限を短縮しない。最初の記録の前に保存済みの期限を読み込む。
func TestRetryAfterDeadlineRecordNeverShortensLaterDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		later := start.Add(10 * time.Minute).Round(0)
		persisted := &retryAfterStoreStub{state: RetryAfterState{NotBefore: later, AppliedThrough: later}}
		worker := newRetryAfterStoreTestWorker(t, &workerStoreStub{}, &activeStoreStub{}, &senderStub{}, persisted)
		deadline := worker.RetryAfterDeadline()

		deadline.RecordRetryAfter(start.Add(time.Minute))
		assert.Equal(t, []string{"load"}, persisted.calls, "an earlier deadline neither shortens nor rewrites the saved one")
		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.True(t, report.NotBefore.Equal(later), "NotBefore = %s, want the saved %s", report.NotBefore, later)

		longer := start.Add(20 * time.Minute)
		deadline.RecordRetryAfter(longer)
		deadline.RecordRetryAfter(start.Add(15 * time.Minute))
		require.Len(t, persisted.saves, 1)
		assert.True(t, persisted.saves[0].NotBefore.Equal(longer))
		report, err = worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.True(t, report.NotBefore.Equal(longer))
	})
}

// 通常送信の期限も、記録した時点から上限までに短縮して記録・保存する。
func TestRetryAfterDeadlineRecordClampsNormalSendDeadlineToMaximum(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		persisted := &retryAfterStoreStub{}
		worker := newRetryAfterStoreTestWorker(t, &workerStoreStub{}, &activeStoreStub{}, &senderStub{}, persisted, WithMaxRetryAfter(time.Hour))
		start := time.Now()
		worker.RetryAfterDeadline().RecordRetryAfter(start.Add(3 * time.Hour))
		require.Len(t, persisted.saves, 1)
		assert.True(t, persisted.saves[0].NotBefore.Equal(start.Add(time.Hour)), "saved NotBefore = %s", persisted.saves[0].NotBefore)
		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.True(t, report.NotBefore.Equal(start.Add(time.Hour)))
	})
}

// 周期の最初の確認の後、fileを選んで読み込む間に通常送信の保存処理が期限を記録した場合も、
// 送信の直前に期限を確認してそのfileを送信しない。fileはprocessingに残し、期限後に送信する。
func TestRunOnceSpoolFileProcessRechecksDeadlineJustBeforeSend(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/selected-before-deadline"
		start := time.Now()
		notBefore := start.Add(time.Minute)
		store := &workerStoreStub{
			processingPaths: []string{path},
			readRecords:     map[string][]spool.SpoolRecord{path: {testRecord("selected-before-deadline")}},
		}
		var worker *Worker
		recorded := false
		store.listCandidatesFn = func(_ context.Context, after string, limit int) ([]spool.FileCandidate, error) {
			if !recorded {
				// 周期の最初の期限確認の後に、通常送信の保存処理が期限を記録します。
				recorded = true
				worker.RetryAfterDeadline().RecordRetryAfter(notBefore)
			}
			return firstProcessingCandidatesAfter(store.processingPaths, after, limit), nil
		}
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{mustExportReport(t, successOutcome(start))}}
		worker = newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, &retryAfterStoreStub{})

		deferred, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.True(t, recorded, "the deadline must be recorded while the worker selects the file")
		assert.Equal(t, ProcessStatusSkipped, deferred.Status)
		assert.Equal(t, ProcessSkipReasonRetryAfter, deferred.SkipReason)
		assert.True(t, deferred.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", deferred.NotBefore, notBefore)
		assert.Zero(t, sender.calls, "the file selected before the deadline was recorded must not be sent")

		time.Sleep(time.Until(notBefore))
		resumed, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, resumed.Status)
		assert.Equal(t, path, resumed.ProcessingPath)
		assert.Equal(t, 1, sender.calls)
	})
}

// inFlightDeadlineSenderは、送信の途中で通常送信の保存処理が期限を記録する状況を再現し、
// その時点で送信のcontextが終了していないかを記録します。
type inFlightDeadlineSender struct {
	record func()
	report spool.ExportReport

	calls     int
	ctxErrors []error
}

func (s *inFlightDeadlineSender) Send(ctx context.Context, _ []spool.SpoolRecord) (spool.ExportReport, error) {
	s.calls++
	s.record()
	s.ctxErrors = append(s.ctxErrors, ctx.Err())
	return s.report, nil
}

// 送信中に記録された期限は、送信中のrequestを取り消さず、その結果を通常どおり反映する。
// 期限は次の周期から適用する。
func TestRunOnceSpoolFileProcessKeepsSendInFlightWhenDeadlineIsRecorded(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const inFlight, next = "processing/in-flight", "processing/next"
		start := time.Now()
		notBefore := start.Add(time.Minute)
		store := &workerStoreStub{
			processingPaths: []string{inFlight, next},
			readRecords: map[string][]spool.SpoolRecord{
				inFlight: {testRecord("in-flight")},
				next:     {testRecord("next")},
			},
		}
		var worker *Worker
		sender := &inFlightDeadlineSender{report: mustExportReport(t, successOutcome(start))}
		sender.record = func() { worker.RetryAfterDeadline().RecordRetryAfter(notBefore) }
		worker = newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, &retryAfterStoreStub{})

		sent, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessStatusCompleted, sent.Status, "the in-flight result is applied")
		assert.Equal(t, inFlight, sent.ProcessingPath)
		assert.Equal(t, 1, sent.SuccessRecordCount)
		assert.Equal(t, []error{nil}, sender.ctxErrors, "the in-flight request is not cancelled")

		deferred, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, deferred.SkipReason, "the deadline applies from the next cycle")
		assert.True(t, deferred.NotBefore.Equal(notBefore))
		assert.Equal(t, 1, sender.calls)
	})
}

// clampFailingRetryAfterStoreは、読み込み時に上限より先の保存済み期限を短縮したものの、短縮した
// 期限を保存できなかった状態を返します。以後の保存はretryAfterStoreStubとして成功します。
type clampFailingRetryAfterStore struct {
	retryAfterStoreStub
	clampSaveErr error
}

func (s *clampFailingRetryAfterStore) Load(now time.Time, maxWait time.Duration) (RetryAfterState, *spool.RetryAfterClamp, error) {
	s.calls = append(s.calls, "load")
	clamped := now.Add(maxWait).Round(0)
	return RetryAfterState{NotBefore: clamped, AppliedThrough: s.state.AppliedThrough},
		&spool.RetryAfterClamp{
			Requested:     s.state.NotBefore.Sub(now),
			MaxRetryAfter: maxWait,
			NotBefore:     clamped,
			Stored:        true,
		},
		s.clampSaveErr
}

// 読み込み時に短縮した期限を保存できなかった場合も、process内では短縮後の期限まで待ち、runnerの
// 周期の後に保存し直す。短縮と保存の失敗は一度だけ通知する。
func TestRunOnceSpoolFileProcessRetriesSavingDeadlineClampedAtLoad(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const interval = 100 * time.Millisecond
		start := time.Now()
		farFuture := start.Add(10 * time.Hour).Round(0)
		injected := errors.New("injected clamped marker save failure")
		persisted := &clampFailingRetryAfterStore{
			retryAfterStoreStub: retryAfterStoreStub{state: RetryAfterState{NotBefore: farFuture, AppliedThrough: farFuture}},
			clampSaveErr:        injected,
		}
		sender := &senderStub{}
		worker := newRetryAfterStoreTestWorker(t, &workerStoreStub{}, &activeStoreStub{}, sender, persisted,
			WithMaxRetryAfter(time.Hour), WithRetryAfterSaveRetryInterval(interval))

		clamped, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		wantNotBefore := start.Add(time.Hour)
		assert.Equal(t, ProcessSkipReasonRetryAfter, clamped.SkipReason)
		assert.True(t, clamped.NotBefore.Equal(wantNotBefore), "NotBefore = %s, want %s", clamped.NotBefore, wantNotBefore)
		require.NotNil(t, clamped.RetryAfterClamp)
		assert.True(t, clamped.RetryAfterClamp.Stored)
		assert.ErrorIs(t, clamped.RetryAfterMarkerErr, injected)
		assert.Empty(t, persisted.saves, "the retry waits for one runner interval")

		time.Sleep(interval)
		retried, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, retried.SkipReason)
		assert.Nil(t, retried.RetryAfterClamp, "the clamp is reported once")
		assert.NoError(t, retried.RetryAfterMarkerErr, "the save failure is reported once")
		require.Len(t, persisted.saves, 1, "the clamped deadline is saved again after one runner interval")
		assert.True(t, persisted.saves[0].NotBefore.Equal(wantNotBefore), "saved NotBefore = %s", persisted.saves[0].NotBefore)
		assert.True(t, persisted.saves[0].AppliedThrough.Equal(farFuture), "the clamped stored deadline stays applied")
		assert.Zero(t, sender.calls)
	})
}

// 保存に失敗した期限は、期限が残る間の後の周期で保存し直し、再起動後も引き継ぐ。
// 警告は一連の失敗につき一度だけ残し、成功後に再び失敗した場合は改めて警告する。
func TestRunOnceSpoolFileProcessRetriesFailedDeadlineSave(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const path = "processing/throttled"
		injected := errors.New("injected transient marker save failure")
		persisted := &retryAfterStoreStub{transientSaveErr: injected, transientSaveFailures: 2}
		store := &workerStoreStub{
			claimPath:   path,
			claimOK:     true,
			readRecords: map[string][]spool.SpoolRecord{path: {testRecord("throttled")}},
		}
		start := time.Now()
		sender := &retryAfterSenderStub{reports: []spool.ExportReport{
			mustExportReport(t, throttledRetryableOutcome(start, time.Hour)),
			mustExportReport(t, successOutcome(start)),
		}}
		worker := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)

		throttled, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.ErrorIs(t, throttled.RetryAfterMarkerErr, injected)
		notBefore := start.Add(time.Hour)
		require.False(t, persisted.saved)

		time.Sleep(time.Minute)
		retried, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, retried.SkipReason)
		assert.NoError(t, retried.RetryAfterMarkerErr, "the ongoing failure streak is reported once")
		assert.False(t, persisted.saved)

		time.Sleep(time.Minute)
		saved, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.Equal(t, ProcessSkipReasonRetryAfter, saved.SkipReason)
		assert.NoError(t, saved.RetryAfterMarkerErr)
		require.True(t, persisted.saved, "a later cycle saves the deadline")
		assert.True(t, persisted.state.NotBefore.Equal(notBefore))

		restarted := newRetryAfterStoreTestWorker(t, store, &activeStoreStub{}, sender, persisted)
		restored, err := restarted.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.True(t, restored.NotBefore.Equal(notBefore))
		assert.Equal(t, 1, sender.calls)

		// 保存に成功した後の新しい失敗は、新しい一連の失敗として改めて警告する。
		persisted.transientSaveFailures = 1
		restarted.RetryAfterDeadline().RecordRetryAfter(notBefore.Add(time.Minute))
		again, err := restarted.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		assert.ErrorIs(t, again.RetryAfterMarkerErr, injected)
	})
}

// 通常送信の保存処理は、再送の周期と並行して期限を記録できる。記録はより後の期限だけを残す。
func TestRetryAfterDeadlineRecordIsSafeWithConcurrentCycles(t *testing.T) {
	t.Parallel()

	persisted := &lockedRetryAfterStore{}
	worker := newRetryAfterStoreTestWorker(t, &workerStoreStub{}, &activeStoreStub{}, &senderStub{}, persisted)
	deadline := worker.RetryAfterDeadline()
	// 上限より十分短い期限を記録し、上限による短縮の影響を受けないようにします。
	base := time.Now().Add(time.Minute)
	const writers, records = 4, 50
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for record := range records {
				deadline.RecordRetryAfter(base.Add(time.Duration(record*writers+writer) * time.Millisecond))
			}
		}()
	}
	for range records {
		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		require.NoError(t, report.RetryAfterMarkerErr)
	}
	wg.Wait()

	want := base.Add(time.Duration(records*writers-1) * time.Millisecond)
	report, err := worker.RunOnceSpoolFileProcess(context.Background())
	require.NoError(t, err)
	assert.Equal(t, ProcessSkipReasonRetryAfter, report.SkipReason)
	assert.True(t, report.NotBefore.Equal(want), "NotBefore = %s, want the latest recorded %s", report.NotBefore, want)
	assert.True(t, persisted.latest().NotBefore.Equal(want), "saved NotBefore = %s, want %s", persisted.latest().NotBefore, want)
}

// lockedRetryAfterStoreは、並行試験で保存内容を参照するための保存先です。
type lockedRetryAfterStore struct {
	mu    sync.Mutex
	state RetryAfterState
}

func (s *lockedRetryAfterStore) Load(time.Time, time.Duration) (RetryAfterState, *spool.RetryAfterClamp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil, nil
}

func (s *lockedRetryAfterStore) Save(state RetryAfterState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}

func (s *lockedRetryAfterStore) Remove() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = RetryAfterState{}
	return nil
}

func (s *lockedRetryAfterStore) latest() RetryAfterState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// saveAttemptGapsは、保存の試行の間隔を返します。
func saveAttemptGaps(attempts []time.Time) []time.Duration {
	gaps := make([]time.Duration, 0, len(attempts))
	for index := 1; index < len(attempts); index++ {
		gaps = append(gaps, attempts[index].Sub(attempts[index-1]))
	}
	return gaps
}

// runRetryAfterCyclesは、runnerの周期ごとにWorkerの周期を実行し、警告を残した周期の数を返します。
func runRetryAfterCycles(t *testing.T, worker *Worker, interval, duration time.Duration) int {
	t.Helper()
	warned := 0
	for elapsed := time.Duration(0); elapsed < duration; elapsed += interval {
		time.Sleep(interval)
		report, err := worker.RunOnceSpoolFileProcess(context.Background())
		require.NoError(t, err)
		if report.RetryAfterMarkerErr != nil {
			warned++
		}
	}
	return warned
}

// 保存に失敗した期限は、runnerの周期から始めて失敗のたびに倍にした間隔（上限1分）で保存し直し、
// 警告は一連の失敗につき一度だけ残す。保存に成功したら間隔を最初の周期へ戻す。
func TestRetryAfterDeadlineSaveRetryBacksOffExponentially(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const interval = 100 * time.Millisecond
		injected := errors.New("injected persistent marker save failure")
		persisted := &retryAfterStoreStub{saveErr: injected}
		worker := newRetryAfterStoreTestWorker(t, &workerStoreStub{}, &activeStoreStub{}, &senderStub{}, persisted,
			WithRetryAfterSaveRetryInterval(interval))
		deadline := worker.RetryAfterDeadline()
		deadline.RecordRetryAfter(time.Now().Add(50 * time.Minute))

		warned := runRetryAfterCycles(t, worker, interval, 4*time.Minute)
		assert.Equal(t, 1, warned, "a failure streak is reported once")
		var want []time.Duration
		for backoff := interval; len(want) < len(persisted.saveAttempts)-1; backoff *= 2 {
			want = append(want, min(backoff, time.Minute))
		}
		require.GreaterOrEqual(t, len(persisted.saveAttempts), 13)
		assert.Equal(t, want, saveAttemptGaps(persisted.saveAttempts),
			"retries start at the runner interval, double, and cap at one minute")

		// 保存に成功すると間隔を最初へ戻し、次の失敗は新しい一連の失敗として通知する。
		persisted.saveErr = nil
		runRetryAfterCycles(t, worker, interval, time.Minute)
		require.True(t, persisted.saved, "a scheduled retry saves the deadline after recovery")
		persisted.saveErr = injected
		persisted.saveAttempts = nil
		deadline.RecordRetryAfter(time.Now().Add(55 * time.Minute))
		warned = runRetryAfterCycles(t, worker, interval, 700*time.Millisecond)
		assert.Equal(t, 1, warned)
		assert.Equal(t, []time.Duration{interval, 2 * interval, 4 * interval}, saveAttemptGaps(persisted.saveAttempts),
			"the backoff restarts from the runner interval after a successful save")
	})
}

// 期限が来たら、保存できていない期限の再試行をやめる。
func TestRetryAfterDeadlineSaveRetryStopsAtDeadline(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const interval = 100 * time.Millisecond
		persisted := &retryAfterStoreStub{saveErr: errors.New("injected persistent marker save failure")}
		worker := newRetryAfterStoreTestWorker(t, &workerStoreStub{}, &activeStoreStub{}, &senderStub{}, persisted,
			WithRetryAfterSaveRetryInterval(interval))
		notBefore := time.Now().Add(2 * time.Second)
		worker.RetryAfterDeadline().RecordRetryAfter(notBefore)

		runRetryAfterCycles(t, worker, interval, 10*time.Second)
		require.NotEmpty(t, persisted.saveAttempts)
		for _, attempt := range persisted.saveAttempts {
			assert.True(t, attempt.Before(notBefore), "save retried at %s after the deadline %s", attempt, notBefore)
		}
		assert.Len(t, persisted.saveAttempts, 5, "attempts at 0, +0.1s, +0.3s, +0.7s and +1.5s")
	})
}
