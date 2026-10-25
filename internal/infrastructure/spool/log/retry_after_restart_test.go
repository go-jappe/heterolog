package logspool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstoreretryafter "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

// synctestFromWallClockForTestは、synctestの仮想時計を開始時点の実時刻まで進めてからfを実行します。
// spool fileやmarkerへ保存する時刻を、bubbleの開始日ではなく実行時の日付に揃えます。
func synctestFromWallClockForTest(t *testing.T, f func(*testing.T)) {
	t.Helper()
	start := time.Now()
	synctest.Test(t, func(t *testing.T) {
		time.Sleep(time.Until(start))
		f(t)
	})
}

// scriptedRetryAfterSenderは、呼び出しごとに指定したRetry-Afterを全recordへ返します。
// 指定がない呼び出しは全recordを受理し、受け取った時刻とrecordを記録します。
type scriptedRetryAfterSender struct {
	latency    time.Duration
	retryAfter []time.Duration
	requests   []scriptedRetryAfterRequest
}

type scriptedRetryAfterRequest struct {
	at  time.Time
	ids []string
}

func (s *scriptedRetryAfterSender) Send(_ context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	time.Sleep(s.latency)
	call := len(s.requests)
	request := scriptedRetryAfterRequest{at: time.Now()}
	for _, record := range records {
		request.ids = append(request.ids, record.RequestID)
	}
	s.requests = append(s.requests, request)
	var retryAfter time.Duration
	if call < len(s.retryAfter) {
		retryAfter = s.retryAfter[call]
	}
	report := make(spool.ExportReport, len(records))
	for index := range records {
		report[index] = spool.RecordOutcome{InputIndex: index, Status: spool.RecordOutcomeStatusSuccess, AttemptedAt: request.at}
		if retryAfter > 0 {
			report[index] = spool.RecordOutcome{
				InputIndex: index, Status: spool.RecordOutcomeStatusRetryable, AttemptedAt: request.at,
				Err: errors.New("throttled"), RetryAfter: retryAfter,
			}
		}
	}
	return report, nil
}

// retryAfterRestartRuntimeは、productionと同じ構築処理でspoolの書き込み口と再送runnerを作ります。
// 同じ設定で作り直すことで、process再起動後のrunnerを再現します。
type retryAfterRestartRuntime struct {
	t    *testing.T
	opts BackendRuntimeOptions
}

func newRetryAfterRestartRuntime(t *testing.T, spoolPath, backend string, maxRetryAfter time.Duration, sender worker.Sender) retryAfterRestartRuntime {
	t.Helper()
	opts := BackendRuntimeOptions{
		Backend:     backend,
		SpoolConfig: pkgdurable.SpoolConfig{SpoolPath: spoolPath, Target: "retry-after-restart", Dir: backend},
		Sender:      sender,
	}
	opts.RunnerConfig.Worker.Finalize.RetryDelay = time.Second
	opts.RunnerConfig.Worker.Finalize.MaxEntries = 100
	opts.RunnerConfig.Worker.MaxRetryAfter = maxRetryAfter
	opts.RunnerConfig.LifeCycle.SizePurgeInterval = time.Second
	// 期限の保存の再試行はrunnerの周期から始めます。workerの既定値（1秒）と異なる周期にして、
	// 構築処理が周期を渡していることを試験できるようにします。
	opts.RunnerConfig.Runner.Interval = retryAfterRestartRunnerInterval
	opts.SupervisorConfig.RestartDelay = time.Second
	return retryAfterRestartRuntime{t: t, opts: opts}
}

// retryAfterRestartRunnerIntervalは、retryAfterRestartRuntimeのrunnerの周期です。
const retryAfterRestartRunnerInterval = 250 * time.Millisecond

func (r retryAfterRestartRuntime) start() *Runner {
	r.t.Helper()
	supervisor, err := NewBackendRunnerSupervisor(clock.NewUTCClock(), r.opts, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(r.t, err)
	return supervisor.Runner
}

func (r retryAfterRestartRuntime) store(records ...spool.SpoolRecord) {
	r.t.Helper()
	store, err := fsstorestore.NewSpoolStore(clock.NewUTCClock(), loggerdurable.NewSpoolConfigAdapter(r.opts.SpoolConfig), observer.NopTracer{}, observer.NopMeter{})
	require.NoError(r.t, err)
	require.NoError(r.t, store.StoreByState(context.Background(), spool.StateActive, records))
}

func (r retryAfterRestartRuntime) root() string {
	return filepath.Join(r.opts.SpoolConfig.SpoolPath, r.opts.SpoolConfig.Target, "spool", r.opts.SpoolConfig.Dir)
}

func (r retryAfterRestartRuntime) markerPath() string {
	return filepath.Join(r.root(), fsstoreretryafter.DirectoryName, r.opts.Backend+".json")
}

func (r retryAfterRestartRuntime) stateRecords(state spool.State) []spool.SpoolRecord {
	r.t.Helper()
	return readIntegrationSpoolRecords(r.t, filepath.Join(r.root(), string(state)))
}

func restartTestRecord(id string) spool.SpoolRecord {
	return spool.SpoolRecord{OccurredAt: time.Now(), RequestID: id, Payload: []byte(`{"id":"` + id + `"}`)}
}

func runRetryAfterRestartCycle(t *testing.T, runner *Runner) worker.ProcessReport {
	t.Helper()
	report, err := runner.RunOnce(context.Background())
	require.NoError(t, err)
	return report.ProcessReport
}

// 再起動後は、spool再送で受け取ったRetry-Afterの期限を最初の送信より前に読み込む。
// 期限を保存していない古いrecordが先に読まれる順序でも、期限までその送信先へ送信しない。
func TestSpoolReplayRestartHonorsPersistedRetryAfterRegardlessOfReadOrder(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		sender := &scriptedRetryAfterSender{latency: time.Second, retryAfter: []time.Duration{30 * time.Second}}
		runtime := newRetryAfterRestartRuntime(t, resolvedTempDirForTest(t), "loki", 0, sender)
		runtime.store(restartTestRecord("throttled"))
		time.Sleep(time.Millisecond)
		// 再送待ちへ戻るrecordより古く、期限を持たないrecordです。
		runtime.store(restartTestRecord("older"))

		throttled := runRetryAfterRestartCycle(t, runtime.start())
		require.Equal(t, worker.ProcessStatusCompleted, throttled.Status)
		require.Equal(t, 1, throttled.RetryRecordCount)
		require.NoError(t, throttled.RetryAfterMarkerErr)
		notBefore := sender.requests[0].at.Add(30 * time.Second)
		require.True(t, throttled.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", throttled.NotBefore, notBefore)
		require.FileExists(t, runtime.markerPath())

		restarted := runtime.start()
		deferred := runRetryAfterRestartCycle(t, restarted)
		require.Equal(t, worker.ProcessStatusSkipped, deferred.Status)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, deferred.SkipReason)
		require.True(t, deferred.NotBefore.Equal(notBefore), "restored NotBefore = %s, want %s", deferred.NotBefore, notBefore)
		require.Len(t, sender.requests, 1, "the older record must wait for the persisted deadline")
		require.Len(t, runtime.stateRecords(spool.StateActive), 2)
		require.Empty(t, runtime.stateRecords(spool.StateProcessing))

		time.Sleep(time.Until(notBefore) - time.Nanosecond)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, runRetryAfterRestartCycle(t, restarted).SkipReason)
		require.Len(t, sender.requests, 1)

		time.Sleep(time.Nanosecond)
		for range 2 {
			require.Equal(t, worker.ProcessStatusCompleted, runRetryAfterRestartCycle(t, restarted).Status)
		}
		require.Len(t, sender.requests, 3)
		require.Equal(t, []string{"older"}, sender.requests[1].ids, "the older record is read first after the deadline")
		for _, request := range sender.requests[1:] {
			require.False(t, request.at.Before(notBefore), "request %v at %s before %s", request.ids, request.at, notBefore)
		}
		require.Len(t, runtime.stateRecords(spool.StateDone), 2)
		require.NoFileExists(t, runtime.markerPath(), "the expired deadline is removed")
	})
}

// 上限を超える保存済みの期限は、読み込んだ時点から上限までに短縮して保存し直す。
// 二回目の再起動では短縮後の期限をそのまま使い、待機を延ばさず、短縮も通知し直さない。
func TestSpoolReplayRestartPersistsClampedRetryAfter(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		spoolPath := resolvedTempDirForTest(t)
		sender := &scriptedRetryAfterSender{retryAfter: []time.Duration{10 * time.Hour}}
		// 以前の設定では長い上限で期限を保存していた状態から始めます。
		before := newRetryAfterRestartRuntime(t, spoolPath, "loki", 24*time.Hour, sender)
		before.store(restartTestRecord("throttled"))
		throttled := runRetryAfterRestartCycle(t, before.start())
		require.True(t, throttled.NotBefore.Equal(sender.requests[0].at.Add(10*time.Hour)))

		runtime := newRetryAfterRestartRuntime(t, spoolPath, "loki", time.Hour, sender)
		time.Sleep(time.Second)
		clampedAt := time.Now()
		clamped := runRetryAfterRestartCycle(t, runtime.start())
		wantNotBefore := clampedAt.Add(time.Hour)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, clamped.SkipReason)
		require.True(t, clamped.NotBefore.Equal(wantNotBefore), "clamped NotBefore = %s, want %s", clamped.NotBefore, wantNotBefore)
		require.NotNil(t, clamped.RetryAfterClamp)
		require.True(t, clamped.RetryAfterClamp.Stored)
		require.Equal(t, time.Hour, clamped.RetryAfterClamp.MaxRetryAfter)
		require.NoError(t, clamped.RetryAfterMarkerErr)

		time.Sleep(30 * time.Minute)
		restarted := runtime.start()
		waiting := runRetryAfterRestartCycle(t, restarted)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, waiting.SkipReason)
		require.True(t, waiting.NotBefore.Equal(wantNotBefore), "NotBefore after the second restart = %s, want %s", waiting.NotBefore, wantNotBefore)
		require.Nil(t, waiting.RetryAfterClamp, "the persisted clamp is not reported again")

		time.Sleep(time.Until(wantNotBefore))
		require.Equal(t, worker.ProcessStatusCompleted, runRetryAfterRestartCycle(t, restarted).Status)
		require.Len(t, sender.requests, 2)
		require.False(t, sender.requests[1].at.Before(wantNotBefore))
	})
}

// recordに保存された上限より先の期限も、短縮した期限と反映済みの時刻を保存する。
// 再起動後は短縮後の期限を待ち、同じrecordの期限を読み込んだ時点から数え直さない。
func TestSpoolReplayRestartPersistsClampedStoredRecordDeadline(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		sender := &scriptedRetryAfterSender{}
		runtime := newRetryAfterRestartRuntime(t, resolvedTempDirForTest(t), "opensearch", time.Hour, sender)
		record := restartTestRecord("far-future")
		record.NotBefore = time.Now().Add(10 * 24 * time.Hour).Round(0)
		runtime.store(record)

		clampedAt := time.Now()
		clamped := runRetryAfterRestartCycle(t, runtime.start())
		wantNotBefore := clampedAt.Add(time.Hour)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, clamped.SkipReason)
		require.True(t, clamped.NotBefore.Equal(wantNotBefore))
		require.NotNil(t, clamped.RetryAfterClamp)
		require.True(t, clamped.RetryAfterClamp.Stored)
		require.NoError(t, clamped.RetryAfterMarkerErr)

		time.Sleep(30 * time.Minute)
		restarted := runtime.start()
		waiting := runRetryAfterRestartCycle(t, restarted)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, waiting.SkipReason)
		require.True(t, waiting.NotBefore.Equal(wantNotBefore), "NotBefore after restart = %s, want %s", waiting.NotBefore, wantNotBefore)
		require.Nil(t, waiting.RetryAfterClamp)

		time.Sleep(time.Until(wantNotBefore))
		require.Equal(t, worker.ProcessStatusCompleted, runRetryAfterRestartCycle(t, restarted).Status)
		require.Len(t, sender.requests, 1)
		require.Equal(t, []string{"far-future"}, sender.requests[0].ids)
	})
}

// 一つの出力先の期限は、その出力先のspoolだけに保存し、他の出力先の再送を止めない。
func TestSpoolReplayRestartRetryAfterDoesNotDeferOtherBackends(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		spoolPath := resolvedTempDirForTest(t)
		// 応答までの時間により、再送待ちへ戻すrecordは後から保存したrecordより新しくなります。
		lokiSender := &scriptedRetryAfterSender{latency: time.Second, retryAfter: []time.Duration{time.Hour}}
		openSearchSender := &scriptedRetryAfterSender{}
		loki := newRetryAfterRestartRuntime(t, spoolPath, "loki", 0, lokiSender)
		openSearch := newRetryAfterRestartRuntime(t, spoolPath, "opensearch", 0, openSearchSender)
		loki.store(restartTestRecord("loki-throttled"))
		time.Sleep(time.Millisecond)
		// 再送待ちへ戻るrecordより古く、期限を持たないrecordです。
		loki.store(restartTestRecord("loki-older"))
		throttled := runRetryAfterRestartCycle(t, loki.start())
		require.Equal(t, worker.ProcessStatusCompleted, throttled.Status)
		require.FileExists(t, loki.markerPath())

		openSearch.store(restartTestRecord("opensearch-ready"))
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, runRetryAfterRestartCycle(t, loki.start()).SkipReason)
		sent := runRetryAfterRestartCycle(t, openSearch.start())
		require.Equal(t, worker.ProcessStatusCompleted, sent.Status)
		require.Len(t, openSearchSender.requests, 1)
		require.Len(t, lokiSender.requests, 1)
		require.NoFileExists(t, openSearch.markerPath())
	})
}

// 壊れたmarkerで再送を止めない。警告を残して期限がないものとして送信し、次のRetry-Afterで置き換える。
func TestSpoolReplayRestartReplacesCorruptRetryAfterMarker(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		sender := &scriptedRetryAfterSender{retryAfter: []time.Duration{0, time.Minute}}
		runtime := newRetryAfterRestartRuntime(t, resolvedTempDirForTest(t), "clickhouse", 0, sender)
		runtime.store(restartTestRecord("ready"))
		time.Sleep(time.Millisecond)
		runtime.store(restartTestRecord("throttled"))
		require.NoError(t, os.MkdirAll(filepath.Dir(runtime.markerPath()), 0o700))
		require.NoError(t, os.WriteFile(runtime.markerPath(), []byte("{not json"), 0o600))

		runner := runtime.start()
		sent := runRetryAfterRestartCycle(t, runner)
		require.Equal(t, worker.ProcessStatusCompleted, sent.Status, "a corrupt marker must not block delivery")
		require.ErrorIs(t, sent.RetryAfterMarkerErr, fsstoreretryafter.ErrInvalidMarker)
		var markerErr *fsstoreretryafter.MarkerError
		require.ErrorAs(t, sent.RetryAfterMarkerErr, &markerErr)
		require.Equal(t, fsstoreretryafter.MarkerOperationLoad, markerErr.Operation)

		throttled := runRetryAfterRestartCycle(t, runner)
		require.Equal(t, 1, throttled.RetryRecordCount)
		require.NoError(t, throttled.RetryAfterMarkerErr)
		store, err := fsstoreretryafter.NewLogSpoolStore(runtime.root(), "clickhouse")
		require.NoError(t, err)
		state, _, err := store.Load(time.Now(), time.Hour)
		require.NoError(t, err)
		require.True(t, state.NotBefore.Equal(throttled.NotBefore), "replaced marker NotBefore = %s, want %s", state.NotBefore, throttled.NotBefore)
	})
}

// 期限を過ぎたmarkerは読み込み時に削除し、送信を制限しない。
func TestSpoolReplayRestartRemovesExpiredRetryAfterMarker(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		sender := &scriptedRetryAfterSender{}
		runtime := newRetryAfterRestartRuntime(t, resolvedTempDirForTest(t), "cloudwatchlogs", 0, sender)
		runtime.store(restartTestRecord("ready"))
		store, err := fsstoreretryafter.NewLogSpoolStore(runtime.root(), "cloudwatchlogs")
		require.NoError(t, err)
		require.NoError(t, store.Save(fsstoreretryafter.State{NotBefore: time.Now().Add(-time.Second)}))

		sent := runRetryAfterRestartCycle(t, runtime.start())
		require.Equal(t, worker.ProcessStatusCompleted, sent.Status)
		require.NoError(t, sent.RetryAfterMarkerErr)
		require.Len(t, sender.requests, 1)
		require.NoFileExists(t, runtime.markerPath())
	})
}

// hasRetryAfterMarkerOperationは、errのerror treeにoperationのMarkerErrorが含まれるかを返します。
func hasRetryAfterMarkerOperation(err error, operation fsstoreretryafter.MarkerOperation) bool {
	switch wrapped := err.(type) {
	case nil:
		return false
	case *fsstoreretryafter.MarkerError:
		return wrapped.Operation == operation || hasRetryAfterMarkerOperation(wrapped.Err, operation)
	case interface{ Unwrap() []error }:
		for _, child := range wrapped.Unwrap() {
			if hasRetryAfterMarkerOperation(child, operation) {
				return true
			}
		}
		return false
	case interface{ Unwrap() error }:
		return hasRetryAfterMarkerOperation(wrapped.Unwrap(), operation)
	default:
		return false
	}
}

// 期限を保存できなかった場合は、runnerの周期の後の周期で保存し直す。周期はworkerの既定値（1秒）と
// 異なるため、構築処理がrunnerの周期を保存の再試行の間隔として渡していることも確認できます。
func TestSpoolReplayRetriesFailedMarkerSaveAfterRunnerInterval(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		sender := &scriptedRetryAfterSender{retryAfter: []time.Duration{time.Hour}}
		runtime := newRetryAfterRestartRuntime(t, resolvedTempDirForTest(t), "loki", 0, sender)
		runtime.store(restartTestRecord("throttled"))
		// .retry-afterを通常fileで塞ぎ、最初の期限の保存を失敗させます。
		obstruction := filepath.Join(runtime.root(), fsstoreretryafter.DirectoryName)
		require.NoError(t, os.WriteFile(obstruction, []byte("not a directory"), 0o600))
		runner := runtime.start()

		throttled := runRetryAfterRestartCycle(t, runner)
		require.Equal(t, worker.ProcessStatusCompleted, throttled.Status)
		require.Len(t, sender.requests, 1)
		notBefore := sender.requests[0].at.Add(time.Hour)
		require.True(t, throttled.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", throttled.NotBefore, notBefore)
		require.True(t, hasRetryAfterMarkerOperation(throttled.RetryAfterMarkerErr, fsstoreretryafter.MarkerOperationSave),
			"RetryAfterMarkerErr = %v, want the failed save", throttled.RetryAfterMarkerErr)
		require.NoError(t, os.Remove(obstruction))

		time.Sleep(retryAfterRestartRunnerInterval)
		deferred := runRetryAfterRestartCycle(t, runner)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, deferred.SkipReason)
		require.NoError(t, deferred.RetryAfterMarkerErr)
		store, err := fsstoreretryafter.NewLogSpoolStore(runtime.root(), "loki")
		require.NoError(t, err)
		state, _, err := store.Load(time.Now(), time.Hour)
		require.NoError(t, err)
		require.True(t, state.NotBefore.Equal(notBefore),
			"marker NotBefore one runner interval after the failure = %s, want %s", state.NotBefore, notBefore)
		require.Len(t, sender.requests, 1)
	})
}
