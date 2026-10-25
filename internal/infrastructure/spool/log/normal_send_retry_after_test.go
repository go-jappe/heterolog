package logspool

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	overloadprovider "github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/overload/provider"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	spoolstoreconfig "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

// writerは、productionの構築と同じく、同じspool rootへ通常送信の失敗を保存するwriterを作り、
// runnerの待機期限へ接続します。
func (r retryAfterRestartRuntime) writer(runner *Runner) *spoolstorehandler.SpoolStoreHandler {
	r.t.Helper()
	store, err := fsstorestore.NewSpoolStore(clock.NewUTCClock(), loggerdurable.NewSpoolConfigAdapter(r.opts.SpoolConfig), observer.NopTracer{}, observer.NopMeter{})
	require.NoError(r.t, err)
	writer := spoolstorehandler.NewSpoolStoreHandler(
		context.Background(), clock.NewUTCClock(), store, nil, nil, nil,
		observer.NopTracer{}, observer.NopMeter{}, spoolstorehandler.InternalLogHandlers{},
	)
	writer.SetMaxRetryAfter(r.opts.RunnerConfig.Worker.MaxRetryAfter)
	writer.SetRetryAfterRecorder(runner.RetryAfterDeadline())
	return writer
}

// normalSendRetryAfterは、通常送信がRetry-Afterとともに再送可能な失敗を返した結果を保存します。
func normalSendRetryAfter(t *testing.T, writer *spoolstorehandler.SpoolStoreHandler, id string, retryAfter time.Duration) time.Time {
	t.Helper()
	failedAt := time.Now()
	require.NoError(t, writer.StoreSendResultByState(context.Background(), loggerfailure.SendResult{
		Meta: loggerfailure.Meta{Backend: "loki", RequestID: id, AttemptedAt: failedAt, InputCount: 1},
		RetryableRecords: []loggerfailure.FailedRecord{{
			FailedAt:   failedAt,
			RetryAfter: retryAfter,
			Payload:    map[string]any{"id": id},
		}},
	}))
	return failedAt.Add(retryAfter)
}

// 通常送信で受け取ったRetry-Afterは、保存時に出力先全体の期限として記録・保存する。
// 再送中のrunnerは次の周期から、期限を持たない古いログも期限まで送らない。再起動後も同じ。
func TestNormalSendRetryAfterDefersOlderSpoolRecords(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "running runner"
		if restart {
			name = "after restart"
		}
		t.Run(name, func(t *testing.T) {
			synctestFromWallClockForTest(t, func(t *testing.T) {
				sender := &scriptedRetryAfterSender{}
				runtime := newRetryAfterRestartRuntime(t, resolvedTempDirForTest(t), "loki", 0, sender)
				runner := runtime.start()
				// 稼働中のrunnerは、保存済みの期限をすでに読み込んでいます。
				require.Equal(t, worker.ProcessStatusNoTarget, runRetryAfterRestartCycle(t, runner).Status)
				runtime.store(restartTestRecord("older"))
				time.Sleep(time.Millisecond)

				notBefore := normalSendRetryAfter(t, runtime.writer(runner), "throttled", 30*time.Second)
				require.FileExists(t, runtime.markerPath(), "the normal-send deadline is saved with the record")
				if restart {
					runner = runtime.start()
				}
				deferred := runRetryAfterRestartCycle(t, runner)
				require.Equal(t, worker.ProcessSkipReasonRetryAfter, deferred.SkipReason)
				require.True(t, deferred.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", deferred.NotBefore, notBefore)
				require.Empty(t, sender.requests, "the older record without its own deadline waits too")
				require.Len(t, runtime.stateRecords(spool.StateActive), 2)

				time.Sleep(time.Until(notBefore) - time.Nanosecond)
				require.Equal(t, worker.ProcessSkipReasonRetryAfter, runRetryAfterRestartCycle(t, runner).SkipReason)
				time.Sleep(time.Nanosecond)
				for range 2 {
					require.Equal(t, worker.ProcessStatusCompleted, runRetryAfterRestartCycle(t, runner).Status)
				}
				require.Len(t, sender.requests, 2)
				require.Equal(t, []string{"older"}, sender.requests[0].ids)
				require.Equal(t, []string{"throttled"}, sender.requests[1].ids)
				for _, request := range sender.requests {
					require.False(t, request.at.Before(notBefore), "request %v at %s before %s", request.ids, request.at, notBefore)
				}
			})
		})
	}
}

// Retry-Afterを伴わない通常送信の失敗は、出力先全体の期限を記録しない。再送中のrunnerは次の周期から、
// 古いログも失敗したログも待たずに送る。
func TestNormalSendFailureWithoutRetryAfterDoesNotDeferReplay(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		sender := &scriptedRetryAfterSender{}
		runtime := newRetryAfterRestartRuntime(t, resolvedTempDirForTest(t), "loki", 0, sender)
		runner := runtime.start()
		require.Equal(t, worker.ProcessStatusNoTarget, runRetryAfterRestartCycle(t, runner).Status)
		runtime.store(restartTestRecord("older"))
		time.Sleep(time.Millisecond)

		normalSendRetryAfter(t, runtime.writer(runner), "failed", 0)
		require.NoFileExists(t, runtime.markerPath(), "a failure without Retry-After records no deadline")
		for range 2 {
			require.Equal(t, worker.ProcessStatusCompleted, runRetryAfterRestartCycle(t, runner).Status)
		}
		require.Len(t, sender.requests, 2)
		require.Equal(t, []string{"older"}, sender.requests[0].ids)
		require.Equal(t, []string{"failed"}, sender.requests[1].ids)
	})
}

// 上限を下げて期限を短縮した後でも、短縮前の期限より早い新しい通常送信の期限は、反映済みの
// 保存期限として扱わずに出力先の期限として保存する。再起動後も、そのログを期限より前に送らない。
func TestNormalSendRetryAfterAfterClampSurvivesRestart(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		spoolPath := resolvedTempDirForTest(t)
		sender := &scriptedRetryAfterSender{retryAfter: []time.Duration{10 * time.Hour}}
		// 以前の設定では長い上限で期限を保存していた状態から始めます。
		before := newRetryAfterRestartRuntime(t, spoolPath, "loki", 24*time.Hour, sender)
		before.store(restartTestRecord("throttled"))
		require.Equal(t, worker.ProcessStatusCompleted, runRetryAfterRestartCycle(t, before.start()).Status)

		runtime := newRetryAfterRestartRuntime(t, spoolPath, "loki", time.Hour, sender)
		time.Sleep(time.Second)
		clampedAt := time.Now()
		running := runtime.start()
		clamped := runRetryAfterRestartCycle(t, running)
		clampedNotBefore := clampedAt.Add(time.Hour)
		require.True(t, clamped.NotBefore.Equal(clampedNotBefore))
		require.NotNil(t, clamped.RetryAfterClamp)

		time.Sleep(30 * time.Minute)
		// 短縮後の期限より遅く、短縮前の期限より早い新しい期限です。
		fresh := normalSendRetryAfter(t, runtime.writer(running), "fresh", 45*time.Minute)
		require.True(t, fresh.After(clampedNotBefore))

		time.Sleep(10 * time.Minute)
		restarted := runtime.start()
		waiting := runRetryAfterRestartCycle(t, restarted)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, waiting.SkipReason)
		require.True(t, waiting.NotBefore.Equal(fresh), "NotBefore after restart = %s, want %s", waiting.NotBefore, fresh)

		time.Sleep(time.Until(clampedNotBefore))
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, runRetryAfterRestartCycle(t, restarted).SkipReason)
		require.Len(t, sender.requests, 1, "no record is sent before the fresh deadline")

		time.Sleep(time.Until(fresh))
		for range 2 {
			require.Equal(t, worker.ProcessStatusCompleted, runRetryAfterRestartCycle(t, restarted).Status)
		}
		require.Len(t, sender.requests, 3)
		for _, request := range sender.requests[1:] {
			require.False(t, request.at.Before(fresh), "request %v at %s before %s", request.ids, request.at, fresh)
		}
	})
}

// overloadedSpoolConfigは、activeに一件でもspoolがあれば容量の過負荷と判定させ、dropします。
type overloadedSpoolConfig struct{ integrationSpoolOverloadConfig }

func (overloadedSpoolConfig) MaxDirSize() uint64 { return 1 }

func (c overloadedSpoolConfig) OverloadPolicyConfig() spoolstoreconfig.SpoolStoreOverloadPolicyConfigProvider {
	return c
}

// overloadedWriterは、過負荷でspoolへの保存をdropするwriterを作り、runnerの待機期限へ接続します。
func (r retryAfterRestartRuntime) overloadedWriter(runner *Runner) *spoolstorehandler.SpoolStoreHandler {
	r.t.Helper()
	store, err := fsstorestore.NewSpoolStore(clock.NewUTCClock(), loggerdurable.NewSpoolConfigAdapter(r.opts.SpoolConfig), observer.NopTracer{}, observer.NopMeter{})
	require.NoError(r.t, err)
	writer := spoolstorehandler.NewSpoolStoreHandler(
		context.Background(), clock.NewUTCClock(), store, overloadedSpoolConfig{}, nil,
		overloadprovider.NewOverloadDropHandler(observer.NopMeter{}),
		observer.NopTracer{}, observer.NopMeter{}, spoolstorehandler.InternalLogHandlers{},
	)
	writer.SetMaxRetryAfter(r.opts.RunnerConfig.Worker.MaxRetryAfter)
	writer.SetRetryAfterRecorder(runner.RetryAfterDeadline())
	return writer
}

// 通常送信のRetry-Afterは、過負荷でspoolへの保存をdropした場合も出力先全体の期限として記録・保存する。
// 再送中のrunnerも再起動後のrunnerも、spoolに残る古いログを期限まで送らない。
func TestNormalSendRetryAfterIsRecordedWhenOverloadDropsTheRecord(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "running runner"
		if restart {
			name = "after restart"
		}
		t.Run(name, func(t *testing.T) {
			synctestFromWallClockForTest(t, func(t *testing.T) {
				sender := &scriptedRetryAfterSender{}
				runtime := newRetryAfterRestartRuntime(t, resolvedTempDirForTest(t), "loki", 0, sender)
				runner := runtime.start()
				require.Equal(t, worker.ProcessStatusNoTarget, runRetryAfterRestartCycle(t, runner).Status)
				runtime.store(restartTestRecord("older"))

				failedAt := time.Now()
				err := runtime.overloadedWriter(runner).HandleFailure(context.Background(), loggerfailure.SendResult{
					Meta: loggerfailure.Meta{Backend: "loki", RequestID: "dropped", AttemptedAt: failedAt, InputCount: 1},
					RetryableRecords: []loggerfailure.FailedRecord{{
						FailedAt: failedAt, RetryAfter: 30 * time.Second, Payload: map[string]any{"id": "dropped"},
					}},
				})
				require.ErrorIs(t, err, pkglogger.ErrLogDropped)
				require.Len(t, runtime.stateRecords(spool.StateActive), 1, "the overloaded spool keeps only the older record")
				require.FileExists(t, runtime.markerPath())
				notBefore := failedAt.Add(30 * time.Second)
				if restart {
					runner = runtime.start()
				}

				deferred := runRetryAfterRestartCycle(t, runner)
				require.Equal(t, worker.ProcessSkipReasonRetryAfter, deferred.SkipReason)
				require.True(t, deferred.NotBefore.Equal(notBefore), "NotBefore = %s, want %s", deferred.NotBefore, notBefore)
				require.Empty(t, sender.requests)

				time.Sleep(time.Until(notBefore))
				require.Equal(t, worker.ProcessStatusCompleted, runRetryAfterRestartCycle(t, runner).Status)
				require.Len(t, sender.requests, 1)
				require.Equal(t, []string{"older"}, sender.requests[0].ids)
				require.False(t, sender.requests[0].at.Before(notBefore))
			})
		})
	}
}
