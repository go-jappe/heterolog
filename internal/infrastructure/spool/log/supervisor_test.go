package logspool

import (
	"context"
	"errors"
	"fmt"
	lifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRunnerSupervisorKeepsDependencies(t *testing.T) {
	t.Parallel()

	runner := newNoTargetRunner(t, runnerConfigStub{interval: time.Millisecond})
	cfg := supervisorConfigStub{restartDelay: time.Millisecond, maxRestarts: 2}

	supervisor := NewRunnerSupervisor(runner, cfg, nil, nil)

	assert.Same(t, runner, supervisor.Runner)
	assert.Equal(t, cfg, supervisor.Config)
	assert.False(t, supervisor.IsRunning())
}

func TestRunnerSupervisorRunValidatesDependenciesAndConfig(t *testing.T) {
	t.Parallel()

	validRunner := newNoTargetRunner(t, runnerConfigStub{interval: time.Millisecond})

	tests := []struct {
		name       string
		supervisor *RunnerSupervisor
		wantErr    error
	}{
		{
			name:    "nil supervisor",
			wantErr: ErrNilRunnerSupervisor,
		},
		{
			name:       "nil runner",
			supervisor: &RunnerSupervisor{Config: supervisorConfigStub{}},
			wantErr:    ErrNilRunner,
		},
		{
			name: "invalid runner interval",
			supervisor: &RunnerSupervisor{
				Runner: newNoTargetRunner(t, runnerConfigStub{interval: -time.Millisecond}),
				Config: supervisorConfigStub{},
			},
			wantErr: ErrInvalidRunnerInterval,
		},
		{
			name: "zero restart delay",
			supervisor: &RunnerSupervisor{
				Runner: validRunner,
				Config: supervisorConfigStub{},
			},
			wantErr: ErrInvalidSupervisorRestartDelay,
		},
		{
			name: "negative restart delay",
			supervisor: &RunnerSupervisor{
				Runner: validRunner,
				Config: supervisorConfigStub{restartDelay: -time.Millisecond},
			},
			wantErr: ErrInvalidSupervisorRestartDelay,
		},
		{
			name: "negative max restarts",
			supervisor: &RunnerSupervisor{
				Runner: validRunner,
				Config: supervisorConfigStub{
					restartDelay: time.Millisecond,
					maxRestarts:  -1,
				},
			},
			wantErr: ErrInvalidSupervisorMaxRestarts,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.supervisor.Run(context.Background())

			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestRunnerSupervisorValidateAllowsDocumentedUnlimitedRestarts(t *testing.T) {
	t.Parallel()

	supervisor := &RunnerSupervisor{
		Runner: newNoTargetRunner(t, runnerConfigStub{interval: time.Millisecond}),
		Config: supervisorConfigStub{
			restartDelay: time.Millisecond,
			maxRestarts:  0,
		},
	}

	require.NoError(t, supervisor.validate())
}

func TestRunnerSupervisorReportSpoolRunnerRunStoresSnapshotAndForwards(t *testing.T) {
	t.Parallel()

	forwarded := &recordingRunnerReporter{}
	supervisor := &RunnerSupervisor{Reporter: forwarded}
	runErr := errors.New("run failed")
	report := RunReport{
		ProcessReport: worker.ProcessReport{
			Status: worker.ProcessStatusFailed,
			Err:    runErr,
		},
	}

	supervisor.ReportSpoolRunnerRun(context.Background(), report, runErr)

	snapshot := supervisor.Snapshot()
	assert.Equal(t, report, snapshot.LastRunReport)
	assert.ErrorIs(t, snapshot.LastRunErr, runErr)
	// LastRunErrorは表示用の文字列です。通知したerrorの文言をそのまま保持することを確認します。
	assert.Equal(t, runErr.Error(), snapshot.LastRunError, "the display text mirrors the reported error")
	assert.False(t, snapshot.LastRunReportedAt.IsZero())
	require.Len(t, forwarded.calls, 1)
	assert.Equal(t, report, forwarded.calls[0].report)
	assert.ErrorIs(t, forwarded.calls[0].err, runErr)
	// purgeを実行していない周期では、最後のpurgeの時刻は変わりません。
	assert.True(t, snapshot.LastPurgeAt.IsZero(), "no purge ran")

	purged := RunReport{LifecycleReport: lifecyclereport.RunReport{
		DeadPurgeReport: lifecyclereport.PurgeReport{ByAge: lifecyclereport.PurgeStepReport{Executed: true}},
	}}
	supervisor.ReportSpoolRunnerRun(context.Background(), purged, nil)
	purgedAt := supervisor.Snapshot().LastPurgeAt
	assert.False(t, purgedAt.IsZero(), "a purge ran")
	supervisor.ReportSpoolRunnerRun(context.Background(), report, nil)
	assert.Equal(t, purgedAt, supervisor.Snapshot().LastPurgeAt, "a cycle without a purge keeps the time")
}

func TestRunnerSupervisorRunStopsOnCanceledContextBeforeRunnerStart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	supervisor := &RunnerSupervisor{
		Runner: newNoTargetRunner(t, runnerConfigStub{interval: time.Millisecond}),
		Config: supervisorConfigStub{restartDelay: time.Millisecond},
	}

	err := supervisor.Run(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, supervisor.IsRunning())
	snapshot := supervisor.Snapshot()
	assert.Equal(t, RunnerSupervisorStateStopped, snapshot.State)
	assert.ErrorIs(t, snapshot.LastRunnerStopErr, context.Canceled)
	assert.False(t, snapshot.StartedAt.IsZero())
	assert.True(t, snapshot.LastRunnerStartedAt.IsZero())
}

func TestRunnerSupervisorRunStopsOnNonRestartableRunnerError(t *testing.T) {
	t.Parallel()

	sendErr := errors.New("send failed")
	runner := newSendErrorRunner(t, sendErr, runnerConfigStub{
		interval:    time.Millisecond,
		stopOnError: true,
	})
	supervisor := &RunnerSupervisor{
		Runner: runner,
		Config: supervisorConfigStub{restartDelay: time.Nanosecond, maxRestarts: 1},
	}

	err := supervisor.Run(context.Background())

	require.ErrorIs(t, err, ErrRunnerStoppedOnError)
	assert.ErrorIs(t, err, sendErr)
	assert.False(t, supervisor.IsRunning())
	snapshot := supervisor.Snapshot()
	assert.Equal(t, RunnerSupervisorStateStopped, snapshot.State)
	assert.Zero(t, snapshot.RestartCount)
	assert.ErrorIs(t, snapshot.LastRunnerStopErr, ErrRunnerStoppedOnError)
	assert.ErrorIs(t, snapshot.LastRunnerStopErr, sendErr)
}

func TestRunnerSupervisorRunRestartsUntilLimitExceededAfterRunnerPanic(t *testing.T) {
	t.Parallel()

	for _, maxRestarts := range []int{1, 2} {
		t.Run(fmt.Sprintf("max_restarts_%d", maxRestarts), func(t *testing.T) {
			t.Parallel()

			previousReporter := &recordingRunnerReporter{}
			errorReporter := &recordingRunnerReporter{}
			runner := newTestRunner(t, panicWorker(t),
				newTestLifecycleManager(&spoolLifecycleStoreStub{}),
				runnerConfigStub{interval: time.Nanosecond},
			)
			runner.Reporter = previousReporter
			supervisor := &RunnerSupervisor{
				Runner:   runner,
				Reporter: errorReporter,
				Config: supervisorConfigStub{
					restartDelay: time.Nanosecond,
					maxRestarts:  maxRestarts,
				},
			}

			err := supervisor.Run(context.Background())

			require.ErrorIs(t, err, ErrSupervisorRestartLimitExceeded)
			// 初回実行に加えて、許可された回数だけ再起動します。
			require.Len(t, errorReporter.calls, maxRestarts+1)
			for _, call := range errorReporter.calls {
				var panicErr *RunnerPanicError
				require.ErrorAs(t, call.err, &panicErr)
				require.ErrorIs(t, call.err, ErrRunnerPanic)
				require.NotNil(t, panicErr.Value, "keep the recovered panic value")
			}
			// 上限超過のerrorは、実際の再起動回数と最後の異常停止の原因を保持します。
			var limitErr *RestartLimitExceededError
			require.ErrorAs(t, err, &limitErr)
			assert.Equal(t, maxRestarts, limitErr.RestartCount)
			assert.ErrorIs(t, limitErr.LastErr, ErrRunnerPanic)
			assert.ErrorIs(t, err, ErrRunnerPanic)
			assert.Same(t, previousReporter, runner.Reporter)
			assert.False(t, supervisor.IsRunning())
			snapshot := supervisor.Snapshot()
			assert.Equal(t, RunnerSupervisorStateStopped, snapshot.State)
			// 上限超過で停止した最後の異常終了は、再起動回数に含めません。
			assert.Equal(t, maxRestarts, snapshot.RestartCount)
			assert.ErrorIs(t, snapshot.LastRunnerStopErr, ErrSupervisorRestartLimitExceeded)
			assert.False(t, snapshot.LastRunnerStartedAt.IsZero())
			// 最後の再起動は最後のRunner開始と同じ時点です。上限超過時には更新しません。
			assert.True(t, snapshot.LastRestartedAt.Equal(snapshot.LastRunnerStartedAt))
			assert.True(t, snapshot.LastRestartedAt.Before(snapshot.LastRunnerStoppedAt) ||
				snapshot.LastRestartedAt.Equal(snapshot.LastRunnerStoppedAt))
		})
	}
}

func TestRunnerSupervisorDoesNotCountRestartStoppedDuringDelay(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstFailure := make(chan struct{})
	var reported atomic.Int32
	runner := newTestRunner(t, panicWorker(t),
		newTestLifecycleManager(&spoolLifecycleStoreStub{}),
		runnerConfigStub{interval: time.Nanosecond},
	)
	supervisor := &RunnerSupervisor{
		Runner: runner,
		Reporter: &callbackRunnerReporter{report: func(context.Context, RunReport, error) {
			if reported.Add(1) == 1 {
				close(firstFailure)
			}
		}},
		Config: supervisorConfigStub{
			restartDelay: time.Hour,
			maxRestarts:  1,
		},
	}
	stopped := make(chan error, 1)
	go func() { stopped <- supervisor.Run(ctx) }()

	select {
	case <-firstFailure:
	case <-time.After(hangDetectionTimeout):
		t.Fatal("runner did not report its first failure")
	}
	cancel()
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(hangDetectionTimeout):
		t.Fatal("supervisor did not stop during its restart delay")
	}

	// 待機中に停止したため再起動しておらず、回数・再起動時刻は更新しません。
	assert.Equal(t, int32(1), reported.Load())
	snapshot := supervisor.Snapshot()
	assert.Equal(t, RunnerSupervisorStateStopped, snapshot.State)
	assert.Zero(t, snapshot.RestartCount)
	assert.True(t, snapshot.LastRestartedAt.IsZero())
	assert.ErrorIs(t, snapshot.LastRunnerStopErr, context.Canceled)
}

func TestRunnerSupervisorSnapshotNilReceiver(t *testing.T) {
	t.Parallel()

	var supervisor *RunnerSupervisor

	assert.Equal(t, RunnerSupervisorStateStopped, supervisor.Snapshot().State)
	assert.False(t, supervisor.IsRunning())
}

func TestRunnerSupervisorContainsReporterPanicWhileReportingRunnerFailure(t *testing.T) {
	t.Parallel()
	supervisor := NewRunnerSupervisor(
		newNoTargetRunner(t, runnerConfigStub{interval: time.Nanosecond}),
		supervisorConfigStub{restartDelay: time.Nanosecond, maxRestarts: 1}, nil, nil,
	)
	supervisor.Reporter = &callbackRunnerReporter{report: func(context.Context, RunReport, error) { panic("report failure") }}
	err := supervisor.Run(context.Background())
	require.ErrorIs(t, err, ErrSupervisorRestartLimitExceeded)
	// Reporterのpanicで中断した周期も、回復したpanicとしてSnapshotへ残します。
	var panicErr *RunnerPanicError
	require.ErrorAs(t, supervisor.Snapshot().LastRunErr, &panicErr)
	require.Equal(t, "report failure", panicErr.Value)
}

func TestRunnerSupervisorConfigAdapter(t *testing.T) {
	t.Parallel()

	adapter := NewConfigAdapter(pkgspoolsupervisor.RunnerSupervisorConfig{
		RestartDelay: 25 * time.Millisecond,
		MaxRestarts:  3,
	})

	assert.Equal(t, 25*time.Millisecond, adapter.RestartDelay())
	assert.Equal(t, 3, adapter.MaxRestarts())
}

func newNoTargetRunner(t testing.TB, cfg RunnerConfig) *Runner {
	t.Helper()
	return newTestRunner(t,
		newTestWorker(
			&spoolWorkerStoreStub{},
			&spoolSenderStub{},
		),
		newTestLifecycleManager(&spoolLifecycleStoreStub{}),
		cfg,
	)
}

func newSendErrorRunner(t testing.TB, sendErr error, cfg RunnerConfig) *Runner {
	t.Helper()
	path := "processing/send-fails"
	records := []pkgspool.SpoolRecord{spoolRecord("record-1")}
	return newTestRunner(t,
		newTestWorker(
			&spoolWorkerStoreStub{
				processingPaths: []string{path},
				readRecords: map[string][]pkgspool.SpoolRecord{
					path: records,
				},
			},
			&spoolSenderStub{
				report: newSpoolExportReport(
					pkgspool.RecordOutcomeStatusRetryable,
					len(records),
					time.Time{},
				),
				err: sendErr,
			},
		),
		newTestLifecycleManager(&spoolLifecycleStoreStub{}),
		cfg,
	)
}
