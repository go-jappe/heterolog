package logspool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunnerRunNilReceiver(t *testing.T) {
	t.Parallel()

	var runner *Runner
	require.ErrorIs(t, runner.Run(context.Background()), ErrNilRunner)
}

func TestRunnerSupervisorReportsWithoutReplacingDirectReporter(t *testing.T) {
	t.Parallel()

	runErr := errors.New("purge failed")
	directReporter := &recordingRunnerReporter{}
	forwarded := &recordingRunnerReporter{}
	runner := newTestRunner(t,
		newTestWorker(&spoolWorkerStoreStub{}, &spoolSenderStub{}),
		newTestLifecycleManager(&spoolLifecycleStoreStub{doneErr: runErr}),
		runnerConfigStub{interval: time.Millisecond, stopOnError: true},
	)
	runner.Reporter = directReporter
	supervisor := NewRunnerSupervisor(runner, supervisorConfigStub{restartDelay: time.Millisecond}, nil, nil)
	supervisor.Reporter = &callbackRunnerReporter{report: func(ctx context.Context, report RunReport, err error) {
		assert.Same(t, directReporter, runner.Reporter, "supervision must not replace the direct reporter")
		assert.Equal(t, report, supervisor.Snapshot().LastRunReport)
		forwarded.ReportSpoolRunnerRun(ctx, report, err)
	}}

	err := supervisor.Run(context.Background())

	require.ErrorIs(t, err, ErrRunnerStoppedOnError)
	require.ErrorIs(t, err, runErr)
	require.Len(t, forwarded.calls, 1)
	assert.ErrorIs(t, forwarded.calls[0].err, runErr)
	assert.Empty(t, directReporter.calls)
	assert.Same(t, directReporter, runner.Reporter)
	snapshot := supervisor.Snapshot()
	assert.Equal(t, forwarded.calls[0].report, snapshot.LastRunReport)
	assert.ErrorIs(t, snapshot.LastRunErr, runErr)
	// LastRunErrorは表示用の文字列です。通知したerrorの文言をそのまま保持することを確認します。
	assert.Equal(t, forwarded.calls[0].err.Error(), snapshot.LastRunError)
	assert.False(t, snapshot.LastRunReportedAt.IsZero())
	assert.Equal(t, RunnerSupervisorStateStopped, snapshot.State)
	assert.Zero(t, snapshot.RestartCount)

	err = runner.Run(context.Background())

	require.ErrorIs(t, err, ErrRunnerStoppedOnError)
	require.Len(t, directReporter.calls, 1)
	assert.ErrorIs(t, directReporter.calls[0].err, runErr)
	assert.Len(t, forwarded.calls, 1, "a later direct run must use its own reporter")
	assert.Equal(t, snapshot, supervisor.Snapshot())
}

func TestRunnerConcurrentStartsPreserveActiveReporter(t *testing.T) {
	for _, supervised := range []bool{false, true} {
		name := "direct"
		if supervised {
			name = "supervised"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			var directCalls, supervisedCalls, rejectedCalls atomic.Int32
			reporter := func(calls *atomic.Int32) *callbackRunnerReporter {
				return &callbackRunnerReporter{report: func(ctx context.Context, _ RunReport, _ error) {
					calls.Add(1)
					entered <- struct{}{}
					<-release
				}}
			}
			directReporter := reporter(&directCalls)
			runner := newNoTargetRunner(t, runnerConfigStub{interval: time.Hour})
			runner.Reporter = directReporter
			primary := NewRunnerSupervisor(runner, supervisorConfigStub{restartDelay: time.Millisecond}, nil, nil)
			primary.Reporter = reporter(&supervisedCalls)
			run := runner.Run
			if supervised {
				run = primary.Run
			}
			done := make(chan error, 1)
			go func() { done <- run(ctx) }()
			select {
			case <-entered:
			case <-time.After(hangDetectionTimeout):
				t.Fatal("first run did not report")
			}

			const competingStarts = 16
			start := make(chan struct{})
			results := make(chan error, competingStarts)
			for index := 0; index < competingStarts; index++ {
				attempt := runner.Run
				if index%2 == 0 {
					competing := NewRunnerSupervisor(runner, supervisorConfigStub{restartDelay: time.Millisecond}, nil, nil)
					competing.Reporter = &callbackRunnerReporter{report: func(context.Context, RunReport, error) {
						rejectedCalls.Add(1)
					}}
					attempt = competing.Run
				}
				go func() {
					<-start
					results <- attempt(ctx)
				}()
			}
			close(start)
			for index := 0; index < competingStarts; index++ {
				require.ErrorIs(t, waitRunnerResult(t, results), ErrRunnerAlreadyRunning)
			}

			assert.True(t, runner.IsRunning())
			assert.Same(t, directReporter, runner.Reporter)
			cancel()
			close(release)
			require.ErrorIs(t, waitRunnerResult(t, done), context.Canceled)
			assert.Zero(t, rejectedCalls.Load())
			if supervised {
				assert.Equal(t, int32(1), supervisedCalls.Load())
				assert.Zero(t, directCalls.Load())
				assert.False(t, primary.Snapshot().LastRunReportedAt.IsZero())
				assert.Zero(t, primary.Snapshot().RestartCount)
			} else {
				assert.Equal(t, int32(1), directCalls.Load())
				assert.Zero(t, supervisedCalls.Load())
				assert.True(t, primary.Snapshot().LastRunReportedAt.IsZero())
			}
			assert.Same(t, directReporter, runner.Reporter)
		})
	}
}

type callbackRunnerReporter struct {
	report func(context.Context, RunReport, error)
}

func (r *callbackRunnerReporter) ReportSpoolRunnerRun(ctx context.Context, report RunReport, err error) {
	r.report(ctx, report, err)
}
