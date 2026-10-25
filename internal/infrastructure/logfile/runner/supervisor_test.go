package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	loglifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"

	"github.com/stretchr/testify/require"
)

var errTestSupervisorHandlerPanic = errors.New("supervisor handler panic")

func TestSupervisorRejectsNilConfig(t *testing.T) {
	runner := NewRunner(&loglifecycle.DirectoryLifecycle{}, nil, nil)

	err := newSupervisor(runner, nil).Run(context.Background())

	require.ErrorIs(t, err, ErrNilSupervisorConfig)
}

func TestSupervisorRejectsNonPositiveRestartSettings(t *testing.T) {
	tests := []struct {
		name    string
		config  StaticSupervisorConfig
		wantErr error
	}{
		{
			name:    "zero restart delay",
			config:  StaticSupervisorConfig{MaxRestartsValue: 1},
			wantErr: ErrInvalidSupervisorRestartDelay,
		},
		{
			name: "zero max restarts",
			config: StaticSupervisorConfig{
				RestartDelayValue: time.Millisecond,
			},
			wantErr: ErrInvalidSupervisorMaxRestarts,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			runner := NewRunner(&loglifecycle.DirectoryLifecycle{}, StaticRunnerConfig{
				IntervalValue: time.Minute,
			}, nil)
			err := newSupervisor(runner, testCase.config).Run(context.Background())
			require.ErrorIs(t, err, testCase.wantErr)
		})
	}
}

func TestSupervisorRestartsRunnerStoppedOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var calls atomic.Int32
	restarted := make(chan struct{})
	runner := newFailingArchiveRunner(t, true, lifecycleRunReporterFunc(func(
		context.Context,
		loglifecycle.RunReport,
		error,
	) {
		if calls.Add(1) == 2 {
			close(restarted)
		}
	}))
	WithSupervisor(StaticSupervisorConfig{
		RestartDelayValue: time.Millisecond,
		MaxRestartsValue:  1000,
	})(runner)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case <-restarted:
		cancel()
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("supervisor did not restart the stopped runner")
	}

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("supervisor did not stop after context cancellation")
	}
	require.GreaterOrEqual(t, calls.Load(), int32(2))
}

func TestSupervisorStopsAfterConfiguredMaximumRestarts(t *testing.T) {
	var calls atomic.Int32
	runner := newRegistrationFailingArchiveRunner(t, lifecycleRunReporterFunc(func(
		context.Context,
		loglifecycle.RunReport,
		error,
	) {
		calls.Add(1)
	}), errTestArchiveUpload)
	WithSupervisor(StaticSupervisorConfig{
		RestartDelayValue: time.Millisecond,
		MaxRestartsValue:  2,
	})(runner)

	err := runner.Run(context.Background())

	require.ErrorIs(t, err, ErrSupervisorRestartLimitExceeded)
	require.ErrorIs(t, err, ErrRunnerStoppedOnError)
	require.ErrorIs(t, err, errTestArchiveUpload)
	var limit *SupervisorRestartLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, 2, limit.Restarts)
	// 初回実行に加えて、許可された2回の再起動を実行する。
	require.Equal(t, int32(3), calls.Load())
}

func TestSupervisorRestartsAfterNestedContextErrorWhileParentContextIsAlive(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		operationErr error
	}{
		{name: "durable save deadline", operationErr: errors.Join(errors.New("durable save exceeded its budget"), context.DeadlineExceeded)},
		{name: "remote deadline", operationErr: context.DeadlineExceeded},
		{name: "remote cancellation", operationErr: context.Canceled},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), boundedwait.HangTimeout)
			t.Cleanup(cancel)
			var calls atomic.Int32
			runner := newRegistrationFailingArchiveRunner(t, lifecycleRunReporterFunc(func(context.Context, loglifecycle.RunReport, error) {
				calls.Add(1)
			}), errors.Join(errTestArchiveUpload, testCase.operationErr))
			WithSupervisor(StaticSupervisorConfig{
				RestartDelayValue: time.Millisecond,
				MaxRestartsValue:  1,
			})(runner)

			err := runner.Run(ctx)

			// StopOnErrorによる終了は通常どおり再起動します。checkpoint登録内のcontext errorを
			// Runner自身の終了要求と誤認し、Supervisorまで停止してはいけません。
			require.NoError(t, ctx.Err())
			require.ErrorIs(t, err, ErrSupervisorRestartLimitExceeded)
			require.ErrorIs(t, err, ErrRunnerStoppedOnError)
			require.ErrorIs(t, err, errTestArchiveUpload)
			require.ErrorIs(t, err, testCase.operationErr)
			require.Equal(t, int32(2), calls.Load())
		})
	}
}

func TestSupervisorRecoversRunnerPanicAndRestarts(t *testing.T) {
	var calls atomic.Int32
	runner := newFailingArchiveRunner(t, true, lifecycleRunReporterFunc(func(
		context.Context,
		loglifecycle.RunReport,
		error,
	) {
		calls.Add(1)
		panic(errTestSupervisorHandlerPanic)
	}))
	WithSupervisor(StaticSupervisorConfig{
		RestartDelayValue: time.Millisecond,
		MaxRestartsValue:  1,
	})(runner)

	err := runner.Run(context.Background())

	require.ErrorIs(t, err, ErrSupervisorRestartLimitExceeded)
	require.ErrorIs(t, err, ErrRunnerPanicked)
	require.ErrorIs(t, err, errTestSupervisorHandlerPanic)
	var panicErr *RunnerPanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, errTestSupervisorHandlerPanic, panicErr.Value)
	require.NotEmpty(t, panicErr.Stack)
	var limit *SupervisorRestartLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, 1, limit.Restarts)
	require.Equal(t, int32(2), calls.Load())
}

func TestSupervisorStopsDuringRestartDelayWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var calls atomic.Int32
	firstFailure := make(chan struct{})
	runner := newFailingArchiveRunner(t, true, lifecycleRunReporterFunc(func(
		context.Context,
		loglifecycle.RunReport,
		error,
	) {
		if calls.Add(1) == 1 {
			close(firstFailure)
		}
	}))
	WithSupervisor(StaticSupervisorConfig{
		RestartDelayValue: time.Hour,
		MaxRestartsValue:  1,
	})(runner)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case <-firstFailure:
		cancel()
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("runner did not report its first failure")
	}

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("supervisor did not interrupt its restart delay")
	}
	require.Equal(t, int32(1), calls.Load())
}
