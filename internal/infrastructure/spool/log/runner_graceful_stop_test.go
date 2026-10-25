package logspool

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	"github.com/stretchr/testify/require"
)

func TestRunnerStopWaitsForAcceptedCycleAndDoesNotStartAnother(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan time.Time, 2)
		release := make(chan struct{})
		var cycleContext context.Context
		store := &observedRunnerLifecycleStore{started: started, beforeDone: func(ctx context.Context) error {
			cycleContext = ctx
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}
		reported := 0
		runner := newTestRunner(t,
			newTestWorker(&spoolWorkerStoreStub{}, &spoolSenderStub{}),
			&lifecycle.Manager{Store: store},
			runnerConfigStub{interval: time.Second},
		)
		runner.Reporter = &callbackRunnerReporter{report: func(ctx context.Context, _ RunReport, err error) {
			require.NoError(t, ctx.Err())
			require.NoError(t, err)
			reported++
		}}
		stopped := make(chan error, 1)
		go func() { stopped <- runner.Run(ctx) }()
		<-started
		synctest.Wait()
		cancel()
		synctest.Wait()
		require.NoError(t, cycleContext.Err())
		select {
		case err := <-stopped:
			t.Fatalf("Run stopped before accepted cycle: %v", err)
		default:
		}
		close(release)
		require.ErrorIs(t, <-stopped, context.Canceled)
		require.Equal(t, 1, reported)
		require.Empty(t, started)
	})
}

func TestRunnerStopWaitsForAcceptedReporterWithLiveContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stopContext, stop := context.WithCancel(context.Background())
		defer stop()
		entered := make(chan context.Context, 1)
		finishNotification := make(chan struct{})
		runner := newNoTargetRunner(t, runnerConfigStub{interval: time.Second})
		runner.Reporter = &callbackRunnerReporter{report: func(ctx context.Context, _ RunReport, err error) {
			require.NoError(t, err)
			entered <- ctx
			<-finishNotification
		}}
		stopped := make(chan error, 1)
		go func() { stopped <- runner.Run(stopContext) }()
		notificationContext := <-entered
		stop()
		synctest.Wait()
		require.NoError(t, notificationContext.Err())
		require.Empty(t, stopped, "normal stop must wait for its accepted reporter")
		close(finishNotification)
		require.ErrorIs(t, <-stopped, context.Canceled)
		require.NoError(t, notificationContext.Err(), "downstream notifications retain a live context")
	})
}

func TestRunnerSupervisorExplicitForceCancelsAcceptedCycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stopContext, stop := context.WithCancel(context.Background())
		defer stop()
		operationContext, forceCancel := context.WithCancel(context.Background())
		defer forceCancel()
		entered := make(chan context.Context, 1)
		store := &observedRunnerLifecycleStore{started: make(chan time.Time, 1), beforeDone: func(ctx context.Context) error {
			entered <- ctx
			<-ctx.Done()
			return ctx.Err()
		}}
		runner := newTestRunner(t,
			newTestWorker(&spoolWorkerStoreStub{}, &spoolSenderStub{}),
			&lifecycle.Manager{Store: store},
			runnerConfigStub{interval: time.Second},
		)
		supervisor := NewRunnerSupervisor(runner, supervisorConfigStub{restartDelay: time.Second}, nil, nil)
		stopped := make(chan error, 1)
		go func() { stopped <- supervisor.RunUntilStopped(stopContext, operationContext) }()
		acceptedContext := <-entered
		stop()
		synctest.Wait()
		require.NoError(t, acceptedContext.Err())
		require.Empty(t, stopped, "normal stop must preserve accepted work")
		forceCancel()
		require.ErrorIs(t, <-stopped, context.Canceled)
		require.ErrorIs(t, acceptedContext.Err(), context.Canceled)
		require.False(t, supervisor.IsRunning())
		require.Zero(t, supervisor.Snapshot().RestartCount, "force must not restart the accepted cycle")
	})
}
