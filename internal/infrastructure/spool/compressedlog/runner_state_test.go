package compressedlog

import (
	"context"
	"testing"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestRunnerManualCyclesDoNotStartPeriodicLoop(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260922_1.log.gz")
	upload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}})
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Hour}), nil)
	require.NoError(t, err)
	require.False(t, runner.IsRunning())

	report, err := runner.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, report.CompressedLogFiles, 1)
	require.Equal(t, pkgspool.StateDone, report.CompressedLogFiles[0].State)
	require.Equal(t, 1, upload.CallCount())
	require.False(t, runner.IsRunning())
}

func TestRunnerRunValidatesWorkerAndConfig(t *testing.T) {
	worker := newReplayTestWorker(t, replaytest.NewStore(t), nil)
	for _, test := range []struct {
		name    string
		runner  *Runner
		wantErr error
	}{
		{name: "nil runner", wantErr: ErrNilWorker},
		{name: "nil worker", runner: &Runner{}, wantErr: ErrNilWorker},
		{name: "nil config", runner: &Runner{worker: worker}, wantErr: ErrNilRunnerConfig},
		{
			name:    "zero interval",
			runner:  &Runner{worker: worker, config: NewRunnerConfigAdapter(RunnerConfig{})},
			wantErr: ErrInvalidRunnerInterval,
		},
		{
			name:    "negative interval",
			runner:  &Runner{worker: worker, config: NewRunnerConfigAdapter(RunnerConfig{Interval: -time.Second})},
			wantErr: ErrInvalidRunnerInterval,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, test.runner.Run(context.Background()), test.wantErr)
			require.False(t, test.runner.IsRunning())
		})
	}
}

func TestRunnerRunValidatesPurgeIntervalOnlyWithPurger(t *testing.T) {
	stores, store := replaytest.NewDestinationStores(t)
	worker := newReplayTestWorker(t, store, nil)
	purger, err := objectstoragespool.NewTerminalPurger(stores,
		objectstoragespool.NewPurgeConfigAdapter(objectstoragespool.TerminalPurgeConfig{}))
	require.NoError(t, err)
	for _, interval := range []time.Duration{0, -time.Second} {
		t.Run(interval.String(), func(t *testing.T) {
			config := NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond, PurgeInterval: interval})
			invalidRunner, err := NewRunner(worker, config, nil, WithTerminalPurge(purger, nil))
			require.ErrorIs(t, err, ErrInvalidCompressedLogFileUploadSpoolConfig)
			require.Nil(t, invalidRunner)
			// Runも直接構築されたRunnerの不正設定を拒否します。
			invalidRunner = &Runner{worker: worker, config: config, purger: purger}
			require.ErrorIs(t, invalidRunner.Run(context.Background()), ErrInvalidCompressedLogFileUploadSpoolConfig)
			require.False(t, invalidRunner.IsRunning())

			// replayだけを行うrunnerは、任意指定のpurge intervalを使いません。
			runner, err := NewRunner(worker, config, nil)
			require.NoError(t, err)
			cancel, done := startRunnerStateTestRun(t, runner)
			require.Eventually(t, runner.IsRunning, hangDetectionTimeout, time.Millisecond)
			cancel()
			require.ErrorIs(t, waitRunnerOperationForTest(t, done), context.Canceled)
			require.False(t, runner.IsRunning())
		})
	}
}

func TestRunnerRunWithCanceledContextDoesNotProcessFiles(t *testing.T) {
	store := replaytest.NewStore(t)
	file := replaytest.SaveFile(t, store, "app_20260922_2.log.gz")
	upload := &replaytest.Uploader{}
	worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}})
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, runner.Run(ctx), context.Canceled)
	require.False(t, runner.IsRunning())
	require.Zero(t, upload.CallCount())
	require.Equal(t, file.FileName, replaytest.OnlyFile(t, store, pkgspool.StateActive).FileName)
}

func TestRunnerRejectsConcurrentRun(t *testing.T) {
	worker := newReplayTestWorker(t, replaytest.NewStore(t), nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), runnerReporterFunc(
		func(ctx context.Context, _ RunReport, _ error) {
			close(entered)
			<-release
		},
	))
	require.NoError(t, err)
	cancel, done := startRunnerStateTestRun(t, runner)
	select {
	case <-entered:
	case <-time.After(hangDetectionTimeout):
		t.Fatal("runner did not report its first cycle")
	}
	require.True(t, runner.IsRunning())

	ctx, stop := context.WithTimeout(context.Background(), hangDetectionTimeout)
	defer stop()
	require.ErrorIs(t, runner.Run(ctx), ErrRunnerAlreadyRunning)
	require.True(t, runner.IsRunning())
	select {
	case err := <-done:
		t.Fatalf("duplicate Run stopped the original loop: %v", err)
	default:
	}

	cancel()
	close(release)
	require.ErrorIs(t, waitRunnerOperationForTest(t, done), context.Canceled)
	require.False(t, runner.IsRunning())
}

func TestRunnerCanRestartAfterContextCancellation(t *testing.T) {
	worker := newReplayTestWorker(t, replaytest.NewStore(t), nil)
	reports := make(chan error, 1)
	release := make(chan struct{})
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), runnerReporterFunc(
		func(ctx context.Context, _ RunReport, err error) {
			reports <- err
			<-release
		},
	))
	require.NoError(t, err)

	for attempt := range 2 {
		require.False(t, runner.IsRunning(), "before Run %d", attempt)
		cancel, done := startRunnerStateTestRun(t, runner)
		select {
		case err := <-reports:
			require.NoError(t, err)
		case <-time.After(hangDetectionTimeout):
			t.Fatalf("Run %d did not report its first cycle", attempt)
		}
		require.True(t, runner.IsRunning())
		cancel()
		release <- struct{}{}
		require.ErrorIs(t, waitRunnerOperationForTest(t, done), context.Canceled)
		require.False(t, runner.IsRunning(), "after Run %d returned", attempt)
	}

	_, err = runner.RunOnce(context.Background())
	require.NoError(t, err)
	require.False(t, runner.IsRunning())
}

func startRunnerStateTestRun(t *testing.T, runner *Runner) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		waitRunnerOperationForTest(t, done)
	})
	return cancel, done
}
