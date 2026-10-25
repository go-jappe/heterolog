package compressedlog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
)

func TestRunnerConfigAdapterPreservesIntervalSnapshot(t *testing.T) {
	for _, snapshot := range []RunnerConfig{
		{Interval: 0, PurgeInterval: 17 * time.Millisecond},
		{Interval: -time.Second, PurgeInterval: 0},
		{Interval: 17 * time.Millisecond, PurgeInterval: -time.Second},
	} {
		t.Run(snapshot.Interval.String(), func(t *testing.T) {
			config := snapshot
			adapter := NewRunnerConfigAdapter(config)
			config.Interval = time.Hour
			config.PurgeInterval = 2 * time.Hour
			if got := adapter.Interval(); got != snapshot.Interval {
				t.Fatalf("Interval() = %s, want unchanged snapshot %s", got, snapshot.Interval)
			}
			if got := adapter.PurgeInterval(); got != snapshot.PurgeInterval {
				t.Fatalf("PurgeInterval() = %s, want unchanged snapshot %s", got, snapshot.PurgeInterval)
			}
		})
	}
}

type runnerIntervalProviderFunc func() time.Duration

func (p runnerIntervalProviderFunc) Interval() time.Duration { return p() }

func (runnerIntervalProviderFunc) PurgeInterval() time.Duration { return 0 }

func TestRunnerValidatesConfigProvider(t *testing.T) {
	worker := newReplayTestWorker(t, replaytest.NewStore(t), nil)
	for _, test := range []struct {
		name     string
		provider RunnerConfigProvider
		wantErr  error
	}{
		{name: "nil", wantErr: ErrNilRunnerConfig},
		{
			name:     "zero adapter interval",
			provider: NewRunnerConfigAdapter(RunnerConfig{}),
			wantErr:  ErrInvalidRunnerInterval,
		},
		{
			name:     "negative adapter interval",
			provider: NewRunnerConfigAdapter(RunnerConfig{Interval: -time.Second}),
			wantErr:  ErrInvalidRunnerInterval,
		},
		{
			name:     "zero custom interval",
			provider: runnerIntervalProviderFunc(func() time.Duration { return 0 }),
			wantErr:  ErrInvalidRunnerInterval,
		},
		{
			name:     "negative custom interval",
			provider: runnerIntervalProviderFunc(func() time.Duration { return -time.Second }),
			wantErr:  ErrInvalidRunnerInterval,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner, err := NewRunner(worker, test.provider, nil)
			if runner != nil {
				t.Fatal("NewRunner() returned a runner for invalid configuration")
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("NewRunner() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestRunnerUsesCustomConfigProviderForTimer(t *testing.T) {
	worker := newReplayTestWorker(t, replaytest.NewStore(t), nil)
	var calls atomic.Int64
	provider := runnerIntervalProviderFunc(func() time.Duration {
		calls.Add(1)
		return time.Millisecond
	})
	reports := make(chan error, 1)
	ctx, cancel := runContextStoppedByReporter()
	defer cancel()
	runner, err := NewRunner(worker, provider, runnerReporterFunc(
		func(_ context.Context, _ RunReport, err error) {
			reports <- err
			cancel()
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	// 構築時にproviderの周期を読んで検証し、Runでも読み直して周期処理に使います。
	constructionCalls := calls.Load()
	if constructionCalls == 0 {
		t.Fatal("NewRunner did not read the provider interval")
	}
	runErr := runner.Run(ctx)
	if errors.Is(context.Cause(ctx), errReporterDidNotStopRun) {
		t.Fatalf("custom provider interval did not trigger a periodic run within %s: Run() error = %v", hangDetectionTimeout, runErr)
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled from the reporter", runErr)
	}
	select {
	case err := <-reports:
		if err != nil {
			t.Fatalf("periodic run error = %v", err)
		}
	default:
		t.Fatal("custom provider interval did not trigger a periodic run")
	}
	if got := calls.Load(); got <= constructionCalls {
		t.Fatalf("getter calls = %d after Run, want Run to read the provider again after the %d construction read(s)", got, constructionCalls)
	}
	if runner.IsRunning() {
		t.Fatal("Run() returned without clearing its running state")
	}
}
