package logspool

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	spoolpath "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	pkgspoollifecycle "github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

func TestNewBackendRunnerSupervisorRejectsInvalidRunAndRestartSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*BackendRuntimeOptions)
		want error
	}{
		{
			name: "zero runner interval",
			edit: func(opts *BackendRuntimeOptions) {
				opts.RunnerConfig.Runner.Interval = 0
			},
			want: ErrInvalidRunnerInterval,
		},
		{
			name: "negative runner interval",
			edit: func(opts *BackendRuntimeOptions) {
				opts.RunnerConfig.Runner.Interval = -time.Second
			},
			want: ErrInvalidRunnerInterval,
		},
		{
			name: "zero supervisor restart delay",
			edit: func(opts *BackendRuntimeOptions) {
				opts.SupervisorConfig.RestartDelay = 0
			},
			want: ErrInvalidSupervisorRestartDelay,
		},
		{
			name: "negative supervisor restart delay",
			edit: func(opts *BackendRuntimeOptions) {
				opts.SupervisorConfig.RestartDelay = -time.Second
			},
			want: ErrInvalidSupervisorRestartDelay,
		},
		{
			name: "negative supervisor max restarts",
			edit: func(opts *BackendRuntimeOptions) {
				opts.SupervisorConfig.MaxRestarts = -1
			},
			want: ErrInvalidSupervisorMaxRestarts,
		},
		{
			name: "zero processing file transition retry delay",
			edit: func(opts *BackendRuntimeOptions) {
				opts.RunnerConfig.Worker.Finalize.RetryDelay = 0
			},
			want: worker.ErrInvalidProcessingFileTransitionRetryDelay,
		},
		{
			name: "negative processing file transition retry delay",
			edit: func(opts *BackendRuntimeOptions) {
				opts.RunnerConfig.Worker.Finalize.RetryDelay = -time.Second
			},
			want: worker.ErrInvalidProcessingFileTransitionRetryDelay,
		},
		{
			name: "negative max retry-after",
			edit: func(opts *BackendRuntimeOptions) {
				opts.RunnerConfig.Worker.MaxRetryAfter = -time.Second
			},
			want: worker.ErrInvalidMaxRetryAfter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := BackendRuntimeOptions{
				Backend: "opensearch",
				SpoolConfig: pkgdurable.SpoolConfig{
					SpoolPath: t.TempDir(),
					Target:    "runtime-test",
					Dir:       "opensearch",
				},
				Sender: &backendRuntimeSenderStub{},
			}
			opts.RunnerConfig.Worker.Finalize.RetryDelay = time.Second
			opts.RunnerConfig.Worker.Finalize.MaxEntries = 100
			opts.RunnerConfig.LifeCycle.SizePurgeInterval = time.Second
			opts.RunnerConfig.Runner.Interval = time.Second
			opts.SupervisorConfig.RestartDelay = time.Second
			tt.edit(&opts)

			created, err := NewBackendRunnerSupervisor(
				clock.NewUTCClock(),
				opts,
				observer.NopTracer{},
				observer.NopMeter{},
			)
			if created != nil {
				t.Fatal("NewBackendRunnerSupervisor() supervisor != nil")
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewBackendRunnerSupervisor() error = %v, want %v", err, tt.want)
			}
		})
	}
}

type backendRuntimeSenderStub struct {
	received []pkgspool.SpoolRecord
}

func (s *backendRuntimeSenderStub) Send(
	_ context.Context,
	records []pkgspool.SpoolRecord,
) (pkgspool.ExportReport, error) {
	s.received = append(s.received, records...)
	report := make(pkgspool.ExportReport, 0, len(records))
	attemptedAt := time.Now().UTC()
	for inputIndex := range records {
		report = append(report, pkgspool.RecordOutcome{
			InputIndex:  inputIndex,
			Status:      pkgspool.RecordOutcomeStatusSuccess,
			AttemptedAt: attemptedAt,
		})
	}
	return report, nil
}

func TestNewBackendRunnerSupervisorDrainsConfiguredActiveSpool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := clock.NewUTCClock()
	spoolConfig := pkgdurable.SpoolConfig{
		SpoolPath: t.TempDir(),
		Target:    "runtime-test",
		Dir:       "opensearch",
	}
	sender := &backendRuntimeSenderStub{}

	supervisor, err := NewBackendRunnerSupervisor(
		clk,
		BackendRuntimeOptions{
			Backend:     "opensearch",
			SpoolConfig: spoolConfig,
			RunnerConfig: pkgspoolrunner.BackendSpoolRunnerConfig{
				Runner: pkgspoolrunner.RunnerConfig{
					Interval: time.Second,
				},
				LifeCycle: pkgspoollifecycle.LifecycleConfig{
					SizePurgeInterval: time.Second,
				},
				Worker: pkgspoolrunner.WorkerConfig{
					Finalize: pkgspoolrunner.FinalizeConfig{
						RetryDelay: time.Second,
						MaxEntries: 100,
					},
				},
			},
			SupervisorConfig: pkgspoolsupervisor.RunnerSupervisorConfig{
				RestartDelay: time.Second,
			},
			Sender: sender,
		},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewBackendRunnerSupervisor() error = %v", err)
	}

	store, err := fsstorestore.NewSpoolStore(
		clk,
		loggerdurable.NewSpoolConfigAdapter(spoolConfig),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewSpoolStore() error = %v", err)
	}
	record := pkgspool.SpoolRecord{
		OccurredAt: time.Now().UTC(),
		RequestID:  "runtime-request",
		Payload:    []byte(`{"message":"retry me"}`),
	}
	if err := store.StoreByState(ctx, pkgspool.StateActive, []pkgspool.SpoolRecord{record}); err != nil {
		t.Fatalf("StoreByState(active) error = %v", err)
	}

	report, err := supervisor.Runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("Runner.RunOnce() error = %v", err)
	}
	if report.ProcessReport.SuccessRecordCount != 1 {
		t.Fatalf("SuccessRecordCount = %d, want 1", report.ProcessReport.SuccessRecordCount)
	}
	if len(sender.received) != 1 || sender.received[0].RequestID != record.RequestID {
		t.Fatalf("sender received = %#v, want request ID %q", sender.received, record.RequestID)
	}

	root, err := spoolpath.NewRootDir(spoolConfig.SpoolPath, spoolConfig.Target, spoolConfig.Dir)
	if err != nil {
		t.Fatalf("NewRootDir() error = %v", err)
	}
	doneFiles, err := filepath.Glob(filepath.Join(root.DoneDir().Path(), "*", "*", "*", "*", "*", "*"+spoolpath.SpoolExt))
	if err != nil {
		t.Fatalf("Glob(done) error = %v", err)
	}
	if len(doneFiles) != 1 {
		t.Fatalf("done files = %v, want one", doneFiles)
	}
}
