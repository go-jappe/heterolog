package logspool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/go-jappe/heterolog/pkg/spool/spoolrunner"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRunnerKeepsDependencies(t *testing.T) {
	t.Parallel()

	worker := &worker.Worker{}
	lifecycle := newTestLifecycleManager(&spoolLifecycleStoreStub{})
	cfg := runnerConfigStub{interval: 10 * time.Millisecond, stopOnError: true}

	runner, err := NewRunner(worker, lifecycle, cfg, nil, nil)

	require.NoError(t, err)
	require.NotNil(t, runner)
	assert.Same(t, worker, runner.worker)
	assert.Same(t, lifecycle, runner.lifecycle)
	assert.Equal(t, cfg, runner.Config)
	assert.False(t, runner.IsRunning())
}

func TestNewRunnerRejectsMissingCoreDependencies(t *testing.T) {
	t.Parallel()

	validWorker := &worker.Worker{}
	validLifecycle := newTestLifecycleManager(&spoolLifecycleStoreStub{})

	tests := []struct {
		name      string
		worker    *worker.Worker
		lifecycle *lifecycle.Manager
		wantErr   error
	}{
		{
			name:      "nil worker",
			lifecycle: validLifecycle,
			wantErr:   ErrNilWorker,
		},
		{
			name:    "nil lifecycle",
			worker:  validWorker,
			wantErr: ErrNilLifecycle,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner, err := NewRunner(tt.worker, tt.lifecycle, nil, nil, nil)

			require.ErrorIs(t, err, tt.wantErr)
			assert.Nil(t, runner)
		})
	}
}

func TestRunnerRunRejectsNilReceiver(t *testing.T) {
	t.Parallel()

	var runner *Runner

	require.ErrorIs(t, runner.Run(context.Background()), ErrNilRunner)
}

func TestRunnerRunOnceRejectsNilReceiver(t *testing.T) {
	t.Parallel()

	var runner *Runner

	report, err := runner.RunOnce(context.Background())

	require.ErrorIs(t, err, ErrNilRunner)
	assert.Equal(t, RunReport{}, report)
}

func TestRunnerRunOnceHandlesFileTransitionRetryBeforeNormalProcess(t *testing.T) {
	t.Parallel()

	lifecycleStore := &spoolLifecycleStoreStub{
		doneReport: lifecycleReportWithDeletedFiles(1),
	}
	workerStore := &spoolWorkerStoreStub{
		exists: map[string]bool{"processing/file-transition": true},
	}
	retry, err := worker.NewProcessingFileTransitionRetry(worker.NewProcessingFileTransitionRetryConfigAdapter(worker.ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Nanosecond,
		MaxEntries: 1,
	}))
	require.NoError(t, err)
	retry.Upsert(
		"processing/file-transition",
		worker.ProcessingFileTransitionRetryOpMoveProcessingToDone,
		errors.New("previous move failed"),
		time.Now().Add(-time.Second),
	)
	sender := &spoolSenderStub{}
	runner := newTestRunner(t,
		&worker.Worker{
			Store:                         workerStore,
			Sender:                        sender,
			ProcessingFileTransitionRetry: retry,
		},
		newTestLifecycleManager(lifecycleStore),
		nil,
	)

	report, err := runner.RunOnce(context.Background())

	require.NoError(t, err)
	assert.True(t, report.ProcessingFileTransitionRetryHandled)
	assert.Equal(t, worker.ProcessingFileTransitionRetryStatusSucceeded, report.ProcessingFileTransitionRetryReport.Status)
	assert.Equal(t, "processing/file-transition", report.ProcessingFileTransitionRetryReport.ProcessingPath)
	assert.Equal(t, worker.ProcessingFileTransitionRetryOpMoveProcessingToDone, report.ProcessingFileTransitionRetryReport.Operation)
	assert.Equal(t, []string{"purge_done", "purge_dead", "purge_quarantine"}, lifecycleStore.calls)
	assert.Equal(t, []string{"exists:processing/file-transition", "move_done:processing/file-transition"}, workerStore.calls)
	assert.Zero(t, sender.calls)
	assert.False(t, retry.Contains("processing/file-transition"))
}

func TestRunnerRunOnceJoinsLifecycleAndProcessErrors(t *testing.T) {
	t.Parallel()

	lifecycleErr := errors.New("purge failed")
	sendErr := errors.New("send failed")
	path := "processing/send-fails"
	records := []pkgspool.SpoolRecord{spoolRecord("record-1")}
	workerStore := &spoolWorkerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]pkgspool.SpoolRecord{path: records},
	}
	sender := &spoolSenderStub{
		report: newSpoolExportReport(
			pkgspool.RecordOutcomeStatusRetryable,
			len(records),
			time.Time{},
		),
		err: sendErr,
	}
	runner := newTestRunner(t,
		newTestWorker(workerStore, sender),
		newTestLifecycleManager(&spoolLifecycleStoreStub{doneErr: lifecycleErr}),
		nil,
	)

	report, err := runner.RunOnce(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, lifecycleErr)
	assert.ErrorIs(t, err, sendErr)
	assert.ErrorIs(t, err, ErrRunLifecycle)
	assert.ErrorIs(t, err, ErrRunSpoolFileProcess)
	assert.Equal(t, worker.ProcessStatusCompleted, report.ProcessReport.Status)
	assert.Equal(t, path, report.ProcessReport.ProcessingPath)
	assert.Equal(t, len(records), report.ProcessReport.RetryRecordCount)
	assert.Contains(t, workerStore.calls, "remove:"+path)
	assert.Equal(t, 1, sender.calls)
}

func TestRunnerRunOnceStopsOnLifecycleContextError(t *testing.T) {
	t.Parallel()

	workerStore := &spoolWorkerStoreStub{}
	sender := &spoolSenderStub{}
	runner := newTestRunner(t,
		newTestWorker(workerStore, sender),
		newTestLifecycleManager(&spoolLifecycleStoreStub{doneErr: context.Canceled}),
		nil,
	)

	report, err := runner.RunOnce(context.Background())

	require.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, ErrRunLifecycle)
	assert.Equal(t, RunReport{}, report)
	assert.Empty(t, workerStore.calls)
	assert.Zero(t, sender.calls)
}

func TestRunnerRunReportsAndStopsOnErrorWhenConfigured(t *testing.T) {
	t.Parallel()

	sendErr := errors.New("send failed")
	path := "processing/send-fails"
	records := []pkgspool.SpoolRecord{spoolRecord("record-1")}
	workerStore := &spoolWorkerStoreStub{
		processingPaths: []string{path},
		readRecords:     map[string][]pkgspool.SpoolRecord{path: records},
	}
	reporter := &recordingRunnerReporter{}
	sender := &spoolSenderStub{
		report: newSpoolExportReport(
			pkgspool.RecordOutcomeStatusRetryable,
			len(records),
			time.Time{},
		),
		err: sendErr,
	}
	runner := newTestRunner(t,
		newTestWorker(workerStore, sender),
		newTestLifecycleManager(&spoolLifecycleStoreStub{}),
		runnerConfigStub{interval: time.Millisecond, stopOnError: true},
	)
	runner.Reporter = reporter

	err := runner.Run(context.Background())

	require.ErrorIs(t, err, ErrRunnerStoppedOnError)
	assert.ErrorIs(t, err, sendErr)
	require.Len(t, reporter.calls, 1)
	assert.ErrorIs(t, reporter.calls[0].err, sendErr)
	assert.Equal(t, worker.ProcessStatusCompleted, reporter.calls[0].report.ProcessReport.Status)
	assert.Equal(t, len(records), reporter.calls[0].report.ProcessReport.RetryRecordCount)
	assert.Contains(t, workerStore.calls, "remove:"+path)
	assert.Equal(t, 1, sender.calls)
	assert.False(t, runner.IsRunning())
}

func TestRunnerRunRejectsNonPositiveInterval(t *testing.T) {
	t.Parallel()

	for _, interval := range []time.Duration{0, -time.Millisecond} {
		interval := interval
		t.Run(interval.String(), func(t *testing.T) {
			t.Parallel()
			runner := newTestRunner(t,
				&worker.Worker{},
				newTestLifecycleManager(&spoolLifecycleStoreStub{}),
				runnerConfigStub{interval: interval},
			)

			err := runner.Run(context.Background())

			require.ErrorIs(t, err, ErrInvalidRunnerInterval)
		})
	}
}

func TestRunnerConfigAdapter(t *testing.T) {
	t.Parallel()

	adapter := NewRunnerConfigAdapter(spoolrunner.RunnerConfig{
		Interval:    25 * time.Millisecond,
		StopOnError: true,
	})

	assert.Equal(t, 25*time.Millisecond, adapter.Interval())
	assert.True(t, adapter.StopOnError())
}

func TestWaitContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, waitContext(ctx, time.Minute), context.Canceled)
	require.ErrorIs(t, waitContext(ctx, 0), ErrInvalidRunnerInterval)
	require.NoError(t, waitContext(context.Background(), time.Nanosecond))
}
