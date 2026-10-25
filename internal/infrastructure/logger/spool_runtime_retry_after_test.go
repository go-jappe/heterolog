package logger

import (
	"context"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstoreretryafter "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	logspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Retry-Afterの待機を上限まで短縮した周期だけ、backend・要求時間・上限を含めてOnWarnへ通知する。
func TestFailureSpoolRunReporterWarnsWhenRetryAfterWaitIsCapped(t *testing.T) {
	t.Parallel()

	dispatcher := notification.NewDispatcher()
	var warnings []*logging.LogEntry
	reporter := &failureSpoolRunLogReporter{
		notifications: dispatcher,
		onWarn: func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			warnings = append(warnings, entry)
		},
		contract: pkglogger.LoggerContractSync,
		backend:  pkglogger.LogOutputLoki,
		clock:    clock.NewUTCClock(),
	}
	notBefore := time.Date(2026, 10, 22, 13, 0, 0, 0, time.UTC)
	reporter.ReportSpoolRunnerRun(context.Background(), logspool.RunReport{ProcessReport: worker.ProcessReport{
		Status:         worker.ProcessStatusSkipped,
		SkipReason:     worker.ProcessSkipReasonRetryAfter,
		ProcessingPath: "processing/far-future",
		NotBefore:      notBefore,
		RetryAfterClamp: &pkgspool.RetryAfterClamp{
			Requested:     240 * time.Hour,
			MaxRetryAfter: time.Hour,
			NotBefore:     notBefore,
			Stored:        true,
		},
	}}, nil)
	// 期限中の周期はclampを持たないため、同じ短縮を繰り返し通知しない。
	reporter.ReportSpoolRunnerRun(context.Background(), logspool.RunReport{ProcessReport: worker.ProcessReport{
		Status:     worker.ProcessStatusSkipped,
		SkipReason: worker.ProcessSkipReasonRetryAfter,
		NotBefore:  notBefore,
	}}, nil)
	dispatcher.CloseAfterDrain()
	<-dispatcher.Done()

	require.Len(t, warnings, 1)
	warning := warnings[0]
	assert.Equal(t, pkglogger.LogLevelWarn, warning.Level)
	assert.Equal(t, "failure spool Retry-After wait exceeds the maximum", warning.Message)
	assert.Equal(t, pkglogger.LogOutputLoki, warning.Fields["backend"])
	assert.Equal(t, "processing/far-future", warning.Fields["processing_path"])
	assert.Equal(t, "240h0m0s", warning.Fields["requested_retry_after"])
	assert.Equal(t, "1h0m0s", warning.Fields["max_retry_after"])
	assert.Equal(t, notBefore, warning.Fields["not_before"])
	assert.Equal(t, true, warning.Fields["stored_not_before"])
}

// 送信先の待機期限を保存したmarkerの失敗は、周期のerrorにせず、操作とpathを含めてOnWarnへ通知する。
func TestFailureSpoolRunReporterWarnsWhenRetryAfterMarkerFails(t *testing.T) {
	t.Parallel()

	dispatcher := notification.NewDispatcher()
	var warnings []*logging.LogEntry
	var errorsReported []error
	reporter := &failureSpoolRunLogReporter{
		notifications: dispatcher,
		onError: func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			errorsReported = append(errorsReported, err)
		},
		onWarn: func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			warnings = append(warnings, entry)
		},
		contract: pkglogger.LoggerContractSync,
		backend:  pkglogger.LogOutputOpenSearch,
		clock:    clock.NewUTCClock(),
	}
	markerErr := &fsstoreretryafter.MarkerError{
		Operation: fsstoreretryafter.MarkerOperationSave,
		Path:      "/spool/opensearch/.retry-after/opensearch.json",
		Err:       pkgfsstore.ErrDurabilityUnconfirmed,
	}
	reporter.ReportSpoolRunnerRun(context.Background(), logspool.RunReport{ProcessReport: worker.ProcessReport{
		Status:              worker.ProcessStatusCompleted,
		NotBefore:           time.Date(2026, 10, 22, 13, 0, 0, 0, time.UTC),
		RetryAfterMarkerErr: markerErr,
	}}, nil)
	dispatcher.CloseAfterDrain()
	<-dispatcher.Done()

	require.Empty(t, errorsReported, "a marker failure is a warning, not a runner failure")
	require.Len(t, warnings, 1)
	warning := warnings[0]
	assert.Equal(t, pkglogger.LogLevelWarn, warning.Level)
	assert.Equal(t, "failure spool Retry-After marker failed", warning.Message)
	assert.ErrorIs(t, warning.Err, pkgfsstore.ErrDurabilityUnconfirmed)
	assert.Equal(t, pkglogger.LogOutputOpenSearch, warning.Fields["backend"])
	assert.Equal(t, fsstoreretryafter.MarkerOperationSave, warning.Fields["retry_after_marker_operation"])
	assert.Equal(t, markerErr.Path, warning.Fields["retry_after_marker_path"])
}

// 通常送信で受け取ったRetry-Afterには、同じspoolを再送するrunnerと同じ上限を適用する。
func TestFailureSpoolWriterUsesRunnerMaxRetryAfter(t *testing.T) {
	t.Parallel()

	runtimeConfig := testFailureSpoolRuntimeConfig()
	runtimeConfig.RunnerConfig.OpensearchConfig.Worker.MaxRetryAfter = 2 * time.Minute
	created, err := NewLogger(
		context.Background(),
		testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch"),
		streamrouting.None,
		runtimeConfig,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, created.(applogging.CloseableLogger).Close()) }()
	owned := created.(*topLevelLoggerRuntime)
	require.Len(t, owned.failureSpoolPipelines.pipelines, 1)
	pipeline := owned.failureSpoolPipelines.pipelines[0]
	require.Len(t, pipeline.writers, 1)
	writer := pipeline.writers[0].handler

	before := time.Now()
	failedAt := before.UTC()
	require.NoError(t, writer.StoreSendResultByState(context.Background(), loggerfailure.SendResult{
		Meta: loggerfailure.Meta{Backend: "opensearch", RequestID: "retry-after-cap", AttemptedAt: failedAt},
		RetryableRecords: []loggerfailure.FailedRecord{{
			FailedAt:   failedAt,
			RetryAfter: 3 * time.Hour,
			Payload:    map[string]any{"index": "logs", "id": "doc-1", "op": "create", "doc": map[string]any{"message": "capped"}},
		}},
	}))
	after := time.Now()

	records := readLoggerEnvSpoolRecords(t, writer.Store.RootDir.ActiveDir().Path())
	require.Len(t, records, 1)
	notBefore := records[0].NotBefore
	assert.False(t, notBefore.Before(before.Add(2*time.Minute).Truncate(time.Microsecond)), "NotBefore %s", notBefore)
	assert.False(t, notBefore.After(after.Add(2*time.Minute)), "NotBefore %s exceeds the runner maximum", notBefore)
}

// countingFailureSpoolSenderは、受け取ったrecordを全件受理し、送信回数を数えます。
type countingFailureSpoolSender struct {
	calls int
}

func (s *countingFailureSpoolSender) Send(_ context.Context, records []pkgspool.SpoolRecord) (pkgspool.ExportReport, error) {
	s.calls++
	report := make(pkgspool.ExportReport, len(records))
	for index := range records {
		report[index] = pkgspool.RecordOutcome{InputIndex: index, Status: pkgspool.RecordOutcomeStatusSuccess, AttemptedAt: time.Now()}
	}
	return report, nil
}

// 通常送信のwriterと再送runnerは同じ待機期限を共有する。writerが保存時に記録した期限を、
// runnerは次の周期から期限を持たない古いログにも適用し、出力先のspool内へ保存する。
func TestFailureSpoolWriterSharesRetryAfterDeadlineWithPairedRunner(t *testing.T) {
	start := time.Now()
	synctest.Test(t, func(t *testing.T) {
		// spoolとmarkerへ保存する時刻を、bubbleの開始日ではなく実行時の日付に揃えます。
		time.Sleep(time.Until(start))
		ctx := context.Background()
		cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(resolvedTempDirForTest(t), "opensearch")
		plan, err := buildFailureSpoolPipelinePlan([]failureSpoolPipelineSource{{
			contractKind: pkglogger.LoggerContractSync, config: cfg,
		}})
		require.NoError(t, err)
		require.Len(t, plan, 1)
		sender := &countingFailureSpoolSender{}
		plan[0].sender = sender
		pipelines, err := newFailureSpoolPipelineSet(ctx, plan, testFailureSpoolRuntimeConfig(), observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = pipelines.close() })
		store := newFailureSpoolNotificationTestStore(t, cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig)
		writer := spoolstorehandler.NewSpoolStoreHandler(
			ctx, clock.NewUTCClock(), store, nil, nil, nil,
			observer.NopTracer{}, observer.NopMeter{}, spoolstorehandler.InternalLogHandlers{},
		)
		require.NoError(t, pipelines.registerWriter(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch, writer))
		storeFailureSpoolNotificationTestRecord(t, store, `{"index":"logs","id":"older","op":"create","doc":{"message":"older"}}`)
		time.Sleep(time.Millisecond)

		failedAt := time.Now()
		require.NoError(t, writer.StoreSendResultByState(ctx, loggerfailure.SendResult{
			Meta: loggerfailure.Meta{Backend: "opensearch", RequestID: "throttled", AttemptedAt: failedAt, InputCount: 1},
			RetryableRecords: []loggerfailure.FailedRecord{{
				FailedAt:   failedAt,
				RetryAfter: time.Minute,
				Payload:    map[string]any{"index": "logs", "id": "throttled", "op": "create", "doc": map[string]any{"message": "throttled"}},
			}},
		}))
		require.FileExists(t, filepath.Join(store.RootDir.String(), ".retry-after", "opensearch.json"))

		report, err := pipelines.runtime.runners[0].supervisor.Runner.RunOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, worker.ProcessSkipReasonRetryAfter, report.ProcessReport.SkipReason)
		require.True(t, report.ProcessReport.NotBefore.Equal(failedAt.Add(time.Minute)))
		require.Zero(t, sender.calls, "the paired runner must not send the older record before the normal-send deadline")
	})
}
