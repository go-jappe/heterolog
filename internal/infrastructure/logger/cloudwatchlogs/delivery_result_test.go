package cloudwatchlogs

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/logger"

	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
)

func TestCloudWatchLogsDeliveryFailedResultPreservesError(t *testing.T) {
	wantErr := errors.New("put failed")
	result := cloudWatchLogsDeliveryFailedResult(wantErr)

	assert.Equal(t, deliveryBackendCloudWatchLogs, result.Backend)
	assert.Equal(t, logger.DeliveryFailed, result.Status)
	assert.ErrorIs(t, result.Err, wantErr)
}

func TestCloudWatchLogsTargetDeliveryResultUsesSendResultOutcome(t *testing.T) {
	terminalErr := errors.New("transport failed")
	tests := []struct {
		name       string
		sendResult loggerfailure.SendResult
		wantStatus logger.DeliveryStatus
		wantErr    error
	}{
		{
			name:       "terminal error without acceptance evidence",
			sendResult: loggerfailure.SendResult{TerminalError: terminalErr},
			wantStatus: logger.DeliveryUnconfirmed,
			wantErr:    terminalErr,
		},
		{
			name: "dead record failure",
			sendResult: loggerfailure.SendResult{DeadRecords: []loggerfailure.FailedRecord{{
				DeliveryStatus: logger.DeliveryFailed,
				StatusCode:     http.StatusBadRequest,
			}}},
			wantStatus: logger.DeliveryFailed,
		},
		{
			name: "quarantine record failure",
			sendResult: loggerfailure.SendResult{QuarantineRecords: []loggerfailure.FailedRecord{{
				DeliveryStatus: logger.DeliveryRejected,
				FailureCode:    loggerfailure.FailureCode(infracloudwatchlogs.FailureCodeInvalidEvent),
			}}},
			wantStatus: logger.DeliveryRejected,
		},
		{
			name: "retryable record failure",
			sendResult: loggerfailure.SendResult{RetryableRecords: []loggerfailure.FailedRecord{{
				DeliveryStatus: logger.DeliveryFailed,
				StatusCode:     http.StatusServiceUnavailable,
			}}},
			wantStatus: logger.DeliveryFailed,
		},
		{name: "success", wantStatus: logger.DeliverySucceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cloudWatchLogsTargetDeliveryResult(tt.sendResult)
			assert.Equal(t, tt.wantStatus, result.Status)
			if tt.wantErr != nil {
				assert.ErrorIs(t, result.Err, tt.wantErr)
			} else if tt.wantStatus != logger.DeliverySucceeded {
				assert.Error(t, result.Err)
			} else {
				assert.NoError(t, result.Err)
			}
		})
	}
}

// errorを渡さないErrorWithStackも拒否せず、要求されたstackを持つERRORのeventとして
// 一件送信します。event本文にerrorの欄は含めません。
func TestDeliveryCloudWatchLogsLoggerErrorWithStackWithoutErrorPutsStack(t *testing.T) {
	client := &recordingCloudWatchLogsClient{}
	deliveryLogger, err := NewDeliveryCloudWatchLogsLogger(
		context.Background(),
		NewDeliveryCloudWatchLogsLoggerConfigAdapter(logger.DeliveryCloudWatchLogsLoggerBaseConfig{
			TimeConfig: logger.TimeConfig{TimeZone: "UTC"},
			RuntimeConfig: logger.DeliveryRuntimeConfig{
				AppName: "delivery-stack-test",
				Target:  "cloudwatchlogs",
			},
			LogBodyConfig: logger.LogBodyConfig{
				LogLineDateFormat:        time.RFC3339Nano,
				ErrorTraceDepth:          1,
				ErrorTraceDepthDebugMode: 1,
			},
		}),
		client,
		NewDeliveryFlushManagerConfigAdapter(logger.DeliveryCloudWatchLogsFlushManagerConfig{
			LogGroupName:  "/app/test",
			LogStreamName: "stream-a",
			Resource: logger.Resource{
				ServiceName: "delivery-stack-test",
				Cloud:       &logger.CloudResourceInfo{},
			},
		}, pkgcloudwatchlogs.StreamRoutingNone),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
	)
	if err != nil {
		t.Fatalf("NewDeliveryCloudWatchLogsLogger() error = %v", err)
	}
	t.Cleanup(func() { assert.NoError(t, deliveryLogger.Close()) })

	report, err := handleTestDeliveryErrorWithStack(context.Background(), deliveryLogger, "missing error", nil)

	if err != nil {
		t.Fatalf("ErrorWithStack(nil) error = %v, want nil", err)
	}
	if len(report.Targets) != 1 {
		t.Fatalf("ErrorWithStack(nil) targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, logger.DeliverySucceeded, report.Targets[0].Status)
	assert.NoError(t, report.Targets[0].Err)
	requireCloudWatchLogsStackEventWithoutError(t, client, "missing error")
}

// requireCloudWatchLogsStackEventWithoutErrorは、errorのないErrorWithStackが一件のERROR event
// としてstack trace付きで送信され、errorの欄を含まないことを確認します。
func requireCloudWatchLogsStackEventWithoutError(
	t *testing.T,
	client *recordingCloudWatchLogsClient,
	message string,
) {
	t.Helper()

	if len(client.events) != 1 {
		t.Fatalf("CloudWatch Logs events = %d, want 1", len(client.events))
	}
	eventMessage := client.events[0].Message
	assert.Contains(t, eventMessage, "[ERROR] ")
	assert.Contains(t, eventMessage, " - "+message)
	assert.Contains(t, eventMessage, " (stacktrace): (1) at: (File: ")
	assert.NotContains(t, eventMessage, " - err: ")
}

// FlushManagerがない場合は送信を試みないため、backendの明示的な拒否を表すDeliveryFailedではなく、
// 送信前の拒否としてDeliveryRejectedを返します。
func TestDeliveryCloudWatchLogsLoggerMissingFlushManagerReportsRejected(t *testing.T) {
	deliveryLogger := &DeliveryCloudWatchLogsLogger{CloudWatchLogsLogger: &CloudWatchLogsLogger{
		LoggerBase: &LoggerBase{Clock: clock.NewUTCClock()},
	}}

	report, err := handleTestDeliveryInfo(context.Background(), deliveryLogger, "missing flush manager")
	if err == nil {
		t.Fatal("HandleLogEntry() without flush manager error = nil")
	}
	if len(report.Targets) != 1 {
		t.Fatalf("HandleLogEntry() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, deliveryBackendCloudWatchLogs, report.Targets[0].Backend)
	assert.Equal(t, logger.DeliveryRejected, report.Targets[0].Status)
	assert.ErrorIs(t, report.Targets[0].Err, err)
}

func TestCloudWatchLogsFlushManagerReportsMissingClientAsInternalError(t *testing.T) {
	fm := &CloudWatchLogsFlushManager{LoggerBase: &LoggerBase{Clock: clock.NewUTCClock()}}
	report, err := fm.PutWithReport(context.Background(), &logging.LogEntry{})
	if err == nil {
		t.Fatal("PutWithReport() error = nil")
	}
	if len(report.Targets) != 1 {
		t.Fatalf("PutWithReport() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, logger.DeliveryRejected, report.Targets[0].Status)
	assert.ErrorIs(t, report.Targets[0].Err, err)
}

func TestCloudWatchLogsFlushManagerReportsSuccessfulTarget(t *testing.T) {
	fm := newTestCloudWatchLogsFlushManager(&recordingCloudWatchLogsClient{}, nil)

	report, err := fm.PutWithReport(context.Background(), newTestCloudWatchLogsEntry(logger.LogLevelInfo, "successful delivery"))
	if err != nil {
		t.Fatalf("PutWithReport() error = %v", err)
	}
	if len(report.Targets) != 1 {
		t.Fatalf("PutWithReport() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, logger.DeliverySucceeded, report.Targets[0].Status)
}

func TestCloudWatchLogsFlushManagerPutWithReportReportsTargetAndDurableResult(t *testing.T) {
	sendErr := &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusTooManyRequests, AwsErrorType: "ThrottlingException"}
	handler := &fixedCloudWatchLogsFailureHandler{
		report: logger.DeliveryReport{
			Durables: []logger.DurableResult{{
				Backend: deliveryBackendCloudWatchLogs,
				Kind:    logger.DurableArchive,
				Status:  logger.DurableSucceeded,
			}},
		},
	}
	fm := newTestCloudWatchLogsFlushManagerWithReportingFailureHandler(
		&recordingCloudWatchLogsClient{err: sendErr},
		mustNewCloudWatchLogsReportingFailureComposite(t,
			[]flushfailure.FailureHandlerWithReport{handler},
		),
	)

	report, err := fm.PutWithReport(context.Background(), newTestCloudWatchLogsEntry(logger.LogLevelError, "failed delivery"))
	if err == nil {
		t.Fatal("PutWithReport() error = nil, want target failure error")
	}
	if handler.reportCalls != 1 {
		t.Fatalf("HandleFailureWithReport() calls = %d, want 1", handler.reportCalls)
	}
	if len(report.Targets) != 1 || report.Targets[0].Status != logger.DeliveryFailed {
		t.Fatalf("PutWithReport() targets = %+v, want one failed target", report.Targets)
	}
	assert.ErrorIs(t, err, sendErr)
	assert.ErrorIs(t, report.Targets[0].Err, sendErr)
	if len(report.Durables) != 1 {
		t.Fatalf("PutWithReport() durables = %+v, want one durable result", report.Durables)
	}
	if report.Durables[0].Kind != logger.DurableArchive ||
		report.Durables[0].Status != logger.DurableSucceeded {
		t.Fatalf("PutWithReport() durable = %+v, want succeeded archive", report.Durables[0])
	}
}

func TestCloudWatchDeliveryKeepsTargetAndDurableCausesSeparate(t *testing.T) {
	sendErr := &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusForbidden, AwsErrorType: "AccessDeniedException"}
	storeErr := errors.New("archive unavailable")
	handler := &fixedCloudWatchLogsFailureHandler{
		reportErr: storeErr,
		report: logger.DeliveryReport{Durables: []logger.DurableResult{{
			Backend: deliveryBackendCloudWatchLogs, Kind: logger.DurableArchive,
			Status: logger.DurableFailed, Err: storeErr,
		}}},
	}
	fm := newTestCloudWatchLogsFlushManagerWithReportingFailureHandler(
		&recordingCloudWatchLogsClient{err: sendErr},
		mustNewCloudWatchLogsReportingFailureComposite(t, []flushfailure.FailureHandlerWithReport{handler}),
	)
	report, err := fm.PutWithReport(context.Background(), newTestCloudWatchLogsEntry(logger.LogLevelInfo, "rejected and not archived"))
	if len(report.Targets) != 1 || len(report.Durables) != 1 {
		t.Fatalf("report=%+v, want separate target and durable results", report)
	}
	assert.ErrorIs(t, err, sendErr)
	assert.ErrorIs(t, err, storeErr)
	assert.ErrorIs(t, report.Targets[0].Err, sendErr)
	assert.NotErrorIs(t, report.Targets[0].Err, storeErr)
	assert.ErrorIs(t, report.Durables[0].Err, storeErr)
	assert.NotErrorIs(t, report.Durables[0].Err, sendErr)
}

type fixedCloudWatchLogsFailureHandler struct {
	handleCalls int
	reportCalls int
	handleErr   error
	report      logger.DeliveryReport
	reportErr   error
}

func (h *fixedCloudWatchLogsFailureHandler) HandleFailure(
	context.Context,
	loggerfailure.SendResult,
) error {
	h.handleCalls++
	return h.handleErr
}

func (h *fixedCloudWatchLogsFailureHandler) HandleFailureWithReport(
	context.Context,
	loggerfailure.SendResult,
) (logger.DeliveryReport, error) {
	h.reportCalls++
	return h.report, h.reportErr
}
