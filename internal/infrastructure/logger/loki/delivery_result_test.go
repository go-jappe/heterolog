package loki

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
	"github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"

	"github.com/stretchr/testify/assert"
)

func TestLokiDeliveryFailedResultPreservesError(t *testing.T) {
	wantErr := errors.New("push failed")
	result := lokiDeliveryFailedResult(wantErr)

	assert.Equal(t, deliveryBackendLoki, result.Backend)
	assert.Equal(t, logger.DeliveryFailed, result.Status)
	assert.ErrorIs(t, result.Err, wantErr)
}

func TestLokiTargetDeliveryResultUsesSendResultOutcome(t *testing.T) {
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
				FailureCode:    FailureCodeLogLineTooLarge,
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
			result := lokiTargetDeliveryResult(tt.sendResult)
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

// errorを渡さないErrorWithStackも拒否せず、要求されたstackを持つERRORのlineとして
// 一件Pushします。lineにerrorの欄は含めません。
func TestDeliveryLokiLoggerErrorWithStackWithoutErrorPushesStack(t *testing.T) {
	client := &recordingFlushManagerLokiClient{}
	deliveryLogger, err := NewDeliveryLokiLogger(
		context.Background(),
		NewDeliveryLokiLoggerConfigAdapter(logger.DeliveryLokiLoggerBaseConfig{
			RuntimeConfig: logger.DeliveryRuntimeConfig{
				AppName: "delivery-stack-test",
				Target:  "loki",
			},
			LogBodyConfig: logger.LogBodyConfig{
				LogLineDateFormat:        time.RFC3339Nano,
				ErrorTraceDepth:          1,
				ErrorTraceDepthDebugMode: 1,
			},
			MaxLineBytes:        256 * 1024,
			MaxPushRequestBytes: 4 * 1024 * 1024,
		}),
		client,
		NewDeliveryFlushManagerConfigAdapter(logger.DeliveryLokiFlushManagerConfig{}),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
	)
	if err != nil {
		t.Fatalf("NewDeliveryLokiLogger() error = %v", err)
	}
	t.Cleanup(func() { assert.NoError(t, deliveryLogger.Close()) })

	report, err := deliveryLogger.testErrorWithStack(context.Background(), "missing error", nil)

	if err != nil {
		t.Fatalf("ErrorWithStack(nil) error = %v, want nil", err)
	}
	if len(report.Targets) != 1 {
		t.Fatalf("ErrorWithStack(nil) targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, logger.DeliverySucceeded, report.Targets[0].Status)
	assert.NoError(t, report.Targets[0].Err)
	requireLokiStackLineWithoutError(t, client, "missing error")
}

// requireLokiStackLineWithoutErrorは、errorのないErrorWithStackが一件のERROR lineとして
// stack trace付きでPushされ、errorの欄を含まないことを確認します。
func requireLokiStackLineWithoutError(t *testing.T, client *recordingFlushManagerLokiClient, message string) {
	t.Helper()

	if client.pushCalls != 1 || len(client.streams) != 1 || len(client.streams[0].Entries) != 1 {
		t.Fatalf("Loki push calls = %d, streams = %+v, want one entry", client.pushCalls, client.streams)
	}
	stream := client.streams[0]
	assert.Equal(t, "error", stream.Labels["loglevel"])
	line := stream.Entries[0].Line
	assert.Contains(t, line, "[ERROR] ")
	assert.Contains(t, line, " - "+message)
	assert.Contains(t, line, " (stacktrace): (1) at: (File: ")
	assert.NotContains(t, line, " - err: ")
}

// FlushManagerがない場合は送信を試みないため、backendの明示的な拒否を表すDeliveryFailedではなく、
// 送信前の拒否としてDeliveryRejectedを返します。
func TestDeliveryLokiLoggerMissingFlushManagerReportsRejected(t *testing.T) {
	deliveryLogger := &DeliveryLokiLogger{LokiLogger: &LokiLogger{LoggerBase: &LoggerBase{Clock: clock.NewUTCClock()}}}

	report, err := deliveryLogger.testInfo(context.Background(), "missing flush manager")
	if err == nil {
		t.Fatal("HandleLogEntry() without flush manager error = nil")
	}
	if len(report.Targets) != 1 {
		t.Fatalf("HandleLogEntry() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, deliveryBackendLoki, report.Targets[0].Backend)
	assert.Equal(t, logger.DeliveryRejected, report.Targets[0].Status)
	assert.ErrorIs(t, report.Targets[0].Err, err)
}

func TestLokiFlushManagerReportsMissingClientAsInternalError(t *testing.T) {
	fm := &LokiFlushManager{LoggerBase: &LoggerBase{Clock: clock.NewUTCClock()}}
	report, err := fm.PushWithReport(context.Background(), &logging.LogEntry{})
	if err == nil {
		t.Fatal("PushWithReport() error = nil")
	}
	if len(report.Targets) != 1 {
		t.Fatalf("PushWithReport() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, logger.DeliveryRejected, report.Targets[0].Status)
	assert.ErrorIs(t, report.Targets[0].Err, err)
}

func TestLokiFlushManagerReportsSuccessfulTarget(t *testing.T) {
	fm := newTestLokiFlushManager(&recordingFlushManagerLokiClient{})

	report, err := fm.PushWithReport(context.Background(), newTestLokiLogEntry(logger.LogLevelInfo, "successful delivery"))
	if err != nil {
		t.Fatalf("PushWithReport() error = %v", err)
	}
	if len(report.Targets) != 1 {
		t.Fatalf("PushWithReport() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, logger.DeliverySucceeded, report.Targets[0].Status)
}

func TestLokiFlushManagerPushWithReportWaitsForDurableReport(t *testing.T) {
	handler := &fixedLokiFailureHandler{}
	fm := newTestLokiFlushManagerWithReportingFailureHandler(
		&recordingFlushManagerLokiClient{pushErr: &pkgloki.HTTPStatusError{Code: http.StatusTooManyRequests}},
		mustNewLokiReportingFailureComposite(t,
			[]flushfailure.FailureHandlerWithReport{handler},
		),
	)

	report, err := fm.PushWithReport(context.Background(), newTestLokiLogEntry(logger.LogLevelError, "failed delivery"))
	if err == nil {
		t.Fatal("PushWithReport() error = nil, want target failure error")
	}
	if handler.reportCalls != 1 {
		t.Fatalf("HandleFailureWithReport() calls = %d, want 1", handler.reportCalls)
	}
	if handler.handleCalls != 0 {
		t.Fatalf("HandleFailure() calls = %d, want 0", handler.handleCalls)
	}
	if len(report.Targets) != 1 || report.Targets[0].Status != logger.DeliveryFailed {
		t.Fatalf("PushWithReport() targets = %+v, want one failed target", report.Targets)
	}
	if len(report.Durables) != 1 {
		t.Fatalf("PushWithReport() durables = %+v, want one durable result", report.Durables)
	}
	assert.Equal(t, logger.DurableArchive, report.Durables[0].Kind)
	assert.Equal(t, logger.DurableSucceeded, report.Durables[0].Status)
}

type fixedLokiFailureHandler struct {
	handleCalls int
	reportCalls int
}

func (h *fixedLokiFailureHandler) HandleFailure(
	context.Context,
	loggerfailure.SendResult,
) error {
	h.handleCalls++
	return nil
}

func (h *fixedLokiFailureHandler) HandleFailureWithReport(
	context.Context,
	loggerfailure.SendResult,
) (logger.DeliveryReport, error) {
	h.reportCalls++
	return logger.DeliveryReport{
		Durables: []logger.DurableResult{{
			Backend: deliveryBackendLoki + "_failure_archive",
			Kind:    logger.DurableArchive,
			Status:  logger.DurableSucceeded,
		}},
	}, nil
}
