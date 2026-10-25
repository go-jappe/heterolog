package opensearch

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
)

func TestOpenSearchDeliveryFailedResultPreservesError(t *testing.T) {
	wantErr := errors.New("index failed")
	result := openSearchDeliveryFailedResult(wantErr)

	assert.Equal(t, deliveryBackendOpenSearch, result.Backend)
	assert.Equal(t, logger.DeliveryFailed, result.Status)
	assert.ErrorIs(t, result.Err, wantErr)
}

func TestOpenSearchTargetDeliveryResultUsesSendResultOutcome(t *testing.T) {
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
			name: "retryable record failure",
			sendResult: loggerfailure.SendResult{RetryableRecords: []loggerfailure.FailedRecord{{
				DeliveryStatus: logger.DeliveryFailed,
				StatusCode:     http.StatusServiceUnavailable,
			}}},
			wantStatus: logger.DeliveryFailed,
		},
		{
			name: "quarantine record failure",
			sendResult: loggerfailure.SendResult{QuarantineRecords: []loggerfailure.FailedRecord{{
				DeliveryStatus: logger.DeliveryFailed,
				StatusCode:     http.StatusBadRequest,
			}}},
			wantStatus: logger.DeliveryFailed,
		},
		{name: "success", wantStatus: logger.DeliverySucceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := openSearchTargetDeliveryResult(tt.sendResult)
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

// errorを渡さないErrorWithStackも拒否せず、要求されたstackを持つERROR documentとして
// 一件Indexします。documentのerrorは空のまま、stack traceだけを持ちます。
func TestDeliveryOpenSearchLoggerErrorWithStackWithoutErrorIndexesStack(t *testing.T) {
	client := NewMockOpenSearchClient()
	tracer, meter := observer.NopTracer{}, observer.NopMeter{}
	deliveryLogger := NewDeliveryOpenSearchLogger(
		context.Background(),
		NewDeliveryOpenSearchLoggerConfigAdapter(logger.DeliveryOpenSearchLoggerBaseConfig{
			RuntimeConfig: logger.DeliveryRuntimeConfig{
				AppName: "delivery-stack-test",
				Target:  "opensearch",
			},
			LogBodyConfig: logger.LogBodyConfig{
				LogLineDateFormat:        time.RFC3339Nano,
				ErrorTraceDepth:          1,
				ErrorTraceDepthDebugMode: 1,
			},
		}),
		client,
		NewDeliveryFlushManagerConfigAdapter(logger.DeliveryOpenSearchFlushManagerConfig{
			Index:                      "delivery-index",
			PostWarnThresholdDuration:  time.Hour,
			PostErrorThresholdDuration: 2 * time.Hour,
			Resource: logger.Resource{
				Cloud: &logger.CloudResourceInfo{},
			},
		}),
		NewMockResourceProvider(),
		nil,
		tracer,
		meter,
		&LoggerBase{Clock: clock.NewUTCClock(), Tracer: tracer, Meter: meter},
	)
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
	requireOpenSearchStackDocumentWithoutError(t, client, "missing error")
}

// requireOpenSearchStackDocumentWithoutErrorは、errorのないErrorWithStackが一件のERROR
// documentとしてstack trace付きでIndexされ、errorの値を持たないことを確認します。
func requireOpenSearchStackDocumentWithoutError(t *testing.T, client *MockOpenSearchClient, message string) {
	t.Helper()

	client.mu.RLock()
	requests := append([]pkgopensearch.DocumentWriteRequest(nil), client.IndexDocumentArgs...)
	client.mu.RUnlock()
	if len(requests) != 1 {
		t.Fatalf("indexed documents = %d, want 1", len(requests))
	}
	doc := requests[0].Doc
	assert.Equal(t, logger.LogLevelError.String(), doc["level"])
	assert.Equal(t, message, doc["message"])
	assert.Nil(t, doc["error"])
	assert.Contains(t, doc["stacktrace"], "(stacktrace): (1) at: (File: ")
	assert.NotEmpty(t, doc["stack_trace"])
}

// indexerがない場合は送信を試みないため、backendの明示的な拒否を表すDeliveryFailedではなく、
// 送信前の拒否としてDeliveryRejectedを返します。
func TestDeliveryOpenSearchLoggerMissingIndexerReportsRejected(t *testing.T) {
	deliveryLogger := &DeliveryOpenSearchLogger{LoggerBase: &LoggerBase{Clock: clock.NewUTCClock()}}

	report, err := deliveryLogger.testInfo(context.Background(), "missing indexer")
	if err == nil {
		t.Fatal("HandleLogEntry() without indexer error = nil")
	}
	if len(report.Targets) != 1 {
		t.Fatalf("HandleLogEntry() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, deliveryBackendOpenSearch, report.Targets[0].Backend)
	assert.Equal(t, logger.DeliveryRejected, report.Targets[0].Status)
	assert.ErrorIs(t, report.Targets[0].Err, err)
}

func TestOpenSearchFlushManagerReportsMissingClientAsInternalError(t *testing.T) {
	fm := &OpenSearchFlushManager{LoggerBase: &LoggerBase{Clock: clock.NewUTCClock()}}
	report, err := fm.IndexWithReport(context.Background(), &logging.LogEntry{})
	if err == nil {
		t.Fatal("IndexWithReport() error = nil")
	}
	if len(report.Targets) != 1 {
		t.Fatalf("IndexWithReport() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, logger.DeliveryRejected, report.Targets[0].Status)
	assert.ErrorIs(t, report.Targets[0].Err, err)
}

func TestOpenSearchFlushManagerReportsSuccessfulTarget(t *testing.T) {
	fm := newFlushManagerForDeliveryTest(nil)
	fm.osClient = NewMockOpenSearchClient()

	report, err := fm.IndexWithReport(context.Background(), newFlushManagerDeliveryTestEntry())
	if err != nil {
		t.Fatalf("IndexWithReport() error = %v", err)
	}
	if len(report.Targets) != 1 {
		t.Fatalf("IndexWithReport() targets = %d, want 1", len(report.Targets))
	}
	assert.Equal(t, logger.DeliverySucceeded, report.Targets[0].Status)
}
