package loki

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLokiLogEntryHandlersPreserveCompletedEntry(t *testing.T) {
	occurredAt := time.Date(
		2026,
		time.August,
		21,
		12,
		34,
		56,
		0,
		time.FixedZone("JST", 9*60*60),
	)
	laterClock := fixedLokiClock{t: occurredAt.Add(time.Hour)}
	cfg := newTestLokiLoggerConfig(100, time.Hour, false)

	normal := newLifecycleLokiLogger(t, time.Hour)
	normal.Clock = laterClock
	t.Cleanup(func() {
		if err := normal.Close(context.Background()); err != nil {
			t.Errorf("Close() error: %v", err)
		}
	})
	normalEntry := &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		TraceID:    "trace-id",
		SpanID:     "span-id",
		RequestID:  "request-id",
		Message:    "completed normal entry",
		Fields:     map[string]any{"tenant": "tenant-a"},
	}
	if err := normal.HandleLogEntry(context.Background(), normalEntry); err != nil {
		t.Fatalf("HandleLogEntry() error: %v", err)
	}
	normal.mu.Lock()
	if len(normal.entries) != 1 {
		normal.mu.Unlock()
		t.Fatalf("buffered entries = %d, want 1", len(normal.entries))
	}
	buffered := normal.entries[0]
	normal.mu.Unlock()
	assertLokiCompletedEntry(t, buffered, occurredAt)

	manager := &capturingDeliveryLokiFlushManager{}
	delivery := &DeliveryLokiLogger{LokiLogger: &LokiLogger{
		LoggerBase: &LoggerBase{
			Clock:  laterClock,
			config: cfg,
		},
		flushManager: manager,
	}}
	deliveryEntry := &logging.LogEntry{
		OccurredAt:          occurredAt,
		Level:               pkglogger.LogLevelError,
		TraceID:             "trace-id",
		SpanID:              "span-id",
		RequestID:           "request-id",
		Message:             "completed delivery entry",
		Err:                 errors.New("delivery failure"),
		StackTraceRequested: true,
		Fields:              map[string]any{"tenant": "tenant-a"},
	}
	if _, err := delivery.HandleLogEntry(context.Background(), deliveryEntry); err != nil {
		t.Fatalf("delivery HandleLogEntry() error: %v", err)
	}
	assertLokiCompletedEntry(t, manager.entry, occurredAt)
	if len(manager.entry.FuncName) == 0 || len(manager.entry.File) == 0 || len(manager.entry.Line) == 0 {
		t.Fatal("delivery HandleLogEntry() did not attach requested stack trace")
	}
}

func assertLokiCompletedEntry(
	t *testing.T,
	entry *logging.LogEntry,
	wantOccurredAt time.Time,
) {
	t.Helper()
	if entry == nil {
		t.Fatal("handled entry is nil")
	}
	if !entry.OccurredAt.Equal(wantOccurredAt) || entry.OccurredAt.Location() != time.UTC {
		t.Fatalf("OccurredAt = %v (%v), want %v in UTC", entry.OccurredAt, entry.OccurredAt.Location(), wantOccurredAt)
	}
	if entry.TraceID != "trace-id" || entry.SpanID != "span-id" || entry.RequestID != "request-id" {
		t.Fatalf(
			"IDs = (trace=%q span=%q request=%q), want completed entry IDs",
			entry.TraceID,
			entry.SpanID,
			entry.RequestID,
		)
	}
	if got := entry.Fields["tenant"]; got != "tenant-a" {
		t.Fatalf("Fields[tenant] = %v, want tenant-a", got)
	}
}

type capturingDeliveryLokiFlushManager struct {
	entry *logging.LogEntry
}

func (*capturingDeliveryLokiFlushManager) Flush(context.Context, []*logging.LogEntry) error {
	return nil
}

func (*capturingDeliveryLokiFlushManager) Push(context.Context, *logging.LogEntry) error {
	return nil
}

func (m *capturingDeliveryLokiFlushManager) PushWithReport(
	_ context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	m.entry = entry
	return pkglogger.NewTargetDeliveryReport(lokiDeliverySucceededResult()), nil
}

func (*capturingDeliveryLokiFlushManager) FlushWithNoFallback(
	context.Context,
	[]*logging.LogEntry,
) {
}

func (*capturingDeliveryLokiFlushManager) Close(context.Context) error { return nil }

// 通常APIでもerrorを渡さないErrorWithStackを拒否せず、stackを持つERRORのlineとしてPushします。
func TestLokiLoggerErrorWithStackWithoutErrorPushesStack(t *testing.T) {
	client := &recordingFlushManagerLokiClient{}
	logger, err := NewLokiLogger(
		context.Background(),
		newTestLokiLoggerConfig(100, time.Hour, true),
		client,
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
	)
	if err != nil {
		t.Fatalf("NewLokiLogger() error: %v", err)
	}
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error: %v", err)
		}
	})

	if err := logger.testErrorWithStack(context.Background(), "missing error", nil); err != nil {
		t.Fatalf("ErrorWithStack(nil) error = %v, want nil", err)
	}
	requireLokiStackLineWithoutError(t, client, "missing error")
}
