package opensearch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestOpenSearchLogEntryHandlersPreserveCompletedEntry(t *testing.T) {
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
	laterClock := fixedOpenSearchOptionClock{t: occurredAt.Add(time.Hour)}
	cfg := NewMockOpenSearchConfigProvider()

	normal, _ := newTestLogger(t, cfg, nil)
	normal.Clock = laterClock
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
	assertOpenSearchCompletedEntry(t, buffered, occurredAt)

	indexer := &capturingDeliveryOpenSearchIndexer{}
	delivery := &DeliveryOpenSearchLogger{
		LoggerBase: &LoggerBase{
			Clock:  laterClock,
			config: cfg,
		},
		stackTraceConfig: NewStackTraceConfigAdapter(cfg),
		indexer:          indexer,
	}
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
	assertOpenSearchCompletedEntry(t, indexer.entry, occurredAt)
	if len(indexer.entry.FuncName) == 0 || len(indexer.entry.File) == 0 || len(indexer.entry.Line) == 0 {
		t.Fatal("delivery HandleLogEntry() did not attach requested stack trace")
	}
}

func assertOpenSearchCompletedEntry(
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

type capturingDeliveryOpenSearchIndexer struct {
	entry *logging.LogEntry
}

func (i *capturingDeliveryOpenSearchIndexer) IndexWithReport(
	_ context.Context,
	entry *logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	i.entry = entry
	return pkglogger.NewTargetDeliveryReport(openSearchDeliverySucceededResult()), nil
}

func (*capturingDeliveryOpenSearchIndexer) Close(context.Context) error { return nil }
