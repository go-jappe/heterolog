package opensearch

import (
	"context"
	"testing"
	"time"

	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestPlainConverterKeepsOccurrenceAcrossBufferedDelayAndLocalDisplay(t *testing.T) {
	const layout = "2006/01/02 15:04:05"
	cfg := NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Asia/Tokyo",
		},
		LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: layout},
	})
	displayFormatter, err := NewConfiguredTimestampFormatter(cfg)
	if err != nil {
		t.Fatalf("NewConfiguredTimestampFormatter() error = %v", err)
	}
	occurredAt := time.Date(2026, 8, 20, 18, 4, 5, 0, time.UTC)
	entry := newTestApplicationLogEntry(
		context.Background(),
		occurredAt,
		pkglogger.LogLevelInfo,
		"buffered",
		nil,
		false,
	)
	doc := NewPlainConverter(displayFormatter).convertLogEntryToDocument(entry)

	if !entry.OccurredAt.Equal(occurredAt) {
		t.Fatalf("OccurredAt = %s, want %s", entry.OccurredAt, occurredAt)
	}
	if got, want := doc["timestamp"], "2026/08/21 03:04:05"; got != want {
		t.Fatalf("timestamp = %v, want local display %q", got, want)
	}
}

func TestFallbackHandlerKeepsSourceOccurrence(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.logLevel = pkglogger.LogLevelDebug
	probe := newOpenSearchFallbackBatchProbe()
	fallback := newFallbackControllerTestLogger(t, cfg, probe)
	controller := &fallbackHandlerController{
		initializationAttempted: true,
		logger:                  fallback,
	}
	occurredAt := time.Date(2026, 8, 20, 18, 4, 5, 0, time.UTC)
	entry := &infralogging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "fallback",
	}
	tracer, meter := newTestObserver()

	controller.Handle(context.Background(), entry, tracer, meter)

	if !entry.OccurredAt.Equal(occurredAt) {
		t.Fatalf("fallback OccurredAt = %s, want source occurrence %s", entry.OccurredAt, occurredAt)
	}
	// Handleがentryを破棄しても上の検証は成功するため、fallback送信へ渡ったentryで確認します。
	if err := controller.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	batches, _ := probe.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("fallback batches = %v, want one batch with the handled entry", batches)
	}
	if sent := batches[0][0]; sent.Message != "fallback" || !sent.OccurredAt.Equal(occurredAt) {
		t.Fatalf("sent fallback entry = %q at %s, want %q at source occurrence %s", sent.Message, sent.OccurredAt, "fallback", occurredAt)
	}
}
