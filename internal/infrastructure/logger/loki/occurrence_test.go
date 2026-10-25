package loki

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLokiRecordKeepsOccurrenceAcrossBufferedDelayAndLocalDisplay(t *testing.T) {
	const layout = "2006/01/02 15:04:05"
	cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Asia/Tokyo",
		},
		LogBodyConfig:           pkglogger.LogBodyConfig{LogLineDateFormat: layout},
		FlushInterval:           time.Hour,
		MaxEntries:              100,
		BufferFlushBytes:        1024 * 1024,
		FallbackQueueMaxEntries: 100,
		MaxLineBytes:            256 * 1024,
		MaxPushRequestBytes:     4 * 1024 * 1024,
	})
	displayFormatter, err := NewConfiguredTimestampFormatter(cfg)
	if err != nil {
		t.Fatalf("NewConfiguredTimestampFormatter() error = %v", err)
	}
	inputOccurredAt := time.Date(
		2026,
		8,
		21,
		3,
		4,
		5,
		123456789,
		time.FixedZone("input", 9*60*60),
	)
	occurredAt := inputOccurredAt.UTC()
	base := &LoggerBase{LogFormatter: formatter.NewPlainTextFormatter(displayFormatter)}
	logger, err := NewLokiLogger(
		context.Background(),
		cfg,
		&recordingFlushManagerLokiClient{},
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		base,
	)
	if err != nil {
		t.Fatalf("NewLokiLogger() error = %v", err)
	}
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	if err := logger.testLog(
		context.Background(),
		inputOccurredAt,
		pkglogger.LogLevelInfo,
		"buffered",
	); err != nil {
		t.Fatalf("Log() error = %v", err)
	}

	logger.mu.Lock()
	bufferedEntries := len(logger.entries)
	if bufferedEntries != 1 {
		logger.mu.Unlock()
		t.Fatalf("buffered entries = %d, want 1", bufferedEntries)
	}
	entry := logger.entries[0]
	logger.mu.Unlock()
	record := (&LokiFlushManager{LoggerBase: base}).convertLogEntryToEntryRecord(entry)

	if !entry.OccurredAt.Equal(occurredAt) {
		t.Fatalf("OccurredAt = %s, want %s", entry.OccurredAt, occurredAt)
	}
	if entry.OccurredAt.Location() != time.UTC {
		t.Fatalf("OccurredAt location = %s, want UTC", entry.OccurredAt.Location())
	}
	if !record.pushEntry.Timestamp.Equal(occurredAt) {
		t.Fatalf("protocol timestamp = %s, want %s", record.pushEntry.Timestamp, occurredAt)
	}
	wantDisplay := "2026/08/21 03:04:05"
	if !strings.Contains(record.pushEntry.Line, wantDisplay) {
		t.Fatalf("formatted line = %q, want local occurrence %q", record.pushEntry.Line, wantDisplay)
	}

	durable := (&LokiFlushManager{}).convertLogEntryRecordToDocument(record)
	if got, want := durable["timestamp"], occurredAt.Format(time.RFC3339Nano); got != want {
		t.Fatalf("durable timestamp = %v, want %q", got, want)
	}
}

func TestFallbackHandlerKeepsSourceOccurrence(t *testing.T) {
	cfg := newTestLokiLoggerConfig(100, time.Hour, false)
	probe := newLokiFallbackBatchProbe()
	fallback := newLokiFallbackBatchTestLogger(cfg, probe)
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

	controller.Handle(
		context.Background(),
		entry,
		observer.NopTracer{},
		observer.NopMeter{},
	)

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
