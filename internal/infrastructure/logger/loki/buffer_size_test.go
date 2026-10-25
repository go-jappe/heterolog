package loki

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var lokiTestPlainTextFormatter = formatter.NewPlainTextFormatter(
	timestamp.RFC3339Formatter(),
)

func TestLokiBufferedEntryMeasurementUsesUTF8Bytes(t *testing.T) {
	logger := &LokiLogger{LoggerBase: &LoggerBase{
		LogFormatter: lokiTestPlainTextFormatter,
	}}
	for _, message := range []string{"日本語のログ", "日本\xff\xfe\ufffd\n\"<"} {
		entry := newTestLokiLogEntry(pkglogger.LogLevelInfo, message)
		got, err := logger.measureBufferedEntryBytes(entry)
		if err != nil {
			t.Fatalf("measureBufferedEntryBytes() error: %v", err)
		}
		formatted := lokiTestPlainTextFormatter.Format(entry)
		encoded, err := json.Marshal(formatted)
		if err != nil {
			t.Fatal(err)
		}
		var received string
		if err := json.Unmarshal(encoded, &received); err != nil {
			t.Fatal(err)
		}
		if got != len(received) {
			t.Fatalf("measured bytes = %d, want decoded line bytes %d", got, len(received))
		}
		if got <= utf8.RuneCountInString(formatted) {
			t.Fatalf("measured bytes = %d, rune count = %d; want UTF-8 byte count", got, utf8.RuneCountInString(formatted))
		}
	}
}

func TestLokiLoggerFlushesAtFormattedLineByteThreshold(t *testing.T) {
	fixedTime := time.Date(2026, 8, 19, 12, 34, 56, 0, time.UTC)
	fixedClock := fixedLokiClock{t: fixedTime}
	message := "formatted-line byte threshold"
	entryBytes := len(lokiTestPlainTextFormatter.Format(&logging.LogEntry{
		OccurredAt: fixedTime,
		Level:      pkglogger.LogLevelInfo,
		Message:    message,
	}))
	cfg := newTestLokiLoggerConfigWithBufferLimits(
		100,
		entryBytes*2,
		100,
		time.Hour,
		false,
	)
	client := &recordingFlushManagerLokiClient{}
	logger, err := NewLokiLogger(
		context.Background(),
		cfg,
		client,
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
		WithClock(fixedClock),
	)
	if err != nil {
		t.Fatalf("NewLokiLogger() error: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := logger.Close(context.Background()); closeErr != nil {
			t.Errorf("Close() error: %v", closeErr)
		}
	})

	if err := logger.testInfo(context.Background(), message); err != nil {
		t.Fatalf("first Info() error: %v", err)
	}
	if client.pushCalls != 0 {
		t.Fatalf("push calls below byte threshold = %d, want 0", client.pushCalls)
	}
	bufferedEntries, bufferedEntryBytes, bufferedBytes := lifecycleLokiBufferState(logger)
	if bufferedEntries != 1 || bufferedEntryBytes != 1 || bufferedBytes != entryBytes {
		t.Fatalf(
			"buffer below threshold = (entries=%d, entryBytes=%d, bytes=%d), want (1, 1, %d)",
			bufferedEntries,
			bufferedEntryBytes,
			bufferedBytes,
			entryBytes,
		)
	}

	if err := logger.testInfo(context.Background(), message); err != nil {
		t.Fatalf("second Info() error: %v", err)
	}
	if client.pushCalls != 1 {
		t.Fatalf("push calls at byte threshold = %d, want 1", client.pushCalls)
	}
	bufferedEntries, bufferedEntryBytes, bufferedBytes = lifecycleLokiBufferState(logger)
	if bufferedEntries != 0 || bufferedEntryBytes != 0 || bufferedBytes != 0 {
		t.Fatalf(
			"buffer after byte-threshold flush = (entries=%d, entryBytes=%d, bytes=%d), want zero",
			bufferedEntries,
			bufferedEntryBytes,
			bufferedBytes,
		)
	}
}

func TestLokiLoggerFailedByteThresholdFlushDropsDetachedSnapshot(t *testing.T) {
	entry := newTestLokiLogEntry(pkglogger.LogLevelInfo, "drop failed byte-threshold snapshot")
	entryBytes := len(lokiTestPlainTextFormatter.Format(entry))
	cfg := newTestLokiLoggerConfigWithBufferLimits(
		100,
		entryBytes,
		100,
		time.Hour,
		false,
	)
	logger, err := NewLokiLogger(
		context.Background(),
		cfg,
		&recordingFlushManagerLokiClient{},
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
		if closeErr := logger.Close(context.Background()); closeErr != nil {
			t.Errorf("Close() error: %v", closeErr)
		}
	})

	flushErr := errors.New("flush failed")
	manager := newLifecycleLokiFlushManager(false)
	manager.flushErrors = []error{flushErr}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.mu.Unlock()

	if err := logger.enqueue(context.Background(), entry); !errors.Is(err, flushErr) {
		t.Fatalf("enqueue() error = %v, want %v", err, flushErr)
	}
	logger.mu.Lock()
	bufferedEntryCount := len(logger.entries)
	bufferedEntryByteCount := len(logger.entryBytes)
	bufferedBytes := logger.bufferBytes
	logger.mu.Unlock()
	if bufferedEntryCount != 0 {
		t.Fatalf("buffered entries after failed one-shot flush = %d, want 0", bufferedEntryCount)
	}
	if bufferedEntryByteCount != 0 {
		t.Fatalf("buffered entry measurements after failed one-shot flush = %d, want 0", bufferedEntryByteCount)
	}
	if bufferedBytes != 0 {
		t.Fatalf("buffered bytes after failed one-shot flush = %d, want 0", bufferedBytes)
	}
}
