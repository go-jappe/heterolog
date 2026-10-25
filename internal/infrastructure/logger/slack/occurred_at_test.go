package slack

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFallbackHandlerPreservesOccurrenceThroughSlackBatch(t *testing.T) {
	wantOccurredAt := time.Date(2026, time.August, 20, 3, 4, 5, 0, time.UTC)
	cfg := fallbackSlackTestConfig([]time.Duration{time.Hour})
	cfg.TimeConfig.IsLocalTimeEnabled = true
	cfg.TimeConfig.TimeZone = "Asia/Tokyo"
	cfg.LogBodyConfig.LogLineDateFormat = "2006-01-02 15:04:05 -0700"
	controller := newFallbackHandlerController(NewConfigAdapter(cfg))

	fallback, acquired := controller.acquire(
		context.Background(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if !acquired {
		t.Fatal("fallback logger was not acquired")
	}
	// Keep the entry buffered so the test can inspect the same batch that a delayed send uses.
	fallback.nextNotificationImmediate = false
	fallback.slackAPI = nil
	controller.release()

	entry := &logging.LogEntry{
		OccurredAt: wantOccurredAt,
		Level:      pkglogger.LogLevelError,
		Message:    "fallback",
	}
	controller.Handle(
		context.Background(),
		entry,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	fallback.mu.Lock()
	logs := append([]*logging.LogEntry(nil), fallback.logs...)
	fallback.mu.Unlock()
	if len(logs) != 1 {
		t.Fatalf("buffered logs = %d, want 1", len(logs))
	}
	if !logs[0].OccurredAt.Equal(wantOccurredAt) {
		t.Fatalf("OccurredAt = %s, want %s", logs[0].OccurredAt, wantOccurredAt)
	}

	batches := slackChannelBatches(fallback.config, logs, fallback.LogFormatter)
	if len(batches) != 1 || len(batches[0].messages) != 1 {
		t.Fatalf("batches = %#v, want one message", batches)
	}
	if wantTimestamp := "2026-08-20 12:04:05 +0900"; !strings.Contains(batches[0].messages[0], wantTimestamp) {
		t.Fatalf("Slack batch message = %q, want timestamp %q", batches[0].messages[0], wantTimestamp)
	}

	if err := controller.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}
