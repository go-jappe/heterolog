package opensearch

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestOpenSearchLoggerOptionsUseClockAndTimeProvider(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = time.Hour
	cfg.logLevel = pkglogger.LogLevelDebug
	client := NewMockOpenSearchClient()
	clockTime := time.Date(2026, 6, 24, 1, 2, 3, 0, time.UTC)
	providerTime := time.Date(2026, 6, 24, 4, 5, 6, 0, time.UTC)

	logger, err := NewOpenSearchLogger(
		context.Background(),
		cfg,
		client,
		cfg,
		NewMockResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{Clock: pkgclock.NewUTCClock()},
		WithClock(fixedOpenSearchOptionClock{t: clockTime}),
		WithTimeProvider(func() time.Time { return providerTime }),
	)
	if err != nil {
		t.Fatalf("NewOpenSearchLogger returned error: %v", err)
	}
	defer func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	}()

	if err := logger.testInfo(context.Background(), "option clock message"); err != nil {
		t.Fatalf("Info returned error: %v", err)
	}
	entries := openSearchLoggerEntriesForTest(logger)
	if len(entries) != 1 {
		t.Fatalf("expected one queued entry, got %d", len(entries))
	}
	if !entries[0].OccurredAt.Equal(clockTime) {
		t.Fatalf("expected queued occurrence %s, got %s", clockTime, entries[0].OccurredAt)
	}

	logger.updateLastFlushTime()
	if got := openSearchLoggerLastFlushTimeForTest(logger); !got.Equal(providerTime) {
		t.Fatalf("expected lastFlushTime %s, got %s", providerTime, got)
	}
}

func openSearchLoggerEntriesForTest(logger *OpenSearchLogger) []*logging.LogEntry {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return append([]*logging.LogEntry(nil), logger.entries...)
}

func openSearchLoggerLastFlushTimeForTest(logger *OpenSearchLogger) time.Time {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return logger.lastFlushTime
}

func TestOpenSearchLoggerOptionUsesFlushIntervalGenerator(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = 30 * time.Second
	cfg.logLevel = pkglogger.LogLevelDebug
	client := NewMockOpenSearchClient()
	var gotDuration time.Duration

	logger, err := NewOpenSearchLogger(
		context.Background(),
		cfg,
		client,
		cfg,
		NewMockResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{Clock: pkgclock.NewUTCClock()},
		WithFlushIntervalGenerator(func(d time.Duration) *time.Ticker {
			gotDuration = d
			return time.NewTicker(time.Hour)
		}),
	)
	if err != nil {
		t.Fatalf("NewOpenSearchLogger returned error: %v", err)
	}
	defer func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	}()

	if gotDuration != cfg.flushInterval {
		t.Fatalf("expected flush interval %s, got %s", cfg.flushInterval, gotDuration)
	}
}

type fixedOpenSearchOptionClock struct {
	t time.Time
}

func (c fixedOpenSearchOptionClock) Now() time.Time {
	return c.t
}

var _ pkgclock.Clock = fixedOpenSearchOptionClock{}
