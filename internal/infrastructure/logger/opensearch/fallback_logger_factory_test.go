package opensearch

import (
	"context"
	"testing"
	"time"

	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestOpenSearchFallbackConstructorStartsOwnedBatchWorker(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = time.Millisecond
	cfg.timeConfig = pkglogger.TimeConfig{
		IsLocalTimeEnabled: true,
		TimeZone:           "Asia/Tokyo",
	}
	cfg.logLineDateFormat = "2006/01/02 15:04:05 Z07:00"
	clientCfg := infraopensearch.NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
		Endpoint: "http://127.0.0.1:9200",
		Timeout:  time.Second,
		AuthConfig: pkgopensearch.AuthConfig{
			Type: pkgopensearch.AuthTypeNone,
		},
	})
	resourceProvider := NewMockResourceProvider()
	tracer, meter := newTestObserver()

	tickerCreations := 0
	fallback, err := newFallbackLogger(
		cfg,
		clientCfg,
		nil,
		cfg,
		resourceProvider,
		tracer,
		meter,
		WithFlushIntervalGenerator(func(time.Duration) *time.Ticker {
			tickerCreations++
			return time.NewTicker(time.Hour)
		}),
	)
	if err != nil {
		t.Fatalf("create OpenSearch fallback logger: %v", err)
	}
	t.Cleanup(func() {
		if err := fallback.Close(context.Background()); err != nil {
			t.Fatalf("close OpenSearch fallback logger: %v", err)
		}
	})

	if tickerCreations != 1 {
		t.Fatalf("fallback constructor created %d worker ticker(s), want 1", tickerCreations)
	}
	if fallback.entries == nil {
		t.Fatal("fallback batch queue is nil")
	}
	if fallback.ticker == nil {
		t.Fatal("fallback worker ticker is nil")
	}
	if fallback.stopCh == nil {
		t.Fatal("fallback worker stop channel is nil")
	}
	occurredAt := time.Date(2026, 8, 20, 18, 4, 5, 0, time.UTC)
	if got, want := fallback.TimestampFormatter.Format(occurredAt),
		"2026/08/21 03:04:05 +09:00"; got != want {
		t.Fatalf("fallback display time = %q, want %q", got, want)
	}
}
