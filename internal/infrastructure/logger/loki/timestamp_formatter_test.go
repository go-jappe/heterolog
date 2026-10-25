package loki

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestConfiguredTimestampFormatterUsesMappedTimeConfigForLokiProfiles(t *testing.T) {
	const layout = "2006/01/02 15:04:05 Z07:00"
	timeCfg := pkglogger.TimeConfig{IsLocalTimeEnabled: true, TimeZone: "Asia/Tokyo"}
	bodyCfg := pkglogger.LogBodyConfig{LogLineDateFormat: layout}
	deliveryCfg := pkglogger.DeliveryLokiLoggerBaseConfig{
		TimeConfig:    timeCfg,
		LogBodyConfig: bodyCfg,
	}

	configs := []struct {
		name string
		cfg  LokiConfigProvider
	}{
		{
			name: "normal and async",
			cfg: NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
				TimeConfig:    timeCfg,
				LogBodyConfig: bodyCfg,
			}),
		},
		{name: "delivery", cfg: NewDeliveryLokiLoggerConfigAdapter(deliveryCfg)},
		{
			name: "delivery fallback",
			cfg: NewDeliveryLokiFallbackLoggerConfigAdapter(
				deliveryCfg,
				pkglogger.DeliveryInternalLogLokiBatchConfig{},
			),
		},
	}

	occurredAt := time.Date(2026, 8, 20, 18, 4, 5, 0, time.UTC)
	for _, tt := range configs {
		t.Run(tt.name, func(t *testing.T) {
			configuredFormatter, err := NewConfiguredTimestampFormatter(tt.cfg)
			if err != nil {
				t.Fatalf("NewConfiguredTimestampFormatter() error = %v", err)
			}
			if got, want := configuredFormatter.Format(occurredAt),
				"2026/08/21 03:04:05 +09:00"; got != want {
				t.Fatalf("configured display time = %q, want %q", got, want)
			}
		})
	}
}

func TestLokiConfiguredTimestampFormatterRejectsInvalidLocalTimeZone(t *testing.T) {
	cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Invalid/Loki-TimeZone",
		},
	})

	_, err := NewConfiguredTimestampFormatter(cfg)
	if !errors.Is(err, logging.ErrInvalidTimeZone) {
		t.Fatalf("NewConfiguredTimestampFormatter() error = %v, want %v", err, logging.ErrInvalidTimeZone)
	}
}

func TestLokiFallbackFactoryUsesConfiguredTimestampFormatter(t *testing.T) {
	cfg := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Asia/Tokyo",
		},
		RuntimeConfig: pkglogger.RuntimeConfig{LogLevel: pkglogger.LogLevelDebug},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat: "2006/01/02 15:04:05 Z07:00",
		},
		FlushInterval:           time.Hour,
		MaxEntries:              10,
		BufferFlushBytes:        1024,
		FallbackQueueMaxEntries: 10,
		MaxLineBytes:            1024,
		MaxPushRequestBytes:     4096,
	})
	fallback, err := newFallbackLogger(
		context.Background(),
		cfg,
		&pkgloki.Config{Endpoint: "http://127.0.0.1:3100"},
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("newFallbackLogger() error = %v", err)
	}
	t.Cleanup(func() {
		if err := fallback.Close(context.Background()); err != nil {
			t.Fatalf("FallbackLogger.Close() error = %v", err)
		}
	})

	occurredAt := time.Date(2026, 8, 20, 18, 4, 5, 0, time.UTC)
	formatted := fallback.LogFormatter.Format(&logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "event",
	})
	if want := "2026/08/21 03:04:05 +09:00"; !strings.Contains(formatted, want) {
		t.Fatalf("fallback log = %q, want timestamp %q", formatted, want)
	}
}
