package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestConfiguredTimestampFormatterUsesMappedTimeConfigForClickHouseProfiles(t *testing.T) {
	const layout = "2006/01/02 15:04:05 Z07:00"
	timeCfg := pkglogger.TimeConfig{IsLocalTimeEnabled: true, TimeZone: "Asia/Tokyo"}
	bodyCfg := pkglogger.LogBodyConfig{LogLineDateFormat: layout}

	configs := []struct {
		name string
		cfg  ClickHouseConfigProvider
	}{
		{
			name: "normal, async, and fallback",
			cfg: NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
				TimeConfig:    timeCfg,
				LogBodyConfig: bodyCfg,
			}),
		},
		{
			name: "delivery and delivery fallback",
			cfg: NewDeliveryClickHouseLoggerConfigAdapter(
				pkglogger.DeliveryClickHouseLoggerBaseConfig{
					TimeConfig:    timeCfg,
					LogBodyConfig: bodyCfg,
				},
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

func TestClickHouseConfiguredTimestampFormatterRejectsInvalidLocalTimeZone(t *testing.T) {
	cfg := NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Invalid/ClickHouse-TimeZone",
		},
	})

	_, err := NewConfiguredTimestampFormatter(cfg)
	if !errors.Is(err, logging.ErrInvalidTimeZone) {
		t.Fatalf("NewConfiguredTimestampFormatter() error = %v, want %v", err, logging.ErrInvalidTimeZone)
	}
}

func TestClickHouseFallbackFactoryUsesConfiguredTimestampFormatter(t *testing.T) {
	baseCfg := newTestClickHouseLoggerBaseConfig()
	baseCfg.TimeConfig = pkglogger.TimeConfig{
		IsLocalTimeEnabled: true,
		TimeZone:           "Asia/Tokyo",
	}
	baseCfg.LogBodyConfig.LogLineDateFormat = "2006/01/02 15:04:05 Z07:00"
	cfg := NewClickHouseLoggerConfigAdapter(baseCfg)
	fallback, err := newFallbackLogger(
		context.Background(),
		cfg,
		&pkgclickhouse.Config{
			Endpoint: "http://127.0.0.1:8123",
			Table:    "logs",
		},
		newTestClickHouseFlushManagerConfig(),
		newTestClickHouseResourceProvider(),
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
