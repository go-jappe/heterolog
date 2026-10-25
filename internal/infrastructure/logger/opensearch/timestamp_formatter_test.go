package opensearch

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestConfiguredTimestampFormatterUsesMappedTimeConfigForOpenSearchProfiles(t *testing.T) {
	const layout = "2006/01/02 15:04:05 Z07:00"
	timeCfg := pkglogger.TimeConfig{IsLocalTimeEnabled: true, TimeZone: "Asia/Tokyo"}
	bodyCfg := pkglogger.LogBodyConfig{LogLineDateFormat: layout}
	deliveryCfg := pkglogger.DeliveryOpenSearchLoggerBaseConfig{
		TimeConfig:    timeCfg,
		LogBodyConfig: bodyCfg,
	}

	configs := []struct {
		name string
		cfg  OpenSearchLoggerConfigProvider
	}{
		{
			name: "normal and async",
			cfg: NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
				TimeConfig:    timeCfg,
				LogBodyConfig: bodyCfg,
			}),
		},
		{name: "delivery", cfg: NewDeliveryOpenSearchLoggerConfigAdapter(deliveryCfg)},
		{
			name: "delivery fallback",
			cfg: NewDeliveryOpenSearchFallbackLoggerConfigAdapter(
				deliveryCfg,
				pkglogger.DeliveryInternalLogOpenSearchBatchConfig{},
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

func TestOpenSearchConfiguredTimestampFormatterRejectsInvalidLocalTimeZone(t *testing.T) {
	cfg := NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Invalid/OpenSearch-TimeZone",
		},
	})

	_, err := NewConfiguredTimestampFormatter(cfg)
	if !errors.Is(err, logging.ErrInvalidTimeZone) {
		t.Fatalf("NewConfiguredTimestampFormatter() error = %v, want %v", err, logging.ErrInvalidTimeZone)
	}
}
