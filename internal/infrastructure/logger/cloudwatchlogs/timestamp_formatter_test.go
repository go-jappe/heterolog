package cloudwatchlogs

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestNewConfiguredTimestampFormatterUsesConfiguredLocalTimeZone(t *testing.T) {
	t.Parallel()

	cfg := NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Asia/Tokyo",
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat: time.RFC3339,
		},
	})

	configuredFormatter, err := NewConfiguredTimestampFormatter(cfg)
	if err != nil {
		t.Fatalf("NewConfiguredTimestampFormatter() error = %v", err)
	}
	if got := configuredFormatter.Location().String(); got != "Asia/Tokyo" {
		t.Fatalf("TimestampFormatter.Location() = %q, want %q", got, "Asia/Tokyo")
	}
	if got, want := configuredFormatter.Format(time.Date(2026, 8, 20, 18, 4, 5, 0, time.UTC)),
		"2026-08-21T03:04:05+09:00"; got != want {
		t.Fatalf("TimestampFormatter.Format() = %q, want %q", got, want)
	}
}

func TestNewConfiguredTimestampFormatterRejectsInvalidLocalTimeZone(t *testing.T) {
	t.Parallel()

	cfg := NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Invalid/CloudWatch-TimeZone",
		},
	})

	_, err := NewConfiguredTimestampFormatter(cfg)
	if !errors.Is(err, logging.ErrInvalidTimeZone) {
		t.Fatalf("NewConfiguredTimestampFormatter() error = %v, want %v", err, logging.ErrInvalidTimeZone)
	}
}
