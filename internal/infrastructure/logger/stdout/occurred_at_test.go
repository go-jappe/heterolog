package stdout

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/otel"
)

func TestFallbackFormattingPreservesSourceOccurrence(t *testing.T) {
	t.Parallel()

	wantOccurredAt := time.Date(2026, time.August, 20, 3, 4, 5, 0, time.UTC)
	entry := &logging.LogEntry{
		OccurredAt: wantOccurredAt,
		Level:      pkglogger.LogLevelError,
		Message:    "fallback",
	}
	target, err := NewNotificationTarget(
		NewConfigAdapter(pkglogger.StdoutLoggerConfig{
			TimeConfig: pkglogger.TimeConfig{
				IsLocalTimeEnabled: true,
				TimeZone:           "Asia/Tokyo",
			},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat: "2006-01-02 15:04:05 -0700",
			},
		}),
		nil,
	)
	if err != nil {
		t.Fatalf("new stdout notification target: %v", err)
	}
	fallback := target.newNotificationLogger()

	logging.EnsureEntryOccurredAt(entry, fallback.Clock)
	if !entry.OccurredAt.Equal(wantOccurredAt) {
		t.Fatalf("OccurredAt = %s, want %s", entry.OccurredAt, wantOccurredAt)
	}
	if got, want := fallback.formatter.Format(entry),
		"[ERROR] 2026-08-20 12:04:05 +0900 - fallback"; got != want {
		t.Fatalf("fallback log = %q, want %q", got, want)
	}
}

func TestOpenTelemetryFallbackUsesOccurredAtAsTimestamp(t *testing.T) {
	t.Parallel()

	wantOccurredAt := time.Date(2026, time.August, 20, 3, 4, 5, 0, time.UTC)
	// OpenTelemetry形式はplain-textの時刻設定を使わないため、不正なtimezoneでも構築できます。
	target, err := NewNotificationTarget(
		NewConfigAdapter(pkglogger.StdoutLoggerConfig{
			TimeConfig: pkglogger.TimeConfig{
				IsLocalTimeEnabled: true,
				TimeZone:           "invalid/timezone",
			},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat: "2006/01/02 15:04:05",
			},
			UseOpenTelemetryFormat: true,
		}),
		resource.NewOtelResourceAdapter(&otel.Resource{}),
	)
	if err != nil {
		t.Fatalf("new OpenTelemetry stdout notification target: %v", err)
	}
	fallback := target.newNotificationLogger()

	formatted := fallback.formatter.Format(&logging.LogEntry{
		OccurredAt: wantOccurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "event",
	})
	if want := `"timestamp":"2026-08-20T03:04:05Z"`; !strings.Contains(formatted, want) {
		t.Fatalf("fallback OpenTelemetry log = %q, want %q", formatted, want)
	}
}

func TestNewConfiguredFormatterRejectsMissingOpenTelemetryResource(t *testing.T) {
	t.Parallel()

	_, err := NewConfiguredFormatter(
		true,
		nil,
		timestamp.RFC3339Formatter(),
	)
	if !errors.Is(err, ErrOpenTelemetryResourceRequired) {
		t.Fatalf("NewConfiguredFormatter() error = %v, want %v", err, ErrOpenTelemetryResourceRequired)
	}
}

func TestPlainTextFallbackHandlerDoesNotRequireOpenTelemetryResource(t *testing.T) {
	t.Parallel()

	target, err := NewNotificationTarget(
		NewConfigAdapter(pkglogger.StdoutLoggerConfig{
			TimeConfig: pkglogger.TimeConfig{TimeZone: "UTC"},
			LogBodyConfig: pkglogger.LogBodyConfig{
				LogLineDateFormat: time.RFC3339,
			},
		}),
		nil,
	)
	if err != nil {
		t.Fatalf("NewNotificationTarget() without an OpenTelemetry resource = %v", err)
	}
	if named := target.NamedInfoHandler(); named.Handler == nil {
		t.Fatal("NamedInfoHandler() returned a nil handler")
	}
}
