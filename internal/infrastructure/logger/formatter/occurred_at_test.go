package formatter

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestPlainTextFormatterUsesOccurredAtWithConfiguredDisplayTimezone(t *testing.T) {
	t.Parallel()

	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	timestampFormatter, err := timestamp.NewFormatter(location, "2006-01-02 15:04:05 -0700")
	if err != nil {
		t.Fatalf("create timestamp formatter: %v", err)
	}
	entry := &logging.LogEntry{
		OccurredAt: time.Date(2026, time.August, 20, 3, 4, 5, 0, time.UTC),
		Level:      pkglogger.LogLevelInfo,
		Message:    "event",
	}

	plainTextFormatter := NewPlainTextFormatter(timestampFormatter)
	got := plainTextFormatter.Format(entry)
	if want := "[INFO] 2026-08-20 12:04:05 +0900 - event"; got != want {
		t.Fatalf("PlainTextFormatter.Format() = %q, want %q", got, want)
	}
	if strings.Contains(got, "2026-08-20 03:04:05") {
		t.Fatalf("PlainTextFormatter.Format() used UTC representation instead of configured timezone: %q", got)
	}
}

// zero値のtimestamp.FormatterはUTCのRFC3339として動作する契約です。PlainTextFormatterへ
// 渡した場合も同じ表示規則で整形し、level、本文、error、fieldsを失ってはいけません。
func TestPlainTextFormatterZeroValueUsesUTCRFC3339(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		formatter PlainTextFormatter
	}{
		{name: "zero formatter", formatter: PlainTextFormatter{}},
		{
			name:      "explicit zero timestamp formatter",
			formatter: NewPlainTextFormatter(timestamp.Formatter{}),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := test.formatter.Format(&logging.LogEntry{
				OccurredAt: time.Date(2026, time.August, 20, 12, 4, 5, 0, time.FixedZone("JST", 9*60*60)),
				Level:      pkglogger.LogLevelWarn,
				Message:    "event",
				Err:        errors.New("write failed"),
				Fields:     map[string]any{"attempt": 2},
			})
			want := `[WARN] 2026-08-20T03:04:05Z - event - err: write failed fields:{"attempt":2}`
			if got != want {
				t.Fatalf("PlainTextFormatter.Format() = %q, want %q", got, want)
			}
		})
	}
}

// OpenTelemetry log recordのresourceは省略可能です。resourceを持たないOtelFormatterも
// log recordを出力し、resourceだけを省略します。
func TestOtelFormatterWithoutResourceOmitsOnlyResource(t *testing.T) {
	t.Parallel()

	entry := &logging.LogEntry{
		OccurredAt: time.Date(2026, time.August, 20, 3, 4, 5, 0, time.UTC),
		Level:      pkglogger.LogLevelError,
		Message:    "event",
		Err:        errors.New("write failed"),
		Fields:     map[string]any{"attempt": 2},
	}
	want := `{"attributes":{"exception.message":"write failed","fields":{"attempt":2}},` +
		`"body":"event","severity_number":17,"severity_text":"ERROR","timestamp":"2026-08-20T03:04:05Z"}`
	for name, otelFormatter := range map[string]*OtelFormatter{
		"zero formatter": {},
		"nil formatter":  nil,
	} {
		if got := otelFormatter.Format(entry); got != want {
			t.Fatalf("%s: OtelFormatter.Format() = %q, want %q", name, got, want)
		}
	}
}
