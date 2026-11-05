package formatter

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func decodeGoogleCloudLine(t *testing.T, line string) map[string]any {
	t.Helper()
	if strings.Contains(line, "\n") {
		t.Fatalf("line must not contain a newline: %q", line)
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("line is not JSON: %q: %v", line, err)
	}
	return decoded
}

// Cloud Loggingの名前と異なるWARN・FATALも含め、全levelが読み取れる重大度になることを確かめます。
func TestGoogleCloudLoggingFormatterMapsLevelsToSeverity(t *testing.T) {
	t.Parallel()

	for level, want := range map[pkglogger.LogLevel]string{
		pkglogger.LogLevelDebug: "DEBUG",
		pkglogger.LogLevelInfo:  "INFO",
		pkglogger.LogLevelWarn:  "WARNING",
		pkglogger.LogLevelError: "ERROR",
		pkglogger.LogLevelFatal: "CRITICAL",
		pkglogger.LogLevel(0):   "DEFAULT",
	} {
		line := GoogleCloudLoggingFormatter{}.Format(&logging.LogEntry{Level: level, Message: "event"})
		if got := decodeGoogleCloudLine(t, line)["severity"]; got != want {
			t.Errorf("severity of level %d = %v, want %q", level, got, want)
		}
	}
}

func TestGoogleCloudLoggingFormatterWritesEveryPartOfTheEntry(t *testing.T) {
	t.Parallel()

	line := GoogleCloudLoggingFormatter{}.Format(&logging.LogEntry{
		OccurredAt: time.Date(2026, time.August, 20, 12, 4, 5, 6, time.FixedZone("JST", 9*60*60)),
		Level:      pkglogger.LogLevelError,
		Message:    "first\nsecond <&>",
		TraceID:    "trace",
		SpanID:     "span",
		RequestID:  "request",
		Err:        errors.New("boom"),
		File:       []string{"main.go"},
		Line:       []int{12},
		FuncName:   []string{"main.run"},
		Internal:   true,
		Source:     "stdout",
		Sampled:    true,
		// applicationのkeyが特別な項目を上書きしないことも確かめます。
		Fields: map[string]any{"severity": "DEBUG", "user": "a"},
	})

	got := decodeGoogleCloudLine(t, line)
	want := map[string]any{
		"severity":       "ERROR",
		"message":        "first\nsecond <&>",
		"time":           "2026-08-20T03:04:05.000000006Z",
		"trace_id":       "trace",
		"span_id":        "span",
		"request_id":     "request",
		"error":          "boom",
		"stacktrace":     "at main.run (main.go:12)",
		"internal_error": true,
		"source":         "stdout",
		"sampled":        true,
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s = %v, want %v", key, got[key], value)
		}
	}
	fields, ok := got["fields"].(map[string]any)
	if !ok || fields["severity"] != "DEBUG" || fields["user"] != "a" {
		t.Errorf("fields = %v", got["fields"])
	}
	if len(got) != len(want)+1 {
		t.Errorf("keys = %v, want only the keys above and fields", got)
	}
	if !strings.HasPrefix(line, `{"severity":"ERROR","message":"first\nsecond <&>",`) {
		t.Errorf("line = %q, want the severity and message first, without HTML escaping", line)
	}
}

func TestGoogleCloudLoggingFormatterOmitsEmptyParts(t *testing.T) {
	t.Parallel()

	line := GoogleCloudLoggingFormatter{}.Format(&logging.LogEntry{
		OccurredAt: time.Date(2026, time.August, 20, 3, 4, 5, 0, time.UTC),
		Level:      pkglogger.LogLevelInfo,
		Message:    "event",
	})
	if want := `{"severity":"INFO","message":"event","time":"2026-08-20T03:04:05Z"}`; line != want {
		t.Fatalf("line = %q, want %q", line, want)
	}
}

// fieldsをJSONにできない場合も、重大度と本文を持つ1行のJSONを出力することを確かめます。
func TestGoogleCloudLoggingFormatterKeepsSeverityWhenFieldsCannotBeEncoded(t *testing.T) {
	t.Parallel()

	line := GoogleCloudLoggingFormatter{}.Format(&logging.LogEntry{
		Level:   pkglogger.LogLevelError,
		Message: "event",
		Fields:  map[string]any{"bad": make(chan int)},
	})

	got := decodeGoogleCloudLine(t, line)
	if got["severity"] != "ERROR" || got["message"] != "event" {
		t.Fatalf("line = %q, want the severity and message", line)
	}
	if _, ok := got["fields"]; ok {
		t.Fatalf("line = %q, want no fields", line)
	}
	if reason, _ := got["fields_error"].(string); reason == "" {
		t.Fatalf("line = %q, want the reason in fields_error", line)
	}
}
