package otel

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
)

func TestToOtelLogEntryUsesOccurredAtAsCanonicalProtocolTimestamp(t *testing.T) {
	t.Parallel()

	entry := &logging.LogEntry{
		OccurredAt: time.Date(2026, time.August, 20, 3, 4, 5, 123, time.UTC),
	}

	got := ToOtelLogEntry(entry, &resource.OtelResourceAdapter{})
	if want := "2026-08-20T03:04:05.000000123Z"; got.Timestamp != want {
		t.Fatalf("Timestamp = %q, want %q", got.Timestamp, want)
	}
}

func TestToOtelLogEntryPreservesFields(t *testing.T) {
	t.Parallel()

	want := map[string]any{
		"success": true,
		"count":   2,
		"object":  map[string]any{"name": "value"},
	}
	entry := &logging.LogEntry{
		Fields: want,
	}

	got := ToOtelLogEntry(entry, &resource.OtelResourceAdapter{})
	if !reflect.DeepEqual(got.Attributes.Fields, want) {
		t.Fatalf("Attributes.Fields = %#v, want %#v", got.Attributes.Fields, want)
	}
}

func TestToOtelLogEntryOmitsOptionalResourceWhenNotConfigured(t *testing.T) {
	t.Parallel()

	got := ToOtelLogEntry(&logging.LogEntry{Message: "event"}, nil)
	if got.Resource != nil {
		t.Fatalf("Resource = %#v, want omitted", got.Resource)
	}
	if got.Body != "event" {
		t.Fatalf("Body = %q, want %q", got.Body, "event")
	}
}
