package timestamp

import (
	"testing"
	"time"
)

func TestFormatterFormatsFixedInstantInConfiguredLocation(t *testing.T) {
	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	formatter, err := NewFormatter(location, "2006-01-02 15:04:05 -0700")
	if err != nil {
		t.Fatalf("NewFormatter() error = %v", err)
	}

	at := time.Date(2026, time.August, 22, 3, 4, 5, 0, time.UTC)
	if got, want := formatter.Format(at), "2026-08-22 12:04:05 +0900"; got != want {
		t.Fatalf("Format() = %q, want %q", got, want)
	}
}

func TestFormatterRejectsMissingConfiguration(t *testing.T) {
	if _, err := NewFormatter(nil, time.RFC3339); err == nil {
		t.Fatal("NewFormatter() error = nil, want missing location error")
	}
	if _, err := NewFormatter(time.UTC, " "); err == nil {
		t.Fatal("NewFormatter() error = nil, want missing layout error")
	}
}

func TestZeroFormatterUsesUTCRFC3339(t *testing.T) {
	var formatter Formatter
	at := time.Date(2026, time.August, 22, 3, 4, 5, 0, time.FixedZone("JST", 9*60*60))
	if got, want := formatter.Format(at), "2026-08-21T18:04:05Z"; got != want {
		t.Fatalf("zero formatter Format() = %q, want %q", got, want)
	}
}
