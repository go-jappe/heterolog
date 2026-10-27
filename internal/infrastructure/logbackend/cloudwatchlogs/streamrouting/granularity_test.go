package streamrouting

import (
	"errors"
	"testing"
	"time"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

func TestSuffix(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("JST", 9*60*60)
	at := time.Date(2026, time.August, 13, 17, 42, 0, 0, location)
	tests := []struct {
		name        string
		granularity pkgcloudwatchlogs.StreamRouting
		want        string
	}{
		{name: "zero value", want: ""},
		{name: "none", granularity: pkgcloudwatchlogs.StreamRoutingNone, want: ""},
		{name: "monthly", granularity: pkgcloudwatchlogs.StreamRoutingMonthly, want: "2026/08"},
		{name: "daily", granularity: pkgcloudwatchlogs.StreamRoutingDaily, want: "2026/08/13"},
		{name: "hourly", granularity: pkgcloudwatchlogs.StreamRoutingHourly, want: "2026/08/13/17"},
		{name: "normalizes value", granularity: " HOURLY ", want: "2026/08/13/17"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Suffix(tt.granularity, at)
			if err != nil {
				t.Fatalf("Suffix(%q, at) error = %v", tt.granularity, err)
			}
			if got != tt.want {
				t.Fatalf("Suffix(%q, at) = %q, want %q", tt.granularity, got, tt.want)
			}
		})
	}
}

func TestSuffixUsesProvidedTimeLocation(t *testing.T) {
	t.Parallel()

	instantUTC := time.Date(2026, time.August, 31, 15, 30, 0, 0, time.UTC)
	jst := time.FixedZone("JST", 9*60*60)

	utcSuffix, err := Suffix(pkgcloudwatchlogs.StreamRoutingDaily, instantUTC)
	if err != nil {
		t.Fatalf("Suffix(Daily, UTC) error = %v", err)
	}
	jstSuffix, err := Suffix(pkgcloudwatchlogs.StreamRoutingDaily, instantUTC.In(jst))
	if err != nil {
		t.Fatalf("Suffix(Daily, JST) error = %v", err)
	}
	if utcSuffix != "2026/08/31" {
		t.Fatalf("UTC suffix = %q, want %q", utcSuffix, "2026/08/31")
	}
	if jstSuffix != "2026/09/01" {
		t.Fatalf("JST suffix = %q, want %q", jstSuffix, "2026/09/01")
	}
}

func TestSuffixRejectsUnsupportedGranularity(t *testing.T) {
	t.Parallel()

	_, err := Suffix("weekly", time.Now())
	if !errors.Is(err, pkgcloudwatchlogs.ErrInvalidStreamRouting) {
		t.Fatalf("Suffix(weekly, at) error = %v, want ErrInvalidGranularity", err)
	}
}
