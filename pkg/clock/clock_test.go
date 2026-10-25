package clock

import (
	"testing"
	"time"
)

func TestUTCClockReturnsCurrentTimeInUTC(t *testing.T) {
	t.Parallel()

	clock := NewUTCClock()
	before := time.Now().UTC()
	got := clock.Now()
	after := time.Now().UTC()

	if got.Before(before) || got.After(after) {
		t.Fatalf("Now() = %s, want between %s and %s", got, before, after)
	}
	if got.Location() != time.UTC {
		t.Fatalf("Now().Location() = %s, want UTC", got.Location())
	}
}

func TestFixedClockReturnsConfiguredTime(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("test-zone", 9*60*60)
	want := time.Date(2026, time.August, 13, 10, 2, 3, 456, location)
	clock := NewFixedClock(want)

	first := clock.Now()
	second := clock.Now()
	if first != want {
		t.Fatalf("first Now() = %s, want %s", first, want)
	}
	if second != want {
		t.Fatalf("second Now() = %s, want %s", second, want)
	}
	if first.Location() != location {
		t.Fatalf("Now().Location() = %s, want %s", first.Location(), location)
	}
}

var (
	_ Clock = UTCClock{}
	_ Clock = FixedClock{}
)
