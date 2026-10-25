package logging

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/clock"
)

func TestResolveOccurredAtReturnsClockInstantAsUTC(t *testing.T) {
	location := time.FixedZone("test-local", 9*60*60)
	want := time.Date(2026, 8, 20, 18, 30, 0, 123, location)

	got := ResolveOccurredAt(clock.NewFixedClock(want))
	if !got.Equal(want) {
		t.Fatalf("ResolveOccurredAt() = %s, want same instant as %s", got, want)
	}
	if got.Location() != time.UTC {
		t.Fatalf("ResolveOccurredAt().Location() = %s, want UTC", got.Location())
	}
}

func TestEnsureEntryOccurredAtStoresResolvedClockTimeOnce(t *testing.T) {
	want := time.Date(2026, 8, 20, 9, 31, 0, 123, time.UTC)
	entry := &LogEntry{}

	got := EnsureEntryOccurredAt(entry, clock.NewFixedClock(want))
	if !got.Equal(want) || !entry.OccurredAt.Equal(want) {
		t.Fatalf("EnsureEntryOccurredAt() = %s, entry.OccurredAt = %s, want %s", got, entry.OccurredAt, want)
	}

	if got := EnsureEntryOccurredAt(entry, clock.NewFixedClock(want.Add(time.Hour))); !got.Equal(want) {
		t.Fatalf("EnsureEntryOccurredAt() replaced occurrence: got %s, want %s", got, want)
	}
}

func TestEnsureEntryOccurredAtPreservesZeroReturnedByClock(t *testing.T) {
	t.Parallel()

	entry := &LogEntry{}
	got := EnsureEntryOccurredAt(entry, zeroOccurrenceClock{})

	if !got.IsZero() || !entry.OccurredAt.IsZero() {
		t.Fatalf("EnsureEntryOccurredAt() = %s, entry.OccurredAt = %s, want zero time returned by Clock", got, entry.OccurredAt)
	}
}

type zeroOccurrenceClock struct{}

func (zeroOccurrenceClock) Now() time.Time { return time.Time{} }
