package file

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/clock"
)

func TestInternalEntryUsesFreshEventTimeInsteadOfApplicationOccurrence(t *testing.T) {
	applicationOccurrence := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)
	internalOccurrence := applicationOccurrence.Add(time.Minute)
	logger := &FileLogger{
		Clock:  clock.NewFixedClock(internalOccurrence),
		logger: DefaultLogger,
	}
	entry := logger.InternalInfoEntryWithContext(context.Background(), "internal")
	if !entry.OccurredAt.Equal(internalOccurrence) {
		t.Fatalf("internal OccurredAt = %s, want fresh event time %s", entry.OccurredAt, internalOccurrence)
	}
}
