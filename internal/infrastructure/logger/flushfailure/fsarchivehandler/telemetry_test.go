package fsarchivehandler

import (
	"context"
	"testing"
	"time"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

func TestHandleFailureCountsArchivedRecords(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 29, 1, 2, 3, 0, time.UTC)
	pipeline := newTestArchiveHandler(t, now, nil)
	meter := observertest.NewMeter()
	pipeline.handler.Meter = meter
	result := loggerfailure.SendResult{
		Meta: loggerfailure.Meta{Backend: "loki", RequestID: "request-1", AttemptedAt: now},
		RetryableRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("retry-id", time.Time{}, 429, "rate_limited"),
		},
		DeadRecords: []loggerfailure.FailedRecord{
			archiveFailedRecord("dead-id", time.Time{}, 400, "bad_request"),
		},
	}

	if err := pipeline.HandleFailure(context.Background(), result); err != nil {
		t.Fatalf("HandleFailure() error = %v", err)
	}

	want := map[string]int64{
		observertest.Key(telemetry.FailureRecords, telemetry.AttrBackend, "loki", telemetry.AttrAction, telemetry.ActionArchived): 2,
	}
	got := meter.Counters()
	if len(got) != len(want) || got[observertest.Key(telemetry.FailureRecords, telemetry.AttrBackend, "loki", telemetry.AttrAction, telemetry.ActionArchived)] != 2 {
		t.Errorf("counters = %v, want %v", got, want)
	}
}
