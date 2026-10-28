package telemetry

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

// failureHandlerFuncは、関数をfailureResultHandlerとして使います。
type failureHandlerFunc func(context.Context, failure.SendResult) error

func (f failureHandlerFunc) Handle(ctx context.Context, result failure.SendResult) error {
	return f(ctx, result)
}

func TestCountFailureRecordsCountsHandledRecords(t *testing.T) {
	meter := observertest.NewMeter()
	result := failure.SendResult{
		Meta:             failure.Meta{Backend: "clickhouse"},
		RetryableRecords: make([]failure.FailedRecord, 2),
		DeadRecords:      make([]failure.FailedRecord, 1),
	}
	handler := CountFailureRecords(failureHandlerFunc(func(context.Context, failure.SendResult) error { return nil }), meter, ActionDegraded)

	if err := handler.Handle(context.Background(), result); err != nil {
		t.Fatal(err)
	}

	if got := meter.Count(observertest.Key(FailureRecords, AttrBackend, "clickhouse", AttrAction, ActionDegraded)); got != 3 {
		t.Errorf("degraded records = %d, want 3; counters = %v", got, meter.Counters())
	}
}

func TestCountFailureRecordsCountsFailedHandling(t *testing.T) {
	meter := observertest.NewMeter()
	handleErr := errors.New("stdout closed")
	result := failure.SendResult{
		Meta:             failure.Meta{Backend: "clickhouse"},
		RetryableRecords: make([]failure.FailedRecord, 2),
	}
	handler := CountFailureRecords(failureHandlerFunc(func(context.Context, failure.SendResult) error { return handleErr }), meter, ActionDegraded)

	if err := handler.Handle(context.Background(), result); !errors.Is(err, handleErr) {
		t.Fatalf("Handle error = %v, want %v", err, handleErr)
	}

	want := map[string]int64{observertest.Key(FailureRecords, AttrBackend, "clickhouse", AttrAction, ActionFailed): 2}
	if got := meter.Counters(); !equalCounts(got, want) {
		t.Errorf("counters = %v, want %v", got, want)
	}
}
