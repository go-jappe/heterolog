package opensearch

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func openSearchTelemetryEntries(n int) []*logging.LogEntry {
	entries := make([]*logging.LogEntry, 0, n)
	for range n {
		entries = append(entries, &logging.LogEntry{
			OccurredAt: time.Date(2026, 10, 29, 1, 2, 3, 0, time.UTC),
			Level:      pkglogger.LogLevelInfo,
			Message:    "telemetry",
		})
	}
	return entries
}

func openSearchRecords(meter *observertest.Meter, outcome string) int64 {
	return meter.Count(observertest.Key(telemetry.BackendRecords, telemetry.AttrBackend, "opensearch", telemetry.AttrOutcome, outcome))
}

func TestFlushRecordsBulkRequestAndRecords(t *testing.T) {
	tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
	fm := newTestFlushManager(NewMockOpenSearchClient(), NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	if err := fm.Flush(context.Background(), openSearchTelemetryEntries(3)); err != nil {
		t.Fatal(err)
	}

	if got := openSearchRecords(meter, telemetry.OutcomeSuccess); got != 3 {
		t.Errorf("successful records = %d, want 3; counters = %v", got, meter.Counters())
	}
	spans := tracer.Spans(SpanOpenSearchBulk)
	if len(spans) != 1 || spans[0].Ends() != 1 || len(spans[0].Errors()) != 0 {
		t.Fatalf("bulk spans = %v, want one ended span without errors", spans)
	}
	if v, _ := spans[0].Attr(telemetry.SpanAttrRecords); v != 3 {
		t.Errorf("bulk span records = %v, want 3", v)
	}
	if got := meter.HistogramValues(observertest.Key(telemetry.BackendRequestDuration, telemetry.AttrBackend, "opensearch", telemetry.AttrOutcome, telemetry.OutcomeSuccess)); len(got) != 1 {
		t.Errorf("request durations = %v, want one", got)
	}
}

func TestFlushRecordsIndexRequestForOneEntry(t *testing.T) {
	tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
	fm := newTestFlushManager(NewMockOpenSearchClient(), NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	if err := fm.Flush(context.Background(), openSearchTelemetryEntries(1)); err != nil {
		t.Fatal(err)
	}

	if got := openSearchRecords(meter, telemetry.OutcomeSuccess); got != 1 {
		t.Errorf("successful records = %d, want 1; counters = %v", got, meter.Counters())
	}
	if spans := tracer.Spans(SpanOpenSearchIndex); len(spans) != 1 || spans[0].Ends() != 1 {
		t.Fatalf("index spans = %v, want one ended span", spans)
	}
}

func TestFlushRecordsFailedBulkRequest(t *testing.T) {
	tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
	client := NewMockOpenSearchClient()
	client.returnError = true
	fm := newTestFlushManager(client, NewMockOpenSearchFlushManagerConfig(), tracer, meter)

	// failure handlerがないため、Flushは失敗を返します。
	_ = fm.Flush(context.Background(), openSearchTelemetryEntries(2))

	failed := openSearchRecords(meter, telemetry.OutcomeRetryable) +
		openSearchRecords(meter, telemetry.OutcomeDead) +
		openSearchRecords(meter, telemetry.OutcomeQuarantine)
	if success := openSearchRecords(meter, telemetry.OutcomeSuccess); success != 0 || failed != 2 {
		t.Errorf("records success = %d, failed = %d, want 0 and 2; counters = %v", success, failed, meter.Counters())
	}
	spans := tracer.Spans(SpanOpenSearchBulk)
	if len(spans) == 0 {
		t.Fatal("no bulk span")
	}
	for _, sp := range spans {
		if len(sp.Errors()) != 1 {
			t.Errorf("bulk span errors = %v, want the request error", sp.Errors())
		}
	}
}
