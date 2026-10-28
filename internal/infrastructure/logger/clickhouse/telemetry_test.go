package clickhouse

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func newObservedClickHouseFlushManager(t *testing.T, client Client) (*ClickHouseFlushManager, *observertest.Tracer, *observertest.Meter) {
	t.Helper()
	fm := newTestClickHouseLogger(t, client).flushManager.(*ClickHouseFlushManager)
	tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
	fm.Tracer, fm.Meter = tracer, meter
	return fm, tracer, meter
}

func clickHouseTelemetryEntries(n int) []*logging.LogEntry {
	entries := make([]*logging.LogEntry, 0, n)
	for range n {
		entries = append(entries, newTestLogEntry(pkglogger.LogLevelInfo, "telemetry", nil))
	}
	return entries
}

func clickHouseRecords(meter *observertest.Meter, outcome string) int64 {
	return meter.Count(observertest.Key(telemetry.BackendRecords, telemetry.AttrBackend, "clickhouse", telemetry.AttrOutcome, outcome))
}

func TestFlushRecordsInsertRequestAndRecords(t *testing.T) {
	fm, tracer, meter := newObservedClickHouseFlushManager(t, &recordingClickHouseClient{})

	if err := fm.Flush(context.Background(), clickHouseTelemetryEntries(3)); err != nil {
		t.Fatal(err)
	}

	if got := clickHouseRecords(meter, telemetry.OutcomeSuccess); got != 3 {
		t.Errorf("successful records = %d, want 3; counters = %v", got, meter.Counters())
	}
	spans := tracer.Spans(SpanClickHouseInsert)
	if len(spans) != 1 || spans[0].Ends() != 1 || len(spans[0].Errors()) != 0 {
		t.Fatalf("insert spans = %v, want one ended span without errors", spans)
	}
	if v, _ := spans[0].Attr(telemetry.SpanAttrRecords); v != 3 {
		t.Errorf("insert span records = %v, want 3", v)
	}
	if got := meter.HistogramValues(observertest.Key(telemetry.BackendRequestDuration, telemetry.AttrBackend, "clickhouse", telemetry.AttrOutcome, telemetry.OutcomeSuccess)); len(got) != 1 {
		t.Errorf("request durations = %v, want one", got)
	}
}

func TestFlushRecordsFailedInsert(t *testing.T) {
	insertErr := &pkgclickhouse.HTTPStatusError{Code: http.StatusTooManyRequests, Status: "429 Too Many Requests"}
	fm, tracer, meter := newObservedClickHouseFlushManager(t, &recordingClickHouseClient{err: insertErr})

	// failure handlerがないため、Flushは失敗を返します。
	_ = fm.Flush(context.Background(), clickHouseTelemetryEntries(2))

	if got := clickHouseRecords(meter, telemetry.OutcomeRetryable); got != 2 {
		t.Errorf("retryable records = %d, want 2; counters = %v", got, meter.Counters())
	}
	spans := tracer.Spans(SpanClickHouseInsert)
	if len(spans) != 1 || len(spans[0].Errors()) != 1 {
		t.Fatalf("insert spans = %v, want one span with the error", spans)
	}
	if got := meter.HistogramValues(observertest.Key(telemetry.BackendRequestDuration, telemetry.AttrBackend, "clickhouse", telemetry.AttrOutcome, telemetry.OutcomeFailure)); len(got) != 1 {
		t.Errorf("failed request durations = %v, want one", got)
	}
}

func TestFallbackFlushDoesNotCountRecords(t *testing.T) {
	fm, _, meter := newObservedClickHouseFlushManager(t, &recordingClickHouseClient{})

	fm.FlushWithNoFallback(context.Background(), clickHouseTelemetryEntries(2))

	if got := meter.Counters(); len(got) != 0 {
		t.Errorf("counters = %v, want none for internal logs", got)
	}
}
