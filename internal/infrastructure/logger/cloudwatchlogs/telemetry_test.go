package cloudwatchlogs

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func newObservedCloudWatchLogsFlushManager(client pkgcloudwatchlogs.Client) (*CloudWatchLogsFlushManager, *observertest.Tracer, *observertest.Meter) {
	fm := newTestCloudWatchLogsFlushManager(client, nil)
	tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
	fm.Tracer, fm.Meter = tracer, meter
	return fm, tracer, meter
}

func cloudWatchLogsTelemetryEntries(n int) []*logging.LogEntry {
	entries := make([]*logging.LogEntry, 0, n)
	for range n {
		entries = append(entries, newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "telemetry"))
	}
	return entries
}

func cloudWatchLogsRecords(meter *observertest.Meter, outcome string) int64 {
	return meter.Count(observertest.Key(telemetry.BackendRecords, telemetry.AttrBackend, "cloudwatchlogs", telemetry.AttrOutcome, outcome))
}

func TestFlushRecordsPutRequestAndRecords(t *testing.T) {
	fm, tracer, meter := newObservedCloudWatchLogsFlushManager(&recordingCloudWatchLogsClient{})

	if err := fm.Flush(context.Background(), cloudWatchLogsTelemetryEntries(3)); err != nil {
		t.Fatal(err)
	}

	if got := cloudWatchLogsRecords(meter, telemetry.OutcomeSuccess); got != 3 {
		t.Errorf("successful records = %d, want 3; counters = %v", got, meter.Counters())
	}
	spans := tracer.Spans(SpanCloudWatchLogsPut)
	if len(spans) != 1 || spans[0].Ends() != 1 || len(spans[0].Errors()) != 0 {
		t.Fatalf("put spans = %v, want one ended span without errors", spans)
	}
	if v, _ := spans[0].Attr(telemetry.SpanAttrRecords); v != 3 {
		t.Errorf("put span records = %v, want 3", v)
	}
}

func TestFlushRecordsFailedPut(t *testing.T) {
	putErr := &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusServiceUnavailable, Status: "503 Service Unavailable"}
	fm, tracer, meter := newObservedCloudWatchLogsFlushManager(&recordingCloudWatchLogsClient{err: putErr})

	// failure handlerがないため、Flushは失敗を返します。
	_ = fm.Flush(context.Background(), cloudWatchLogsTelemetryEntries(2))

	if got := cloudWatchLogsRecords(meter, telemetry.OutcomeRetryable); got != 2 {
		t.Errorf("retryable records = %d, want 2; counters = %v", got, meter.Counters())
	}
	spans := tracer.Spans(SpanCloudWatchLogsPut)
	if len(spans) != 1 || len(spans[0].Errors()) != 1 {
		t.Fatalf("put spans = %v, want one span with the error", spans)
	}
	if got := meter.HistogramValues(observertest.Key(telemetry.BackendRequestDuration, telemetry.AttrBackend, "cloudwatchlogs", telemetry.AttrOutcome, telemetry.OutcomeFailure)); len(got) != 1 {
		t.Errorf("failed request durations = %v, want one", got)
	}
}

func TestPutWithReportRecordsTheEntry(t *testing.T) {
	fm, _, meter := newObservedCloudWatchLogsFlushManager(&recordingCloudWatchLogsClient{})

	if _, err := fm.PutWithReport(context.Background(), newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "delivery")); err != nil {
		t.Fatal(err)
	}

	if got := cloudWatchLogsRecords(meter, telemetry.OutcomeSuccess); got != 1 {
		t.Errorf("successful records = %d, want 1; counters = %v", got, meter.Counters())
	}
}
