package loki

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func newObservedLokiFlushManager(client Client) (*LokiFlushManager, *observertest.Tracer, *observertest.Meter) {
	fm := newTestLokiFlushManager(client)
	tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
	fm.Tracer, fm.Meter = tracer, meter
	return fm, tracer, meter
}

func lokiTelemetryEntries(n int) []*logging.LogEntry {
	entries := make([]*logging.LogEntry, 0, n)
	for range n {
		entries = append(entries, newTestLokiLogEntry(pkglogger.LogLevelInfo, "telemetry"))
	}
	return entries
}

func TestFlushRecordsSentRecordsAndPushSpan(t *testing.T) {
	fm, tracer, meter := newObservedLokiFlushManager(&recordingFlushManagerLokiClient{})

	if err := fm.Flush(context.Background(), lokiTelemetryEntries(3)); err != nil {
		t.Fatal(err)
	}

	if got := meter.Count(observertest.Key(telemetry.BackendRecords, telemetry.AttrBackend, "loki", telemetry.AttrOutcome, telemetry.OutcomeSuccess)); got != 3 {
		t.Errorf("successful records = %d, want 3", got)
	}
	durations := meter.HistogramValues(observertest.Key(telemetry.BackendRequestDuration, telemetry.AttrBackend, "loki", telemetry.AttrOutcome, telemetry.OutcomeSuccess))
	if len(durations) != 1 {
		t.Errorf("request durations = %v, want one", durations)
	}
	spans := tracer.Spans(SpanLokiPush)
	if len(spans) != 1 || spans[0].Ends() != 1 || len(spans[0].Errors()) != 0 {
		t.Fatalf("push spans = %v, want one ended span without errors", spans)
	}
	if v, _ := spans[0].Attr(telemetry.SpanAttrRecords); v != 3 {
		t.Errorf("push span records = %v, want 3", v)
	}
}

func TestFlushRecordsFailedRecordsAndPushError(t *testing.T) {
	pushErr := &pkgloki.HTTPStatusError{Code: http.StatusServiceUnavailable, Status: http.StatusText(http.StatusServiceUnavailable)}
	fm, tracer, meter := newObservedLokiFlushManager(&recordingFlushManagerLokiClient{pushErr: pushErr})

	// failure handlerがないため、Flushは失敗を返します。
	_ = fm.Flush(context.Background(), lokiTelemetryEntries(2))

	if got := meter.Count(observertest.Key(telemetry.BackendRecords, telemetry.AttrBackend, "loki", telemetry.AttrOutcome, telemetry.OutcomeRetryable)); got != 2 {
		t.Errorf("retryable records = %d, want 2; counters = %v", got, meter.Counters())
	}
	if got := meter.Count(observertest.Key(telemetry.BackendRecords, telemetry.AttrBackend, "loki", telemetry.AttrOutcome, telemetry.OutcomeSuccess)); got != 0 {
		t.Errorf("successful records = %d, want 0", got)
	}
	durations := meter.HistogramValues(observertest.Key(telemetry.BackendRequestDuration, telemetry.AttrBackend, "loki", telemetry.AttrOutcome, telemetry.OutcomeFailure))
	if len(durations) != 1 {
		t.Errorf("failed request durations = %v, want one", durations)
	}
	spans := tracer.Spans(SpanLokiPush)
	if len(spans) != 1 || len(spans[0].Errors()) != 1 {
		t.Fatalf("push spans = %v, want one span with the error", spans)
	}
}

func TestLoggerFlushRecordsBatchSize(t *testing.T) {
	fixedTime := time.Date(2026, 10, 29, 1, 2, 3, 0, time.UTC)
	message := "flush telemetry"
	entryBytes := len(lokiTestPlainTextFormatter.Format(&logging.LogEntry{
		OccurredAt: fixedTime,
		Level:      pkglogger.LogLevelInfo,
		Message:    message,
	}))
	// 2件目でbyte数の閾値に達し、2件をまとめてflushします。
	cfg := newTestLokiLoggerConfigWithBufferLimits(100, entryBytes*2, 100, time.Hour, false)
	tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
	logger, err := NewLokiLogger(
		context.Background(),
		cfg,
		&recordingFlushManagerLokiClient{},
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		tracer,
		meter,
		&LoggerBase{},
		WithClock(fixedLokiClock{t: fixedTime}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logger.Close(context.Background()) })

	for range 2 {
		if err := logger.testInfo(context.Background(), message); err != nil {
			t.Fatal(err)
		}
	}

	if got := meter.HistogramValues(observertest.Key(telemetry.BackendFlushRecords, telemetry.AttrBackend, "loki")); len(got) != 1 || got[0] != 2 {
		t.Errorf("flush records = %v, want [2]", got)
	}
	if got := meter.HistogramValues(observertest.Key(telemetry.BackendFlushBytes, telemetry.AttrBackend, "loki")); len(got) != 1 || got[0] != float64(entryBytes*2) {
		t.Errorf("flush bytes = %v, want [%d]", got, entryBytes*2)
	}
	flushes := tracer.Spans(SpanLokiLoggerFlush)
	pushes := tracer.Spans(SpanLokiPush)
	if len(flushes) != 1 || len(pushes) != 1 || pushes[0].Parent != flushes[0] {
		t.Errorf("flush spans = %v, push spans = %v, want a push under the flush", flushes, pushes)
	}
}
