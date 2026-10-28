package slack

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// rejectingSlackHTTPClientは、すべての投稿をSlack APIのerrorで拒否します。
type rejectingSlackHTTPClient struct{}

func (rejectingSlackHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"ok":false,"error":"channel_not_found"}`)),
		Request:    req,
	}, nil
}

func newObservedSlackLogger(t *testing.T, client interface {
	Do(*http.Request) (*http.Response, error)
}) (*SlackLogger, *observertest.Tracer, *observertest.Meter) {
	t.Helper()
	logger := newNormalSlackLifecycleLogger(t, []time.Duration{time.Hour}, client)
	tracer, meter := &observertest.Tracer{}, observertest.NewMeter()
	logger.Tracer, logger.Meter = tracer, meter
	return logger, tracer, meter
}

func slackTelemetryEntries(n int) []*logging.LogEntry {
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

func slackRecords(meter *observertest.Meter, outcome string) int64 {
	return meter.Count(observertest.Key(telemetry.BackendRecords, telemetry.AttrBackend, "slack", telemetry.AttrOutcome, outcome))
}

func TestSendLogsRecordsPostedRecords(t *testing.T) {
	logger, tracer, meter := newObservedSlackLogger(t, &recordingSlackHTTPClient{})
	t.Cleanup(func() { _ = logger.Close() })

	if err := logger.sendLogs(context.Background(), slackTelemetryEntries(2)); err != nil {
		t.Fatal(err)
	}

	if got := slackRecords(meter, telemetry.OutcomeSuccess); got != 2 {
		t.Errorf("successful records = %d, want 2; counters = %v", got, meter.Counters())
	}
	sends := tracer.Spans(SpanSlackLoggerSendBatch)
	posts := tracer.Spans(SpanSlackPost)
	if len(sends) != 1 || len(posts) != 1 || posts[0].Parent != sends[0] {
		t.Fatalf("send spans = %v, post spans = %v, want one post under one send", sends, posts)
	}
	if _, ok := sends[0].Attr(telemetry.SpanAttrBytes); ok {
		t.Error("the Slack send span has a bytes attribute")
	}
	if got := meter.HistogramValues(observertest.Key(telemetry.BackendFlushRecords, telemetry.AttrBackend, "slack")); len(got) != 1 || got[0] != 2 {
		t.Errorf("flush records = %v, want [2]", got)
	}
}

func TestSendLogsWithoutLogsStartsNoSpan(t *testing.T) {
	logger, tracer, _ := newObservedSlackLogger(t, &recordingSlackHTTPClient{})
	t.Cleanup(func() { _ = logger.Close() })

	if err := logger.sendLogs(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	if names := tracer.Names(); len(names) != 0 {
		t.Errorf("spans = %v, want none for an empty batch", names)
	}
}

func TestSendLogsRecordsRestoredRecordsOnFailure(t *testing.T) {
	logger, tracer, meter := newObservedSlackLogger(t, rejectingSlackHTTPClient{})
	t.Cleanup(func() { _ = logger.Close() })

	if err := logger.sendLogs(context.Background(), slackTelemetryEntries(2)); err == nil {
		t.Fatal("sendLogs() succeeded, want the Slack API error")
	}

	if got := slackRecords(meter, telemetry.OutcomeRetryable); got != 2 {
		t.Errorf("retryable records = %d, want 2; counters = %v", got, meter.Counters())
	}
	for _, name := range []string{SpanSlackLoggerSendBatch, SpanSlackPost} {
		spans := tracer.Spans(name)
		if len(spans) != 1 || len(spans[0].Errors()) != 1 || spans[0].Ends() != 1 {
			t.Errorf("%s spans = %v, want one ended span with the error", name, spans)
		}
	}
}

func TestEnqueueRecordsDroppedRecords(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _, meter := newObservedSlackLogger(t, &recordingSlackHTTPClient{})
		t.Cleanup(func() { _ = logger.Close() })
		ctx := context.Background()
		// 最初のlogはすぐに送るため、bufferには残りません。
		if err := logger.testInfo(ctx, "immediate"); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()

		maxLogs := logger.config.MaxLogsPerNotification()
		for range maxLogs + 3 {
			if err := logger.testInfo(ctx, "buffered"); err != nil {
				t.Fatal(err)
			}
		}

		if got := slackRecords(meter, telemetry.OutcomeDropped); got != 3 {
			t.Errorf("dropped records = %d, want 3; counters = %v", got, meter.Counters())
		}
	})
}
