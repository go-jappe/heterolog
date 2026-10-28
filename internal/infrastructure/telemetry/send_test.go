package telemetry

import (
	"context"
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

func TestRecordSendResultCountsRecordsByOutcome(t *testing.T) {
	meter := observertest.NewMeter()
	result := failure.SendResult{
		Meta:              failure.Meta{Backend: "loki", InputCount: 10},
		RetryableRecords:  make([]failure.FailedRecord, 2),
		DeadRecords:       make([]failure.FailedRecord, 1),
		QuarantineRecords: make([]failure.FailedRecord, 3),
	}

	RecordSendResult(context.Background(), meter, result)

	want := map[string]int64{
		observertest.Key(BackendRecords, AttrBackend, "loki", AttrOutcome, OutcomeSuccess):    4,
		observertest.Key(BackendRecords, AttrBackend, "loki", AttrOutcome, OutcomeRetryable):  2,
		observertest.Key(BackendRecords, AttrBackend, "loki", AttrOutcome, OutcomeDead):       1,
		observertest.Key(BackendRecords, AttrBackend, "loki", AttrOutcome, OutcomeQuarantine): 3,
	}
	if got := meter.Counters(); !equalCounts(got, want) {
		t.Errorf("counters = %v, want %v", got, want)
	}
}

func TestRecordSendResultDoesNotCountUnconfirmedRecordsAsSuccess(t *testing.T) {
	meter := observertest.NewMeter()
	result := failure.SendResult{
		Meta:             failure.Meta{Backend: "opensearch", InputCount: 3},
		TerminalError:    errors.New("connection reset"),
		RetryableRecords: make([]failure.FailedRecord, 3),
	}

	RecordSendResult(context.Background(), meter, result)

	want := map[string]int64{
		observertest.Key(BackendRecords, AttrBackend, "opensearch", AttrOutcome, OutcomeRetryable): 3,
	}
	if got := meter.Counters(); !equalCounts(got, want) {
		t.Errorf("counters = %v, want %v", got, want)
	}
}

func TestRecordSendResultAcceptsNilMeter(t *testing.T) {
	RecordSendResult(context.Background(), nil, failure.SendResult{Meta: failure.Meta{InputCount: 1}})
	RecordRecords(context.Background(), nil, "slack", OutcomeSuccess, 1)
}

func TestStartFlushRecordsBatchSize(t *testing.T) {
	meter := observertest.NewMeter()
	tracer := &observertest.Tracer{}

	_, span := StartFlush(context.Background(), tracer, meter, "heterolog.test.flush", "loki", 5, 1200)
	EndSpan(span, nil)

	if got := meter.HistogramValues(observertest.Key(BackendFlushRecords, AttrBackend, "loki")); len(got) != 1 || got[0] != 5 {
		t.Errorf("flush records = %v, want [5]", got)
	}
	if got := meter.HistogramValues(observertest.Key(BackendFlushBytes, AttrBackend, "loki")); len(got) != 1 || got[0] != 1200 {
		t.Errorf("flush bytes = %v, want [1200]", got)
	}
	spans := tracer.Spans("heterolog.test.flush")
	if len(spans) != 1 || spans[0].Ends() != 1 {
		t.Fatalf("flush spans = %v, want one ended span", spans)
	}
	if v, _ := spans[0].Attr(SpanAttrRecords); v != 5 {
		t.Errorf("records attribute = %v, want 5", v)
	}
	if v, _ := spans[0].Attr(SpanAttrBytes); v != 1200 {
		t.Errorf("bytes attribute = %v, want 1200", v)
	}
}

func TestStartFlushWithoutBytes(t *testing.T) {
	meter := observertest.NewMeter()
	tracer := &observertest.Tracer{}

	_, span := StartFlush(context.Background(), tracer, meter, "heterolog.test.flush", "slack", 2, -1)
	EndSpan(span, nil)

	if got := meter.HistogramValues(observertest.Key(BackendFlushBytes, AttrBackend, "slack")); got != nil {
		t.Errorf("flush bytes = %v, want none", got)
	}
	if _, ok := tracer.Spans("heterolog.test.flush")[0].Attr(SpanAttrBytes); ok {
		t.Error("the span has a bytes attribute without a byte count")
	}
}

func TestEndSpanRecordsError(t *testing.T) {
	tracer := &observertest.Tracer{}
	sendErr := errors.New("send failed")

	_, span := tracer.StartSpan(context.Background(), "heterolog.test")
	EndSpan(span, sendErr)
	EndSpan(nil, sendErr)

	recorded := tracer.Spans("heterolog.test")[0]
	if errs := recorded.Errors(); len(errs) != 1 || errs[0] != sendErr {
		t.Errorf("errors = %v, want [%v]", errs, sendErr)
	}
	if recorded.Ends() != 1 {
		t.Errorf("ends = %d, want 1", recorded.Ends())
	}
}

func TestRequestRecordsDurationByOutcome(t *testing.T) {
	meter := observertest.NewMeter()
	tracer := &observertest.Tracer{}
	sendErr := errors.New("503")

	parentCtx, parent := tracer.StartSpan(context.Background(), "heterolog.test.flush")
	requestCtx, succeeded := StartRequest(parentCtx, tracer, meter, "heterolog.test.request", "loki", 3)
	if requestCtx == parentCtx {
		t.Error("StartRequest did not return the request span context")
	}
	succeeded.End(nil)
	_, failed := StartRequest(parentCtx, tracer, meter, "heterolog.test.request", "loki", 3)
	failed.End(sendErr)

	if got := meter.HistogramValues(observertest.Key(BackendRequestDuration, AttrBackend, "loki", AttrOutcome, OutcomeSuccess)); len(got) != 1 || got[0] < 0 {
		t.Errorf("success durations = %v, want one value", got)
	}
	if got := meter.HistogramValues(observertest.Key(BackendRequestDuration, AttrBackend, "loki", AttrOutcome, OutcomeFailure)); len(got) != 1 {
		t.Errorf("failure durations = %v, want one value", got)
	}
	spans := tracer.Spans("heterolog.test.request")
	if len(spans) != 2 {
		t.Fatalf("request spans = %d, want 2", len(spans))
	}
	for _, sp := range spans {
		if sp.Parent != parent.(*observertest.Span) || sp.Ends() != 1 {
			t.Errorf("request span parent = %v, ends = %d, want the flush span and one end", sp.Parent, sp.Ends())
		}
		if v, _ := sp.Attr(SpanAttrRecords); v != 3 {
			t.Errorf("records attribute = %v, want 3", v)
		}
	}
	if errs := spans[1].Errors(); len(errs) != 1 || errs[0] != sendErr {
		t.Errorf("failed request errors = %v, want [%v]", errs, sendErr)
	}
	if errs := spans[0].Errors(); len(errs) != 0 {
		t.Errorf("successful request errors = %v, want none", errs)
	}
}

func equalCounts(got, want map[string]int64) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
