package otel

import (
	"context"
	"errors"
	"testing"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func newRecordedTracer(t *testing.T) (*OtelTracer, *tracetest.SpanRecorder) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return NewOtelTracer(provider.Tracer("heterolog-test")), recorder
}

func TestRecordErrorMarksSpanFailed(t *testing.T) {
	tracer, recorder := newRecordedTracer(t)

	_, span := tracer.StartSpan(context.Background(), "heterolog.test")
	span.RecordError(errors.New("send failed"))
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	status := ended[0].Status()
	if status.Code != codes.Error || status.Description != "send failed" {
		t.Errorf("status = %+v, want an error with the message", status)
	}
	if events := ended[0].Events(); len(events) != 1 || events[0].Name != "exception" {
		t.Errorf("events = %+v, want one exception event", events)
	}
}

func TestRecordNilErrorKeepsStatus(t *testing.T) {
	tracer, recorder := newRecordedTracer(t)

	_, span := tracer.StartSpan(context.Background(), "heterolog.test")
	span.RecordError(nil)
	span.End()

	if status := recorder.Ended()[0].Status(); status.Code != codes.Unset {
		t.Errorf("status = %+v, want unset", status)
	}
}

func TestSpanAttributesKeepTheirTypes(t *testing.T) {
	tracer, recorder := newRecordedTracer(t)

	_, span := tracer.StartSpan(context.Background(), "heterolog.test",
		pkgobserver.Attr{Key: "heterolog.backend", Value: "loki"},
	)
	span.SetAttributes(
		pkgobserver.Attr{Key: "heterolog.records", Value: 3},
		pkgobserver.Attr{Key: "heterolog.retry", Value: true},
	)
	span.End()

	got := make(map[attribute.Key]attribute.Value)
	for _, kv := range recorder.Ended()[0].Attributes() {
		got[kv.Key] = kv.Value
	}
	if v := got["heterolog.backend"]; v.Type() != attribute.STRING || v.AsString() != "loki" {
		t.Errorf("backend attribute = %v, want the string loki", v)
	}
	if v := got["heterolog.records"]; v.Type() != attribute.INT64 || v.AsInt64() != 3 {
		t.Errorf("records attribute = %v, want the integer 3", v)
	}
	if v := got["heterolog.retry"]; v.Type() != attribute.BOOL || !v.AsBool() {
		t.Errorf("retry attribute = %v, want true", v)
	}
}
