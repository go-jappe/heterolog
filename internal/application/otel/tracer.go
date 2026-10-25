package otel

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/observer"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type OtelTracer struct {
	t trace.Tracer
}

func NewOtelTracer(t trace.Tracer) *OtelTracer {
	return &OtelTracer{t: t}
}

type otelSpan struct {
	s trace.Span
}

func (sp *otelSpan) AddEvent(name string, attrs ...observer.Attr) {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}

	otelAttrs := toOtelAttrs(attrs)
	sp.s.AddEvent(name, trace.WithAttributes(otelAttrs...))
}

func (sp *otelSpan) RecordError(err error, attrs ...observer.Attr) {
	if err == nil {
		return
	}
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}

	otelAttrs := toOtelAttrs(attrs)
	sp.s.RecordError(err, trace.WithAttributes(otelAttrs...))
}

func (sp *otelSpan) End() {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}

	sp.s.End()
}

func (sp *otelSpan) TraceID() observer.TraceID {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return ""
	}

	sc := sp.s.SpanContext()
	if !sc.IsValid() {
		return ""
	}

	return observer.TraceID(sc.TraceID().String())
}

func (sp *otelSpan) SetAttributes(attrs ...observer.Attr) {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}

	otelAttrs := toOtelAttrs(attrs)
	sp.s.SetAttributes(otelAttrs...)
}

func (o *OtelTracer) StartSpan(ctx context.Context, name string, attrs ...observer.Attr) (context.Context, observer.Span) {
	if ctx == nil {
		ctx = context.Background()
	}

	if o == nil || observer.IsNilInterface(o.t) {
		return observer.NopTracer{}.StartSpan(ctx, name, attrs...)
	}

	otelAttrs := toOtelAttrs(attrs)

	ctx, span := o.t.Start(ctx, name, trace.WithAttributes(otelAttrs...))
	if observer.IsNilInterface(span) {
		return observer.NopTracer{}.StartSpan(ctx, name, attrs...)
	}

	wrapped := &otelSpan{s: span}

	sc := span.SpanContext()
	if sc.IsValid() {
		ctx = observer.WithTraceID(ctx, observer.TraceID(sc.TraceID().String()))
	}

	ctx = observer.WithSpan(ctx, wrapped)

	return ctx, wrapped
}

func toOtelAttrs(attrs []observer.Attr) []attribute.KeyValue {
	otelAttrs := make([]attribute.KeyValue, 0, len(attrs))

	for _, a := range attrs {
		otelAttrs = append(otelAttrs, attribute.String(a.Key, fmt.Sprint(a.Value)))
	}

	return otelAttrs
}
