package otel

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"go.opentelemetry.io/otel/codes"
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

func (sp *otelSpan) AddEvent(name string, attrs ...pkgobserver.Attr) {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}

	otelAttrs := toOtelAttrs(attrs)
	sp.s.AddEvent(name, trace.WithAttributes(otelAttrs...))
}

func (sp *otelSpan) RecordError(err error, attrs ...pkgobserver.Attr) {
	if err == nil {
		return
	}
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}

	otelAttrs := toOtelAttrs(attrs)
	sp.s.RecordError(err, trace.WithAttributes(otelAttrs...))
	// RecordErrorだけではspanのstatusが変わらず、trace UIでは成功として表示されます。
	// errorを記録したspanは失敗した操作として扱います。
	sp.s.SetStatus(codes.Error, err.Error())
}

func (sp *otelSpan) SetStatus(code pkgobserver.StatusCode, description string) {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}
	switch code {
	case pkgobserver.StatusOK:
		sp.s.SetStatus(codes.Ok, "")
	case pkgobserver.StatusError:
		sp.s.SetStatus(codes.Error, description)
	}
}

func (sp *otelSpan) End() {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}

	sp.s.End()
}

func (sp *otelSpan) TraceID() pkgobserver.TraceID {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return ""
	}

	sc := sp.s.SpanContext()
	if !sc.IsValid() {
		return ""
	}

	return pkgobserver.TraceID(sc.TraceID().String())
}

func (sp *otelSpan) SetAttributes(attrs ...pkgobserver.Attr) {
	if sp == nil || observer.IsNilInterface(sp.s) {
		return
	}

	otelAttrs := toOtelAttrs(attrs)
	sp.s.SetAttributes(otelAttrs...)
}

func (o *OtelTracer) StartSpan(ctx context.Context, name string, attrs ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
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
		ctx = observer.WithTraceID(ctx, pkgobserver.TraceID(sc.TraceID().String()))
	}

	ctx = observer.WithSpan(ctx, wrapped)

	return ctx, wrapped
}
