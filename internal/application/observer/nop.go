package observer

import (
	"context"
	"reflect"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func EnsureTracer(t pkgobserver.Tracer) pkgobserver.Tracer {
	if IsNilInterface(t) {
		return NopTracer{}
	}
	return t
}

func EnsureMeter(m pkgobserver.Meter) pkgobserver.Meter {
	if IsNilInterface(m) {
		return NopMeter{}
	}
	return m
}

// interface に typed nil が入っているケースも nil として扱う。
func IsNilInterface(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)

	switch rv.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

type NopTracer struct{}

func (NopTracer) StartSpan(ctx context.Context, name string, attrs ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	if ctx == nil {
		ctx = context.Background()
	}

	span := NopSpan{}
	ctx = WithSpan(ctx, span)

	return ctx, span
}

type NopSpan struct{}

func (NopSpan) AddEvent(name string, attrs ...pkgobserver.Attr) {}

func (NopSpan) RecordError(err error, attrs ...pkgobserver.Attr) {}

func (NopSpan) End() {}

func (NopSpan) TraceID() pkgobserver.TraceID {
	return ""
}

func (NopSpan) SetAttributes(attrs ...pkgobserver.Attr) {}

type NopMeter struct{}

func (NopMeter) Counter(name string) pkgobserver.Counter {
	return NopCounter{}
}

func (NopMeter) Gauge(name string) pkgobserver.Gauge {
	return NopGauge{}
}

func (NopMeter) Histogram(name string) pkgobserver.Histogram {
	return NopHistogram{}
}

type NopCounter struct{}

func (NopCounter) Add(ctx context.Context, value int64, attrs ...pkgobserver.Attr) {}

type NopGauge struct{}

func (NopGauge) Record(ctx context.Context, value float64, attrs ...pkgobserver.Attr) {}

type NopHistogram struct{}

func (NopHistogram) Record(ctx context.Context, value float64, attrs ...pkgobserver.Attr) {}
