package observer

import (
	"context"
	"reflect"
)

func EnsureTracer(t Tracer) Tracer {
	if IsNilInterface(t) {
		return NopTracer{}
	}
	return t
}

func EnsureMeter(m Meter) Meter {
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

func (NopTracer) StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span) {
	if ctx == nil {
		ctx = context.Background()
	}

	span := NopSpan{}
	ctx = WithSpan(ctx, span)

	return ctx, span
}

type NopSpan struct{}

func (NopSpan) AddEvent(name string, attrs ...Attr) {}

func (NopSpan) RecordError(err error, attrs ...Attr) {}

func (NopSpan) End() {}

func (NopSpan) TraceID() TraceID {
	return ""
}

func (NopSpan) SetAttributes(attrs ...Attr) {}

type NopMeter struct{}

func (NopMeter) Counter(name string) Counter {
	return NopCounter{}
}

func (NopMeter) Gauge(name string) Gauge {
	return NopGauge{}
}

type NopCounter struct{}

func (NopCounter) Add(ctx context.Context, value int64, attrs ...Attr) {}

type NopGauge struct{}

func (NopGauge) Record(ctx context.Context, value float64, attrs ...Attr) {}
