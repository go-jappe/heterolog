// Package observer defines the instrumentation interfaces that heterolog
// loggers report to. Pass implementations to heterolog.WithObservers; a nil
// Tracer or Meter uses a no-op implementation. The Tracer of
// heterolog.NewTelemetry implements Tracer and OptionsTracer and sends the
// spans it records.
//
// It also carries span contexts between services: ParseTraceParent and
// SpanContext.TraceParent read and write the W3C traceparent header, and
// ContextWithSpanContext and SpanContextFromContext keep a span context in a
// context.
package observer

import "context"

// TraceID identifies a trace. An empty TraceID means that no trace is known.
type TraceID string

// Attr is a key and value attached to a span, an event or a measurement.
type Attr struct {
	Key   string
	Value any
}

// Tracer starts spans. StartSpan returns a context that carries the new span.
type Tracer interface {
	StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
}

// Span is an operation started by a Tracer. End finishes it.
type Span interface {
	// TraceID returns the ID of the trace that the span belongs to, or an empty
	// TraceID when it is not known.
	TraceID() TraceID
	End()
	AddEvent(name string, attrs ...Attr)
	SetAttributes(attrs ...Attr)
	// RecordError records err as an event of the span. Implementations may
	// also set the status of the span to StatusError.
	RecordError(err error, attrs ...Attr)
	// SetStatus sets the result of the operation. description is used only
	// with StatusError. Once the status is StatusOK, later calls do not
	// change it, and StatusUnset never replaces another status.
	SetStatus(code StatusCode, description string)
}

// StatusCode is the result of the operation of a span.
type StatusCode int

const (
	// StatusUnset means that the status was not set.
	StatusUnset StatusCode = iota
	// StatusOK means that the operation succeeded.
	StatusOK
	// StatusError means that the operation failed.
	StatusError
)

// Meter creates the instruments that loggers record measurements with.
type Meter interface {
	Counter(name string) Counter
	Gauge(name string) Gauge
	Histogram(name string) Histogram
}

// Counter records a cumulative value.
type Counter interface {
	Add(ctx context.Context, value int64, attrs ...Attr)
}

// Gauge records the current value of a measurement, such as a memory pressure
// ratio or the available bytes, instead of a cumulative one.
type Gauge interface {
	Record(ctx context.Context, value float64, attrs ...Attr)
}

// Histogram records the distribution of a measurement, such as the duration
// of a request in seconds or the number of records in a batch.
type Histogram interface {
	Record(ctx context.Context, value float64, attrs ...Attr)
}
