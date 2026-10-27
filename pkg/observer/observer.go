// Package observer defines the instrumentation interfaces that heterolog
// loggers report to. Pass implementations to heterolog.WithObservers; a nil
// Tracer or Meter uses a no-op implementation.
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
	RecordError(err error, attrs ...Attr)
}

// Meter creates the instruments that loggers record measurements with.
type Meter interface {
	Counter(name string) Counter
	Gauge(name string) Gauge
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
