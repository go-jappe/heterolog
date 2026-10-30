package observer

import "context"

// SpanKind describes how a span relates to the other spans of its trace, as
// in OpenTelemetry. The zero value is SpanKindInternal.
type SpanKind int

const (
	// SpanKindInternal is an operation inside the application.
	SpanKindInternal SpanKind = iota
	// SpanKindServer handles a synchronous request from a remote client.
	SpanKindServer
	// SpanKindClient sends a synchronous request to a remote server.
	SpanKindClient
	// SpanKindProducer sends a message that a consumer processes later.
	SpanKindProducer
	// SpanKindConsumer processes a message that a producer sent.
	SpanKindConsumer
)

// Link relates a span to a span other than its parent, such as one of the
// messages that a batch consumer processes.
type Link struct {
	SpanContext SpanContext
	Attrs       []Attr
}

// StartOptions holds the settings of a new span besides its name and
// attributes.
type StartOptions struct {
	Kind  SpanKind
	Links []Link
}

// OptionsTracer is a Tracer that also accepts StartOptions. The Tracer of
// heterolog.NewTelemetry implements it.
type OptionsTracer interface {
	Tracer
	StartSpanWithOptions(ctx context.Context, name string, opts StartOptions, attrs ...Attr) (context.Context, Span)
}

// StartSpanWithOptions starts a span with t. When t implements OptionsTracer,
// it receives opts; otherwise opts are ignored and t.StartSpan starts the span.
func StartSpanWithOptions(
	ctx context.Context,
	t Tracer,
	name string,
	opts StartOptions,
	attrs ...Attr,
) (context.Context, Span) {
	if ot, ok := t.(OptionsTracer); ok {
		return ot.StartSpanWithOptions(ctx, name, opts, attrs...)
	}
	return t.StartSpan(ctx, name, attrs...)
}
