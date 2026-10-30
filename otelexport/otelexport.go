// Package otelexport passes spans from the OpenTelemetry SDK to a
// heterolog.Telemetry, which delivers them together with the spans of its own
// Tracer. It is a separate package so that programs that do not use the
// OpenTelemetry SDK do not link it.
package otelexport

import (
	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/internal/application/telemetryruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/presentation/otelexport"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ErrExporterShutdown is returned by ExportSpans after Shutdown.
var ErrExporterShutdown = otelexport.ErrExporterShutdown

// NewSpanExporter returns a SpanExporter that queues the spans it receives in
// t, which sends them to its destinations with its own spans, in the same
// batches and with the same retries. Pass it to an SDK span processor, such
// as sdktrace.WithBatcher or sdktrace.WithSyncer.
//
// ExportSpans returns without waiting for the spans to be sent; spans that do
// not fit in the queue are dropped and counted in the heterolog_trace_spans_total
// metric. The spans keep the Resource and instrumentation scope of the SDK; a
// span with an empty Resource is sent with the Resource of t. Shutdown of the
// exporter only stops it from accepting spans: shut down the
// TracerProvider before t, so that the spans that the SDK still holds reach
// t before t.Shutdown sends the last batches. When t is nil or has no
// destinations, the spans are dropped.
func NewSpanExporter(t *heterolog.Telemetry) sdktrace.SpanExporter {
	runtime := bootstrap.TelemetryRuntimeOf(t)
	if runtime == nil {
		// nilのTelemetryは、zero値と同じく、受け取ったspanを捨てます。
		runtime = &telemetryruntime.Runtime{}
	}
	return otelexport.NewSpanExporter(runtime.SpanSink())
}
