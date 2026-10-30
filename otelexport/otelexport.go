// Package otelexport passes spans and metrics from the OpenTelemetry SDK to a
// heterolog.Telemetry, which delivers them together with the spans and metrics
// of its own Tracer and Meter. It is a separate package so that programs that
// do not use the OpenTelemetry SDK do not link it.
package otelexport

import (
	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/internal/application/telemetryruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/presentation/otelexport"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ErrExporterShutdown is returned by ExportSpans after Shutdown.
var ErrExporterShutdown = otelexport.ErrExporterShutdown

// ErrMetricExporterShutdown is returned by Export of a metric exporter after
// its Shutdown.
var ErrMetricExporterShutdown = otelexport.ErrMetricExporterShutdown

// ErrUnsupportedMetric is wrapped by the error that Export of a metric
// exporter returns when it receives exponential histograms or summaries,
// which a heterolog.Telemetry cannot send. The other metrics are sent.
var ErrUnsupportedMetric = otelexport.ErrUnsupportedMetric

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

// NewMetricExporter returns a metric Exporter that queues the metrics it
// receives in t, which sends them to its metric destinations with the metrics
// of its own Meter, with the same retries and failure stores. Pass it to an
// SDK reader, such as sdkmetric.NewPeriodicReader; the reader decides when to
// collect, and t sends each collection as it arrives.
//
// The exporter asks the SDK for cumulative temporality for every instrument,
// so a value resent from the spool does not change the totals, and uses the
// default aggregation of the SDK. Export returns without waiting for the
// metrics to be sent; it returns an error wrapping ErrUnsupportedMetric for
// exponential histograms and summaries, which are not sent. The metrics keep
// the Resource and instrumentation scope of the SDK; metrics with an empty
// Resource are sent with the Resource of t. Shut down the MeterProvider before
// t, so that its last collection reaches t before t.Shutdown sends the last
// metrics. When t is nil or has no metric destinations, the metrics are
// dropped.
func NewMetricExporter(t *heterolog.Telemetry) sdkmetric.Exporter {
	runtime := bootstrap.TelemetryRuntimeOf(t)
	if runtime == nil {
		// nilのTelemetryは、zero値と同じく、受け取ったmetricsを捨てます。
		runtime = &telemetryruntime.Runtime{}
	}
	return otelexport.NewMetricExporter(runtime.MetricSink())
}
