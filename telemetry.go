package heterolog

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/telemetryruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// TelemetryConfig configures NewTelemetry. See pkg/telemetry for its settings.
type TelemetryConfig = pkgtelemetry.Config

// TraceExporter names a destination of spans in TelemetryConfig.
type TraceExporter = pkgtelemetry.TraceExporter

const (
	TraceExporterOTLP   = pkgtelemetry.TraceExporterOTLP
	TraceExporterFile   = pkgtelemetry.TraceExporterFile
	TraceExporterStdout = pkgtelemetry.TraceExporterStdout
)

// MetricExporter names a destination of metrics in TelemetryConfig.
type MetricExporter = pkgtelemetry.MetricExporter

const (
	MetricExporterOTLP   = pkgtelemetry.MetricExporterOTLP
	MetricExporterFile   = pkgtelemetry.MetricExporterFile
	MetricExporterStdout = pkgtelemetry.MetricExporterStdout

	MetricExporterVictoriaMetrics = pkgtelemetry.MetricExporterVictoriaMetrics
)

// Telemetry records spans and metrics with its Tracer and Meter and delivers
// them, together with those from the OpenTelemetry SDK passed through package
// otelexport, to the destinations of its TelemetryConfig. Obtain one with NewTelemetry; the zero
// value records nothing. Do not copy a Telemetry.
type Telemetry struct {
	// telemetryは処理を受け持つ内部のruntimeです。zero値のTelemetryではnilです。
	telemetry *telemetryruntime.Runtime
}

// DefaultTelemetryConfig returns settings with no destinations, which record
// nothing, and the host name and OS in Resource. Choose the destinations in
// Traces.Exporters and Metrics.Exporters, and set Resource.ServiceName.
func DefaultTelemetryConfig() TelemetryConfig { return bootstrap.DefaultTelemetryConfig() }

// NewTelemetry starts delivering spans and metrics to the destinations of cfg.
// It returns the errors of cfg.Validate, and ErrNilContext for a nil ctx. ctx
// applies only to construction. Call Shutdown when the application exits to
// send the spans and metrics that are still queued.
//
// Of opts, only the Meter of WithObservers and WithTelemetryErrorHandler are
// used: the Meter records the heterolog_trace_* and heterolog_metric_* metrics
// of the delivery, and the error handler receives its errors. The delivery
// does not create spans for its own sending, so the Tracer of WithObservers is
// not used.
//
// To trace and measure the work of loggers, pass the Tracer and the Meter of
// the Telemetry to the loggers with WithObservers.
func NewTelemetry(ctx context.Context, cfg TelemetryConfig, opts ...Option) (*Telemetry, error) {
	return newTelemetry(bootstrap.NewTelemetry(ctx, cfg, opts...))
}

// 公開package otelexportが、Telemetryの内部のruntimeへspanを渡せるようにします。
func init() {
	bootstrap.TelemetryRuntimeOf = func(telemetry any) *telemetryruntime.Runtime {
		t, ok := telemetry.(*Telemetry)
		if !ok || t == nil {
			return nil
		}
		return t.impl()
	}
}

// newTelemetryは、内部のruntimeを公開の型で包みます。errがある場合はnilを返します。
func newTelemetry(telemetry *telemetryruntime.Runtime, err error) (*Telemetry, error) {
	if err != nil {
		return nil, err
	}
	return &Telemetry{telemetry: telemetry}, nil
}

// implは、処理を渡す内部のruntimeを返します。zero値のTelemetryには、何も記録しない
// runtimeを返します。
func (t *Telemetry) impl() *telemetryruntime.Runtime {
	if t.telemetry == nil {
		return &telemetryruntime.Runtime{}
	}
	return t.telemetry
}

// Tracer returns the Tracer that records spans. It also implements
// pkg/observer OptionsTracer, which accepts a span kind and links. A span
// becomes a child of the span context in the context passed to StartSpan, and
// otherwise starts a new trace that TracesConfig.SamplingRatio decides to
// record or not. Without destinations, the Tracer records nothing.
func (t *Telemetry) Tracer() Tracer { return t.impl().Tracer() }

// Meter returns the Meter that records metrics. Counters are sent as
// cumulative sums, gauges as their last value, and histograms with the
// buckets of MetricsConfig.HistogramBounds, every MetricsConfig.ExportInterval.
// Without metric destinations, the Meter records nothing.
func (t *Telemetry) Meter() Meter { return t.impl().Meter() }

// Shutdown stops accepting spans and waits until the queued spans are sent,
// then collects the values of the Meter one last time, sends the queued
// metrics, and closes the destinations, or until ctx ends. If ctx ends first,
// it returns ctx.Err() and the sending continues in the background. Spans and
// metrics recorded after Shutdown started are dropped. A later Shutdown waits
// for the same cleanup.
func (t *Telemetry) Shutdown(ctx context.Context) error { return t.impl().Shutdown(ctx) }
