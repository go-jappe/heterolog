package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	goruntime "runtime"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	"github.com/go-jappe/heterolog/internal/application/telemetryruntime"
	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/failurestore"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/jsonlines"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/otlphttp"
	metricsdelivery "github.com/go-jappe/heterolog/internal/infrastructure/metrics"
	"github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// DefaultTelemetryConfigは、送り先を選んでいないtelemetryの既定の設定を返します。
// Resourceには、host名とOSを入れます。
func DefaultTelemetryConfig() pkgtelemetry.Config {
	host, _ := os.Hostname()
	return pkgtelemetry.Config{
		Resource: pkglogger.Resource{HostName: host, OSType: goruntime.GOOS},
		Traces:   traces.DefaultConfig(),
		Metrics:  metricsdelivery.DefaultConfig(),
	}
}

// NewTelemetryは、cfgを検証し、traceとmetricsの送り先と、TracerとMeterを組み立てます。
// optsは、WithObserversのMeterと、WithTelemetryErrorHandlerの関数だけを使います。Meterには、
// 配送の仕組みのmetricsを記録し、関数には配送のerrorを渡します。配送の仕組みは自分の
// 送信のspanを作らないため、Tracerは使いません。送り先のない信号は、何も記録しません。
func NewTelemetry(ctx context.Context, cfg pkgtelemetry.Config, opts ...loggerruntime.Option) (*telemetryruntime.Runtime, error) {
	if ctx == nil {
		return nil, loggerruntime.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	options := loggerruntime.ResolveOptions(opts)
	report := delivery.NewErrorHandler(options.TelemetryErrorHandler)
	resource := otlpcommon.NewResource(cfg.Resource)
	var parts telemetryruntime.Parts
	if len(cfg.Traces.Exporters) > 0 {
		destinations, err := newTraceDestinations(ctx, cfg, resource, options.Meter, report)
		if err != nil {
			return nil, err
		}
		pipeline := traces.NewPipeline(traces.Config{
			QueueSize:      cfg.Traces.QueueSize,
			BatchSize:      cfg.Traces.BatchSize,
			ExportInterval: cfg.Traces.ExportInterval,
		}, destinations, options.Meter, report)
		parts.Tracer, parts.Spans = tracing.NewTracer(pipeline, cfg.Traces.SamplingRatio), pipeline
	}
	if len(cfg.Metrics.Exporters) > 0 {
		destinations, err := newMetricDestinations(ctx, cfg, resource, options.Meter, report)
		if err != nil {
			return nil, errors.Join(err, telemetryruntime.New(parts).Shutdown(context.WithoutCancel(ctx)))
		}
		meter := metrics.NewMeter(metrics.Config{
			HistogramBounds:  cfg.Metrics.HistogramBounds,
			CardinalityLimit: cfg.Metrics.CardinalityLimit,
		}, nil)
		parts.Meter = meter
		parts.Metrics = metricsdelivery.NewPipeline(metricsdelivery.Config{
			ExportInterval: cfg.Metrics.ExportInterval,
			QueueSize:      cfg.Metrics.QueueSize,
		}, meter, destinations, options.Meter, report)
	}
	return telemetryruntime.New(parts), nil
}

// newTraceDestinationsは、cfgで選んだtraceの送り先を作ります。作れない送り先があれば、
// 作った送り先を閉じてerrorを返します。
func newTraceDestinations(
	ctx context.Context,
	cfg pkgtelemetry.Config,
	resource *telemetrydata.Resource,
	meter pkgobserver.Meter,
	report delivery.ErrorHandler,
) ([]traces.Destination, error) {
	destinations := make([]traces.Destination, 0, len(cfg.Traces.Exporters))
	closeAll := func() error {
		return closeDestinations(destinations, func(d traces.Destination) (delivery.Exporter[tracing.SpanData], delivery.FailureStore[tracing.SpanData]) {
			return d.Exporter, d.Store
		})
	}
	for _, name := range cfg.Traces.Exporters {
		switch name {
		case pkgtelemetry.TraceExporterOTLP:
			exporter := traces.NewOTLPExporter(otlpExporterConfig(cfg.Traces.OTLP), resource, meter)
			store, err := newFailureStore(ctx, cfg.Traces.OTLP.FailureStore, failurestore.Options[tracing.SpanData]{
				Backend:   traces.OTLPBackendName,
				Operation: traces.FailureOperation,
				Exporter:  exporter,
				Codec:     traces.NewCodec(resource),
				Meter:     meter,
				Report:    report,
			})
			if err != nil {
				return nil, errors.Join(fmt.Errorf("create the failure store of the otlp trace exporter: %w", err), exporter.Close(), closeAll())
			}
			destinations = append(destinations, traces.Destination{Exporter: exporter, Store: store})
		case pkgtelemetry.TraceExporterStdout:
			destinations = append(destinations, traces.Destination{Exporter: jsonlines.NewStdout(traces.MarshalJSON(resource))})
		case pkgtelemetry.TraceExporterFile:
			exporter, err := jsonlines.OpenFile(cfg.Traces.File.Path, traces.MarshalJSON(resource))
			if err != nil {
				return nil, errors.Join(fmt.Errorf("create the file trace exporter: %w", err), closeAll())
			}
			destinations = append(destinations, traces.Destination{Exporter: exporter})
		}
	}
	return destinations, nil
}

// newMetricDestinationsは、cfgで選んだmetricsの送り先を作ります。作れない送り先があれば、
// 作った送り先を閉じてerrorを返します。
func newMetricDestinations(
	ctx context.Context,
	cfg pkgtelemetry.Config,
	resource *telemetrydata.Resource,
	meter pkgobserver.Meter,
	report delivery.ErrorHandler,
) ([]metricsdelivery.Destination, error) {
	destinations := make([]metricsdelivery.Destination, 0, len(cfg.Metrics.Exporters))
	closeAll := func() error {
		return closeDestinations(destinations, func(d metricsdelivery.Destination) (delivery.Exporter[metrics.Metric], delivery.FailureStore[metrics.Metric]) {
			return d.Exporter, d.Store
		})
	}
	for _, name := range cfg.Metrics.Exporters {
		switch name {
		case pkgtelemetry.MetricExporterOTLP:
			exporter := metricsdelivery.NewOTLPExporter(otlpExporterConfig(cfg.Metrics.OTLP), resource, meter)
			store, err := newFailureStore(ctx, cfg.Metrics.OTLP.FailureStore, failurestore.Options[metrics.Metric]{
				Backend:   metricsdelivery.OTLPBackendName,
				Operation: metricsdelivery.FailureOperation,
				Exporter:  exporter,
				Codec:     metricsdelivery.NewCodec(resource),
				Meter:     meter,
				Report:    report,
			})
			if err != nil {
				return nil, errors.Join(fmt.Errorf("create the failure store of the otlp metric exporter: %w", err), exporter.Close(), closeAll())
			}
			destinations = append(destinations, metricsdelivery.Destination{Exporter: exporter, Store: store})
		case pkgtelemetry.MetricExporterStdout:
			destinations = append(destinations, metricsdelivery.Destination{Exporter: jsonlines.NewStdout(metricsdelivery.MarshalJSON(resource))})
		case pkgtelemetry.MetricExporterFile:
			exporter, err := jsonlines.OpenFile(cfg.Metrics.File.Path, metricsdelivery.MarshalJSON(resource))
			if err != nil {
				return nil, errors.Join(fmt.Errorf("create the file metric exporter: %w", err), closeAll())
			}
			destinations = append(destinations, metricsdelivery.Destination{Exporter: exporter})
		}
	}
	return destinations, nil
}

// otlpExporterConfigは、公開の設定を、OTLP/HTTPの送り先の設定にします。
func otlpExporterConfig(cfg pkgtelemetry.HTTPConfig) otlphttp.Config {
	return otlphttp.Config{
		Endpoint:       cfg.Endpoint,
		Headers:        cfg.Headers,
		Compression:    cfg.Compression,
		Timeout:        cfg.Timeout,
		Retries:        cfg.Retries,
		RetryBaseDelay: cfg.RetryBaseDelay,
		RetryMaxDelay:  cfg.RetryMaxDelay,
		CircuitBreaker: cfg.CircuitBreaker,
	}
}

// newFailureStoreは、cfgで選んだfailure storeを作ります。何も残さない設定なら、nilを
// 返します。
func newFailureStore[T any](ctx context.Context, cfg pkgtelemetry.FailureStoreConfig, opts failurestore.Options[T]) (delivery.FailureStore[T], error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	store, err := failurestore.New(ctx, failurestore.Config{
		Store:            cfg.Store,
		Replay:           cfg.Replay,
		ReplaySupervisor: cfg.ReplaySupervisor,
	}, opts)
	if err != nil || store == nil {
		return nil, err
	}
	return store, nil
}

// closeDestinationsは、作った送り先を、failure storeを止めてから閉じます。
func closeDestinations[D, T any](destinations []D, parts func(D) (delivery.Exporter[T], delivery.FailureStore[T])) error {
	var errs []error
	for _, destination := range destinations {
		exporter, store := parts(destination)
		if store != nil {
			errs = append(errs, store.Shutdown(context.Background()))
		}
		errs = append(errs, exporter.Close())
	}
	return errors.Join(errs...)
}

// TelemetryRuntimeOfは、root packageの*heterolog.Telemetryから内部のruntimeを取り出す
// 関数です。root packageが初期化のときに設定し、root packageの隣の公開package otelexportが
// 使います。root packageの型はここから参照できない（importが循環する）ため、関数で
// 受け渡します。*heterolog.Telemetryでない値とnilには、nilを返します。
var TelemetryRuntimeOf func(telemetry any) *telemetryruntime.Runtime
