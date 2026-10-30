package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	goruntime "runtime"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/telemetryruntime"
	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/failurestore"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/otlphttp"
	"github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/jsonlines"
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
	}
}

// NewTelemetryは、cfgを検証し、送り先とTracerを組み立てます。optsは、WithObserversの
// Meterと、WithTelemetryErrorHandlerの関数だけを使います。Meterには、traceの配送の仕組みの
// metricsを記録し、関数には配送のerrorを渡します。配送の仕組みは自分の送信のspanを
// 作らないため、Tracerは使いません。送り先がなければ、何も記録しないtelemetryを返します。
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
	if len(cfg.Traces.Exporters) == 0 {
		return telemetryruntime.New(nil, nil), nil
	}
	options := loggerruntime.ResolveOptions(opts)
	report := delivery.NewErrorHandler(options.TelemetryErrorHandler)
	destinations, err := newTraceDestinations(ctx, cfg, options.Meter, report)
	if err != nil {
		return nil, err
	}
	pipeline := traces.NewPipeline(traces.Config{
		QueueSize:      cfg.Traces.QueueSize,
		BatchSize:      cfg.Traces.BatchSize,
		ExportInterval: cfg.Traces.ExportInterval,
	}, destinations, options.Meter, report)
	return telemetryruntime.New(tracing.NewTracer(pipeline, cfg.Traces.SamplingRatio), pipeline), nil
}

// newTraceDestinationsは、cfgで選んだ送り先を作ります。作れない送り先があれば、作った
// 送り先を閉じてerrorを返します。
func newTraceDestinations(
	ctx context.Context,
	cfg pkgtelemetry.Config,
	meter pkgobserver.Meter,
	report delivery.ErrorHandler,
) ([]traces.Destination, error) {
	resource := otlpcommon.NewResource(cfg.Resource)
	destinations := make([]traces.Destination, 0, len(cfg.Traces.Exporters))
	for _, name := range cfg.Traces.Exporters {
		switch name {
		case pkgtelemetry.TraceExporterOTLP:
			otlpCfg := cfg.Traces.OTLP
			exporter := traces.NewOTLPExporter(otlphttp.Config{
				Endpoint:       otlpCfg.Endpoint,
				Headers:        otlpCfg.Headers,
				Compression:    otlpCfg.Compression,
				Timeout:        otlpCfg.Timeout,
				Retries:        otlpCfg.Retries,
				RetryBaseDelay: otlpCfg.RetryBaseDelay,
				RetryMaxDelay:  otlpCfg.RetryMaxDelay,
				CircuitBreaker: otlpCfg.CircuitBreaker,
			}, resource, meter)
			destination := traces.Destination{Exporter: exporter}
			if otlpCfg.FailureStore.Enabled() {
				store, err := failurestore.New(ctx, failurestore.Config{
					Store:            otlpCfg.FailureStore.Store,
					Replay:           otlpCfg.FailureStore.Replay,
					ReplaySupervisor: otlpCfg.FailureStore.ReplaySupervisor,
				}, failurestore.Options[tracing.SpanData]{
					Backend:   traces.OTLPBackendName,
					Operation: traces.FailureOperation,
					Exporter:  exporter,
					Codec:     traces.NewCodec(resource),
					Meter:     meter,
					Report:    report,
				})
				if err != nil {
					return nil, errors.Join(fmt.Errorf("create the failure store of the otlp trace exporter: %w", err),
						exporter.Close(), closeTraceDestinations(destinations))
				}
				if store != nil {
					destination.Store = store
				}
			}
			destinations = append(destinations, destination)
		case pkgtelemetry.TraceExporterStdout:
			destinations = append(destinations, traces.Destination{Exporter: jsonlines.NewStdout(resource)})
		case pkgtelemetry.TraceExporterFile:
			exporter, err := jsonlines.OpenFile(cfg.Traces.File.Path, resource)
			if err != nil {
				return nil, errors.Join(fmt.Errorf("create the file trace exporter: %w", err), closeTraceDestinations(destinations))
			}
			destinations = append(destinations, traces.Destination{Exporter: exporter})
		}
	}
	return destinations, nil
}

// closeTraceDestinationsは、作った送り先を、failure storeを止めてから閉じます。
func closeTraceDestinations(destinations []traces.Destination) error {
	var errs []error
	for _, destination := range destinations {
		if destination.Store != nil {
			errs = append(errs, destination.Store.Shutdown(context.Background()))
		}
		errs = append(errs, destination.Exporter.Close())
	}
	return errors.Join(errs...)
}

// TelemetryRuntimeOfは、root packageの*heterolog.Telemetryから内部のruntimeを取り出す
// 関数です。root packageが初期化のときに設定し、root packageの隣の公開package otelexportが
// 使います。root packageの型はここから参照できない（importが循環する）ため、関数で
// 受け渡します。*heterolog.Telemetryでない値とnilには、nilを返します。
var TelemetryRuntimeOf func(telemetry any) *telemetryruntime.Runtime
