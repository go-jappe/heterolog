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
	"github.com/go-jappe/heterolog/internal/infrastructure/traces"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/jsonlines"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlp"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlphttp"
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
// Meterだけを使い、traceの配送の仕組みのmetricsを記録します。配送の仕組みは自分の送信の
// spanを作らないため、Tracerは使いません。送り先がなければ、何も記録しないtelemetryを
// 返します。
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
	meter := loggerruntime.ResolveOptions(opts).Meter
	exporters, err := newTraceExporters(cfg, meter)
	if err != nil {
		return nil, err
	}
	pipeline := traces.NewPipeline(traces.Config{
		QueueSize:      cfg.Traces.QueueSize,
		BatchSize:      cfg.Traces.BatchSize,
		ExportInterval: cfg.Traces.ExportInterval,
	}, exporters, meter)
	return telemetryruntime.New(tracing.NewTracer(pipeline, cfg.Traces.SamplingRatio), pipeline), nil
}

// newTraceExportersは、cfgで選んだ送り先を作ります。作れない送り先があれば、作った
// 送り先を閉じてerrorを返します。
func newTraceExporters(cfg pkgtelemetry.Config, meter pkgobserver.Meter) ([]traces.Exporter, error) {
	resource := otlp.NewResource(cfg.Resource)
	exporters := make([]traces.Exporter, 0, len(cfg.Traces.Exporters))
	for _, name := range cfg.Traces.Exporters {
		switch name {
		case pkgtelemetry.TraceExporterOTLP:
			otlpCfg := cfg.Traces.OTLP
			exporters = append(exporters, otlphttp.New(otlphttp.Config{
				Endpoint:       otlpCfg.Endpoint,
				Headers:        otlpCfg.Headers,
				Compression:    otlpCfg.Compression,
				Timeout:        otlpCfg.Timeout,
				Retries:        otlpCfg.Retries,
				RetryBaseDelay: otlpCfg.RetryBaseDelay,
				RetryMaxDelay:  otlpCfg.RetryMaxDelay,
			}, resource, meter))
		case pkgtelemetry.TraceExporterStdout:
			exporters = append(exporters, jsonlines.NewStdout(resource))
		case pkgtelemetry.TraceExporterFile:
			exporter, err := jsonlines.OpenFile(cfg.Traces.File.Path, resource)
			if err != nil {
				return nil, errors.Join(fmt.Errorf("create the file trace exporter: %w", err), closeTraceExporters(exporters))
			}
			exporters = append(exporters, exporter)
		}
	}
	return exporters, nil
}

func closeTraceExporters(exporters []traces.Exporter) error {
	var errs []error
	for _, exporter := range exporters {
		errs = append(errs, exporter.Close())
	}
	return errors.Join(errs...)
}

// TelemetryRuntimeOfは、root packageの*heterolog.Telemetryから内部のruntimeを取り出す
// 関数です。root packageが初期化のときに設定し、root packageの隣の公開package otelexportが
// 使います。root packageの型はここから参照できない（importが循環する）ため、関数で
// 受け渡します。*heterolog.Telemetryでない値とnilには、nilを返します。
var TelemetryRuntimeOf func(telemetry any) *telemetryruntime.Runtime
