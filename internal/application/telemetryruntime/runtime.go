// Package telemetryruntimeは、NewTelemetryが返すtelemetryの実体です。TracerとMeterと、
// spanとmetricsを受け取る配送の仕組みを持ち、終了を配送の仕組みへ渡します。配送の仕組みの
// 組み立ては、internal/bootstrapが受け持ちます。
package telemetryruntime

import (
	"context"
	"errors"

	"github.com/go-jappe/heterolog/internal/application/configview"
	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/tracing"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// Deliveryは、spanを受け取って送る配送の仕組みです。Shutdownは、受け付けを止め、
// 受け取ったspanを送り終えるのを待ちます。
type Delivery interface {
	tracing.SpanSink
	Shutdown(ctx context.Context) error
}

// MetricSinkは、OpenTelemetry SDKから届いたmetricsを受け取る配送の仕組みです。Pushは、
// 送り終わるのを待たずに返ります。batchは、Pushが返った後も変更してはいけません。
type MetricSink interface {
	Push(batch []metrics.Metric)
}

// MetricDeliveryは、metricsを集めて送る配送の仕組みです。Shutdownは、最後に集めた分を
// 含めて送り終えるのを待ちます。
type MetricDelivery interface {
	MetricSink
	Shutdown(ctx context.Context) error
}

// Partsは、Runtimeの部品です。送り先のない信号の部品は、nilにします。
type Parts struct {
	// Configは、telemetryを作った設定です。管理用APIが設定の文書を作るために返します。
	Config pkgtelemetry.Config
	// InternalMeterは、配送の仕組みが自分のmetrics（送ったspanの数など）を記録する内部の
	// Meterです。statusの報告が読みます。nilなら、Metricsは何も返しません。
	InternalMeter *metrics.Meter
	Tracer        pkgobserver.Tracer
	Spans         Delivery
	Meter         pkgobserver.Meter
	// Metricsは、Meterの値と、MetricSinkへ届いたmetricsを送ります。
	Metrics MetricDelivery
}

// Runtimeは、NewTelemetryが返すtelemetryです。zero値は、何も記録しないtelemetryとして
// 振る舞います。
type Runtime struct {
	parts Parts
}

// Newは、partsを持つRuntimeを返します。
func New(parts Parts) *Runtime {
	return &Runtime{parts: parts}
}

// Metricsは、配送の仕組みが記録したmetricsの累積値を返します。statusの報告が、送り先
// ごとの結果と所要時間を読むために使います。
func (r *Runtime) Metrics() []metrics.Metric {
	if r.parts.InternalMeter == nil {
		return nil
	}
	return r.parts.InternalMeter.Collect()
}

// Configurationは、このtelemetryを作った設定を返します。管理用APIが設定の文書を作るための
// ものです。
func (r *Runtime) Configuration() configview.Configuration {
	return configview.Configuration{Kind: configview.KindTelemetry, Value: r.parts.Config}
}

// Tracerは、spanを記録するTracerを返します。traceの送り先がなければ、何もしないTracerです。
func (r *Runtime) Tracer() pkgobserver.Tracer {
	if r.parts.Tracer == nil {
		return observer.NopTracer{}
	}
	return r.parts.Tracer
}

// Meterは、metricsを記録するMeterを返します。metricsの送り先がなければ、何もしない
// Meterです。
func (r *Runtime) Meter() pkgobserver.Meter {
	if r.parts.Meter == nil {
		return observer.NopMeter{}
	}
	return r.parts.Meter
}

// SpanSinkは、OpenTelemetry SDKから届いたspanを渡す先を返します。traceの送り先がなければ、
// 受け取ったspanを捨てるSpanSinkです。
func (r *Runtime) SpanSink() tracing.SpanSink {
	if r.parts.Spans == nil {
		return discardSink{}
	}
	return r.parts.Spans
}

// MetricSinkは、OpenTelemetry SDKから届いたmetricsを渡す先を返します。metricsの送り先が
// なければ、受け取ったmetricsを捨てるMetricSinkです。
func (r *Runtime) MetricSink() MetricSink {
	if r.parts.Metrics == nil {
		return discardSink{}
	}
	return r.parts.Metrics
}

// Shutdownは、spanの配送の仕組みを止めてから、metricsの配送の仕組みを止めます。spanの
// 配送の仕組みが終わり際に記録したmetricsも、最後に集めて送るためです。
func (r *Runtime) Shutdown(ctx context.Context) error {
	var errs []error
	if r.parts.Spans != nil {
		errs = append(errs, r.parts.Spans.Shutdown(ctx))
	}
	if r.parts.Metrics != nil {
		errs = append(errs, r.parts.Metrics.Shutdown(ctx))
	}
	return errors.Join(errs...)
}

type discardSink struct{}

func (discardSink) Enqueue(tracing.SpanData) {}

func (discardSink) Push([]metrics.Metric) {}
