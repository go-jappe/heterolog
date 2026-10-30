// Package telemetryruntimeは、NewTelemetryが返すtelemetryの実体です。Tracerと、spanを
// 受け取る配送の仕組みを持ち、終了を配送の仕組みへ渡します。配送の仕組みの組み立ては
// internal/bootstrapが受け持ちます。
package telemetryruntime

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/tracing"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// Deliveryは、spanを受け取って送る配送の仕組みです。Shutdownは、受け付けを止め、
// 受け取ったspanを送り終えるのを待ちます。
type Delivery interface {
	tracing.SpanSink
	Shutdown(ctx context.Context) error
}

// Runtimeは、NewTelemetryが返すtelemetryです。zero値は、何も記録しないtelemetryとして
// 振る舞います。
type Runtime struct {
	tracer   pkgobserver.Tracer
	delivery Delivery
}

// Newは、tracerと、そのspanを送るdeliveryを持つRuntimeを返します。送り先がない場合は、
// 両方ともnilを渡します。
func New(tracer pkgobserver.Tracer, delivery Delivery) *Runtime {
	return &Runtime{tracer: tracer, delivery: delivery}
}

// Tracerは、spanを記録するTracerを返します。送り先がなければ、何もしないTracerです。
func (r *Runtime) Tracer() pkgobserver.Tracer {
	if r.tracer == nil {
		return observer.NopTracer{}
	}
	return r.tracer
}

// SpanSinkは、OpenTelemetry SDKから届いたspanを渡す先を返します。送り先がなければ、
// 受け取ったspanを捨てるSpanSinkです。
func (r *Runtime) SpanSink() tracing.SpanSink {
	if r.delivery == nil {
		return discardSink{}
	}
	return r.delivery
}

// Shutdownは、配送の仕組みを止め、受け取ったspanを送り終えるのを待ちます。
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r.delivery == nil {
		return nil
	}
	return r.delivery.Shutdown(ctx)
}

type discardSink struct{}

func (discardSink) Enqueue(tracing.SpanData) {}
