package observer

import (
	"context"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ExposeSpansは、tracerが始めたspanとそのtrace IDを、返すcontextへ入れるtracerを返します。
// loggerはSpanFromContextでspanへeventを足し、TraceIDFromContextで送信失敗の記録に
// trace IDを残します。利用者が実装したTracerはWithSpanを呼べないため、constructorへ
// 渡されたtracerはこの関数で包みます。nilは何もしない実装にします。
func ExposeSpans(t pkgobserver.Tracer) pkgobserver.Tracer {
	t = EnsureTracer(t)
	switch t.(type) {
	case NopTracer, spanExposingTracer:
		return t
	}
	return spanExposingTracer{next: t}
}

type spanExposingTracer struct {
	next pkgobserver.Tracer
}

func (t spanExposingTracer) StartSpan(ctx context.Context, name string, attrs ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	spanCtx, span := t.next.StartSpan(ctx, name, attrs...)
	if spanCtx == nil {
		spanCtx = ctx
	}
	if IsNilInterface(span) {
		span = NopSpan{}
	}
	spanCtx = WithTraceID(spanCtx, span.TraceID())
	return WithSpan(spanCtx, span), span
}
