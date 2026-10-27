package observer

import (
	"context"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type traceIDKey struct{}

type TraceIDProvider interface {
	TraceIDFromContext(ctx context.Context) pkgobserver.TraceID
}

// これはOTEL SDKは使っていない
type Logger interface {
	Log(ctx context.Context, level string, msg string, attrs ...pkgobserver.Attr)
}

// 上のTracerで物足りなくなった用に拡張に備える
//type AdvancedTracer interface {
//	StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
//}

func WithTraceID(ctx context.Context, tid pkgobserver.TraceID) context.Context {
	if tid == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDKey{}, tid)
}

func TraceIDFromContext(ctx context.Context) pkgobserver.TraceID {
	v := ctx.Value(traceIDKey{})
	if v == nil {
		return ""
	}
	tid, _ := v.(pkgobserver.TraceID)
	return tid
}

type spanContextKey struct{}

func WithSpan(ctx context.Context, sp pkgobserver.Span) context.Context {
	if ctx == nil || sp == nil {
		return ctx
	}
	return context.WithValue(ctx, spanContextKey{}, sp)
}

func SpanFromContext(ctx context.Context) pkgobserver.Span {
	if ctx == nil {
		return nil
	}
	sp, _ := ctx.Value(spanContextKey{}).(pkgobserver.Span)
	return sp
}
