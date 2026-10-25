package observer

import "context"

type TraceID string

type traceIDKey struct{}

type Attr struct {
	Key   string
	Value any
}

type Tracer interface {
	StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
}

type TraceIDProvider interface {
	TraceIDFromContext(ctx context.Context) TraceID
}

type Meter interface {
	Counter(name string) Counter
	Gauge(name string) Gauge
}

type Counter interface {
	Add(ctx context.Context, value int64, attrs ...Attr)
}

// Gauge は、その時点の値を観測系メトリクスとして記録するための interface です。
//
// Counter と違い、累積ではなく現在値を表現する用途に使います。
// memory pressure ratio や available bytes のような値は Gauge が自然です。
type Gauge interface {
	Record(ctx context.Context, value float64, attrs ...Attr)
}

// これはOTEL SDKは使っていない
type Logger interface {
	Log(ctx context.Context, level string, msg string, attrs ...Attr)
}

// 上のTracerで物足りなくなった用に拡張に備える
type Span interface {
	// TraceID は Span に紐づく trace id を返す。未対応/不明なら ""。
	TraceID() TraceID
	End()
	AddEvent(name string, attrs ...Attr)
	SetAttributes(attrs ...Attr)
	RecordError(err error, attrs ...Attr)
}

// 上のTracerで物足りなくなった用に拡張に備える
//type AdvancedTracer interface {
//	StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
//}

func WithTraceID(ctx context.Context, tid TraceID) context.Context {
	if tid == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDKey{}, tid)
}

func TraceIDFromContext(ctx context.Context) TraceID {
	v := ctx.Value(traceIDKey{})
	if v == nil {
		return ""
	}
	tid, _ := v.(TraceID)
	return tid
}

type spanContextKey struct{}

func WithSpan(ctx context.Context, sp Span) context.Context {
	if ctx == nil || sp == nil {
		return ctx
	}
	return context.WithValue(ctx, spanContextKey{}, sp)
}

func SpanFromContext(ctx context.Context) Span {
	if ctx == nil {
		return nil
	}
	sp, _ := ctx.Value(spanContextKey{}).(Span)
	return sp
}
