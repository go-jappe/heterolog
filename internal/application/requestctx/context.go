package requestctx

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type requestIDKey struct{}

func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

func RequestID(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	v, ok := ctx.Value(requestIDKey{}).(string)
	return v, ok && v != ""
}

type IDs struct {
	RequestID string
	TraceID   string
	SpanID    string
}

// IDsFromContextは、ctxのrequest ID、trace ID、span IDを返します。trace IDとspan IDは、
// OpenTelemetry SDKのspanがあればそれを、なければheterologのspan contextを使います。
func IDsFromContext(ctx context.Context) IDs {
	var ids IDs
	if ctx == nil {
		return ids
	}

	if id, ok := RequestID(ctx); ok {
		ids.RequestID = id
	}

	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		ids.TraceID = sc.TraceID().String()
		ids.SpanID = sc.SpanID().String()
		return ids
	}
	if heterologSpan := pkgobserver.SpanContextFromContext(ctx); heterologSpan.IsValid() {
		ids.TraceID = string(heterologSpan.TraceID)
		ids.SpanID = string(heterologSpan.SpanID)
	}

	return ids
}
