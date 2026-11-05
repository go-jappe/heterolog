package requestctx

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

const (
	testTraceID = "0af7651916cd43dd8448eb211c80319c"
	testSpanID  = "b7ad6b7169203331"
)

// ParseTraceParentで読んだ受信requestのtraceなど、heterologのspan contextのIDをログへ渡せることを
// 確かめます。
func TestIDsFromContextReadsHeterologSpanContext(t *testing.T) {
	t.Parallel()

	ctx := pkgobserver.ContextWithSpanContext(context.Background(), pkgobserver.SpanContext{
		TraceID: testTraceID,
		SpanID:  testSpanID,
	})

	ids := IDsFromContext(ctx)
	if ids.TraceID != testTraceID || ids.SpanID != testSpanID {
		t.Fatalf("IDsFromContext() = %+v, want the IDs of the span context", ids)
	}
}

func TestIDsFromContextPrefersOpenTelemetrySpan(t *testing.T) {
	t.Parallel()

	otelTraceID, err := trace.TraceIDFromHex("11111111111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	otelSpanID, err := trace.SpanIDFromHex("2222222222222222")
	if err != nil {
		t.Fatal(err)
	}
	ctx := pkgobserver.ContextWithSpanContext(context.Background(), pkgobserver.SpanContext{
		TraceID: testTraceID,
		SpanID:  testSpanID,
	})
	ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: otelTraceID,
		SpanID:  otelSpanID,
	}))

	ids := IDsFromContext(ctx)
	if ids.TraceID != otelTraceID.String() || ids.SpanID != otelSpanID.String() {
		t.Fatalf("IDsFromContext() = %+v, want the IDs of the OpenTelemetry span", ids)
	}
}

func TestIDsFromContextWithoutSpanHasNoIDs(t *testing.T) {
	t.Parallel()

	if ids := IDsFromContext(context.Background()); ids != (IDs{}) {
		t.Fatalf("IDsFromContext() = %+v, want no IDs", ids)
	}
}
