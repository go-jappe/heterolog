package observer

import (
	"context"
	"testing"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// callerTracerは、利用者が実装したtracerのように、contextへspanを入れずに返します。
type callerTracer struct{}

func (callerTracer) StartSpan(ctx context.Context, _ string, _ ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	return ctx, &callerSpan{}
}

type callerSpan struct{ NopSpan }

func (*callerSpan) TraceID() pkgobserver.TraceID { return "caller-trace" }

func TestExposeSpansPutsCallerSpanInContext(t *testing.T) {
	tracer := ExposeSpans(callerTracer{})

	ctx, span := tracer.StartSpan(context.Background(), "heterolog.test")

	if got := SpanFromContext(ctx); got != span {
		t.Errorf("SpanFromContext = %v, want the started span %v", got, span)
	}
	if got := TraceIDFromContext(ctx); got != "caller-trace" {
		t.Errorf("TraceIDFromContext = %q, want caller-trace", got)
	}
}

// nilContextTracerは、contextもspanもnilで返す不完全な実装です。
type nilContextTracer struct{}

func (nilContextTracer) StartSpan(context.Context, string, ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	return nil, nil
}

func TestExposeSpansReplacesNilResults(t *testing.T) {
	ctx, span := ExposeSpans(nilContextTracer{}).StartSpan(context.Background(), "heterolog.test")

	if ctx == nil {
		t.Fatal("StartSpan returned a nil context")
	}
	if _, ok := span.(NopSpan); !ok {
		t.Errorf("span = %T, want NopSpan", span)
	}
	if SpanFromContext(ctx) != span {
		t.Error("the replaced span is not in the context")
	}
}

func TestExposeSpansKeepsKnownTracers(t *testing.T) {
	if _, ok := ExposeSpans(nil).(NopTracer); !ok {
		t.Error("ExposeSpans(nil) is not NopTracer")
	}
	wrapped := ExposeSpans(callerTracer{})
	if again := ExposeSpans(wrapped); again != wrapped {
		t.Error("ExposeSpans wrapped an already wrapped tracer again")
	}
}
