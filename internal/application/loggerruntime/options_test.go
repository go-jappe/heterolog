package loggerruntime_test

import (
	"context"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/observer"
)

func TestResolveOptionsExposesCallerSpans(t *testing.T) {
	o, err := loggerruntime.ResolveOptions([]loggerruntime.Option{
		loggerruntime.WithObservers(&runtimeObserverTracer{}, nil),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, span := o.Tracer.StartSpan(context.Background(), "heterolog.test")
	defer span.End()

	if observer.SpanFromContext(ctx) != span {
		t.Error("the span started by the caller's tracer is not in the context")
	}
}
