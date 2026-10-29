package loggerruntime_test

import (
	"context"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

func TestResolveOptionsExposesCallerSpans(t *testing.T) {
	o := loggerruntime.ResolveOptions([]loggerruntime.Option{
		loggerruntime.WithObservers(&observertest.Tracer{}, nil),
	})

	ctx, span := o.Tracer.StartSpan(context.Background(), "heterolog.test")
	defer span.End()

	if observer.SpanFromContext(ctx) != span {
		t.Error("the span started by the caller's tracer is not in the context")
	}
}
