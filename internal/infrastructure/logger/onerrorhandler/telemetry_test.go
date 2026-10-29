package onerrorhandler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// TestCombinedHandlersTraceEachDestinationAndCountInternalLogsは、内部ログ1件を1回数え、
// 通知先ごとに呼び出し元のspanの子のspanを作り、そのspanのcontextを通知先へ渡すことを
// 確認します。
func TestCombinedHandlersTraceEachDestinationAndCountInternalLogs(t *testing.T) {
	tracer := &observertest.Tracer{}
	meter := observertest.NewMeter()
	ctx, parent := tracer.StartSpan(context.Background(), "request")
	defer parent.End()

	received := make(chan context.Context, 4)
	warnHandler := func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		received <- ctx
	}
	errorHandler := func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		received <- ctx
	}

	CombineWarnHandlers(
		logging.NamedLoggerHandler{Name: "file", Handler: warnHandler},
		logging.NamedLoggerHandler{Name: "slack", Handler: warnHandler},
	)(ctx, &logging.LogEntry{Level: pkglogger.LogLevelWarn, Message: "slow write"}, tracer, meter)
	CombineErrorHandlers(
		logging.NamedLoggerErrorHandler{Name: "stdout", Handler: errorHandler},
	)(ctx, errors.New("write failed"), nil, tracer, meter)

	require.Equal(t, int64(1), meter.Count(observertest.Key("heterolog_internal_logs_total", "level", "warn")))
	require.Equal(t, int64(1), meter.Count(observertest.Key("heterolog_internal_logs_total", "level", "error")))

	spans := tracer.Spans(SpanInternalLogNotify)
	require.Len(t, spans, 3)
	destinations := map[any]any{}
	for _, span := range spans {
		require.Same(t, parent, span.Parent)
		require.Equal(t, 1, span.Ends())
		destination, _ := span.Attr("heterolog.destination")
		level, _ := span.Attr("heterolog.level")
		destinations[destination] = level
	}
	require.Equal(t, map[any]any{"file": "warn", "slack": "warn", "stdout": "error"}, destinations)

	close(received)
	for handlerCtx := range received {
		span, ok := observer.SpanFromContext(handlerCtx).(*observertest.Span)
		require.True(t, ok)
		require.Contains(t, spans, span)
	}
}
