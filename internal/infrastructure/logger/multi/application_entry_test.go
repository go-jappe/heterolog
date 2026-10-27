package multi

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/requestctx"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestMultiLoggerBuildsOneApplicationEntryBeforeFanOut(t *testing.T) {
	firstSeen := &occurrenceRecorder{}
	secondSeen := &occurrenceRecorder{}
	first := &syncHandlerStub{handleFn: func(ctx context.Context, entry *infralogging.LogEntry) error {
		firstSeen.record(ctx, entry)
		return nil
	}}
	second := &syncHandlerStub{handleFn: func(ctx context.Context, entry *infralogging.LogEntry) error {
		secondSeen.record(ctx, entry)
		return nil
	}}
	logger := newTestMultiLogger(first, second)

	// nowは呼出しごとに異なる時刻を返します。全childが同じ時刻を受け取ることで、発生時刻を
	// fan-out前に一度だけ確定したことを、呼出し回数に依存せず確認します。
	base := time.Date(2026, time.August, 21, 14, 25, 30, 123456789, time.FixedZone("UTC+9", 9*60*60))
	var nowCalls atomic.Int32
	logger.now = func() time.Time {
		return base.Add(time.Duration(nowCalls.Add(1)) * time.Second)
	}

	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{17, 18, 19, 20, 21, 22, 23, 24}
	ctx := trace.ContextWithSpanContext(
		requestctx.WithRequestID(context.Background(), "context-request"),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}),
	)

	traceOverride, err := pkglogger.String("trace_id", "field-trace")
	require.NoError(t, err)
	component, err := pkglogger.String("component", "checkout")
	require.NoError(t, err)
	payloadSource := map[string]any{"mode": "safe", "enabled": true}
	payload, err := pkglogger.Any("payload", payloadSource)
	require.NoError(t, err)
	payloadSource["mode"] = "mutated-after-field-construction"

	require.NoError(t, logger.Info(
		ctx,
		"accepted",
		pkglogger.WithRequestID("field-request"),
		traceOverride,
		component,
		payload,
	))

	firstEntry := firstSeen.entry(t)
	secondEntry := secondSeen.entry(t)
	require.NotSame(t, firstEntry, secondEntry)
	require.Equal(t, firstEntry.OccurredAt, secondEntry.OccurredAt, "every child must share one occurrence time")
	requireOccurredAtFromClock(t, firstEntry.OccurredAt, base, int(nowCalls.Load()))
	for _, entry := range []*infralogging.LogEntry{firstEntry, secondEntry} {
		require.Equal(t, pkglogger.LogLevelInfo, entry.Level)
		require.Equal(t, "accepted", entry.Message)
		require.Equal(t, "field-request", entry.RequestID)
		require.Equal(t, "field-trace", entry.TraceID)
		require.Equal(t, spanID.String(), entry.SpanID)
		require.Equal(t, "checkout", entry.Fields["component"])
		require.Equal(t, map[string]any{"mode": "safe", "enabled": true}, entry.Fields["payload"])
		require.NotContains(t, entry.Fields, "request_id")
		require.NotContains(t, entry.Fields, "trace_id")
		require.NotContains(t, entry.Fields, "span_id")
	}
	firstEntry.Fields["component"] = "first-backend-only"
	require.Equal(t, "checkout", secondEntry.Fields["component"])
}

func TestNilMultiLoggerRejectsApplicationInput(t *testing.T) {
	var logger *MultiLogger

	err := logger.Info(context.Background(), "must not be lost")

	require.ErrorIs(t, err, ErrNoMultiLogger)
	var opErr *OperationError
	require.ErrorAs(t, err, &opErr)
	require.Equal(t, "multi.Info", opErr.Op)
}

func TestAsyncAndDeliveryMultiLoggerBuildApplicationEntryBeforeFanOut(t *testing.T) {
	fixed := time.Date(2026, time.August, 21, 15, 0, 0, 987654321, time.UTC)

	t.Run("async", func(t *testing.T) {
		seen := &occurrenceRecorder{}
		child := &syncHandlerStub{handleFn: func(ctx context.Context, entry *infralogging.LogEntry) error {
			seen.record(ctx, entry)
			return nil
		}}
		logger, err := NewAsyncBestEffortMultiLogger(
			asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
			child,
		)
		require.NoError(t, err)
		logger.now = func() time.Time { return fixed }

		logger.Info(context.Background(), "async")
		require.NoError(t, logger.Close())
		require.Equal(t, fixed, seen.entry(t).OccurredAt)
	})

	t.Run("delivery", func(t *testing.T) {
		seen := &occurrenceRecorder{}
		child := &deliveryHandlerStub{backend: "child", handleFn: func(ctx context.Context, entry *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
			seen.record(ctx, entry)
			return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
				Backend: "child",
				Status:  pkglogger.DeliverySucceeded,
			}), nil
		}}
		logger := newTestDeliveryMultiLogger(child)
		logger.now = func() time.Time { return fixed }

		report, err := logger.Info(context.Background(), "delivery")
		require.NoError(t, err)
		require.Len(t, report.Targets, 1)
		require.Equal(t, fixed, seen.entry(t).OccurredAt)
	})
}

// requireOccurredAtFromClockは、occurredAtがclockの返した時刻（base+1秒からbase+calls秒）の
// いずれかをUTCで表したものであることを確認します。
func requireOccurredAtFromClock(t *testing.T, occurredAt, base time.Time, calls int) {
	t.Helper()
	require.Equal(t, time.UTC, occurredAt.Location())
	for call := 1; call <= calls; call++ {
		if occurredAt.Equal(base.Add(time.Duration(call) * time.Second)) {
			return
		}
	}
	t.Fatalf("OccurredAt = %s, want one of the %d clock readings after %s", occurredAt, calls, base)
}
