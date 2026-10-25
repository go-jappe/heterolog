package multi

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

type occurrenceRecorder struct {
	mu       sync.Mutex
	entries  []*infralogging.LogEntry
	contexts []context.Context
}

func (r *occurrenceRecorder) record(ctx context.Context, entry *infralogging.LogEntry) {
	r.mu.Lock()
	r.entries = append(r.entries, entry)
	r.contexts = append(r.contexts, ctx)
	r.mu.Unlock()
}

func (r *occurrenceRecorder) entry(t *testing.T) *infralogging.LogEntry {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Len(t, r.entries, 1)
	return r.entries[0]
}

func TestMultiLoggerCanonicalizesCompletedEntryOccurrenceBeforeFanOut(t *testing.T) {
	inputOccurredAt := time.Date(2026, 8, 20, 10, 0, 0, 123456789, time.FixedZone("JST", 9*60*60))
	want := inputOccurredAt.UTC()
	entry := &infralogging.LogEntry{
		OccurredAt: inputOccurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "shared",
		Fields:     map[string]any{"request": "one"},
	}
	firstSeen := &occurrenceRecorder{}
	secondSeen := &occurrenceRecorder{}
	first := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		firstSeen.record(ctx, got)
		return nil
	}}
	second := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		secondSeen.record(ctx, got)
		return nil
	}}

	require.NoError(t, newTestMultiLogger(first, second).runAll(context.Background(), entry))
	for _, recorder := range []*occurrenceRecorder{firstSeen, secondSeen} {
		got := recorder.entry(t)
		require.Equal(t, want, got.OccurredAt)
		require.Equal(t, time.UTC, got.OccurredAt.Location())
		require.Equal(t, entry.Level, got.Level)
		require.Equal(t, entry.Message, got.Message)
	}
	require.Equal(t, inputOccurredAt, entry.OccurredAt)
	require.NotSame(t, firstSeen.entry(t), secondSeen.entry(t))
}

func TestAsyncMultiLoggerCanonicalizesCompletedEntryOccurrenceBeforeFanOut(t *testing.T) {
	inputOccurredAt := time.Date(2026, 8, 20, 10, 1, 0, 456, time.FixedZone("UTC-4", -4*60*60))
	want := inputOccurredAt.UTC()
	entry := &infralogging.LogEntry{OccurredAt: inputOccurredAt, Level: pkglogger.LogLevelInfo, Message: "async"}
	firstSeen := &occurrenceRecorder{}
	secondSeen := &occurrenceRecorder{}
	first := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		firstSeen.record(ctx, got)
		return nil
	}}
	second := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		secondSeen.record(ctx, got)
		return nil
	}}
	m, err := NewAsyncBestEffortMultiLogger(
		asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 2},
		first,
		second,
	)
	require.NoError(t, err)

	m.runAll(context.Background(), entry)
	require.NoError(t, m.Close())
	require.Equal(t, want, firstSeen.entry(t).OccurredAt)
	require.Equal(t, time.UTC, firstSeen.entry(t).OccurredAt.Location())
	require.Equal(t, want, secondSeen.entry(t).OccurredAt)
	require.Equal(t, time.UTC, secondSeen.entry(t).OccurredAt.Location())
	require.Equal(t, inputOccurredAt, entry.OccurredAt)
}

func TestDeliveryMultiLoggerCanonicalizesCompletedEntryOccurrenceBeforeFanOut(t *testing.T) {
	inputOccurredAt := time.Date(2026, 8, 20, 10, 2, 0, 789, time.FixedZone("UTC+2", 2*60*60))
	want := inputOccurredAt.UTC()
	entry := &infralogging.LogEntry{OccurredAt: inputOccurredAt, Level: pkglogger.LogLevelWarn, Message: "delivery"}
	firstSeen := &occurrenceRecorder{}
	secondSeen := &occurrenceRecorder{}
	first := &deliveryHandlerStub{backend: "first", handleFn: func(ctx context.Context, got *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		firstSeen.record(ctx, got)
		return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{Backend: "first", Status: pkglogger.DeliverySucceeded}), nil
	}}
	second := &deliveryHandlerStub{backend: "second", handleFn: func(ctx context.Context, got *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		secondSeen.record(ctx, got)
		return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{Backend: "second", Status: pkglogger.DeliverySucceeded}), nil
	}}

	report, err := newTestDeliveryMultiLogger(first, second).runAll(context.Background(), entry)
	require.NoError(t, err)
	require.Len(t, report.Targets, 2)
	require.Equal(t, want, firstSeen.entry(t).OccurredAt)
	require.Equal(t, time.UTC, firstSeen.entry(t).OccurredAt.Location())
	require.Equal(t, want, secondSeen.entry(t).OccurredAt)
	require.Equal(t, time.UTC, secondSeen.entry(t).OccurredAt.Location())
	require.Equal(t, inputOccurredAt, entry.OccurredAt)
}

func TestMultiLoggerCapturesMissingOccurrenceOnceBeforeFanOut(t *testing.T) {
	// nowは呼出しごとに異なる時刻を返します。全childが同じ時刻を受け取ることで、欠けた
	// 発生時刻をfan-out前に一度だけ確定したことを、呼出し回数に依存せず確認します。
	base := time.Date(2026, 8, 20, 10, 3, 0, 123, time.FixedZone("JST", 9*60*60))
	var nowCalls atomic.Int32
	entry := &infralogging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "missing occurrence"}
	firstSeen := &occurrenceRecorder{}
	secondSeen := &occurrenceRecorder{}
	first := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		firstSeen.record(ctx, got)
		return nil
	}}
	second := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		secondSeen.record(ctx, got)
		return nil
	}}
	m := newTestMultiLogger(first, second)
	m.now = func() time.Time {
		return base.Add(time.Duration(nowCalls.Add(1)) * time.Second)
	}

	require.NoError(t, m.runAll(context.Background(), entry))
	firstEntry, secondEntry := firstSeen.entry(t), secondSeen.entry(t)
	require.Equal(t, firstEntry.OccurredAt, secondEntry.OccurredAt, "every child must share one occurrence time")
	requireOccurredAtFromClock(t, firstEntry.OccurredAt, base, int(nowCalls.Load()))
	require.True(t, entry.OccurredAt.IsZero())
}

func TestAsyncMultiLoggerCapturesMissingOccurrenceOnceBeforeFanOut(t *testing.T) {
	// nowは呼出しごとに異なる時刻を返します。全childが同じ時刻を受け取ることで、欠けた
	// 発生時刻をfan-out前に一度だけ確定したことを、呼出し回数に依存せず確認します。
	base := time.Date(2026, 8, 20, 10, 4, 0, 456, time.FixedZone("UTC-4", -4*60*60))
	var nowCalls atomic.Int32
	entry := &infralogging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "missing async occurrence"}
	firstSeen := &occurrenceRecorder{}
	secondSeen := &occurrenceRecorder{}
	first := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		firstSeen.record(ctx, got)
		return nil
	}}
	second := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		secondSeen.record(ctx, got)
		return nil
	}}
	m, err := NewAsyncBestEffortMultiLogger(
		asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 2},
		first,
		second,
	)
	require.NoError(t, err)
	m.now = func() time.Time {
		return base.Add(time.Duration(nowCalls.Add(1)) * time.Second)
	}

	m.runAll(context.Background(), entry)
	require.NoError(t, m.Close())
	firstEntry, secondEntry := firstSeen.entry(t), secondSeen.entry(t)
	require.Equal(t, firstEntry.OccurredAt, secondEntry.OccurredAt, "every child must share one occurrence time")
	requireOccurredAtFromClock(t, firstEntry.OccurredAt, base, int(nowCalls.Load()))
	require.True(t, entry.OccurredAt.IsZero())
}

func TestDeliveryMultiLoggerCapturesMissingOccurrenceOnceBeforeFanOut(t *testing.T) {
	// nowは呼出しごとに異なる時刻を返します。全childが同じ時刻を受け取ることで、欠けた
	// 発生時刻をfan-out前に一度だけ確定したことを、呼出し回数に依存せず確認します。
	base := time.Date(2026, 8, 20, 10, 5, 0, 789, time.FixedZone("UTC+2", 2*60*60))
	var nowCalls atomic.Int32
	entry := &infralogging.LogEntry{Level: pkglogger.LogLevelWarn, Message: "missing delivery occurrence"}
	firstSeen := &occurrenceRecorder{}
	secondSeen := &occurrenceRecorder{}
	first := &deliveryHandlerStub{backend: "first", handleFn: func(ctx context.Context, got *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		firstSeen.record(ctx, got)
		return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{Backend: "first", Status: pkglogger.DeliverySucceeded}), nil
	}}
	second := &deliveryHandlerStub{backend: "second", handleFn: func(ctx context.Context, got *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		secondSeen.record(ctx, got)
		return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{Backend: "second", Status: pkglogger.DeliverySucceeded}), nil
	}}
	m := newTestDeliveryMultiLogger(first, second)
	m.now = func() time.Time {
		return base.Add(time.Duration(nowCalls.Add(1)) * time.Second)
	}

	report, err := m.runAll(context.Background(), entry)
	require.NoError(t, err)
	require.Len(t, report.Targets, 2)
	firstEntry, secondEntry := firstSeen.entry(t), secondSeen.entry(t)
	require.Equal(t, firstEntry.OccurredAt, secondEntry.OccurredAt, "every child must share one occurrence time")
	requireOccurredAtFromClock(t, firstEntry.OccurredAt, base, int(nowCalls.Load()))
	require.True(t, entry.OccurredAt.IsZero())
}

func TestMultiLoggersPreserveStackRequestOnCompletedEntry(t *testing.T) {
	entry := &infralogging.LogEntry{
		OccurredAt:          time.Date(2026, 8, 20, 10, 3, 0, 0, time.UTC),
		Level:               pkglogger.LogLevelError,
		Message:             "stack",
		StackTraceRequested: true,
	}

	t.Run("sync", func(t *testing.T) {
		seen := &occurrenceRecorder{}
		child := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
			seen.record(ctx, got)
			return nil
		}}
		require.NoError(t, newTestMultiLogger(child).runAll(context.Background(), entry))
		require.True(t, seen.entry(t).StackTraceRequested)
	})

	t.Run("async", func(t *testing.T) {
		seen := &occurrenceRecorder{}
		child := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
			seen.record(ctx, got)
			return nil
		}}
		m, err := NewAsyncBestEffortMultiLogger(
			asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
			child,
		)
		require.NoError(t, err)
		m.runAll(context.Background(), entry)
		require.NoError(t, m.Close())
		require.True(t, seen.entry(t).StackTraceRequested)
	})

	t.Run("delivery", func(t *testing.T) {
		seen := &occurrenceRecorder{}
		child := &deliveryHandlerStub{backend: "child", handleFn: func(ctx context.Context, got *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
			seen.record(ctx, got)
			return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{Backend: "child", Status: pkglogger.DeliverySucceeded}), nil
		}}
		_, err := newTestDeliveryMultiLogger(child).runAll(context.Background(), entry)
		require.NoError(t, err)
		require.True(t, seen.entry(t).StackTraceRequested)
	})
}

// errorを渡さないErrorWithStackも、fan-outはERRORとして受理し、Errを持たないまま
// stackの要求と呼び出し位置を各backendへ渡します。拒否するかどうかをcoreでは判断しません。
func TestMultiLoggersAcceptErrorWithStackWithoutError(t *testing.T) {
	requireStackRequestWithoutError := func(t *testing.T, got *infralogging.LogEntry) {
		t.Helper()
		require.Equal(t, pkglogger.LogLevelError, got.Level)
		require.Equal(t, "stack without error", got.Message)
		require.NoError(t, got.Err)
		require.True(t, got.StackTraceRequested)
		infralogging.EnsureEntryStackTrace(got, 0)
		require.Len(t, got.FuncName, 1)
		require.Contains(t, got.FuncName[0], "TestMultiLoggersAcceptErrorWithStackWithoutError")
	}

	t.Run("sync", func(t *testing.T) {
		seen := &occurrenceRecorder{}
		child := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
			seen.record(ctx, got)
			return nil
		}}
		require.NoError(t, newTestMultiLogger(child).ErrorWithStack(context.Background(), "stack without error", nil))
		requireStackRequestWithoutError(t, seen.entry(t))
	})

	t.Run("async", func(t *testing.T) {
		seen := &occurrenceRecorder{}
		child := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
			seen.record(ctx, got)
			return nil
		}}
		m, err := NewAsyncBestEffortMultiLogger(
			asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
			child,
		)
		require.NoError(t, err)
		m.ErrorWithStack(context.Background(), "stack without error", nil)
		require.NoError(t, m.Close())
		requireStackRequestWithoutError(t, seen.entry(t))
	})

	t.Run("delivery", func(t *testing.T) {
		seen := &occurrenceRecorder{}
		child := &deliveryHandlerStub{backend: "child", handleFn: func(ctx context.Context, got *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
			seen.record(ctx, got)
			return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{Backend: "child", Status: pkglogger.DeliverySucceeded}), nil
		}}
		report, err := newTestDeliveryMultiLogger(child).ErrorWithStack(context.Background(), "stack without error", nil)
		require.NoError(t, err)
		require.Len(t, report.Targets, 1)
		require.Equal(t, pkglogger.DeliverySucceeded, report.Targets[0].Status)
		requireStackRequestWithoutError(t, seen.entry(t))
	})
}

func TestMultiLoggerPreservesContextValuesWithoutCallerCancellation(t *testing.T) {
	type contextKey string
	const key contextKey = "request"
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), key, "value"))
	cancel()

	seen := &occurrenceRecorder{}
	child := &syncHandlerStub{handleFn: func(ctx context.Context, got *infralogging.LogEntry) error {
		seen.record(ctx, got)
		return nil
	}}
	require.NoError(t, newTestMultiLogger(child).runAll(ctx, testLogEntry(pkglogger.LogLevelInfo, "context")))

	seen.mu.Lock()
	received := seen.contexts[0]
	seen.mu.Unlock()
	require.Equal(t, "value", received.Value(key))
}
