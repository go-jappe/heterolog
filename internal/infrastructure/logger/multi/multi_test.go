package multi

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogEntry(level pkglogger.LogLevel, message string) *infralogging.LogEntry {
	return &infralogging.LogEntry{
		OccurredAt: time.Date(2026, 8, 21, 12, 34, 56, 789, time.UTC),
		Level:      level,
		Message:    message,
	}
}

const testMultiLoggerTimeout = 10 * time.Second

type testMultiLoggerConfig struct {
	timeout          time.Duration
	maxLogEntryBytes int
}

func (c testMultiLoggerConfig) Timeout() time.Duration { return c.timeout }
func (c testMultiLoggerConfig) MaxLogEntryBytes() int  { return c.maxLogEntryBytes }

func testMultiLoggerConfigWithTimeout(timeout time.Duration) testMultiLoggerConfig {
	return testMultiLoggerConfig{
		timeout:          timeout,
		maxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
	}
}

func newTestMultiLogger(loggers ...infralogging.BackendLogEntryHandler) *MultiLogger {
	return newMultiLogger(
		testMultiLoggerTimeout,
		pkglogger.DefaultMaxLogEntryBytes,
		loggers...,
	)
}

func newTestDeliveryMultiLogger(
	loggers ...infralogging.DeliveryBackendLogEntryHandler,
) *DeliveryMultiLogger {
	return newDeliveryMultiLogger(
		testMultiLoggerTimeout,
		pkglogger.DefaultMaxLogEntryBytes,
		testDeliveryTargets(loggers...)...,
	)
}

// newTestDeliveryMultiLoggerWithConfigは、cfgを検証してDelivery multi loggerを作ります。
// 論理出力名を確認しない試験のため、childの型名をBackendとして登録します。
func newTestDeliveryMultiLoggerWithConfig(
	cfg DeliveryMultiLoggerConfigProvider,
	loggers ...infralogging.DeliveryBackendLogEntryHandler,
) (*DeliveryMultiLogger, error) {
	return NewDeliveryMultiLoggerWithTargets(cfg, testDeliveryTargets(loggers...)...)
}

// testDeliveryTargetsは、論理出力名を確認しない試験のために、childの型名をBackendとする
// DeliveryTargetを返します。論理出力名の規則はTestDeliveryMultiLoggerSynthesizedResultsUseLogicalBackendNames
// が確認します。
func testDeliveryTargets(loggers ...infralogging.DeliveryBackendLogEntryHandler) []DeliveryTarget {
	targets := make([]DeliveryTarget, 0, len(loggers))
	for _, l := range loggers {
		targets = append(targets, DeliveryTarget{Backend: fmt.Sprintf("%T", l), Logger: l})
	}
	return targets
}

func TestMultiLoggersDoNotExposeHandleLogEntry(t *testing.T) {
	for name, logger := range map[string]any{
		"sync":     newTestMultiLogger(),
		"async":    &AsyncBestEffortMultiLogger{},
		"delivery": newTestDeliveryMultiLogger(),
	} {
		t.Run(name, func(t *testing.T) {
			_, exposed := reflect.TypeOf(logger).MethodByName("HandleLogEntry")
			require.False(t, exposed)
		})
	}
}

var (
	_ pkglogger.Logger         = (*MultiLogger)(nil)
	_ pkglogger.AsyncLogger    = (*AsyncBestEffortMultiLogger)(nil)
	_ pkglogger.DeliveryLogger = (*DeliveryMultiLogger)(nil)
)

// syncHandlerStubは、backend非依存のlogical LogEntryだけを受け取る
// infra backend契約の試験用実装です。application向けmethodは持ちません。
type syncHandlerStub struct {
	handleFn func(context.Context, *infralogging.LogEntry) error
	closeFn  func() error

	handleCalls atomic.Int32
	closeCalls  atomic.Int32
}

func (h *syncHandlerStub) HandleLogEntry(
	ctx context.Context,
	entry *infralogging.LogEntry,
) error {
	h.handleCalls.Add(1)
	if h.handleFn != nil {
		return h.handleFn(ctx, entry)
	}
	return nil
}

func (h *syncHandlerStub) Close() error {
	h.closeCalls.Add(1)
	if h.closeFn != nil {
		return h.closeFn()
	}
	return nil
}

var _ infralogging.BackendLogEntryHandler = (*syncHandlerStub)(nil)

type contextCloseHandlerStub struct {
	closeCalls atomic.Int32
}

func (*contextCloseHandlerStub) HandleLogEntry(
	context.Context,
	*infralogging.LogEntry,
) error {
	return nil
}

func (h *contextCloseHandlerStub) Close(context.Context) error {
	h.closeCalls.Add(1)
	return nil
}

func TestTopLevelMultiLoggerOwnsApplicationContract(t *testing.T) {
	syncChild := &syncHandlerStub{}
	_, childIsApplicationLogger := any(syncChild).(pkglogger.Logger)
	require.False(t, childIsApplicationLogger)

	logger := newTestMultiLogger(syncChild)
	require.NoError(t, logger.Info(context.Background(), "top-level entry"))
	require.Equal(t, int32(1), syncChild.handleCalls.Load())
	require.NoError(t, logger.Close())

	asyncChild := &syncHandlerStub{}
	_, childIsAsyncApplicationLogger := any(asyncChild).(pkglogger.AsyncLogger)
	require.False(t, childIsAsyncApplicationLogger)
	asyncLogger, err := NewAsyncBestEffortMultiLogger(
		asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
		asyncChild,
	)
	require.NoError(t, err)
	asyncLogger.Info(context.Background(), "top-level async entry")
	require.NoError(t, asyncLogger.Close())
	require.Equal(t, int32(1), asyncChild.handleCalls.Load())
}

func TestMultiLoggerClosesMethodOnlyHandlers(t *testing.T) {
	plainCloseChild := &syncHandlerStub{}
	contextCloseChild := &contextCloseHandlerStub{}

	require.NoError(t, newTestMultiLogger(plainCloseChild, contextCloseChild).Close())
	require.Equal(t, int32(1), plainCloseChild.closeCalls.Load())
	require.Equal(t, int32(1), contextCloseChild.closeCalls.Load())
}

func TestMultiLoggerTimesOutBlockedHandler(t *testing.T) {
	// timeoutはsynctestの仮想時刻で進め、実時間の上限には依存しません。
	synctest.Test(t, func(t *testing.T) {
		const timeout = 20 * time.Millisecond
		release := make(chan struct{})
		t.Cleanup(sync.OnceFunc(func() { close(release) }))

		child := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
			<-release
			return nil
		}}
		m, err := NewMultiLogger(testMultiLoggerConfigWithTimeout(timeout), child)
		require.NoError(t, err)

		started := time.Now()
		err = m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "blocked"))

		require.Error(t, err)
		assert.GreaterOrEqual(t, time.Since(started), timeout, "runAll must wait for the configured timeout")
		var multiErr *pkglogger.MultiLoggerError
		require.ErrorAs(t, err, &multiErr)
		require.Len(t, multiErr.Failures, 1)
		assert.ErrorIs(t, multiErr.Failures[0].Err, context.DeadlineExceeded)
	})
}

func TestMultiLoggerIgnoresCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	child := &syncHandlerStub{handleFn: func(ctx context.Context, _ *infralogging.LogEntry) error {
		return ctx.Err()
	}}
	m, err := NewMultiLogger(testMultiLoggerConfigWithTimeout(time.Second), child)
	require.NoError(t, err)
	require.NoError(t, m.runAll(ctx, testLogEntry(pkglogger.LogLevelInfo, "canceled caller")))
}

func TestMultiLoggerErrorDisplaysChildTypeAndPreservesCause(t *testing.T) {
	childErr := errors.New("child handler failed")
	child := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
		return childErr
	}}

	err := newTestMultiLogger(child).runAll(
		context.Background(),
		testLogEntry(pkglogger.LogLevelInfo, "failed"),
	)

	require.Error(t, err)
	require.ErrorIs(t, err, childErr)
	var multiErr *pkglogger.MultiLoggerError
	require.ErrorAs(t, err, &multiErr)
	require.Len(t, multiErr.Failures, 1)
	require.Equal(t, fmt.Sprintf("%T", child), multiErr.Failures[0].HandlerType)
}

func TestMultiLoggerRecoversHandlerPanic(t *testing.T) {
	panicErr := errors.New("child handler panic")
	child := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
		panic(panicErr)
	}}

	err := newTestMultiLogger(child).runAll(
		context.Background(),
		testLogEntry(pkglogger.LogLevelWarn, "panic"),
	)

	require.ErrorIs(t, err, panicErr)
	require.ErrorIs(t, err, ErrLoggerPanicked)
	var multiErr *pkglogger.MultiLoggerError
	require.ErrorAs(t, err, &multiErr)
	require.Len(t, multiErr.Failures, 1)
	require.Equal(t, fmt.Sprintf("%T", child), multiErr.Failures[0].HandlerType)
}

func TestMultiLoggerRecoversNonErrorHandlerPanic(t *testing.T) {
	child := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
		panic("non-error panic")
	}}
	err := newTestMultiLogger(child).runAll(
		context.Background(),
		testLogEntry(pkglogger.LogLevelDebug, "panic"),
	)
	require.ErrorIs(t, err, ErrLoggerPanicked)
	var panicErr *PanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "non-error panic", panicErr.Value)
}

func TestNewMultiLoggerRejectsTimeoutThatIsNotPositive(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		_, err := NewMultiLogger(testMultiLoggerConfigWithTimeout(timeout))
		require.ErrorIs(t, err, pkglogger.ErrNonPositiveSetting)
	}
}

func TestNewMultiLoggerRequiresConfig(t *testing.T) {
	_, err := NewMultiLogger(nil)
	require.ErrorIs(t, err, ErrConfigRequired)
}

func TestMultiLoggerWithoutChildrenRejectsEntriesAfterClose(t *testing.T) {
	m := newTestMultiLogger()
	entry := testLogEntry(pkglogger.LogLevelInfo, "entry")
	require.Equal(t, testMultiLoggerTimeout, m.timeout)
	require.NoError(t, m.runAll(context.Background(), entry))
	require.NoError(t, m.Close())
	require.ErrorIs(t, m.runAll(context.Background(), entry), pkglogger.ErrLoggerClosed)
	require.NoError(t, m.Close())
}

func TestMultiLoggerCloseWaitsForHandlerStillRunningAfterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		releaseChild := sync.OnceFunc(func() { close(release) })
		t.Cleanup(releaseChild)
		var active atomic.Bool
		var closeWhileActive atomic.Bool
		child := &syncHandlerStub{}
		child.handleFn = func(context.Context, *infralogging.LogEntry) error {
			active.Store(true)
			started <- struct{}{}
			<-release
			active.Store(false)
			return nil
		}
		child.closeFn = func() error {
			if active.Load() {
				closeWhileActive.Store(true)
			}
			return nil
		}
		m, err := NewMultiLogger(testMultiLoggerConfigWithTimeout(20*time.Millisecond), child)
		require.NoError(t, err)

		logDone := make(chan error, 1)
		go func() {
			logDone <- m.runAll(
				context.Background(),
				testLogEntry(pkglogger.LogLevelInfo, "accepted before close"),
			)
		}()
		<-started
		require.Error(t, <-logDone)

		closeDone := make(chan error, 1)
		go func() { closeDone <- m.Close() }()
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned while timed-out handler was still running: %v", err)
		default:
		}

		err = m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "while closing"))
		require.ErrorIs(t, err, pkglogger.ErrLoggerClosed)
		var opErr *OperationError
		require.ErrorAs(t, err, &opErr)
		require.Equal(t, "multi.Info", opErr.Op)
		require.Equal(t, int32(1), child.handleCalls.Load())

		releaseChild()
		require.NoError(t, <-closeDone)
		require.False(t, closeWhileActive.Load())
		require.Equal(t, int32(1), child.closeCalls.Load())
	})
}

func TestMultiLoggerCloseRetriesOnlyFailedChildren(t *testing.T) {
	closeErr := errors.New("first close failed")
	var failedMu sync.Mutex
	failedOnce := false
	failedFirst := &syncHandlerStub{closeFn: func() error {
		failedMu.Lock()
		defer failedMu.Unlock()
		if !failedOnce {
			failedOnce = true
			return closeErr
		}
		return nil
	}}
	succeededFirst := &syncHandlerStub{}
	m := newTestMultiLogger(failedFirst, succeededFirst)

	require.ErrorIs(t, m.Close(), closeErr)
	require.Equal(t, loggercommon.LoggerCloseStateStarted, m.closeState)
	require.Equal(t, int32(1), failedFirst.closeCalls.Load())
	require.Equal(t, int32(1), succeededFirst.closeCalls.Load())

	require.NoError(t, m.Close())
	require.Equal(t, loggercommon.LoggerCloseStateCompleted, m.closeState)
	require.Equal(t, int32(2), failedFirst.closeCalls.Load())
	require.Equal(t, int32(1), succeededFirst.closeCalls.Load())
}

func TestMultiLoggerClonesEntryForEachHandler(t *testing.T) {
	original := testLogEntry(pkglogger.LogLevelInfo, "clone")
	original.File = []string{"original.go"}
	original.Fields = map[string]any{"shared": "original"}

	seen := make(chan *infralogging.LogEntry, 2)
	mutating := &syncHandlerStub{handleFn: func(_ context.Context, entry *infralogging.LogEntry) error {
		entry.File[0] = "mutated.go"
		entry.Fields["shared"] = "mutated"
		seen <- entry
		return nil
	}}
	observing := &syncHandlerStub{handleFn: func(_ context.Context, entry *infralogging.LogEntry) error {
		seen <- entry
		return nil
	}}

	require.NoError(t, newTestMultiLogger(mutating, observing).runAll(context.Background(), original))
	first, second := <-seen, <-seen
	require.NotSame(t, first, second)
	require.Equal(t, "original.go", original.File[0])
	require.Equal(t, "original", original.Fields["shared"])

	values := map[string]bool{first.Fields["shared"].(string): true, second.Fields["shared"].(string): true}
	require.True(t, values["original"])
	require.True(t, values["mutated"])
}
