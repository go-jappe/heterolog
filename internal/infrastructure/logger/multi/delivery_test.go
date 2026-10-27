package multi

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

type deliveryHandlerStub struct {
	backend  string
	handleFn func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error)
	closeFn  func() error

	handleCalls atomic.Int32
	closeCalls  atomic.Int32
}

func (h *deliveryHandlerStub) successReport() pkglogger.DeliveryReport {
	return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
		Backend: h.backend,
		Status:  pkglogger.DeliverySucceeded,
	})
}

func (h *deliveryHandlerStub) HandleLogEntry(
	ctx context.Context,
	entry *infralogging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	h.handleCalls.Add(1)
	if h.handleFn != nil {
		return h.handleFn(ctx, entry)
	}
	return h.successReport(), nil
}

func (h *deliveryHandlerStub) Close() error {
	h.closeCalls.Add(1)
	if h.closeFn != nil {
		return h.closeFn()
	}
	return nil
}

var _ infralogging.DeliveryBackendLogEntryHandler = (*deliveryHandlerStub)(nil)

func TestDeliveryMultiLoggerAcceptsHandlerWithoutApplicationMethods(t *testing.T) {
	child := &deliveryHandlerStub{backend: "handler-only"}
	_, childIsApplicationLogger := any(child).(pkglogger.DeliveryLogger)
	require.False(t, childIsApplicationLogger)

	multi := newTestDeliveryMultiLogger(child)
	report, err := multi.Info(context.Background(), "top-level entry")
	require.NoError(t, err)
	require.Len(t, report.Targets, 1)
	require.Equal(t, int32(1), child.handleCalls.Load())

	require.NoError(t, multi.Close())
	require.Equal(t, int32(1), child.closeCalls.Load())
}

func TestDeliveryMultiLoggerMergesReportsInConfiguredOrder(t *testing.T) {
	// firstはsecondの完了を待ってから戻るため、完了順は設定順と逆になります。
	secondDone := make(chan struct{})
	first := &deliveryHandlerStub{backend: "first", handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		<-secondDone
		return pkglogger.DeliveryReport{
			Targets:  []pkglogger.DeliveryResult{{Backend: "first", Status: pkglogger.DeliverySucceeded}},
			Durables: []pkglogger.DurableResult{{Backend: "first-durable"}},
		}, nil
	}}
	second := &deliveryHandlerStub{backend: "second", handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		defer close(secondDone)
		return pkglogger.DeliveryReport{
			Targets:     []pkglogger.DeliveryResult{{Backend: "second", Status: pkglogger.DeliverySucceeded}},
			Evacuations: []pkglogger.EvacuationResult{{}},
		}, nil
	}}

	report, err := newTestDeliveryMultiLogger(first, second).runAll(
		context.Background(),
		testLogEntry(pkglogger.LogLevelInfo, "ordered report"),
	)
	require.NoError(t, err)
	require.Len(t, report.Targets, 2)
	require.Equal(t, "first", report.Targets[0].Backend)
	require.Equal(t, "second", report.Targets[1].Backend)
	require.Len(t, report.Durables, 1)
	require.Len(t, report.Evacuations, 1)
}

func TestDeliveryMultiLoggerAcceptsRegisteredEnqueuedAndSkippedStatuses(t *testing.T) {
	for _, status := range []pkglogger.DeliveryStatus{
		pkglogger.DeliverySucceeded,
		pkglogger.DeliveryEnqueued,
		pkglogger.DeliverySkipped,
	} {
		status := status
		t.Run(status.String(), func(t *testing.T) {
			child := &deliveryHandlerStub{backend: status.String(), handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
				return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
					Backend: status.String(),
					Status:  status,
				}), nil
			}}
			_, err := newTestDeliveryMultiLogger(child).runAll(
				context.Background(),
				testLogEntry(pkglogger.LogLevelDebug, "accepted status"),
			)
			require.NoError(t, err)
		})
	}
}

func TestDeliveryMultiLoggerPropagatesTargetErrorsFromSuccessfulStatuses(t *testing.T) {
	for _, status := range []pkglogger.DeliveryStatus{
		pkglogger.DeliverySucceeded,
		pkglogger.DeliveryEnqueued,
		pkglogger.DeliverySkipped,
	} {
		t.Run(status.String(), func(t *testing.T) {
			cause := errors.New("target-level failure")
			child := &deliveryHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
				return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
					Backend: "target", Status: status, Err: cause,
				}), nil
			}}
			logger := newTestDeliveryMultiLogger(child)
			report, err := logger.Info(context.Background(), "target error")
			require.NoError(t, logger.Close())

			// childは操作のerrorを返していません。target errorだけで、集約した呼び出しが
			// 成功を報告しないようにしなければなりません。
			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, pkglogger.ErrTargetDeliveryNotRegistered)
			require.Len(t, report.Targets, 1)
			require.Equal(t, "target", report.Targets[0].Backend)
			require.Equal(t, status, report.Targets[0].Status)
			require.ErrorIs(t, report.Targets[0].Err, cause)
		})
	}
}

func TestDeliveryMultiLoggerRejectsUnregisteredTargetStatuses(t *testing.T) {
	for _, status := range []pkglogger.DeliveryStatus{
		pkglogger.DeliveryFailed,
		pkglogger.DeliveryRejected,
		pkglogger.DeliveryUnconfirmed,
	} {
		status := status
		t.Run(status.String(), func(t *testing.T) {
			cause := errors.New("target failure")
			child := &deliveryHandlerStub{backend: "target", handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
				return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
					Backend: "target",
					Status:  status,
					Err:     cause,
				}), nil
			}}
			report, err := newTestDeliveryMultiLogger(child).runAll(
				context.Background(),
				testLogEntry(pkglogger.LogLevelWarn, "failed target"),
			)
			require.Error(t, err)
			require.Len(t, report.Targets, 1)
			require.ErrorIs(t, err, pkglogger.ErrTargetDeliveryNotRegistered)
			require.ErrorIs(t, err, cause)
			var targetErr *TargetDeliveryError
			require.ErrorAs(t, err, &targetErr)
			require.Equal(t, "target", targetErr.Backend)
			require.Equal(t, status, targetErr.Status)
		})
	}
}

func TestDeliveryMultiLoggerAggregatesChildErrorAndReport(t *testing.T) {
	childErr := errors.New("delivery child failed")
	child := &deliveryHandlerStub{backend: "child", handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
			Backend: "child",
			Status:  pkglogger.DeliveryFailed,
			Err:     childErr,
		}), childErr
	}}

	report, err := newTestDeliveryMultiLogger(child).runAll(
		context.Background(),
		testLogEntry(pkglogger.LogLevelError, "child errors"),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, childErr)
	require.Len(t, report.Targets, 1)
	require.Equal(t, pkglogger.DeliveryFailed, report.Targets[0].Status)
}

func TestDeliveryMultiLoggerTimesOutBlockedHandlerAsUnconfirmed(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	child := &deliveryHandlerStub{backend: "blocked", handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		<-release
		return pkglogger.DeliveryReport{}, nil
	}}
	m, err := newTestDeliveryMultiLoggerWithConfig(
		testMultiLoggerConfigWithTimeout(20*time.Millisecond),
		child,
	)
	require.NoError(t, err)

	report, err := m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "blocked"))
	require.Error(t, err)
	require.Len(t, report.Targets, 1)
	require.Equal(t, pkglogger.DeliveryUnconfirmed, report.Targets[0].Status)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDeliveryMultiLoggerRecoversHandlerPanic(t *testing.T) {
	child := &deliveryHandlerStub{backend: "panic", handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		panic("delivery panic")
	}}
	report, err := newTestDeliveryMultiLogger(child).runAll(
		context.Background(),
		&infralogging.LogEntry{
			OccurredAt:          time.Now().UTC(),
			Level:               pkglogger.LogLevelError,
			Message:             "panic",
			StackTraceRequested: true,
		},
	)
	require.ErrorIs(t, err, ErrLoggerPanicked)
	var handlerErr *HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Same(t, child, handlerErr.Handler)
	require.Len(t, report.Targets, 1)
	require.Equal(t, pkglogger.DeliveryUnconfirmed, report.Targets[0].Status)
	var panicErr *PanicError
	require.ErrorAs(t, report.Targets[0].Err, &panicErr)
	require.Equal(t, "delivery logger", panicErr.Logger)
	require.Equal(t, "delivery panic", panicErr.Value)
	require.False(t, report.HasTargetFailure())
	require.Empty(t, report.FailedTargets())
	require.True(t, report.HasUnregisteredTarget())
	require.False(t, report.HasRegisteredTarget())
}

func TestDeliveryMultiLoggerPanicPreservesCauseAndSiblingOutcomes(t *testing.T) {
	panicCause := errors.New("observer panicked after sending")
	refusal := errors.New("backend refused entry")
	panicking := &deliveryHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		panic(panicCause)
	}}
	succeeded := &deliveryHandlerStub{backend: "succeeded"}
	failed := &deliveryHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
		return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
			Backend: "failed", Status: pkglogger.DeliveryFailed, Err: refusal,
		}), refusal
	}}
	m := newTestDeliveryMultiLogger(succeeded, panicking, failed)
	report, err := m.Info(context.Background(), "independent target outcomes")
	require.NoError(t, m.Close())
	require.ErrorIs(t, err, panicCause)
	require.ErrorIs(t, err, refusal)
	require.Len(t, report.Targets, 3)
	require.Equal(t, pkglogger.DeliveryResult{Backend: "succeeded", Status: pkglogger.DeliverySucceeded}, report.Targets[0])
	require.Equal(t, pkglogger.DeliveryUnconfirmed, report.Targets[1].Status)
	require.ErrorIs(t, report.Targets[1].Err, panicCause)
	require.Equal(t, pkglogger.DeliveryResult{Backend: "failed", Status: pkglogger.DeliveryFailed, Err: refusal}, report.Targets[2])
	require.Equal(t, []pkglogger.DeliveryResult{report.Targets[2]}, report.FailedTargets())
	require.True(t, report.HasUnregisteredTarget())
	require.True(t, report.HasRegisteredTarget())
	for _, child := range []*deliveryHandlerStub{succeeded, panicking, failed} {
		require.Equal(t, int32(1), child.handleCalls.Load())
		require.Equal(t, int32(1), child.closeCalls.Load())
	}
}

func TestDeliveryMultiLoggerRejectsNoHandlersAndNilEntry(t *testing.T) {
	m := newTestDeliveryMultiLogger()
	report, err := m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "no target"))
	require.ErrorIs(t, err, pkglogger.ErrNoDeliveryTargets)
	require.Empty(t, report.Targets)

	child := &deliveryHandlerStub{backend: "child"}
	report, err = newTestDeliveryMultiLogger(child).runAll(context.Background(), nil)
	require.Error(t, err)
	require.Len(t, report.Targets, 1)
	require.Equal(t, pkglogger.DeliveryRejected, report.Targets[0].Status)
	require.Equal(t, int32(0), child.handleCalls.Load())
}

func TestNewDeliveryMultiLoggerRejectsNonPositiveTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		_, err := newTestDeliveryMultiLoggerWithConfig(testMultiLoggerConfigWithTimeout(timeout))
		require.ErrorIs(t, err, ErrInvalidTimeout)
	}
}

func TestNewDeliveryMultiLoggerRequiresConfig(t *testing.T) {
	_, err := newTestDeliveryMultiLoggerWithConfig(nil)
	require.ErrorIs(t, err, ErrConfigRequired)
}

func TestDeliveryMultiLoggerCloseWaitsForTimedOutHandlerAndRejectsNewEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		releaseChild := sync.OnceFunc(func() { close(release) })
		t.Cleanup(releaseChild)
		child := &deliveryHandlerStub{backend: "child", handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
			started <- struct{}{}
			<-release
			return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
				Backend: "child",
				Status:  pkglogger.DeliverySucceeded,
			}), nil
		}}
		m, err := newTestDeliveryMultiLoggerWithConfig(
			testMultiLoggerConfigWithTimeout(20*time.Millisecond),
			child,
		)
		require.NoError(t, err)

		_, err = m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "accepted"))
		require.Error(t, err)
		<-started

		closeDone := make(chan error, 1)
		go func() { closeDone <- m.Close() }()
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned while handler remained active: %v", err)
		default:
		}

		report, err := m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "closing"))
		require.ErrorIs(t, err, pkglogger.ErrLoggerClosed)
		var opErr *OperationError
		require.ErrorAs(t, err, &opErr)
		require.Equal(t, "delivery.Info", opErr.Op)
		require.Len(t, report.Targets, 1)
		require.Equal(t, pkglogger.DeliveryRejected, report.Targets[0].Status)

		releaseChild()
		require.NoError(t, <-closeDone)
		require.Equal(t, int32(1), child.closeCalls.Load())
	})
}

func TestDeliveryMultiLoggerCloseRetriesOnlyFailedChildren(t *testing.T) {
	closeErr := errors.New("first delivery close failed")
	var mu sync.Mutex
	failedOnce := false
	failedFirst := &deliveryHandlerStub{backend: "failed", closeFn: func() error {
		mu.Lock()
		defer mu.Unlock()
		if !failedOnce {
			failedOnce = true
			return closeErr
		}
		return nil
	}}
	succeededFirst := &deliveryHandlerStub{backend: "succeeded"}
	m := newTestDeliveryMultiLogger(failedFirst, succeededFirst)

	require.ErrorIs(t, m.Close(), closeErr)
	require.Equal(t, loggercommon.LoggerCloseStateStarted, m.closeState)
	require.NoError(t, m.Close())
	require.Equal(t, loggercommon.LoggerCloseStateCompleted, m.closeState)
	require.Equal(t, int32(2), failedFirst.closeCalls.Load())
	require.Equal(t, int32(1), succeededFirst.closeCalls.Load())
}

// multi loggerが作る送達結果（childのpanic・timeout、Close開始後の拒否、entry検証での拒否）は、
// child自身の結果と同じ論理出力名をBackendに使います。二つのchildを登録し、名前が登録順の
// childと対応することも確認します。
func TestDeliveryMultiLoggerSynthesizedResultsUseLogicalBackendNames(t *testing.T) {
	ctx := context.Background()
	newTargets := func(second infralogging.DeliveryBackendLogEntryHandler) []DeliveryTarget {
		return []DeliveryTarget{
			{Backend: "first-output", Logger: &deliveryHandlerStub{backend: "first-output"}},
			{Backend: "second-output", Logger: second},
		}
	}
	requireTargets := func(t *testing.T, report pkglogger.DeliveryReport, want ...pkglogger.DeliveryResult) {
		t.Helper()
		require.Len(t, report.Targets, len(want))
		for index, target := range report.Targets {
			require.Equal(t, want[index].Backend, target.Backend, "target %d", index)
			require.Equal(t, want[index].Status, target.Status, "target %d", index)
		}
	}

	t.Run("panic", func(t *testing.T) {
		panicking := &deliveryHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
			panic("delivery panic")
		}}
		m, err := NewDeliveryMultiLoggerWithTargets(testMultiLoggerConfigWithTimeout(time.Second), newTargets(panicking)...)
		require.NoError(t, err)
		t.Cleanup(func() { _ = m.Close() })

		report, err := m.Info(ctx, "panic")
		require.ErrorIs(t, err, ErrLoggerPanicked)
		requireTargets(t, report,
			pkglogger.DeliveryResult{Backend: "first-output", Status: pkglogger.DeliverySucceeded},
			pkglogger.DeliveryResult{Backend: "second-output", Status: pkglogger.DeliveryUnconfirmed},
		)
	})

	t.Run("timeout", func(t *testing.T) {
		// timeoutはsynctestの仮想時刻で進めます。
		synctest.Test(t, func(t *testing.T) {
			release := make(chan struct{})
			blocked := &deliveryHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
				<-release
				return pkglogger.DeliveryReport{}, nil
			}}
			m, err := NewDeliveryMultiLoggerWithTargets(testMultiLoggerConfigWithTimeout(20*time.Millisecond), newTargets(blocked)...)
			require.NoError(t, err)
			// Cleanupは後に登録したものから実行されるため、解放をCloseより後に登録します。
			t.Cleanup(func() { _ = m.Close() })
			t.Cleanup(sync.OnceFunc(func() { close(release) }))

			report, err := m.Info(ctx, "timeout")
			require.ErrorIs(t, err, context.DeadlineExceeded)
			requireTargets(t, report,
				pkglogger.DeliveryResult{Backend: "first-output", Status: pkglogger.DeliverySucceeded},
				pkglogger.DeliveryResult{Backend: "second-output", Status: pkglogger.DeliveryUnconfirmed},
			)
		})
	})

	t.Run("closing", func(t *testing.T) {
		m, err := NewDeliveryMultiLoggerWithTargets(testMultiLoggerConfigWithTimeout(time.Second), newTargets(&deliveryHandlerStub{backend: "second-output"})...)
		require.NoError(t, err)
		require.NoError(t, m.Close())

		report, err := m.Info(ctx, "after close")
		require.ErrorIs(t, err, pkglogger.ErrLoggerClosed)
		requireTargets(t, report,
			pkglogger.DeliveryResult{Backend: "first-output", Status: pkglogger.DeliveryRejected},
			pkglogger.DeliveryResult{Backend: "second-output", Status: pkglogger.DeliveryRejected},
		)
	})

	t.Run("entry_too_large", func(t *testing.T) {
		cfg := testMultiLoggerConfig{timeout: time.Second, maxLogEntryBytes: 64}
		m, err := NewDeliveryMultiLoggerWithTargets(cfg, newTargets(&deliveryHandlerStub{backend: "second-output"})...)
		require.NoError(t, err)
		t.Cleanup(func() { _ = m.Close() })

		report, err := m.Info(ctx, strings.Repeat("x", 128))
		require.ErrorIs(t, err, pkglogger.ErrLogEntryTooLarge)
		requireTargets(t, report,
			pkglogger.DeliveryResult{Backend: "first-output", Status: pkglogger.DeliveryRejected},
			pkglogger.DeliveryResult{Backend: "second-output", Status: pkglogger.DeliveryRejected},
		)
	})
}
