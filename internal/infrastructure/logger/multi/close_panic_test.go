package multi

import (
	"context"
	"errors"
	"testing"
	"time"

	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/stretchr/testify/require"
)

type panicCloseState struct {
	calls            int
	panicValue       any
	completed        bool
	completedOnPanic bool
}

func (s *panicCloseState) close() error {
	s.calls++
	if s.calls == 1 {
		s.completed = s.completedOnPanic
		panic(s.panicValue)
	}
	s.completed = true
	return nil
}

func (s *panicCloseState) ShutdownComplete() bool { return s.completed }

type panicSyncCloser struct{ *panicCloseState }

func (*panicSyncCloser) HandleLogEntry(context.Context, *infralogging.LogEntry) error { return nil }
func (h *panicSyncCloser) Close() error                                               { return h.close() }

type panicContextSyncCloser struct{ *panicSyncCloser }

func (h *panicContextSyncCloser) Close(context.Context) error { return h.close() }

type panicDeliveryCloser struct{ *panicCloseState }

func (*panicDeliveryCloser) HandleLogEntry(context.Context, *infralogging.LogEntry) (pkglogger.DeliveryReport, error) {
	return pkglogger.DeliveryReport{}, nil
}
func (h *panicDeliveryCloser) Close() error { return h.close() }

type panicContextDeliveryCloser struct{ *panicDeliveryCloser }

func (h *panicContextDeliveryCloser) Close(context.Context) error { return h.close() }

func TestMultiClosePanicContinuesSiblingsAndRetriesOnlyIncompleteChildren(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		for _, signature := range []string{"Close()", "Close(context.Context)"} {
			for _, completedOnPanic := range []bool{false, true} {
				completion := "incomplete"
				if completedOnPanic {
					completion = "already_completed"
				}
				t.Run(kind+"/"+signature+"/"+completion, func(t *testing.T) {
					cause := errors.New("child cleanup callback panicked")
					state := &panicCloseState{panicValue: cause, completedOnPanic: completedOnPanic}
					var owner interface {
						Close() error
						ShutdownComplete() bool
					}
					siblingCalls := 0
					closeSibling := func() error { siblingCalls++; return nil }
					if kind == "delivery" {
						var child infralogging.DeliveryBackendLogEntryHandler = &panicDeliveryCloser{state}
						if signature == "Close(context.Context)" {
							child = &panicContextDeliveryCloser{child.(*panicDeliveryCloser)}
						}
						owner = newTestDeliveryMultiLogger(child, &deliveryHandlerStub{closeFn: closeSibling})
					} else {
						var child infralogging.BackendLogEntryHandler = &panicSyncCloser{state}
						if signature == "Close(context.Context)" {
							child = &panicContextSyncCloser{child.(*panicSyncCloser)}
						}
						sibling := &syncHandlerStub{closeFn: closeSibling}
						if kind == "sync" {
							owner = newTestMultiLogger(child, sibling)
						} else {
							logger, err := NewAsyncBestEffortMultiLogger(asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1}, child, sibling)
							require.NoError(t, err)
							owner = logger
						}
					}
					var firstErr error
					require.NotPanics(t, func() { firstErr = owner.Close() })
					require.ErrorIs(t, firstErr, cause)
					var handlerErr *HandlerError
					require.ErrorAs(t, firstErr, &handlerErr)
					require.Equal(t, "Close", handlerErr.Op)
					require.Equal(t, 1, siblingCalls, "one child panic must not prevent a sibling from closing")
					require.Equal(t, completedOnPanic, owner.ShutdownComplete())
					for range 2 {
						err := owner.Close()
						if completedOnPanic {
							require.ErrorIs(t, err, cause, "completed child retains its terminal diagnostic")
						} else {
							require.NoError(t, err, "unfinished child is retried through normal cleanup")
						}
					}
					require.True(t, owner.ShutdownComplete())
					wantCalls := 2
					if completedOnPanic {
						wantCalls = 1
					}
					require.Equal(t, wantCalls, state.calls)
					require.Equal(t, 1, siblingCalls, "successfully closed sibling must not be closed again")
				})
			}
		}
	}
}

func TestCloseLoggerPreservesNonErrorPanicDiagnostic(t *testing.T) {
	state := &panicCloseState{panicValue: "cleanup callback failed"}
	err := closeLogger(&panicSyncCloser{state})
	require.ErrorIs(t, err, ErrLoggerPanicked)
	var panicErr *PanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "cleanup callback failed", panicErr.Value)
	require.NoError(t, closeLogger(&panicSyncCloser{state}))
}
