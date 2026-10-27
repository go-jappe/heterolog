package multi

import (
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

type completedSyncHandler struct{ *syncHandlerStub }

func (h *completedSyncHandler) ShutdownComplete() bool { return h.closeCalls.Load() > 0 }

type completedDeliveryHandler struct{ *deliveryHandlerStub }

func (h *completedDeliveryHandler) ShutdownComplete() bool { return h.closeCalls.Load() > 0 }

func TestMultiCloseRetainsTerminalErrorWithoutRetryingCompletedChild(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		t.Run(kind, func(t *testing.T) {
			terminalErr := errors.New("completed with terminal failure")
			transientErr := errors.New("cleanup must retry")
			terminalCalls, transientCalls := 0, 0
			terminalClose := func() error { terminalCalls++; return terminalErr }
			transientClose := func() error {
				transientCalls++
				if transientCalls == 1 {
					return transientErr
				}
				return nil
			}
			var owner interface {
				Close() error
				ShutdownComplete() bool
			}
			switch kind {
			case "sync":
				owner = newTestMultiLogger(
					&completedSyncHandler{&syncHandlerStub{closeFn: terminalClose}},
					&syncHandlerStub{closeFn: transientClose},
				)
			case "async":
				logger, err := NewAsyncBestEffortMultiLogger(asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
					&completedSyncHandler{&syncHandlerStub{closeFn: terminalClose}},
					&syncHandlerStub{closeFn: transientClose},
				)
				require.NoError(t, err)
				owner = logger
			case "delivery":
				owner = newTestDeliveryMultiLogger(
					&completedDeliveryHandler{&deliveryHandlerStub{closeFn: terminalClose}},
					&deliveryHandlerStub{closeFn: transientClose},
				)
			}
			err := owner.Close()
			require.ErrorIs(t, err, terminalErr)
			require.ErrorIs(t, err, transientErr)
			require.False(t, owner.ShutdownComplete())
			err = owner.Close()
			require.ErrorIs(t, err, terminalErr)
			require.NotErrorIs(t, err, transientErr)
			require.True(t, owner.ShutdownComplete())
			require.ErrorIs(t, owner.Close(), terminalErr)
			require.Equal(t, 1, terminalCalls)
			require.Equal(t, 2, transientCalls)
		})
	}
}

type forcedChildCompletion struct{ done chan struct{} }

func (c *forcedChildCompletion) ShutdownDone() <-chan struct{} { return c.done }
func (c *forcedChildCompletion) ShutdownComplete() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

type forcedSyncHandler struct {
	*syncHandlerStub
	*forcedChildCompletion
}

type forcedDeliveryHandler struct {
	*deliveryHandlerStub
	*forcedChildCompletion
}

func TestMultiCloseWaitsForForcedChildActualCleanup(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				completion := &forcedChildCompletion{done: make(chan struct{})}
				started := make(chan struct{})
				closeChild := func() error { close(started); return pkgshutdown.ErrForcedClose }
				var owner interface {
					Close() error
					ShutdownComplete() bool
				}
				switch kind {
				case "sync":
					owner = newTestMultiLogger(&forcedSyncHandler{&syncHandlerStub{closeFn: closeChild}, completion})
				case "async":
					logger, err := NewAsyncBestEffortMultiLogger(asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
						&forcedSyncHandler{&syncHandlerStub{closeFn: closeChild}, completion})
					require.NoError(t, err)
					owner = logger
				case "delivery":
					owner = newTestDeliveryMultiLogger(&forcedDeliveryHandler{&deliveryHandlerStub{closeFn: closeChild}, completion})
				}
				// 途中の確認が失敗しても、childの実際の後片付けを完了させ、Closeを待つgoroutineを
				// 残さずにbubbleを終えます。
				completeChild := sync.OnceFunc(func() { close(completion.done) })
				t.Cleanup(completeChild)
				closed := make(chan error, 1)
				go func() { closed <- owner.Close() }()
				boundedwait.ReceiveInBubble(t, started, "the child Close")
				synctest.Wait()
				require.Empty(t, closed)
				require.False(t, owner.ShutdownComplete())
				completeChild()
				require.ErrorIs(t, boundedwait.ReceiveInBubble(t, closed, "Close after the child cleanup"), pkgshutdown.ErrForcedClose)
				require.True(t, owner.ShutdownComplete())
				require.ErrorIs(t, owner.Close(), pkgshutdown.ErrForcedClose)
			})
		})
	}
}
