package multi

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

type forceSyncHandler struct {
	*syncHandlerStub
	force func() error
}

func (h *forceSyncHandler) ForceClose() error { return h.force() }

type forceDeliveryHandler struct {
	*deliveryHandlerStub
	force func() error
}

func (h *forceDeliveryHandler) ForceClose() error { return h.force() }

func TestMultiForceCloseForwardsWhileCloseWaitsForChild(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				closeStarted, finishClose := make(chan struct{}), make(chan struct{})
				cause := errors.New("child force failure")
				forces := 0
				force := func() error { forces++; return cause }
				closeChild := func() error { close(closeStarted); <-finishClose; return nil }
				var logger interface {
					Close() error
					ForceClose() error
				}
				var reject func()
				// handleCallsはClose完了後に読みます。asyncは受理した要求を別goroutineで処理する
				// ため、呼出し直後の0件では受付停止を確認できません。
				var handleCalls func() int32
				switch kind {
				case "sync":
					child := &forceSyncHandler{syncHandlerStub: &syncHandlerStub{closeFn: closeChild}, force: force}
					owner := newTestMultiLogger(child)
					logger = owner
					handleCalls = child.handleCalls.Load
					reject = func() { require.ErrorIs(t, owner.Info(context.Background(), "late"), applogging.ErrLoggerClosed) }
				case "async":
					child := &forceSyncHandler{syncHandlerStub: &syncHandlerStub{closeFn: closeChild}, force: force}
					owner, err := NewAsyncBestEffortMultiLogger(asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1}, child)
					require.NoError(t, err)
					logger = owner
					handleCalls = child.handleCalls.Load
					reject = func() { owner.Info(context.Background(), "late") }
				case "delivery":
					child := &forceDeliveryHandler{deliveryHandlerStub: &deliveryHandlerStub{closeFn: closeChild}, force: force}
					owner := newTestDeliveryMultiLogger(child)
					logger = owner
					handleCalls = child.handleCalls.Load
					reject = func() {
						_, err := owner.Info(context.Background(), "late")
						require.ErrorIs(t, err, applogging.ErrLoggerClosed)
					}
				}
				releaseClose := sync.OnceFunc(func() { close(finishClose) })
				t.Cleanup(releaseClose)
				closed := make(chan error, 1)
				go func() { closed <- logger.Close() }()
				<-closeStarted
				err := logger.ForceClose()
				require.ErrorIs(t, err, shutdown.ErrForcedClose)
				require.ErrorIs(t, err, cause)
				require.Equal(t, 1, forces)
				reject()
				synctest.Wait()
				require.Empty(t, closed)
				releaseClose()
				require.NoError(t, <-closed)
				require.Zero(t, handleCalls(), "requests after Close started must not reach the child")
			})
		})
	}
}

func TestMultiForceCloseContinuesPastChildPanic(t *testing.T) {
	forces := 0
	first := &forceSyncHandler{syncHandlerStub: &syncHandlerStub{}, force: func() error { panic("first force") }}
	second := &forceSyncHandler{syncHandlerStub: &syncHandlerStub{}, force: func() error { forces++; return nil }}
	owner := newTestMultiLogger(first, second)
	err := owner.ForceClose()
	require.ErrorIs(t, err, shutdown.ErrForcedClose)
	var handlerErr *HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, "ForceClose", handlerErr.Op)
	require.Same(t, first, handlerErr.Handler)
	var panicErr *loggercommon.ForceClosePanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "first force", panicErr.Value)
	require.Equal(t, 1, forces)
	require.NoError(t, owner.Close())
}
