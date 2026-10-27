package clickhouse

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

var (
	_ interface{ ForceClose() error } = (*DeliveryClickHouseLogger)(nil)
	_ interface{ ForceClose() error } = (*FallbackLogger)(nil)
)

func TestFallbackControllerForceDoesNotWaitForInitializationOrCreateLogger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		controller := &fallbackHandlerController{}
		// 遅延初期化がI/O中に保持するmutexを再現します。
		controller.mu.Lock()
		require.ErrorIs(t, controller.RequestForceClose(), pkgshutdown.ErrForcedClose)
		controller.Handle(context.Background(), nil, nil, nil)
		require.False(t, controller.initializationAttempted)
		controller.mu.Unlock()
		synctest.Wait()
		require.False(t, controller.ShutdownComplete(), "request alone must not start resource cleanup")
		require.NoError(t, controller.Close())
		require.True(t, controller.ShutdownComplete())
	})
}

func TestFallbackControllerForceForwardsDuringAcceptedHandlerCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probe := &forceCloseFailureProbe{
			shutdownStarted: make(chan struct{}),
			finishShutdown:  make(chan struct{}),
		}
		t.Cleanup(probe.releaseShutdown)
		composite, err := flushfailure.NewCompositeFailureHandler([]flushfailure.FailureHandler{probe}, 1)
		require.NoError(t, err)
		fallback := &FallbackLogger{ClickHouseLogger: &ClickHouseLogger{}}
		fallback.flushManager = &ClickHouseFlushManager{FailureHandler: composite}
		controller := &fallbackHandlerController{logger: fallback, initializationAttempted: true}
		controller.active.Add(1)
		var activeDone sync.Once
		finishActive := func() { activeDone.Do(controller.active.Done) }
		// cleanupは登録と逆順に実行するため、失敗時も受理済みhandlerを終えてからchildを解放します。
		t.Cleanup(finishActive)
		closed := make(chan error, 1)
		go func() { closed <- controller.Close() }()
		synctest.Wait()
		require.ErrorIs(t, controller.RequestForceClose(), pkgshutdown.ErrForcedClose)
		synctest.Wait()
		require.Equal(t, 1, probe.forces)
		// shutdownStartedはunbufferedでcloseだけを通知に使うため、require.Emptyでは
		// close済みでもlen==0として成功します。受信可否で開始の有無を検査します。
		select {
		case <-probe.shutdownStarted:
			t.Fatal("force must not start child resource cleanup")
		default:
		}
		require.Empty(t, closed)
		require.False(t, controller.ShutdownComplete())
		finishActive()
		<-probe.shutdownStarted
		synctest.Wait()
		require.Empty(t, closed, "wrapper must wait for the child's actual resource exit")
		probe.releaseShutdown()
		require.NoError(t, <-closed)
		require.True(t, controller.ShutdownComplete())
	})
}

type forceCloseFailureProbe struct {
	shutdownStarted chan struct{}
	finishShutdown  chan struct{}
	finishOnce      sync.Once
	forceErr        error
	forces          int
	handled         int
}

// releaseShutdownはchildの資源解放の完了を一度だけ通知します。試験の途中で失敗した場合も
// cleanupから呼び、bubble内で待機しているCloseをdeadlockではなく終了させます。
func (h *forceCloseFailureProbe) releaseShutdown() {
	h.finishOnce.Do(func() { close(h.finishShutdown) })
}

func (h *forceCloseFailureProbe) HandleFailure(context.Context, loggerfailure.SendResult) error {
	h.handled++
	return nil
}

func (h *forceCloseFailureProbe) Shutdown(context.Context) error {
	close(h.shutdownStarted)
	return pkgshutdown.ErrForcedClose
}

func (h *forceCloseFailureProbe) ShutdownDone() <-chan struct{} { return h.finishShutdown }

func (h *forceCloseFailureProbe) ShutdownComplete() bool {
	select {
	case <-h.finishShutdown:
		return true
	default:
		return false
	}
}

func (h *forceCloseFailureProbe) ForceClose() error {
	h.forces++
	return h.forceErr
}

func TestClickHouseLoggerForceCloseReachesRecoveryDuringConcurrentClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probe := &forceCloseFailureProbe{
			shutdownStarted: make(chan struct{}),
			finishShutdown:  make(chan struct{}),
			forceErr:        errors.New("force forwarded"),
		}
		t.Cleanup(probe.releaseShutdown)
		composite, err := flushfailure.NewCompositeFailureHandler([]flushfailure.FailureHandler{probe}, 1)
		require.NoError(t, err)
		logger := &ClickHouseLogger{}
		logger.mu.Lock()
		logger.flushManager = &ClickHouseFlushManager{FailureHandler: composite}
		logger.mu.Unlock()
		closed := make(chan error, 1)
		go func() { closed <- logger.Close(context.Background()) }()
		<-probe.shutdownStarted
		err = logger.ForceClose()
		require.ErrorIs(t, err, pkgshutdown.ErrForcedClose)
		require.ErrorIs(t, err, probe.forceErr)
		require.Equal(t, 1, probe.forces)
		require.ErrorIs(t, logger.loggerClosedError(), pkglogger.ErrLoggerClosed)
		synctest.Wait()
		require.Empty(t, closed, "internal Close must retain resources until child shutdown returns")
		require.Zero(t, probe.handled, "forced shutdown must not start replay or failure handling")
		probe.releaseShutdown()
		require.ErrorIs(t, <-closed, pkgshutdown.ErrForcedClose)
		require.True(t, logger.ShutdownComplete(), "terminal force error must not strand parent cleanup")
	})
}
