package logger

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

// Closeは受付を止めてからappend log maintenanceの終了を待つため、その待機中の要求は
// childへ渡しません。待機に入ったことはsynctest.Waitで全goroutineの停止を待ってから確認し、
// 実時間のpollingに依存しません。
func TestTopLevelLoggerCloseStopsAcceptingBeforeAppendLogMaintenanceWait(t *testing.T) {
	t.Run("sync", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			child := &topLevelAdmissionSyncChild{}
			delegate, err := multi.NewMultiLogger(rootMultiLoggerTestConfig{}, child)
			require.NoError(t, err)
			maintenance, releaseMaintenance := blockedTopLevelAdmissionMaintenance()
			// 途中の確認が失敗しても、maintenanceを解放してCloseを完了させます。
			t.Cleanup(releaseMaintenance)
			runtime := &topLevelLoggerRuntime{
				topLevelLoggerDelegate:      delegate,
				appendLogMaintenanceRuntime: maintenance,
			}

			closeDone := make(chan error, 1)
			go func() { closeDone <- runtime.Close() }()
			waitForTopLevelAdmissionMaintenanceClose(t, maintenance)

			err = runtime.Info(context.Background(), "rejected while maintenance is stopping")
			require.ErrorIs(t, err, applogging.ErrLoggerClosed)
			require.Equal(t, int32(0), child.handleCalls.Load())

			releaseMaintenance()
			require.NoError(t, boundedwait.ReceiveInBubble(t, closeDone, "top-level logger Close after the maintenance was released"))
		})
	})

	t.Run("async", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			child := &topLevelAdmissionSyncChild{}
			delegate, err := multi.NewAsyncBestEffortMultiLogger(
				topLevelAdmissionAsyncConfig{},
				child,
			)
			require.NoError(t, err)
			maintenance, releaseMaintenance := blockedTopLevelAdmissionMaintenance()
			t.Cleanup(releaseMaintenance)
			runtime := &topLevelAsyncLoggerRuntime{
				topLevelAsyncLoggerDelegate: delegate,
				appendLogMaintenanceRuntime: maintenance,
			}

			closeDone := make(chan error, 1)
			go func() { closeDone <- runtime.Close() }()
			waitForTopLevelAdmissionMaintenanceClose(t, maintenance)

			runtime.Info(context.Background(), "silently rejected while maintenance is stopping")
			releaseMaintenance()
			require.NoError(t, boundedwait.ReceiveInBubble(t, closeDone, "top-level logger Close after the maintenance was released"))
			// Closeは受理済みの非同期処理を待つため、ここで0件なら待機中の要求は受理されていません。
			require.Equal(t, int32(0), child.handleCalls.Load())
		})
	})

	t.Run("delivery", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			child := &topLevelAdmissionDeliveryChild{backend: "delivery-test"}
			delegate, err := multi.NewDeliveryMultiLoggerWithTargets(
				rootMultiLoggerTestConfig{},
				multi.DeliveryTarget{Backend: child.backend, Logger: child},
			)
			require.NoError(t, err)
			maintenance, releaseMaintenance := blockedTopLevelAdmissionMaintenance()
			t.Cleanup(releaseMaintenance)
			runtime := &topLevelDeliveryLoggerRuntime{
				topLevelDeliveryLoggerDelegate: delegate,
				appendLogMaintenanceRuntime:    maintenance,
			}

			closeDone := make(chan error, 1)
			go func() { closeDone <- runtime.Close() }()
			waitForTopLevelAdmissionMaintenanceClose(t, maintenance)

			report, err := runtime.Info(
				context.Background(),
				"rejected while maintenance is stopping",
			)
			require.ErrorIs(t, err, applogging.ErrLoggerClosed)
			require.Len(t, report.Targets, 1)
			require.Equal(t, pkglogger.DeliveryRejected, report.Targets[0].Status)
			require.Equal(t, child.backend, report.Targets[0].Backend)
			require.Equal(t, int32(0), child.handleCalls.Load())

			releaseMaintenance()
			require.NoError(t, boundedwait.ReceiveInBubble(t, closeDone, "top-level logger Close after the maintenance was released"))
		})
	})
}

func blockedTopLevelAdmissionMaintenance() (*appendLogMaintenanceRuntime, func()) {
	runtime := &appendLogMaintenanceRuntime{}
	runtime.wg.Add(1)
	return runtime, sync.OnceFunc(runtime.wg.Done)
}

// waitForTopLevelAdmissionMaintenanceCloseは、Closeがmaintenanceの終了待ちに入るまで待ちます。
// synctestのbubble内で呼び、全goroutineが停止した時点でmaintenanceのCloseが始まっていることを
// 確認します。
func waitForTopLevelAdmissionMaintenanceClose(
	t *testing.T,
	runtime *appendLogMaintenanceRuntime,
) {
	t.Helper()
	synctest.Wait()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	require.True(t, runtime.closed, "top-level Close did not reach the append log maintenance wait")
}

type topLevelAdmissionAsyncConfig struct{}

func (topLevelAdmissionAsyncConfig) Timeout() time.Duration { return time.Second }
func (topLevelAdmissionAsyncConfig) MaxParallel() int       { return 1 }
func (topLevelAdmissionAsyncConfig) MaxLogEntryBytes() int {
	return pkglogger.DefaultMaxLogEntryBytes
}

type topLevelAdmissionSyncChild struct {
	applogging.Logger
	handleCalls atomic.Int32
}

func (c *topLevelAdmissionSyncChild) HandleLogEntry(
	context.Context,
	*infralogging.LogEntry,
) error {
	c.handleCalls.Add(1)
	return nil
}

type topLevelAdmissionDeliveryChild struct {
	applogging.DeliveryLogger
	backend     string
	handleCalls atomic.Int32
}

func (c *topLevelAdmissionDeliveryChild) HandleLogEntry(
	context.Context,
	*infralogging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	c.handleCalls.Add(1)
	return pkglogger.NewTargetDeliveryReport(pkglogger.DeliveryResult{
		Backend: c.backend,
		Status:  pkglogger.DeliverySucceeded,
	}), nil
}
