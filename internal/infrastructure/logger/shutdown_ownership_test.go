package logger

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestTopLevelForceRetainsFileLeaseUntilExternalMaintenanceStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := runnerFileConfigForInternalLogRuntime(t.TempDir(), "forced-maintenance")
		files, err := file.NewLoggerSet(file.NewConfigAdapter(*cfg), observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		maintenance := &appendLogMaintenanceRuntime{}
		release := make(chan struct{})
		releaseMaintenance := sync.OnceFunc(func() { close(release) })
		// Cleanupは後に登録したものから実行されます。途中の確認が失敗しても、受理済みの周期処理を
		// 解放してからfileを閉じ、bubbleのgoroutineを残しません。
		t.Cleanup(func() { _ = files.Close() })
		t.Cleanup(releaseMaintenance)
		maintenance.wg.Add(1)
		go func() {
			// file自身のbackgroundWGとは別の、受理済み周期処理を再現します。
			defer maintenance.wg.Done()
			<-release
		}()
		owner := &topLevelLoggerRuntime{
			topLevelLoggerDelegate:      &closeStageTestSyncBackend{},
			appendLogMaintenanceRuntime: maintenance,
			fileLoggerSet:               files,
		}
		require.ErrorIs(t, owner.ForceClose(), applogging.ErrForcedClose)
		synctest.Wait()
		require.False(t, files.ShutdownComplete(), "force must not independently dispose dependent files")
		lease, acquireErr := directorylease.Acquire("test.forced-maintenance", cfg.FsstoreConfig.Layout.LogPath)
		if lease != nil {
			lease.Release()
		}
		require.ErrorIs(t, acquireErr, directorylease.ErrDirectoryLeaseConflict)
		select {
		case <-owner.closeController.Done():
			t.Fatal("root cleanup abandoned accepted external maintenance")
		default:
		}
		releaseMaintenance()
		boundedwait.ReceiveInBubble(t, owner.closeController.Done(), "the root cleanup after the maintenance ended")
		require.True(t, files.ShutdownComplete())
		lease, err = directorylease.Acquire("test.forced-maintenance-replacement", cfg.FsstoreConfig.Layout.LogPath)
		require.NoError(t, err)
		lease.Release()
	})
}

type detachedForcedBackend struct {
	applogging.Logger
	start, done, release, requested chan struct{}
	startOnce, requestOnce          sync.Once
	// forceCloseCalledは親が中止要求の代わりにchildの後片付けを開始したことを記録します。
	// common.ForceCloseIfSupportedはpanicをerrorへ変換するため、panicでは検出できません。
	forceCloseCalled atomic.Bool
}

func (*detachedForcedBackend) StopAccepting() {}

func (b *detachedForcedBackend) Close() error {
	b.startOnce.Do(func() {
		close(b.start)
		go func() { <-b.release; close(b.done) }()
	})
	return applogging.ErrForcedClose
}

func (b *detachedForcedBackend) RequestForceClose() error {
	b.requestOnce.Do(func() { close(b.requested) })
	return applogging.ErrForcedClose
}

func (b *detachedForcedBackend) ForceClose() error {
	b.forceCloseCalled.Store(true)
	return applogging.ErrForcedClose
}

func (b *detachedForcedBackend) ShutdownDone() <-chan struct{} { return b.done }

func (b *detachedForcedBackend) ShutdownComplete() bool {
	select {
	case <-b.done:
		return true
	default:
		return false
	}
}

func TestTopLevelForceCompletesDownstreamSpoolCleanupAfterDetachedChild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root := t.TempDir()
		leases, err := acquireFailureSpoolDirectoryLeases([]failureSpoolPipelinePlan{
			testFailureSpoolPipelinePlan(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch, root),
		})
		require.NoError(t, err)
		backend := &detachedForcedBackend{
			start: make(chan struct{}), done: make(chan struct{}),
			release: make(chan struct{}), requested: make(chan struct{}),
		}
		releaseBackend := sync.OnceFunc(func() { close(backend.release) })
		owner := &topLevelLoggerRuntime{
			topLevelLoggerDelegate: backend,
			failureSpoolPipelines: &failureSpoolPipelineSet{
				runtime: &FailureSpoolRuntime{}, leases: leases, writersClosed: true,
			},
		}
		// 途中の確認が失敗しても、切り離したchildの後片付けを解放し、rootの終了処理の完了を
		// 待ってからbubbleを終えます。終了処理は再試行の間隔（shutdown.ReportInterval）ごとに
		// childの完了を確かめるため、cleanupの中で待って仮想時刻を進めます（bubbleの終了後は
		// 時刻が進みません）。上限は再試行の周期より十分長いboundedwait.BubbleTimeoutです。この
		// cleanupは失敗の後片付けのためだけに待ち、完了の確認は試験の本文で行います。
		t.Cleanup(func() {
			releaseBackend()
			select {
			case <-owner.closeController.Done():
			case <-time.After(boundedwait.BubbleTimeout):
			}
		})
		result := make(chan error, 1)
		go func() { result <- owner.Close() }()
		boundedwait.ReceiveInBubble(t, backend.start, "the backend Close")
		require.ErrorIs(t, owner.ForceClose(), applogging.ErrForcedClose)
		require.ErrorIs(t, boundedwait.ReceiveInBubble(t, result, "the forced Close"), applogging.ErrForcedClose)
		boundedwait.ReceiveInBubble(t, backend.requested, "the force request to the child")
		synctest.Wait()
		lease, acquireErr := directorylease.Acquire("test.forced-spool-live", root)
		if lease != nil {
			lease.Release()
		}
		require.ErrorIs(t, acquireErr, directorylease.ErrDirectoryLeaseConflict)
		releaseBackend()
		boundedwait.ReceiveInBubble(t, owner.closeController.Done(), "the root cleanup after the child ended")
		lease, err = directorylease.Acquire("test.forced-spool-replacement", root)
		require.NoError(t, err)
		lease.Release()
		require.Equal(t, topLevelLoggerCloseStageCompleted, owner.closeStage)
		require.False(t, backend.forceCloseCalled.Load(), "parent must request cancellation without starting child cleanup")
	})
}
