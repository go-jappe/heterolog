package fsarchivehandler

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestFailureArchiveShutdownWaitsForNotificationsUntilForce(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "graceful"
		if force {
			name = "forced"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan context.Context, 1)
				release := make(chan struct{})
				var releaseOnce sync.Once
				releaseNotification := func() { releaseOnce.Do(func() { close(release) }) }
				pipeline := newTestArchiveHandler(t, time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC),
					func(cfg *testArchiveConfig) {
						cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
						cfg.lifecycleRunnerConfig.Interval = time.Hour
					}, InternalLogHandlers{OnDebug: func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
						entered <- ctx
						<-release
					}})
				// 試験が途中で失敗しても、止めた通知を再開してからPipelineを閉じます。
				t.Cleanup(releaseNotification)
				producer, cancelProducer := context.WithCancel(context.Background())
				pipeline.runnerRuntime.runners[0].Reporter.ReportRun(producer, logfilelifecycle.RunReport{}, nil)
				cancelProducer()
				synctest.Wait()
				notificationContext := <-entered
				result := make(chan error, 1)
				go func() { result <- pipeline.Shutdown(context.Background()) }()
				synctest.Wait()
				require.Empty(t, result, "normal shutdown must wait for the accepted notification")
				require.NoError(t, notificationContext.Err(), "normal close and producer cancellation must not cancel notification output")
				if force {
					require.ErrorIs(t, pipeline.RequestForceClose(), pkgshutdown.ErrForcedClose)
					synctest.Wait()
					require.ErrorIs(t, <-result, pkgshutdown.ErrForcedClose)
					require.ErrorIs(t, notificationContext.Err(), context.Canceled)
					select {
					case <-pipeline.ShutdownDone():
						t.Fatal("resources were released while notification was still running")
					default:
					}
				}
				releaseNotification()
				synctest.Wait()
				if !force {
					require.NoError(t, <-result)
				}
				<-pipeline.ShutdownDone()
				require.True(t, pipeline.ShutdownComplete())
			})
		})
	}
}
