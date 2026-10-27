package file

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestFileLoggerShutdownRetainsAcceptedWriteUntilItsNotificationReturns(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "graceful"
		if force {
			name = "forced"
		}
		t.Run(name, func(t *testing.T) {
			synctestWithHangWatchdogForTest(t, func(t *testing.T) {
				cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "accepted-write-notification")
				cfg.FsstoreConfig.Duration.WriteFileLatencyEnabled = true
				cfg.RuntimeConfig.IsDebug = true
				logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
				require.NoError(t, err)
				entered, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				logger.OnDebug = func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
					close(entered)
					<-release
				}
				written := make(chan error, 1)
				go func() { written <- logger.testInfo(context.Background(), "accepted write") }()
				receiveForTest(t, entered, "blocked operation entry")

				closed := make(chan error, 1)
				go func() { closed <- logger.Close() }()
				synctest.Wait()
				require.Empty(t, written)
				require.Empty(t, closed, "normal Close must include the accepted write's internal notification")
				require.NotNil(t, logger.Fsstore.File)
				if force {
					require.ErrorIs(t, logger.ForceClose(), pkgshutdown.ErrForcedClose)
					require.ErrorIs(t, receiveForTest(t, closed, "Close result"), pkgshutdown.ErrForcedClose)
					synctest.Wait()
					require.NotNil(t, logger.Fsstore.File, "force releases callers, not resources still owned by an accepted write")
					select {
					case <-logger.ShutdownDone():
						t.Fatal("shutdown completed while the write's notification was still running")
					default:
					}
				}
				unblock()
				require.NoError(t, receiveForTest(t, written, "accepted write result"))
				if !force {
					require.NoError(t, receiveForTest(t, closed, "Close result"))
					require.True(t, logger.ShutdownComplete(), "normal Close returns only after actual cleanup")
					require.Nil(t, logger.Fsstore.File)
				}
				receiveForTest(t, logger.ShutdownDone(), "shutdown worker completion")
				require.Nil(t, logger.Fsstore.File)
				require.True(t, logger.ShutdownComplete())
			})
		})
	}
}
