package file

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

func TestFileLoggerCloseWaitsForAcceptedBackgroundNotifications(t *testing.T) {
	for _, operation := range []string{"rotation", "cleanup"} {
		for _, force := range []bool{false, true} {
			mode := "graceful"
			if force {
				mode = "forced"
			}
			t.Run(operation+"/"+mode, func(t *testing.T) {
				synctestWithHangWatchdogForTest(t, func(t *testing.T) {
					now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
					codec := &inlineRecoveryCompressionCodec{}
					codec.failCompression.Store(true)
					logger := newCloseContractRotationLogger(t, now, "accepted-background-notification", codec)
					entered := make(chan context.Context, 1)
					release := make(chan struct{})
					unblock := sync.OnceFunc(func() { close(release) })
					defer unblock()
					notify := func(ctx context.Context) {
						entered <- ctx
						<-release
					}
					if operation == "rotation" {
						logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
							notify(ctx)
						}
						forceCloseContractRotation(t, logger)
						require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
					} else {
						logger.triggerCleanupUsing(context.Background(), logger.Fsstore.FileName,
							func(ctx context.Context, _ logpattern.FileNamePattern, _ inlinePurgePlan, delivery taskNotificationDelivery) error {
								delivery(func() { notify(ctx) })
								return nil
							}, true, "")
					}
					notificationContext := receiveForTest(t, entered, "blocked operation entry")
					codec.failCompression.Store(false)
					closed := make(chan error, 1)
					go func() { closed <- logger.Close() }()
					// 圧縮の再試行が完了しても、通常Closeには通知の完了待ちが残ります。
					time.Sleep(10 * time.Second)
					synctest.Wait()
					require.Empty(t, closed)
					require.NoError(t, notificationContext.Err())
					require.NotNil(t, logger.Fsstore.File)
					if force {
						require.ErrorIs(t, logger.RequestForceClose(), shutdown.ErrForcedClose)
						synctest.Wait()
						require.ErrorIs(t, receiveForTest(t, closed, "Close result"), shutdown.ErrForcedClose)
						require.ErrorIs(t, notificationContext.Err(), context.Canceled)
						require.NotNil(t, logger.Fsstore.File, "force does not release a file while its accepted notification still uses the owner")
					}
					unblock()
					synctest.Wait()
					if !force {
						require.NoError(t, receiveForTest(t, closed, "Close result"))
					}
					receiveForTest(t, logger.ShutdownDone(), "shutdown worker completion")
					require.Nil(t, logger.Fsstore.File)
				})
			})
		}
	}
}
