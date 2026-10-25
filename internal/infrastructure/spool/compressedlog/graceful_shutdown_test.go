package compressedlog

import (
	"context"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestRootRuntimeGracefulCloseDrainsAcceptedReplayAndNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval, cfg.LifecycleInterval = time.Second, time.Hour
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		uploadEntered := make(chan struct{}, 1)
		uploadRelease, notificationRelease := make(chan struct{}), make(chan struct{})
		uploadUnblock := sync.OnceFunc(func() { close(uploadRelease) })
		notificationUnblock := sync.OnceFunc(func() { close(notificationRelease) })
		defer uploadUnblock()
		defer notificationUnblock()
		notificationEntered := make(chan context.Context, 1)
		upload := &replaytest.Uploader{Ready: uploadEntered, Block: uploadRelease}
		route, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
			RunObserver: func(ctx context.Context, _ *RunReport, _ *objectstoragespool.TerminalPurgeReport, _ error) {
				notificationEntered <- ctx
				<-notificationRelease
			},
		}, upload)
		require.NoError(t, err)
		for _, name := range []string{"app_20261020_1.log.gz", "app_20261020_2.log.gz"} {
			_, err := route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, name))
			require.NoError(t, err)
		}
		synctest.Wait()
		time.Sleep(time.Second)
		<-uploadEntered
		require.NoError(t, runtime.Close()) // Release the root owner; the route still owns its accepted work.
		closed := make(chan error, 1)
		go func() { closed <- route.Close() }()
		synctest.Wait()
		require.Empty(t, closed, "normal close must wait for the accepted upload")
		require.False(t, runtime.ShutdownComplete())
		_ = replaytest.OnlyFile(t, runtime.store, pkgspool.StateProcessing)

		uploadUnblock()
		notificationCtx := <-notificationEntered
		synctest.Wait()
		require.NoError(t, notificationCtx.Err(), "normal stop must leave the notification context valid")
		require.Empty(t, closed, "normal close must also wait for the accepted notification")
		notificationUnblock()
		require.NoError(t, <-closed)
		<-runtime.ShutdownDone()
		require.NoError(t, notificationCtx.Err(), "closing the root must not cancel queued downstream notifications")
		require.Equal(t, 1, upload.CallCount(), "shutdown must preserve the remaining backlog for a later process")
		_ = replaytest.OnlyFile(t, runtime.store, pkgspool.StateDone)
		_ = replaytest.OnlyFile(t, runtime.store, pkgspool.StateActive)
	})
}

func TestManagedRouteCloseWaitsForCapturedManualNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Hour
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		entered, release, reported := make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		route, err := runtime.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
			RunObserver: func(context.Context, *RunReport, *objectstoragespool.TerminalPurgeReport, error) {
				close(entered)
				<-release
			},
		}, &replaytest.Uploader{})
		require.NoError(t, err)
		go func() {
			runtime.ReportSpoolRunnerRun(context.Background(), RunReport{}, nil)
			close(reported)
		}()
		<-entered
		require.NoError(t, runtime.Close())
		closed := make(chan error, 1)
		go func() { closed <- route.Close() }()
		synctest.Wait()
		require.Empty(t, closed)
		require.False(t, runtime.ShutdownComplete())
		unblock()
		<-reported
		require.NoError(t, <-closed)
		<-runtime.ShutdownDone()
	})
}

func TestForceCloseReleasesWaitsAndRetainsInFlightResources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Second
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		entered, release := make(chan struct{}, 1), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		route, err := runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{
			Ready: entered, Block: release, IgnoreContext: true,
		})
		require.NoError(t, err)
		_, err = route.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "app_20261020_3.log.gz"))
		require.NoError(t, err)
		synctest.Wait()
		time.Sleep(time.Second)
		<-entered
		require.NoError(t, runtime.Close())
		closed := make(chan error, 1)
		go func() { closed <- route.Close() }()
		synctest.Wait()
		require.ErrorIs(t, route.RequestForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, runtime.RequestForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, <-closed, shutdown.ErrForcedClose)
		require.ErrorIs(t, runtime.Close(), shutdown.ErrForcedClose)
		synctest.Wait()
		require.False(t, route.ShutdownComplete())
		require.False(t, runtime.ShutdownComplete())
		_ = replaytest.OnlyFile(t, runtime.store, pkgspool.StateProcessing)
		unblock()
		<-route.ShutdownDone()
		<-runtime.ShutdownDone()
		require.True(t, runtime.ShutdownComplete())
	})
}

func TestRouteForceCancelsItsUploadWithoutCancelingOtherRoutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Second
		runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		entered := make(chan struct{}, 1)
		blocked := make(chan struct{})
		first, err := runtime.RegisterRoute(ManagedRouteConfig{FileName: replaytest.FileNamePattern("first_")}, &replaytest.Uploader{Ready: entered, Block: blocked})
		require.NoError(t, err)
		secondUpload := &replaytest.Uploader{}
		second, err := runtime.RegisterRoute(ManagedRouteConfig{FileName: replaytest.FileNamePattern("second_")}, secondUpload)
		require.NoError(t, err)
		defer second.Close()
		_, err = first.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "first_20261020_1.log.gz"))
		require.NoError(t, err)
		synctest.Wait()
		time.Sleep(time.Second)
		<-entered
		require.ErrorIs(t, first.ForceClose(), shutdown.ErrForcedClose)
		<-first.ShutdownDone()
		require.False(t, runtime.closeController.IsForced(), "forcing one route must not force its shared root")
		_, err = second.SaveRetryableFailure(context.Background(), replaytest.WriteSource(t, "second_20261020_1.log.gz"))
		require.NoError(t, err)
		synctest.Wait()
		// The first route's requeued file stays active. Remove it from consideration by aging
		// the second file ahead of it, so the next actual replay exercises the live route.
		files, err := runtime.store.ListFiles(context.Background(), pkgspool.StateActive)
		require.NoError(t, err)
		for _, file := range files {
			if file.FileName == "second_20261020_1.log.gz" {
				old := time.Now().Add(-time.Hour)
				require.NoError(t, os.Chtimes(file.Path, old, old))
			}
		}
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 1, secondUpload.CallCount())
	})
}

func TestRootForceRequestRejectsNewRoutesBeforeClose(t *testing.T) {
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Hour
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	first, err := runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{})
	require.NoError(t, err)
	defer first.Close()
	require.ErrorIs(t, runtime.RequestForceClose(), shutdown.ErrForcedClose)
	_, err = runtime.RegisterRoute(stateTestRouteConfig(), &replaytest.Uploader{})
	require.ErrorIs(t, err, ErrRootRuntimeClosed)
	// addRoute rechecks admission after filesystem preparation, which may overlap force.
	err = runtime.addRoute(&ManagedRoute{}, stateTestRouteConfig().FileName, &replaytest.Uploader{}, nil, nil)
	require.ErrorIs(t, err, ErrRootRuntimeClosed)
	require.NoError(t, first.Close())
	require.ErrorIs(t, runtime.Close(), shutdown.ErrForcedClose)
	<-runtime.ShutdownDone()
}

func TestRouteForceCancelsItsNotificationWithoutCancelingOtherRoutes(t *testing.T) {
	for _, notification := range []string{"observer", "warning"} {
		t.Run(notification, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := managedUploaderTestConfig(t.TempDir())
				runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
				entered, canceled := make(chan struct{}), make(chan error, 1)
				siblingContexts := make(chan context.Context, 2)
				firstConfig, secondConfig := stateTestRouteConfig(), stateTestRouteConfig()
				firstConfig.DisableMaintenance, secondConfig.DisableMaintenance = true, true
				firstNotify := func(ctx context.Context) {
					close(entered)
					<-ctx.Done()
					canceled <- ctx.Err()
				}
				secondNotify := func(ctx context.Context) { siblingContexts <- ctx }
				if notification == "observer" {
					firstConfig.RunObserver = func(ctx context.Context, _ *RunReport, _ *objectstoragespool.TerminalPurgeReport, _ error) {
						firstNotify(ctx)
					}
					secondConfig.RunObserver = func(ctx context.Context, _ *RunReport, _ *objectstoragespool.TerminalPurgeReport, _ error) {
						secondNotify(ctx)
					}
				} else {
					firstConfig.OnWarn = func(ctx context.Context, _ error) { firstNotify(ctx) }
					secondConfig.OnWarn = func(ctx context.Context, _ error) { secondNotify(ctx) }
				}
				first, err := runtime.RegisterRoute(firstConfig, &replaytest.Uploader{})
				require.NoError(t, err)
				second, err := runtime.RegisterRoute(secondConfig, &replaytest.Uploader{})
				require.NoError(t, err)
				defer second.Close()
				if notification == "warning" {
					_, err = first.PreserveFile(context.Background(), replaytest.WriteSource(t, "app_20261020_4.log.gz"))
					require.NoError(t, err)
				}
				reported := make(chan struct{})
				go func() {
					runtime.ReportSpoolRunnerRun(context.Background(), RunReport{}, nil)
					close(reported)
				}()
				<-entered
				require.ErrorIs(t, first.ForceClose(), shutdown.ErrForcedClose)
				require.ErrorIs(t, <-canceled, context.Canceled)
				<-reported
				<-first.ShutdownDone()
				require.NoError(t, (<-siblingContexts).Err())
				runtime.ReportSpoolRunnerRun(context.Background(), RunReport{}, nil)
				require.NoError(t, (<-siblingContexts).Err(), "the remaining route must continue accepting notifications")
				require.False(t, runtime.closeController.IsForced())
			})
		})
	}
}

func TestUninitializedRuntimeForceRequestsAreNoOps(t *testing.T) {
	for _, runtime := range []*RootRuntime{nil, {}} {
		require.NoError(t, runtime.RequestForceClose())
		require.NoError(t, runtime.ForceClose())
	}
	for _, route := range []*ManagedRoute{nil, {}} {
		require.NoError(t, route.RequestForceClose())
		require.NoError(t, route.ForceClose())
	}
}
