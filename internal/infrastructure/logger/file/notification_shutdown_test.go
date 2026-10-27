package file

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestFileUploadCloseDrainsNotificationAfterProducerCancellation(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
		dispatcher := notification.NewDispatcher()
		managed := &managedFileArchiveUploader{owner: owner, maintenanceNotifications: dispatcher}
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		dispatcher.Enqueue(func() { <-release })
		seen := make(chan error, 1)
		owner.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
			seen <- ctx.Err()
		}
		ctx, cancel := context.WithCancel(context.Background())
		owner.notifyArchiveCompressedLogMaintenanceError(ctx, "test", errors.New("upload failed"), dispatcher)
		cancel()
		closed := make(chan error, 1)
		go func() { closed <- managed.Close() }()
		synctest.Wait()
		require.Empty(t, closed)
		unblock()
		require.NoError(t, receiveForTest(t, closed, "Close result"))
		require.NoError(t, receiveForTest(t, seen, "accepted notification delivery"), "producer shutdown must not cancel accepted notification delivery")
	})
}

func TestFileUploadForceCloseReleasesCallerButRetainsRunningNotification(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
		dispatcher := notification.NewDispatcher()
		managed := &managedFileArchiveUploader{owner: owner, maintenanceNotifications: dispatcher}
		entered := make(chan context.Context, 1)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		owner.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
			entered <- ctx
			<-release
		}
		owner.notifyArchiveCompressedLogMaintenanceError(context.Background(), "test", errors.New("upload failed"), dispatcher)
		ctx := receiveForTest(t, entered, "blocked operation entry")
		closed := make(chan error, 1)
		go func() { closed <- managed.Close() }()
		synctest.Wait()
		require.Empty(t, closed)
		require.NoError(t, ctx.Err())
		require.ErrorIs(t, managed.ForceClose(), pkgshutdown.ErrForcedClose)
		require.ErrorIs(t, receiveForTest(t, closed, "Close result"), pkgshutdown.ErrForcedClose)
		synctest.Wait()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		select {
		case <-managed.ShutdownDone():
			t.Fatal("cleanup completed while notification was still running")
		default:
		}
		unblock()
		receiveForTest(t, managed.ShutdownDone(), "shutdown worker completion")
		require.True(t, managed.ShutdownComplete())
	})
}

func TestFileUploadCloseWaitsForPostUploadNotificationRegistration(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
		managed := &managedFileArchiveUploader{owner: owner, maintenanceNotifications: notification.NewDispatcher()}
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		entered := make(chan struct{})
		warnings := make(chan error, 1)
		owner.OnError = func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {}
		owner.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
			warnings <- entry.Err
		}
		failure, warning := errors.New("checkpoint failed"), errors.New("data preserved")
		managed.next = newMultiArchiveUploader(t, uploader.Destination{
			ID: "destination-a", Uploader: &archivePreservationNotificationUploader{
				notifier: managed.collectUploadFailureNotification, warning: warning, failure: failure,
			},
		})
		request, err := uploader.NewUploadRequest(filepath.Join(t.TempDir(), "app_20261003_1.log.gz"))
		require.NoError(t, err)
		uploaded := make(chan uploader.UploadReport, 1)
		go func() {
			uploaded <- managed.uploadDestinationsWithNotifications(
				context.Background(), request, []objectstorage.DestinationID{"destination-a"},
				func(notify func()) {
					close(entered)
					<-release
					notify()
				},
			)
		}()
		receiveForTest(t, entered, "blocked operation entry") // 委譲先は戻っていますが、wrapperの保全警告はまだ登録前です。
		managed.RequestClose()
		synctest.Wait()
		select {
		case <-managed.ShutdownDone():
			t.Fatal("uploader closed before the accepted call finished registering its warning")
		default:
		}
		require.ErrorIs(t, managed.Upload(context.Background(), request), compressedlogfailure.ErrUploaderClosed)
		unblock()
		receiveForTest(t, uploaded, "upload completion")
		require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "managed Close", managed))
		require.ErrorIs(t, receiveForTest(t, warnings, "warning notification"), warning, "post-upload warning must not be lost during Close")
	})
}
