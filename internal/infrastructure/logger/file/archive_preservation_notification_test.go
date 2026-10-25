package file

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
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
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type archivePreservationNotificationUploader struct {
	notifier compressedlogfailure.Notifier
	warning  error
	failure  error
}

func (u *archivePreservationNotificationUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	u.notifier(context.WithoutCancel(ctx), compressedlogfailure.Notification{
		SourcePath: request.FilePath(), DestinationID: "destination-a",
		PreservationWarning: u.warning, SaveErr: u.failure, CheckpointErr: u.failure,
	})
	return u.failure
}

func TestManagedFileArchiveUploaderCloseWaitsForPreservationWarning(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
		managed := &managedFileArchiveUploader{owner: owner, maintenanceNotifications: notification.NewDispatcher()}
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "managed Close", func() closerForTest { return managed }, false)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		t.Cleanup(unblock)
		type notification struct {
			ctx   context.Context
			entry *logging.LogEntry
		}
		warnings := make(chan notification, 1)
		errorsSeen := make(chan error, 1)
		owner.OnWarn = func(ctx context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			<-release
			managed.RequestClose()
			warnings <- notification{ctx: ctx, entry: entry}
		}
		owner.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			errorsSeen <- err
		}
		warning := errors.New("conflicting saved target preserved as /failure/.compressed-log-conflict-retained")
		managed.next = newMultiArchiveUploader(t, uploader.Destination{
			ID: "destination-a", Uploader: &archivePreservationNotificationUploader{notifier: managed.collectUploadFailureNotification, warning: warning},
		})
		ctx := context.WithValue(context.Background(), uploadFailureNotificationTaskKey{}, "preserved")
		path := filepath.Join(t.TempDir(), "app_20261003_1.log.gz")
		notifications := &backgroundTaskNotificationQueue{}
		require.True(t, owner.tryUploadRotatedFileUsingWithNotifications(ctx, managed, path, notifications.enqueue),
			"a successful preservation must not turn the upload into a failure")
		synctest.Wait()
		require.Empty(t, warnings, "the blocked OnWarn callback must not block the upload call")
		require.Empty(t, errorsSeen)
		require.Empty(t, notifications.notifications, "preservation warnings use the asynchronous notification dispatcher")
		closed := make(chan error, 1)
		go func() { closed <- managed.Close() }()
		synctest.Wait()
		require.Empty(t, closed, "normal Close must wait for its warning callback")
		unblock()
		synctest.Wait()
		require.NoError(t, receiveForTest(t, closed, "Close result"))
		require.Len(t, warnings, 1, "warning-only upload failure notifications must survive the no-upload-error return and Close")
		got := receiveForTest(t, warnings, "warning notification")
		require.Equal(t, "preserved", got.ctx.Value(uploadFailureNotificationTaskKey{}))
		require.Equal(t, pkglogger.LogLevelWarn, got.entry.Level)
		require.True(t, got.entry.Internal)
		require.ErrorIs(t, got.entry.Err, warning)
		// 警告文はpathを%qで表示します。Windowsのpathはbackslashがescapeされるため、
		// 期待値も同じ形式で照合します。
		for _, detail := range []string{strconv.Quote(path), "destination-a", "/failure/.compressed-log-conflict-retained"} {
			require.Contains(t, got.entry.Message, detail)
		}
		require.Empty(t, errorsSeen, "successful preservation is a warning, not a save or upload failure")
		select {
		case <-managed.maintenanceNotifications.Done():
		default:
			t.Fatal("notification worker did not finish after draining its accepted warning")
		}
	})
}

func TestManagedFileArchiveUploaderPreservationFailureUsesUploadErrorNotification(t *testing.T) {
	owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
	var warnings []*logging.LogEntry
	var errorsSeen []error
	owner.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		warnings = append(warnings, entry)
	}
	owner.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		errorsSeen = append(errorsSeen, err)
	}
	managed := &managedFileArchiveUploader{owner: owner}
	failure := errors.New("cannot preserve conflicting saved target")
	managed.next = newMultiArchiveUploader(t, uploader.Destination{
		ID: "destination-a", Uploader: &archivePreservationNotificationUploader{notifier: managed.collectUploadFailureNotification, failure: failure},
	})
	path := filepath.Join(t.TempDir(), "app_20261003_1.log.gz")
	notifications := &backgroundTaskNotificationQueue{}
	require.False(t, owner.tryUploadRotatedFileUsingWithNotifications(context.Background(), managed, path, notifications.enqueue),
		"failed preservation must keep the caller's upload incomplete")
	require.Empty(t, errorsSeen, "OnError still waits until the archive task releases ownership")
	notifications.runDeferredNotifications()
	require.Len(t, errorsSeen, 2, "the existing checkpoint and upload error routes remain separate")
	for _, got := range errorsSeen {
		require.ErrorIs(t, got, failure)
	}
	requireFileLoggerInternalError(t, errorsSeen[0], DefaultLogger, OperationArchiveUploadCheckpoint)
	requireFileLoggerInternalError(t, errorsSeen[1], DefaultLogger, OperationArchiveUpload)
	require.Empty(t, warnings, "failed preservation must not claim a safely retained conflict")
}

func TestFileLoggerSpoolPreservationWarningIsAsynchronousAndCanCloseDispatcher(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
		dispatcher := notification.NewDispatcher()
		t.Cleanup(dispatcher.CloseAfterDrain)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		t.Cleanup(unblock)
		warnings := make(chan *logging.LogEntry, 1)
		errorsSeen := make(chan error, 1)
		owner.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			<-release
			dispatcher.CloseAfterDrain()
			warnings <- entry
		}
		owner.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			errorsSeen <- err
		}
		warning := errors.New("preserved invalid spool file /spool/active/operator-note.txt as /spool/.preserved/retained")
		owner.notifyArchiveCompressedLogMaintenanceWarning(context.Background(), warning, dispatcher)
		synctest.Wait()
		require.Empty(t, warnings, "OnWarn may block without blocking maintenance")
		unblock()
		synctest.Wait()
		got := receiveForTest(t, warnings, "warning notification")
		require.Equal(t, pkglogger.LogLevelWarn, got.Level)
		require.ErrorIs(t, got.Err, warning)
		// 原因の判別は上のErrorIsで行います。ここでは管理者が読むログ本文に、保全した
		// pathを含む警告の内容が出力されること（ログ出力の契約）を確認します。
		require.Contains(t, got.Message, warning.Error())
		require.Empty(t, errorsSeen)
		select {
		case <-dispatcher.Done():
		default:
			t.Fatal("warning callback must be able to close its dispatcher")
		}
	})
}
