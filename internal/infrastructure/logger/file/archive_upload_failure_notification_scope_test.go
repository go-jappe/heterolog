package file

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/compressedlogfailure"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/stretchr/testify/require"
)

type uploadFailureNotificationTaskKey struct{}

type scopedUploadFailureNotificationUploader struct {
	notifier    compressedlogfailure.Notifier
	beforeReply func(string)
}

func (s *scopedUploadFailureNotificationUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"destination-a"}
}

func (s *scopedUploadFailureNotificationUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	return s.UploadDestinations(ctx, request, s.DestinationIDs()).Err()
}

func (s *scopedUploadFailureNotificationUploader) UploadDestinations(
	ctx context.Context,
	request uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	path := request.FilePath()
	s.notifier(context.WithoutCancel(ctx), compressedlogfailure.Notification{
		SourcePath:    path,
		DestinationID: "destination-a",
		ConflictRenames: []infrafsstore.ConflictRename{{
			OriginalPath: path + ".original",
			RetainedPath: path + ".retained",
		}},
	})
	if s.beforeReply != nil {
		s.beforeReply(path)
	}
	results := make([]uploader.DestinationResult, len(destinationIDs))
	for i, destination := range destinationIDs {
		results[i].DestinationID = destination
	}
	return uploader.UploadReport{Results: results}
}

// requirePreservedConflictNotificationForTestは、OnErrorへの通知が、pathの送信で
// scopedUploadFailureNotificationUploaderが報告した同名衝突の保全だけを示すことを確認します。
// 保全の情報は型付きerrorの値で比べ、別の送信の衝突が混ざっていないことも確かめます。
func requirePreservedConflictNotificationForTest(t *testing.T, err error, path string) {
	t.Helper()
	requireFileLoggerInternalError(t, err, DefaultLogger, OperationArchiveUpload)
	var conflict *PreservedCompressedLogConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, PreservedCompressedLogConflictError{
		OriginalPath:  path + ".original",
		RetainedPath:  path + ".retained",
		DestinationID: "destination-a",
	}, *conflict)
}

func TestManagedFileArchiveUploaderKeepsConcurrentNotificationsWithTheirUploadTasks(t *testing.T) {
	type notification struct {
		task string
		err  error
	}
	notified := make(chan notification, 2)
	owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
	owner.OnError = func(ctx context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		notified <- notification{task: ctx.Value(uploadFailureNotificationTaskKey{}).(string), err: err}
	}
	base := t.TempDir()
	pathA := filepath.Join(base, "app_20260929_1.log.gz")
	pathB := filepath.Join(base, "app_20260929_2.log.gz")
	requestA, err := uploader.NewUploadRequest(pathA)
	require.NoError(t, err)
	requestB, err := uploader.NewUploadRequest(pathB)
	require.NoError(t, err)
	bNotificationCollected := make(chan struct{})
	releaseB := make(chan struct{}, 1)
	t.Cleanup(func() { close(releaseB) })
	managed := &managedFileArchiveUploader{owner: owner}
	managed.next = &scopedUploadFailureNotificationUploader{
		notifier: managed.collectUploadFailureNotification,
		beforeReply: func(path string) {
			if path == pathB {
				close(bNotificationCollected)
				<-releaseB
			}
		},
	}
	ids := []objectstorage.DestinationID{"destination-a"}
	queueA := &backgroundTaskNotificationQueue{}
	queueB := &backgroundTaskNotificationQueue{}
	ctxA := context.WithValue(context.Background(), uploadFailureNotificationTaskKey{}, "a")
	ctxB := context.WithValue(context.Background(), uploadFailureNotificationTaskKey{}, "b")
	bDone := make(chan uploader.UploadReport, 1)
	go func() {
		bDone <- uploadArchiveDestinationsWithNotifications(ctxB, managed, requestB, ids, queueB.enqueue)
	}()
	receiveForTest(t, bNotificationCollected, "B did not reach its notifier")

	require.NoError(t, uploadArchiveDestinationsWithNotifications(ctxA, managed, requestA, ids, queueA.enqueue).Err())
	require.Len(t, queueA.notifications, 1, "A must not drain B's notification")
	require.Empty(t, queueB.notifications, "B is still inside its upload")
	queueA.runDeferredNotifications()
	select {
	case got := <-notified:
		require.Equal(t, "a", got.task)
		requirePreservedConflictNotificationForTest(t, got.err, pathA)
	default:
		t.Fatal("A's queued notification was not dispatched")
	}
	select {
	case got := <-notified:
		t.Fatalf("B was notified before its upload returned: %+v", got)
	default:
	}

	releaseB <- struct{}{}
	report := receiveForTest(t, bDone, "B did not finish after release")
	require.NoError(t, report.Err())
	require.Len(t, queueB.notifications, 1)
	queueB.runDeferredNotifications()
	select {
	case got := <-notified:
		require.Equal(t, "b", got.task)
		requirePreservedConflictNotificationForTest(t, got.err, pathB)
	default:
		t.Fatal("B's queued notification was not dispatched")
	}
}

func TestManagedFileArchiveUploaderPublicUploadMethodsSendTheirOwnNotifications(t *testing.T) {
	for _, method := range []string{"Upload", "UploadDestinations"} {
		t.Run(method, func(t *testing.T) {
			type notification struct {
				task string
				err  error
			}
			notified := make(chan notification, 1)
			owner := &FileLogger{logger: DefaultLogger, Clock: clock.NewUTCClock(), Tracer: observer.NopTracer{}, Meter: observer.NopMeter{}}
			owner.OnError = func(ctx context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				notified <- notification{task: ctx.Value(uploadFailureNotificationTaskKey{}).(string), err: err}
			}
			managed := &managedFileArchiveUploader{owner: owner}
			managed.next = &scopedUploadFailureNotificationUploader{notifier: managed.collectUploadFailureNotification}
			path := filepath.Join(t.TempDir(), "app_20260929_1.log.gz")
			request, err := uploader.NewUploadRequest(path)
			require.NoError(t, err)
			ctx := context.WithValue(context.Background(), uploadFailureNotificationTaskKey{}, method)
			if method == "Upload" {
				require.NoError(t, managed.Upload(ctx, request))
			} else {
				require.NoError(t, managed.UploadDestinations(ctx, request, managed.DestinationIDs()).Err())
			}
			select {
			case got := <-notified:
				require.Equal(t, method, got.task)
				requirePreservedConflictNotificationForTest(t, got.err, path)
			default:
				t.Fatal("public upload method did not send its notification")
			}
		})
	}
}
