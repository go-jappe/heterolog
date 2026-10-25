package lifecycle

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestDirectoryLifecycleWaitsForClosedRouteNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		owner := &pendingRouteNotifications{done: make(chan struct{})}
		directory := &DirectoryLifecycle{
			objectStorageCompressedLogFileUploadFailureRouteOwners: []ObjectStorageUploadFailureRouteOwner{owner},
		}
		require.NoError(t, boundedwait.CloseInBubble(t, "directory.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", directory.CloseObjectStorageCompressedLogFileUploadFailureRoutes, directory.ForceClose))
		done := directory.NotificationsDone()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("closed route still has an accepted notification")
		default:
		}
		require.NoError(t, directory.ForceClose())
		require.True(t, owner.forced, "force must still reach a route whose producer has closed")
		close(owner.done)
		synctest.Wait()
		boundedwait.ReceiveInBubble(t, done, "receive from done")
	})
}

func TestDispatchedFailureNotificationIgnoresProducerCancelAndHonorsForce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type contextKey struct{}
		producer, stopProducer := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "report origin"))
		force, forceCancel := context.WithCancel(context.Background())
		defer forceCancel()
		dispatcher := notification.NewDispatcher()
		entered := make(chan context.Context, 1)
		finished := make(chan struct{})
		reporter := &dispatchedCompressedLogFileUploadFailureRunReporter{
			dispatcher:   dispatcher,
			forceContext: force,
			next: runReporterFunc(func(ctx context.Context, _ RunReport, _ error) {
				entered <- ctx
				<-ctx.Done()
				close(finished)
			}),
		}
		reporter.ReportRun(producer, RunReport{}, nil)
		stopProducer()
		dispatcher.CloseAfterDrain()
		synctest.Wait()
		notificationContext := boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		require.NoError(t, notificationContext.Err())
		require.Equal(t, "report origin", notificationContext.Value(contextKey{}))
		select {
		case <-dispatcher.Done():
			t.Fatal("dispatcher finished before the accepted notification")
		default:
		}
		forceCancel()
		synctest.Wait()
		boundedwait.ReceiveInBubble(t, finished, "receive from finished")
		boundedwait.ReceiveInBubble(t, dispatcher.Done(), "receive from dispatcher.Done()")
	})
}

func TestDirectManagerOwnerWaitsForAcceptedCheckpointNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		directory := resolvedTempDirForTest(t)
		entered, finish := make(chan struct{}), make(chan struct{})
		// 予定より多く通知された場合も、二度目のcloseでprocessを止めません。
		notifyEntered := sync.OnceFunc(func() { close(entered) })
		releaseFinish := sync.OnceFunc(func() { close(finish) })
		store, err := logfile.NewCheckpointStore(directory, "direct-manager-notifications", func(error) {
			notifyEntered()
			<-finish
		})
		require.NoError(t, err)
		// managerへ渡す前に失敗しても保存の再試行と通知を止め、通知の待機はStoreの終了より先に解除します。
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), nil)
		t.Cleanup(releaseFinish)
		restore := blockRunnerCheckpointDocument(t, filepath.Join(directory, "direct-manager-notifications.log-file-uploads.json"))
		_, err = store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20261020-1"}, []objectstorage.DestinationID{"archive"})
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		restore()
		manager := newCheckpointStoreCloseManager(t, store)
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose), "producer shutdown waits for persistence, independently of callback drain")
		done := manager.NotificationsDone()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("direct owner lost the accepted checkpoint notification")
		default:
		}
		releaseFinish()
		boundedwait.ReceiveInBubble(t, done, "receive from done")
		var absent *FileSeriesManager
		boundedwait.ReceiveInBubble(t, absent.NotificationsDone(), "receive from absent.NotificationsDone()")
	})
}

type pendingRouteNotifications struct {
	done   chan struct{}
	forced bool
}

func (*pendingRouteNotifications) Close() error                         { return nil }
func (o *pendingRouteNotifications) RequestForceClose() error           { o.forced = true; return nil }
func (o *pendingRouteNotifications) NotificationsDone() <-chan struct{} { return o.done }
