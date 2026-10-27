package logfile

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestNewCheckpointStoreWithoutErrorHandlerReturnsFilesystemStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := NewCheckpointStore(t.TempDir(), "without-notifications", nil)
		require.NoError(t, err)
		require.IsType(t, &checkpointstore.FilesystemStore{}, store)
		require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
		// 不要な通知workerがあれば、raw storeはそのworkerを閉じられないため、
		// このsynctest bubbleの終了時にblockされたまま残ります。
	})
}

func TestNewCheckpointStoreFailureClosesNotificationWorker(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "without handler"
		if configured {
			name = "with handler"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				var onError func(error)
				if configured {
					onError = func(error) { calls++ }
				}
				store, err := NewCheckpointStore(filepath.Join(t.TempDir(), "missing"), "constructor-failure", onError)
				require.Nil(t, store)
				require.ErrorIs(t, err, os.ErrNotExist)
				synctest.Wait()
				require.Zero(t, calls, "constructor errors are returned without retry notifications")
				// bubbleを完了するには、constructorが起動したworkerが終了している必要があります。
			})
		})
	}
}

func TestCheckpointStoreCloseStopsProducerAndNotificationsDoneDrainsCallbacks(t *testing.T) {
	// このStore部品のCloseは保存処理と通知受付を止めます。上位ownerはさらに
	// NotificationsDoneを待つため、通知自身から上位ownerを同期終了できるという試験ではありません。
	for _, externalClose := range []bool{false, true} {
		name := "callback closes store"
		if externalClose {
			name = "caller closes while callback blocked"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const source = "checkpoint-close-notifications"
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				type notification struct{ reported, closeErr error }
				reports := make(chan notification, 4)
				var store checkpointstore.Store
				var err error
				directory := resolvedTempDirForTest(t)
				store, err = NewCheckpointStore(directory, source, func(err error) {
					<-release
					boundedwait.SendOrReport(t, reports, notification{reported: err, closeErr: store.Close()}, "notification")
				})
				require.NoError(t, err)
				boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), nil)
				t.Cleanup(unblock)
				owned := store.(*checkpointStoreWithErrorNotifications)
				path := filepath.Join(directory, source+".log-file-uploads.json")
				require.NoError(t, os.Rename(path, path+".previous"))
				require.NoError(t, os.Mkdir(path, 0o700))
				repair := sync.OnceFunc(func() { require.NoError(t, os.Remove(path)) })
				t.Cleanup(repair)
				_, err = store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{
					Generation: "20260927-1", Compressed: true,
				}, []objectstorage.DestinationID{"archive"})
				var pending *checkpointstore.PendingSaveError
				require.ErrorAs(t, err, &pending)
				synctest.Wait()
				// 1回目のretryはblockされたcallbackに入ります。2回目はその待機中に受理され、
				// Close後も排出されなければなりません。
				for range 2 {
					time.Sleep(checkpointstore.SaveRecoveryInterval)
					synctest.Wait()
				}
				require.Empty(t, reports)
				closed := make(chan error, 1)
				if externalClose {
					go func() { closed <- store.Close() }()
					synctest.Wait()
					require.Empty(t, closed, "Close waits for the pending save to succeed")
					select {
					case <-owned.NotificationsDone():
						t.Fatal("notification worker exited before the accepted callbacks finished")
					default:
					}
				}
				unblock()
				synctest.Wait()
				require.Empty(t, reports, "notification-triggered Close also waits for persistence")
				repair()
				time.Sleep(checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				if externalClose {
					require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
				}
				require.Len(t, reports, 2, "both accepted retry errors survive Close")
				first, last := boundedwait.ReceiveInBubble(t, reports, "receive from reports"), boundedwait.ReceiveInBubble(t, reports, "receive from reports")
				for _, report := range []notification{first, last} {
					var retryErr *CheckpointSaveRetryError
					require.ErrorAs(t, report.reported, &retryErr)
					require.Equal(t, source, retryErr.SourceID)
					require.Equal(t, path, retryErr.Path)
				}
				require.NoError(t, first.closeErr)
				require.NoError(t, last.closeErr)
				boundedwait.ReceiveInBubble(t, owned.NotificationsDone(), "receive from owned.NotificationsDone()")
				time.Sleep(2 * checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				require.Empty(t, reports, "Close stops save retries before closing notification acceptance")
			})
		})
	}
}

func TestCheckpointStoreForceClosePreservesAcceptedNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const source = "force-checkpoint-notifications"
		notificationStarted, finishNotification := make(chan struct{}), make(chan struct{})
		reports := make(chan error, 2)
		directory := resolvedTempDirForTest(t)
		store, err := NewCheckpointStore(directory, source, func(err error) {
			select {
			case <-notificationStarted:
			default:
				close(notificationStarted)
			}
			<-finishNotification
			// 予定外の追加通知で通知workerを止めないよう、受け取り手がなければ失敗として記録します。
			boundedwait.SendOrReport(t, reports, err, "retry error notification")
		})
		require.NoError(t, err)
		owned := store.(*checkpointStoreWithErrorNotifications)
		releaseNotification := sync.OnceFunc(func() { close(finishNotification) })
		// 途中で失敗しても、保存の再試行と通知を止めてからbubbleを終了します。
		t.Cleanup(func() {
			if t.Failed() {
				_ = owned.ForceClose()
				releaseNotification()
			}
		})
		path := filepath.Join(directory, source+".log-file-uploads.json")
		require.NoError(t, os.Rename(path, path+".previous"))
		require.NoError(t, os.Mkdir(path, 0o700))
		_, err = store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{
			Generation: "20261007-1",
		}, []objectstorage.DestinationID{"archive"})
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		for range 2 {
			synctest.Wait()
			time.Sleep(checkpointstore.SaveRecoveryInterval)
			synctest.Wait()
		}
		boundedwait.ReceiveInBubble(t, notificationStarted, "receive from notificationStarted")
		require.ErrorIs(t, owned.ForceClose(), pkgshutdown.ErrForcedClose)
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "owned.Close()", owned.Close, owned.ForceClose), pkgshutdown.ErrForcedClose)
		boundedwait.ReceiveInBubble(t, owned.Done(), "receive from owned.Done()")
		select {
		case <-owned.NotificationsDone():
			t.Fatal("accepted notifications were abandoned")
		default:
		}
		releaseNotification()
		boundedwait.ReceiveInBubble(t, owned.NotificationsDone(), "receive from owned.NotificationsDone()")
		require.Len(t, reports, 2)
		time.Sleep(2 * checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Len(t, reports, 2, "forced shutdown must not create more save retries")
	})
}
