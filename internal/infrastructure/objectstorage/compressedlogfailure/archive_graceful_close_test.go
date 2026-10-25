package compressedlogfailure

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestArchiveCloseWaitsForAcceptedPurgeAndNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := compressedlogarchive.NewStore(newArchiveRoot(t, t.TempDir()))
		require.NoError(t, err)
		notificationStarted := make(chan context.Context, 1)
		releaseNotification := make(chan struct{})
		owner, coordinator, err := acquireArchiveLifecycleWithMaintenance(store, NewFileArchiveConfigAdapter(
			objectstorage.CompressedLogFileUploadFailureArchiveConfig{LifecycleInterval: time.Hour},
		), nil, func(ctx context.Context, _ compressedlogarchive.PurgeReport, err error) {
			require.NoError(t, err)
			notificationStarted <- ctx
			<-releaseNotification
		}, true)
		require.NoError(t, err)
		releaseSave, err := coordinator.lockPurgeDuringSave(context.Background())
		require.NoError(t, err)
		go coordinator.runOnce(coordinator.operationContext)
		synctest.Wait() // この周期は通知先を確定し、保存処理の終了を待っています。
		closed := make(chan error, 1)
		go func() { closed <- owner.close() }()
		synctest.Wait()
		require.NoError(t, coordinator.operationContext.Err())
		select {
		case err := <-closed:
			t.Fatalf("Close returned before accepted purge: %v", err)
		default:
		}
		releaseSave()
		notificationContext := <-notificationStarted
		synctest.Wait()
		require.NoError(t, notificationContext.Err())
		select {
		case err := <-closed:
			t.Fatalf("Close returned before accepted notification: %v", err)
		default:
		}
		close(releaseNotification)
		require.NoError(t, <-closed)
		// 通知先が後段のdispatcherへ渡したcontextも、通常終了後に無効化しません。
		require.NoError(t, notificationContext.Err())
	})
}

func TestArchiveForceOneOwnerDoesNotCancelOtherOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := compressedlogarchive.NewStore(newArchiveRoot(t, t.TempDir()))
		require.NoError(t, err)
		cfg := NewFileArchiveConfigAdapter(objectstorage.CompressedLogFileUploadFailureArchiveConfig{LifecycleInterval: time.Hour})
		started := make(chan struct{})
		first, coordinator, err := acquireArchiveLifecycleWithMaintenance(store, cfg, nil,
			func(ctx context.Context, _ compressedlogarchive.PurgeReport, _ error) {
				close(started)
				<-ctx.Done()
			}, true)
		require.NoError(t, err)
		secondContext := make(chan context.Context, 1)
		second, _, err := acquireArchiveLifecycleWithMaintenance(store, cfg, nil,
			func(ctx context.Context, _ compressedlogarchive.PurgeReport, _ error) { secondContext <- ctx }, true)
		require.NoError(t, err)
		go coordinator.runOnce(coordinator.operationContext)
		<-started
		closed := make(chan error, 1)
		go func() { closed <- first.close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("nonlast owner closed before its accepted callback: %v", err)
		default:
		}
		first.requestForceClose()
		require.NoError(t, <-closed)
		require.NoError(t, coordinator.operationContext.Err())
		require.NoError(t, (<-secondContext).Err())
		require.NoError(t, second.close())
	})
}

func TestArchiveForcedCoordinatorRejectsNewOwnerUntilCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := compressedlogarchive.NewStore(newArchiveRoot(t, t.TempDir()))
		require.NoError(t, err)
		cfg := NewFileArchiveConfigAdapter(objectstorage.CompressedLogFileUploadFailureArchiveConfig{LifecycleInterval: time.Hour})
		owner, coordinator, err := acquireArchiveLifecycleWithMaintenance(store, cfg, nil, nil, true)
		require.NoError(t, err)
		owner.requestForceClose()
		type result struct {
			owner       *archiveLifecycleOwner
			coordinator *sharedArchiveLifecycle
			err         error
		}
		acquired := make(chan result, 1)
		go func() {
			owner, coordinator, err := acquireArchiveLifecycleWithMaintenance(store, cfg, nil, nil, true)
			acquired <- result{owner, coordinator, err}
		}()
		synctest.Wait()
		select {
		case <-acquired:
			t.Fatal("new owner joined a force-canceled coordinator")
		default:
		}
		require.NoError(t, owner.close())
		replacement := <-acquired
		require.NoError(t, replacement.err)
		require.NotSame(t, coordinator, replacement.coordinator)
		require.NoError(t, replacement.coordinator.operationContext.Err())
		require.NoError(t, replacement.owner.close())
	})
}
