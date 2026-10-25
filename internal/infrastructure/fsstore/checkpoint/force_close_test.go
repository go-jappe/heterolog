package checkpoint

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestSaveRecoveryForceCloseCancelsPendingAndOwnerLockWait(t *testing.T) {
	for _, awaitingLock := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry timer", true: "owner lock"}[awaitingLock], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var mu infrafsstore.ContextMutex
				attempts := 0
				recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
					attempts++
					return false, errors.New("unavailable")
				}, nil)
				mu.Lock()
				// 途中で失敗しても、試験が保持するStoreのlockを解放してから終了処理を待ちます。
				unlock := sync.OnceFunc(mu.Unlock)
				stopSaveRecoveryOnCleanup(t, recovery, unlock)
				recovery.StartLocked()
				synctest.Wait()
				if awaitingLock {
					time.Sleep(SaveRecoveryInterval)
					synctest.Wait()
				}
				stopped := make(chan error, 1)
				go func() { stopped <- recovery.Stop() }()
				synctest.Wait()
				require.Empty(t, stopped)
				require.ErrorIs(t, recovery.ForceClose(), shutdown.ErrForcedClose)
				// 強制終了はStoreのlockが別処理に使われていても進められます。
				require.ErrorIs(t, boundedwait.ReceiveInBubble(t, stopped, "receive from stopped"), shutdown.ErrForcedClose)
				boundedwait.ReceiveInBubble(t, recovery.Done(), "receive from recovery.Done()")
				recovery.StartLocked()
				unlock()
				time.Sleep(2 * SaveRecoveryInterval)
				synctest.Wait()
				require.Zero(t, attempts)
			})
		})
	}
}

func TestSaveRecoveryForceCloseWaitsForAcceptedIOAndDoesNotRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		entered := make(chan context.Context, 1)
		finishIO := make(chan struct{})
		// 途中で失敗しても、受理済みのI/Oを解除してから終了処理を待ちます。
		releaseIO := sync.OnceFunc(func() { close(finishIO) })
		reported := make(chan error, 1)
		cause := errors.New("I/O interrupted")
		attempts := 0
		recovery := NewSaveRecovery(&mu, func(ctx context.Context) (bool, error) {
			attempts++
			// 予定外の再試行や通知で止まらないよう、受け取り手がなければ失敗として記録します。
			boundedwait.SendOrReport(t, entered, ctx, "attempt")
			<-finishIO
			return false, cause
		}, func(err error) { boundedwait.SendOrReport(t, reported, err, "retry error") })
		stopSaveRecoveryOnCleanup(t, recovery, releaseIO)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		advanceSaveRecovery()
		ctx := boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		stopped := make(chan error, 8)
		for range cap(stopped) {
			go func() { stopped <- recovery.Stop() }()
		}
		synctest.Wait()
		require.ErrorIs(t, recovery.ForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		synctest.Wait()
		require.Empty(t, stopped)
		select {
		case <-recovery.Done():
			t.Fatal("Done closed before accepted I/O returned")
		default:
		}
		releaseIO()
		for range cap(stopped) {
			require.ErrorIs(t, boundedwait.ReceiveInBubble(t, stopped, "receive from stopped"), shutdown.ErrForcedClose)
		}
		boundedwait.ReceiveInBubble(t, recovery.Done(), "receive from recovery.Done()")
		require.ErrorIs(t, boundedwait.ReceiveInBubble(t, reported, "receive from reported"), cause)
		time.Sleep(2 * SaveRecoveryInterval)
		synctest.Wait()
		require.Equal(t, 1, attempts)
	})
}

func TestSaveRecoveryForceCloseWaitsForOldNotificationAndCancelsNextLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		notificationStarted, finishNotification := make(chan struct{}), make(chan struct{})
		// 予定外の通知で二度目のcloseによるpanicを起こさず、途中で失敗しても通知を解除してから終了処理を待ちます。
		notifyStarted := sync.OnceFunc(func() { close(notificationStarted) })
		releaseNotification := sync.OnceFunc(func() { close(finishNotification) })
		attempts := 0
		recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
			attempts++
			return true, errors.New("completed with notification")
		}, func(error) {
			notifyStarted()
			<-finishNotification
		})
		stopSaveRecoveryOnCleanup(t, recovery, releaseNotification)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		advanceSaveRecovery()
		boundedwait.ReceiveInBubble(t, notificationStarted, "receive from notificationStarted")
		// 旧ループの通知中もlockは保持されていません。
		lockOwnerWithinBubble(t, &mu, "lock during the old loop's notification")
		recovery.StartLocked()
		mu.Unlock()
		require.ErrorIs(t, recovery.ForceClose(), shutdown.ErrForcedClose)
		stopped := make(chan error, 1)
		go func() { stopped <- recovery.Stop() }()
		synctest.Wait()
		require.Empty(t, stopped, "old notification registration still owns the worker")
		releaseNotification()
		require.ErrorIs(t, boundedwait.ReceiveInBubble(t, stopped, "receive from stopped"), shutdown.ErrForcedClose)
		time.Sleep(2 * SaveRecoveryInterval)
		synctest.Wait()
		require.Equal(t, 1, attempts)
	})
}

func TestFilesystemStoreForceClosePreservesUnsavedStateAndCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newCheckpointTestStore(t, "force-pending-save")
		blockCheckpointReplacement(t, store)
		key := CheckpointKey{Generation: "20261007-1"}
		_, err := store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{"archive"})
		pending := pendingSaveForTest(t, err)
		store.stateOrSaveMu.Lock()
		require.ErrorIs(t, store.ForceClose(), shutdown.ErrForcedClose)
		store.stateOrSaveMu.Unlock()
		closeErr := boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose)
		require.ErrorIs(t, closeErr, shutdown.ErrForcedClose)
		require.ErrorIs(t, closeErr, pending.Err)
		boundedwait.ReceiveInBubble(t, store.Done(), "receive from store.Done()")
		require.Equal(t, []DestinationCursor{{DestinationID: "archive", Generation: key.Generation}}, checkpointStateForTest(store).Cursors)
		require.ErrorIs(t, store.ForceClose(), shutdown.ErrForcedClose)
	})
}
