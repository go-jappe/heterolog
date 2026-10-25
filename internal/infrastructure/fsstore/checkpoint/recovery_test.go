package checkpoint

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

func advanceSaveRecovery() {
	// Let the worker install its timer before advancing the virtual clock.
	synctest.Wait()
	time.Sleep(SaveRecoveryInterval)
	synctest.Wait()
}

// lockOwnerWithinBubbleは、通知の実行中などに試験のgoroutineから所有者のlockをbubble時間の
// 期限付きで取得します。通知がlockを保持したまま止まる退行でも、bubble全体のdeadlockで
// packageの試験を止めず、この試験の失敗として報告します。
func lockOwnerWithinBubble(t *testing.T, mu *infrafsstore.ContextMutex, description string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), boundedwait.BubbleTimeout)
	defer cancel()
	if err := mu.LockContext(ctx); err != nil {
		t.Fatalf("%s: the owner lock was not released within %s: %v", description, boundedwait.BubbleTimeout, err)
	}
}

// stopSaveRecoveryOnCleanupは、試験の終了時に、releaseで試験側の待機を解除してから
// recovery.Stopをbubble時間の期限付きで実行します。期限を過ぎたらForceCloseで再試行を中止して
// 失敗させます。途中で失敗した試験では先にForceCloseで再試行を中止し、再試行やStopの
// goroutineをbubbleに残しません。
func stopSaveRecoveryOnCleanup(t *testing.T, recovery *SaveRecovery, release ...func()) {
	t.Helper()
	t.Cleanup(func() {
		for _, r := range release {
			r()
		}
		if t.Failed() {
			_ = recovery.ForceClose()
		}
		_ = boundedwait.CloseInBubble(t, "recovery.Stop()", recovery.Stop, recovery.ForceClose)
	})
}

// stopSaveRecoveryInBackgroundは、recovery.Stopを別goroutineで始め、戻りを通知するchannelを返します。
// 試験が途中で失敗しても、cleanupでreleaseによる待機の解除とForceCloseを行ってから、Stopの戻りを
// bubble時間の期限付きで待ち、Stopのgoroutineをbubbleに残しません。
func stopSaveRecoveryInBackground(t *testing.T, recovery *SaveRecovery, release ...func()) <-chan struct{} {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		recovery.Stop()
		close(stopped)
	}()
	t.Cleanup(func() {
		select {
		case <-stopped:
			return
		default:
		}
		for _, r := range release {
			r()
		}
		_ = recovery.ForceClose()
		boundedwait.ReceiveInBubble(t, stopped, "recovery.Stop() after ForceClose")
	})
	return stopped
}

func TestSaveRecoveryStartsOnceAndRetriesLatestState(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		var state int
		var persisted []int
		persistedStates := func() []int {
			mu.Lock()
			defer mu.Unlock()
			return append([]int(nil), persisted...)
		}
		canSave := false
		recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
			persisted = append(persisted, state)
			return canSave, nil
		}, nil)
		stopSaveRecoveryOnCleanup(t, recovery, func() {
			mu.Lock()
			canSave = true
			mu.Unlock()
		})

		const updates = 64
		var callers sync.WaitGroup
		for range updates {
			callers.Add(1)
			go func() {
				defer callers.Done()
				mu.Lock()
				defer mu.Unlock()
				state++
				recovery.StartLocked()
			}()
		}
		callers.Wait()
		synctest.Wait()
		time.Sleep(SaveRecoveryInterval - time.Nanosecond)
		synctest.Wait()
		require.Empty(t, persistedStates(), "the first retry waits for its interval")
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.Equal(t, []int{updates}, persistedStates(), "concurrent failures start only one recovery loop")

		mu.Lock()
		state++
		recovery.StartLocked()
		mu.Unlock()
		advanceSaveRecovery()
		require.Equal(t, []int{updates, updates + 1}, persistedStates(), "each attempt reads the latest state under the owner's lock")

		mu.Lock()
		state++
		canSave = true
		mu.Unlock()
		advanceSaveRecovery()
		require.Equal(t, []int{updates, updates + 1, updates + 2}, persistedStates())
		advanceSaveRecovery()
		require.Len(t, persistedStates(), 3, "successful recovery stops instead of becoming a periodic save")
	})
}

func TestSaveRecoveryKeepsNewFailureAtPreviousRecoveryCompletion(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		state := 1
		var persisted []int
		persistedStates := func() []int {
			mu.Lock()
			defer mu.Unlock()
			return append([]int(nil), persisted...)
		}
		firstAttemptStarted := make(chan struct{})
		finishFirstAttempt := make(chan struct{})
		releaseFirstAttempt := sync.OnceFunc(func() { close(finishFirstAttempt) })
		recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
			persisted = append(persisted, state)
			if len(persisted) == 1 {
				close(firstAttemptStarted)
				<-finishFirstAttempt
			}
			return true, nil
		}, nil)
		stopSaveRecoveryOnCleanup(t, recovery, releaseFirstAttempt)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		synctest.Wait()
		time.Sleep(SaveRecoveryInterval)
		boundedwait.ReceiveInBubble(t, firstAttemptStarted, "receive from firstAttemptStarted")

		// Queue another accepted update while the first save still owns the lock.
		// Ending that recovery must not erase this next registration.
		updated := make(chan struct{})
		go func() {
			mu.Lock()
			state = 2
			recovery.StartLocked()
			mu.Unlock()
			close(updated)
		}()
		synctest.Wait()
		releaseFirstAttempt()
		boundedwait.ReceiveInBubble(t, updated, "receive from updated")
		advanceSaveRecovery()
		require.Equal(t, []int{1, 2}, persistedStates())
		advanceSaveRecovery()
		require.Len(t, persistedStates(), 2)
	})
}

func TestSaveRecoveryFinishingLoopDoesNotClearNextLoop(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		var attemptErrors []error
		oldNotificationStarted := make(chan struct{})
		// 予定より多く通知された場合も、二度目のcloseでprocessを止めないようにし、
		// 通知の回数は最後にnotificationsで確認して試験の失敗として報告します。
		notifyStarted := sync.OnceFunc(func() { close(oldNotificationStarted) })
		var notifications atomic.Int32
		releaseOldNotification := make(chan struct{})
		release := sync.OnceFunc(func() { close(releaseOldNotification) })
		recovery := NewSaveRecovery(&mu, func(ctx context.Context) (bool, error) {
			attemptErrors = append(attemptErrors, ctx.Err())
			if len(attemptErrors) == 1 {
				return true, errors.New("completed save notification")
			}
			return true, nil
		}, func(error) {
			notifications.Add(1)
			notifyStarted()
			<-releaseOldNotification
		})
		stopSaveRecoveryOnCleanup(t, recovery, release)

		// 登録解除後の通知登録だけを止め、旧ループのreturnより先に
		// 次のループが登録される順序を確実に作ります。
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		advanceSaveRecovery()
		boundedwait.ReceiveInBubble(t, oldNotificationStarted, "receive from oldNotificationStarted")

		// 旧ループの通知中もlockは保持されていません。
		lockOwnerWithinBubble(t, &mu, "lock during the old loop's notification")
		previousUnregistered := !recovery.running
		recovery.StartLocked()
		mu.Unlock()
		require.True(t, previousUnregistered)
		synctest.Wait()
		release()
		synctest.Wait()
		mu.Lock()
		nextRegistered := recovery.running
		mu.Unlock()
		require.True(t, nextRegistered, "old cleanup must not clear the next registration")
		advanceSaveRecovery()
		mu.Lock()
		errs := append([]error(nil), attemptErrors...)
		mu.Unlock()
		require.Equal(t, []error{nil, nil}, errs, "both loops must attempt saving with their own live context")
		require.EqualValues(t, 1, notifications.Load(), "only the first save's error is notified, exactly once")
	})
}

func TestSaveRecoveryStopWaitsForDelayedRetriesToSucceed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		attempts := 0
		attemptCount := func() int {
			mu.Lock()
			defer mu.Unlock()
			return attempts
		}
		recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
			attempts++
			return attempts == 2, nil
		}, nil)
		stopSaveRecoveryOnCleanup(t, recovery)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		synctest.Wait()
		stopped := stopSaveRecoveryInBackground(t, recovery)
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("Stop skipped the scheduled save")
		default:
		}
		require.Zero(t, attemptCount())
		advanceSaveRecovery()
		require.Equal(t, 1, attemptCount())
		select {
		case <-stopped:
			t.Fatal("Stop abandoned a failed retry")
		default:
		}
		advanceSaveRecovery()
		boundedwait.ReceiveInBubble(t, stopped, "receive from stopped")
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Stop()", recovery.Stop, recovery.ForceClose))
		time.Sleep(2 * SaveRecoveryInterval)
		synctest.Wait()
		require.Equal(t, 2, attempts)
	})
}

func TestSaveRecoveryStopWhileWaitingForOwnerLockStillSaves(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		attempts := 0
		recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
			attempts++
			return true, nil
		}, nil)
		stopSaveRecoveryOnCleanup(t, recovery)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		synctest.Wait()

		mu.Lock()
		// 途中で失敗しても、cleanupで所有者のlockを解放してからStopの戻りを待ちます。
		unlock := sync.OnceFunc(mu.Unlock)
		time.Sleep(SaveRecoveryInterval)
		synctest.Wait()
		stopped := stopSaveRecoveryInBackground(t, recovery, unlock)
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("Stop skipped the save waiting for its owner lock")
		default:
		}
		unlock()
		boundedwait.ReceiveInBubble(t, stopped, "receive from stopped")
		require.Equal(t, 1, attempts)
	})
}

func TestSaveRecoveryStopWaitsForAcceptedSave(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		attemptStarted := make(chan struct{})
		// 予定より多く試行された場合も、二度目のcloseでprocessを止めず試験の失敗として報告します。
		notifyAttemptStarted := sync.OnceFunc(func() { close(attemptStarted) })
		finishAttempt := make(chan struct{})
		releaseAttempt := sync.OnceFunc(func() { close(finishAttempt) })
		attempts := 0
		var saveContext context.Context
		recovery := NewSaveRecovery(&mu, func(ctx context.Context) (bool, error) {
			saveContext = ctx
			attempts++
			notifyAttemptStarted()
			<-finishAttempt
			return true, nil
		}, nil)
		stopSaveRecoveryOnCleanup(t, recovery, releaseAttempt)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		synctest.Wait()
		time.Sleep(SaveRecoveryInterval)
		boundedwait.ReceiveInBubble(t, attemptStarted, "receive from attemptStarted")
		stopped := stopSaveRecoveryInBackground(t, recovery, releaseAttempt)
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("Stop returned before the accepted save completed")
		default:
		}
		require.NoError(t, saveContext.Err(), "normal shutdown must not cancel accepted persistence")
		_, hasDeadline := saveContext.Deadline()
		require.False(t, hasDeadline)
		releaseAttempt()
		boundedwait.ReceiveInBubble(t, stopped, "receive from stopped")
		time.Sleep(2 * SaveRecoveryInterval)
		synctest.Wait()
		require.Equal(t, 1, attempts)
	})
}

func TestPendingSaveErrorKeepsUnderlyingCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("checkpoint replacement failed")
	err := &PendingSaveError{Err: cause}
	require.ErrorIs(t, err, cause)
	// 保存待ちは内部ログへそのまま通知されるため、原因を含むError()の形式をこの試験で固定します。
	// 呼び出し側の判定はerrors.Is/Asで行い、文言には依存しません。
	require.Equal(t, "checkpoint save pending: "+cause.Error(), err.Error())
}

func TestSaveRecoveryReportsRetryErrorOutsideOwnerLockAndContinues(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		cause := errors.New("retry write failed")
		attempts := 0
		reports := make(chan error, 1)
		recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
			attempts++
			if attempts == 1 {
				return false, cause
			}
			return true, nil
		}, func(err error) {
			// 通知の登録はStoreのlockを保持しません。lockを保持したまま通知する退行でも自己deadlockで
			// bubbleを止めないよう、bubble時間の期限付きでlockを取得し、取得できなければ失敗として記録します。
			lockCtx, cancel := context.WithTimeout(context.Background(), SaveRecoveryInterval/2)
			defer cancel()
			if lockErr := mu.LockContext(lockCtx); lockErr != nil {
				t.Errorf("retry error notification ran while the Store lock was held: %v", lockErr)
				return
			}
			// 予定外の追加通知で止まらないよう、受け取り手がなければ失敗として記録します。
			boundedwait.SendOrReport(t, reports, err, "retry error")
			mu.Unlock()
		})
		stopSaveRecoveryOnCleanup(t, recovery)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		advanceSaveRecovery()
		require.ErrorIs(t, boundedwait.ReceiveInBubble(t, reports, "receive from reports"), cause)
		advanceSaveRecovery()
		advanceSaveRecovery()
		mu.Lock()
		require.Equal(t, 2, attempts)
		require.False(t, recovery.running)
		mu.Unlock()
		require.Empty(t, reports, "successful retries do not report an error")
	})
}

func TestSaveRecoveryNilOnErrorContinuesAfterFailedRetry(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		attempts := 0
		recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
			attempts++
			if attempts == 1 {
				return false, errors.New("retry write failed")
			}
			return true, nil
		}, nil)
		stopSaveRecoveryOnCleanup(t, recovery)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		advanceSaveRecovery()
		advanceSaveRecovery()
		advanceSaveRecovery()
		mu.Lock()
		require.Equal(t, 2, attempts)
		require.False(t, recovery.running)
		mu.Unlock()
	})
}

func TestSaveRecoveryStopWaitsForNotificationRegistration(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu infrafsstore.ContextMutex
		cause := errors.New("retry write failed")
		reports := make(chan error, 1)
		finishRegistration := make(chan struct{})
		releaseRegistration := sync.OnceFunc(func() { close(finishRegistration) })
		attempts := 0
		recovery := NewSaveRecovery(&mu, func(context.Context) (bool, error) {
			attempts++
			if attempts == 2 {
				return true, nil
			}
			return false, cause
		}, func(err error) {
			// 予定外の追加通知で止まらないよう、受け取り手がなければ失敗として記録します。
			boundedwait.SendOrReport(t, reports, err, "retry error")
			<-finishRegistration
		})
		stopSaveRecoveryOnCleanup(t, recovery, releaseRegistration)
		mu.Lock()
		recovery.StartLocked()
		mu.Unlock()
		advanceSaveRecovery()
		require.ErrorIs(t, boundedwait.ReceiveInBubble(t, reports, "receive from reports"), cause)
		stopped := stopSaveRecoveryInBackground(t, recovery, releaseRegistration)
		synctest.Wait()
		select {
		case <-stopped:
			t.Fatal("Stop returned before the retry goroutine finished registering its notification")
		default:
		}
		releaseRegistration()
		boundedwait.ReceiveInBubble(t, stopped, "receive from stopped")
		advanceSaveRecovery()
		require.Equal(t, 2, attempts)
		require.Empty(t, reports, "no new notification is registered after Stop returns")
	})
}
