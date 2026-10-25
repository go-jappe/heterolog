package logfile

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
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

// closeRecoveryOnCleanupは、試験の終了時にreleaseで試験側の待機を解除してから、recoveryを
// bubble時間の期限付きで閉じ、通知の完了も期限付きで待ちます。途中で失敗した試験では先に
// ForceCloseで受理済みの再試行を中止し、操作・通知のgoroutineをbubbleに残しません。
func closeRecoveryOnCleanup[K comparable](t *testing.T, recovery *LogFileProcessingRecovery[K], release ...func()) {
	t.Helper()
	t.Cleanup(func() {
		for _, r := range release {
			r()
		}
		if t.Failed() {
			_ = recovery.ForceClose()
		}
		_ = boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose)
		boundedwait.ReceiveInBubble(t, recovery.NotificationsDone(), "recovery.NotificationsDone()")
	})
}

func TestLogFileProcessingRecoveryDistinguishesStructuredKeys(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type recoveryKey struct {
			generation  string
			compressed  bool
			preparation bool
		}
		recovery, err := NewLogFileProcessingRecovery[recoveryKey](time.Second)
		require.NoError(t, err)
		keys := []recoveryKey{
			{generation: "20260915-3"},
			{generation: "20260915-3", compressed: true},
			{generation: "prepare", compressed: true},
			{preparation: true},
		}
		attempted, released := 0, 0
		for _, key := range keys {
			require.True(t, recovery.Schedule(key, errors.New("retry later"),
				func(context.Context) (bool, error) { attempted++; return true, nil }, nil,
				func() { released++ }))
			require.True(t, recovery.Has(key))
			require.False(t, recovery.Schedule(key, nil,
				func(context.Context) (bool, error) { return true, nil }, nil, nil))
		}
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.Equal(t, len(keys), attempted)
		require.Equal(t, len(keys), released)
	})
}

func TestLogFileProcessingRecoveryRetriesWithoutFurtherOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		closeRecoveryOnCleanup(t, recovery)
		attempts, releases := 0, 0
		finished := make(chan struct{})
		require.True(t, recovery.Schedule("20260923-1", errors.New("save failed"), func(context.Context) (bool, error) {
			attempts++
			if attempts < 3 {
				return false, errors.New("temporary checkpoint save failure")
			}
			return true, nil
		}, func(err error) func() {
			return func() {
				if err == nil {
					close(finished)
				}
			}
		}, func() { releases++ }))
		boundedwait.ReceiveInBubble(t, finished, "receive from finished")
		require.False(t, recovery.Has("20260923-1"))
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.Equal(t, 3, attempts)
		require.Equal(t, 1, releases)
	})
}

func TestLogFileProcessingRecoveryCloseWaitsForLockWithoutCanceling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		var lock infrafsstore.ContextMutex
		lock.Lock()
		unlock := sync.OnceFunc(lock.Unlock)
		closeRecoveryOnCleanup(t, recovery, unlock)
		entered := make(chan context.Context, 1)
		releases := 0
		require.True(t, recovery.Schedule("locked", nil, func(ctx context.Context) (bool, error) {
			// 再試行で再び呼ばれても、最初の通知だけを受け取ります。
			select {
			case entered <- ctx:
			default:
			}
			if err := lock.LockContext(ctx); err != nil {
				return false, err
			}
			lock.Unlock()
			return true, nil
		}, nil, func() { releases++ }))
		operationContext := boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		closed := make(chan error, 1)
		go func() { closed <- recovery.Close() }()
		synctest.Wait()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.Empty(t, closed, "Close must wait for the accepted operation")
		require.NoError(t, operationContext.Err())
		require.True(t, recovery.Has("locked"))
		require.Zero(t, releases, "protection must remain until completion")
		require.False(t, recovery.Schedule("late", nil, func(context.Context) (bool, error) {
			t.Error("work accepted after drain started")
			return true, nil
		}, nil, nil))
		unlock()
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		require.NoError(t, operationContext.Err())
		require.Equal(t, 1, releases)
		require.False(t, recovery.Has("locked"))
	})
}

func TestLogFileProcessingRecoveryNotificationCanCloseRecoveryWithPendingRetries(t *testing.T) {
	// 部品自身のCloseは操作だけを待ちます。上位ownerの終了では、これに加えて
	// NotificationsDoneも待つため、通知内から上位ownerを同期終了させる試験ではありません。
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		closeRecoveryOnCleanup(t, recovery)
		closed := make(chan error, 1)
		attempts, queuedAttempts, releases := 0, 0, 0
		require.True(t, recovery.Schedule("notify", nil, func(context.Context) (bool, error) {
			attempts++
			if attempts == 1 {
				return false, errors.New("save failed")
			}
			return true, nil
		}, func(err error) func() {
			if err != nil {
				return func() { boundedwait.SendOrReport(t, closed, recovery.Close(), "notification") }
			}
			return nil
		}, func() { releases++ }))
		require.True(t, recovery.Schedule("queued", errors.New("save failed"), func(context.Context) (bool, error) {
			queuedAttempts++
			return true, nil
		}, nil, func() { releases++ }))
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"), "notification must allow the remaining rounds to finish")
		require.Equal(t, 2, attempts)
		require.Equal(t, 1, queuedAttempts)
		require.Equal(t, 2, releases)
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
	})
}

func TestLogFileProcessingRecoveryRejectsDuplicatesAndClosedRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		attempts, releases := 0, 0
		attempt := func(context.Context) (bool, error) { attempts++; return true, nil }
		release := func() { releases++ }
		require.False(t, recovery.Schedule("nil", nil, nil, nil, release))
		require.True(t, recovery.Schedule("key", errors.New("save failed"), attempt, nil, release))
		require.False(t, recovery.Schedule("key", nil, attempt, nil, release))
		require.True(t, recovery.Has("key"))
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.False(t, recovery.Schedule("other", nil, attempt, nil, release))
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.Equal(t, 1, attempts, "accepted queued work must execute during Close")
		require.Equal(t, 1, releases, "rejected callbacks belong to the caller")
	})
}

func TestLogFileProcessingRecoveryCloseWaitsForAcceptedAndQueuedAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		entered, finish := make(chan struct{}), make(chan struct{})
		// 再試行で再び呼ばれても、二度目のcloseでprocessを止めません。
		notifyEntered := sync.OnceFunc(func() { close(entered) })
		releaseFinish := sync.OnceFunc(func() { close(finish) })
		closeRecoveryOnCleanup(t, recovery, releaseFinish)
		releases, laterAttempts := 0, 0
		require.True(t, recovery.Schedule("first", nil, func(context.Context) (bool, error) {
			notifyEntered()
			<-finish
			return true, nil
		}, nil, func() { releases++ }))
		boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		// 操作中も登録のmutexは保持されていません。
		require.True(t, recovery.Schedule("second", errors.New("save failed"), func(context.Context) (bool, error) {
			laterAttempts++
			return true, nil
		}, nil, func() { releases++ }))
		closed := make(chan error, 1)
		go func() { closed <- recovery.Close() }()
		synctest.Wait()
		require.Empty(t, closed)
		require.True(t, recovery.Has("first"))
		require.True(t, recovery.Has("second"))
		require.Zero(t, releases)
		releaseFinish()
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		require.Equal(t, 1, laterAttempts)
		require.Equal(t, 2, releases)
	})
}

func TestLogFileProcessingRecoveryCloseKeepsRetryingUntilFailureIsRepaired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		closeRecoveryOnCleanup(t, recovery)
		var available atomic.Bool
		attempts, releases := 0, 0
		require.True(t, recovery.Schedule("unavailable", errors.New("save failed"), func(ctx context.Context) (bool, error) {
			attempts++
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if !available.Load() {
				return false, errors.New("checkpoint remains unavailable")
			}
			return true, nil
		}, nil, func() { releases++ }))
		closed := make(chan error, 1)
		go func() { closed <- recovery.Close() }()
		synctest.Wait()
		time.Sleep(6 * time.Second)
		synctest.Wait()
		require.Empty(t, closed, "failure must keep Close waiting without discarding accepted work")
		require.Equal(t, 6, attempts)
		require.Zero(t, releases)
		require.True(t, recovery.Has("unavailable"))
		available.Store(true)
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		require.Equal(t, 7, attempts)
		require.Equal(t, 1, releases)
		require.False(t, recovery.Has("unavailable"))
	})
}

func TestLogFileProcessingRecoveryNotificationCanJoinConcurrentRecoveryClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		allowNotification, repair := make(chan struct{}), make(chan struct{})
		allow := sync.OnceFunc(func() { close(allowNotification) })
		repaired := sync.OnceFunc(func() { close(repair) })
		closeRecoveryOnCleanup(t, recovery, allow, repaired)
		closed := make(chan error, 2)
		attempts := 0
		require.True(t, recovery.Schedule("notify", nil, func(context.Context) (bool, error) {
			attempts++
			if attempts == 1 {
				return false, errors.New("not repaired yet")
			}
			<-repair
			return true, nil
		}, func(err error) func() {
			if err == nil {
				return nil
			}
			return func() {
				<-allowNotification
				boundedwait.SendOrReport(t, closed, recovery.Close(), "notification")
			}
		}, nil))
		synctest.Wait()
		go func() { boundedwait.SendOrReport(t, closed, recovery.Close(), "notification") }()
		synctest.Wait()
		allow()
		time.Sleep(time.Second)
		synctest.Wait()
		require.Empty(t, closed)
		require.Equal(t, 2, attempts)
		repaired()
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
	})
}

func TestLogFileProcessingRecoveryPanicDuringCloseDoesNotLosePendingOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		closeRecoveryOnCleanup(t, recovery)
		calls, releases := 0, 0
		notified := make(chan error, 2)
		require.True(t, recovery.Schedule("panic", errors.New("save failed"), func(context.Context) (bool, error) {
			calls++
			if calls == 1 {
				panic("temporary failure")
			}
			return true, nil
		}, func(err error) func() { return func() { boundedwait.SendOrReport(t, notified, err, "notification") } }, func() { releases++ }))
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		synctest.Wait()
		var panicErr *LogFileProcessingRecoveryPanicError
		require.ErrorAs(t, boundedwait.ReceiveInBubble(t, notified, "receive from notified"), &panicErr)
		require.ErrorIs(t, panicErr, ErrLogFileProcessingRecoveryPanicked)
		require.Equal(t, "temporary failure", panicErr.Value)
		require.NoError(t, boundedwait.ReceiveInBubble(t, notified, "receive from notified"))
		require.Equal(t, 2, calls)
		require.Equal(t, 1, releases)
	})
}

func TestNewLogFileProcessingRecoveryRejectsInvalidInterval(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		recovery, err := NewLogFileProcessingRecovery[string](interval)
		require.Error(t, err)
		require.Nil(t, recovery)
	}
}

func TestLogFileProcessingRecoveryFailureDoesNotBlockOtherPendingOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		failedAttempts, successfulAttempts := 0, 0
		fixed := false
		require.True(t, recovery.Schedule("failing", errors.New("save failed"), func(context.Context) (bool, error) {
			failedAttempts++
			if fixed {
				return true, nil
			}
			return false, errors.New("checkpoint remains unavailable")
		}, nil, nil))
		require.True(t, recovery.Schedule("ready", errors.New("save failed"), func(context.Context) (bool, error) {
			successfulAttempts++
			return true, nil
		}, nil, nil))
		time.Sleep(time.Second)
		synctest.Wait()
		require.True(t, recovery.Has("failing"))
		require.False(t, recovery.Has("ready"))
		require.Equal(t, 1, failedAttempts)
		require.Equal(t, 1, successfulAttempts)
		fixed = true
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.Equal(t, 2, failedAttempts)
	})
}

func TestLogFileProcessingRecoveryNotificationReceivesLiveOperationContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		closeRecoveryOnCleanup(t, recovery)
		var operationContext context.Context
		notified := make(chan error, 1)
		require.True(t, recovery.Schedule("notify-context", nil, func(ctx context.Context) (bool, error) {
			operationContext = ctx
			return true, nil
		}, func(error) func() {
			ctx := operationContext
			return func() {
				boundedwait.SendOrReport(t, notified, errors.Join(recovery.Close(), ctx.Err()), "notification")
			}
		}, nil))
		require.NoError(t, boundedwait.ReceiveInBubble(t, notified, "receive from notified"), "notification must retain a live context after Close")
	})
}

func TestLogFileProcessingRecoveryImmediateWorkPreemptsRetryTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Hour)
		require.NoError(t, err)
		var ordinaryAttempts atomic.Int32
		require.True(t, recovery.Schedule("ordinary", errors.New("remove failed"), func(context.Context) (bool, error) {
			ordinaryAttempts.Add(1)
			return true, nil
		}, nil, nil))
		finished := make(chan struct{})
		require.True(t, recovery.Schedule("ready", nil, func(context.Context) (bool, error) {
			return true, nil
		}, func(error) func() { return func() { close(finished) } }, nil))
		boundedwait.ReceiveInBubble(t, finished, "receive from finished")
		require.Zero(t, ordinaryAttempts.Load(), "ready work must preempt the unrelated retry timer")
		time.Sleep(time.Hour)
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.EqualValues(t, 1, ordinaryAttempts.Load())
	})
}

func TestLogFileProcessingRecoveryRegistrationDuringRoundIsNotLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		entered, releaseFirst := make(chan struct{}), make(chan struct{})
		// 予定外の再試行で二度目のcloseによるpanicを繰り返さないようにし、試行の回数は最後に
		// runningAttempts・newAttemptsで確認して試験の失敗として報告します。
		notifyEntered := sync.OnceFunc(func() { close(entered) })
		// 途中で失敗しても、実行中の試行を解放してから終了処理を待ちます。
		releaseRunning := sync.OnceFunc(func() { close(releaseFirst) })
		closeRecoveryOnCleanup(t, recovery, releaseRunning)
		executing, overlaps := false, false
		runningAttempts, newAttempts := 0, 0
		require.True(t, recovery.Schedule("running", nil, func(context.Context) (bool, error) {
			runningAttempts++
			executing = true
			notifyEntered()
			<-releaseFirst
			executing = false
			return true, nil
		}, nil, nil))
		boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		require.True(t, recovery.Schedule("new", nil, func(context.Context) (bool, error) {
			newAttempts++
			overlaps = executing
			return true, nil
		}, nil, nil))
		releaseRunning()
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.False(t, overlaps, "attempts must remain serialized")
		require.False(t, recovery.Has("new"))
		// 完了した処理は再び実行しません。
		require.Equal(t, 1, runningAttempts, "a completed operation must not run again")
		require.Equal(t, 1, newAttempts, "the operation registered during the round runs once")
	})
}

func TestLogFileProcessingRecoveryClosePreservesNotificationSnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		allowNotifications := make(chan struct{})
		allow := sync.OnceFunc(func() { close(allowNotifications) })
		closeRecoveryOnCleanup(t, recovery, allow)
		notified := make(chan int, 3)
		attemptState := 0
		require.True(t, recovery.Schedule("snapshots", nil, func(context.Context) (bool, error) {
			attemptState++
			return attemptState == 3, nil
		}, func(error) func() {
			state := attemptState
			return func() {
				<-allowNotifications
				boundedwait.SendOrReport(t, notified, state, "notification")
			}
		}, nil))
		// Every notification remains blocked while all accepted retries drain.
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.Equal(t, 3, attemptState)
		require.Empty(t, notified)
		notificationsDone := recovery.NotificationsDone()
		select {
		case <-notificationsDone:
			t.Fatal("notification completion must wait for accepted callbacks")
		default:
		}
		allow()
		synctest.Wait()
		got := make([]int, 0, 3)
		for range 3 {
			got = append(got, boundedwait.ReceiveInBubble(t, notified, "receive from notified"))
		}
		require.Equal(t, []int{1, 2, 3}, got)
		boundedwait.ReceiveInBubble(t, notificationsDone, "receive from notificationsDone")
	})
}

func TestLogFileProcessingRecoveryClosesDownstreamNotificationsAfterDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		closeRecoveryOnCleanup(t, recovery)
		downstream := notification.NewDispatcher()
		// 途中で失敗しても下流の受付を閉じ、配送のgoroutineを残しません。
		t.Cleanup(func() {
			downstream.CloseAfterDrain()
			boundedwait.ReceiveInBubble(t, downstream.Done(), "downstream.Done()")
		})
		delivered := make(chan int, 3)
		attempts := 0
		require.True(t, recovery.Schedule("reports", nil, func(context.Context) (bool, error) {
			attempts++
			return attempts == 3, nil
		}, func(error) func() {
			attempt := attempts
			return func() {
				if attempt == 1 {
					// 部品の操作を終了し、全通知が下流へ渡り終わってから下流の受付を閉じます。
					// 通知のgoroutineで実行するため、ここでは試験を失敗させるhelperを使いません。
					_ = recovery.Close()
					go func() {
						<-recovery.NotificationsDone()
						downstream.CloseAfterDrain()
					}()
				}
				downstream.Enqueue(func() { boundedwait.SendOrReport(t, delivered, attempt, "notification") })
			}
		}, nil))
		boundedwait.ReceiveInBubble(t, downstream.Done(), "receive from downstream.Done()")
		got := make([]int, 0, 3)
		for range 3 {
			got = append(got, boundedwait.ReceiveInBubble(t, delivered, "receive from delivered"))
		}
		require.Equal(t, []int{1, 2, 3}, got)
	})
}

func TestLogFileProcessingRecoveryConcurrentCloseWaitsForRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		releaseStarted, allowRelease := make(chan struct{}), make(chan struct{})
		// 途中で失敗しても、保護の解放を許可してから終了処理を待ちます。
		notifyReleaseStarted := sync.OnceFunc(func() { close(releaseStarted) })
		permitRelease := sync.OnceFunc(func() { close(allowRelease) })
		closeRecoveryOnCleanup(t, recovery, permitRelease)
		releases := 0
		require.True(t, recovery.Schedule("release", nil, func(context.Context) (bool, error) {
			return true, nil
		}, nil, func() {
			notifyReleaseStarted()
			<-allowRelease
			releases++
		}))
		boundedwait.ReceiveInBubble(t, releaseStarted, "receive from releaseStarted")
		closed := make(chan error, 8)
		for range cap(closed) {
			go func() { closed <- recovery.Close() }()
		}
		synctest.Wait()
		require.Empty(t, closed)
		require.True(t, recovery.Has("release"))
		permitRelease()
		for range cap(closed) {
			require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		}
		require.Equal(t, 1, releases)
		require.False(t, recovery.Has("release"))
	})
}

func TestLogFileProcessingRecoveryCloseReportsReleaseError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Second)
		require.NoError(t, err)
		closeRecoveryOnCleanup(t, recovery)
		notified := make(chan error, 1)
		releases := 0
		require.True(t, recovery.Schedule("release", errors.New("retry"), func(context.Context) (bool, error) {
			return true, nil
		}, func(err error) func() { return func() { boundedwait.SendOrReport(t, notified, err, "notification") } }, func() {
			releases++
			panic("release failed")
		}))
		closed := make(chan error, 2)
		for range cap(closed) {
			go func() { closed <- recovery.Close() }()
		}
		first := boundedwait.ReceiveInBubble(t, closed, "receive from closed")
		var releasePanic *LogFileProcessingRecoveryPanicError
		require.ErrorAs(t, first, &releasePanic)
		require.ErrorIs(t, releasePanic, ErrLogFileProcessingRecoveryReleasePanicked)
		require.Equal(t, "release failed", releasePanic.Value)
		require.Same(t, first, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		require.Same(t, first, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		var notifiedPanic *LogFileProcessingRecoveryPanicError
		require.ErrorAs(t, boundedwait.ReceiveInBubble(t, notified, "receive from notified"), &notifiedPanic)
		require.ErrorIs(t, notifiedPanic, ErrLogFileProcessingRecoveryReleasePanicked)
		require.Equal(t, "release failed", notifiedPanic.Value)
		require.Equal(t, 1, releases)
	})
}

func TestLogFileProcessingRecoveryNotificationPanicsDoNotStopDrain(t *testing.T) {
	for _, snapshotPanic := range []bool{false, true} {
		name := "callback"
		if snapshotPanic {
			name = "snapshot"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				recovery, err := NewLogFileProcessingRecovery[string](time.Second)
				require.NoError(t, err)
				attempts := 0
				require.True(t, recovery.Schedule("notification-panic", nil, func(context.Context) (bool, error) {
					attempts++
					return attempts == 3, nil
				}, func(error) func() {
					if snapshotPanic {
						panic("snapshot failed")
					}
					return func() { panic("notification failed") }
				}, nil))
				require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
				require.Equal(t, 3, attempts)
			})
		})
	}
}

func TestLogFileProcessingRecoveryZeroValueDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recovery LogFileProcessingRecovery[string]
		attempts := 0
		require.True(t, recovery.Schedule("zero", errors.New("retry"), func(context.Context) (bool, error) {
			attempts++
			return true, nil
		}, nil, nil))
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		require.Equal(t, 1, attempts)
		require.NoError(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose))
		var unused LogFileProcessingRecovery[string]
		require.NoError(t, boundedwait.CloseInBubble(t, "unused.Close()", unused.Close, unused.ForceClose))
		require.NoError(t, boundedwait.CloseInBubble(t, "unused.Close()", unused.Close, unused.ForceClose))
		boundedwait.ReceiveInBubble(t, unused.NotificationsDone(), "receive from unused.NotificationsDone()")
	})
}
