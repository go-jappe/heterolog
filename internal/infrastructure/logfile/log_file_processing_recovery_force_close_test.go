package logfile

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
)

func TestLogFileProcessingRecoveryForceCloseDiscardsPendingAndStaleWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recovery, err := NewLogFileProcessingRecovery[string](time.Hour)
		require.NoError(t, err)
		attempts, releases := 0, 0
		require.True(t, recovery.Schedule("pending", errors.New("unavailable"), func(context.Context) (bool, error) {
			attempts++
			return false, errors.New("still unavailable")
		}, nil, func() { releases++ }))
		recovery.mu.Lock()
		staleWake := recovery.wake
		recovery.mu.Unlock()
		require.ErrorIs(t, recovery.ForceClose(), shutdown.ErrForcedClose)
		boundedwait.ReceiveInBubble(t, recovery.Done(), "receive from recovery.Done()")
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose), shutdown.ErrForcedClose)
		// Stopより先に実行待ちへ入っていた旧timerも再開できません。
		recovery.runRound(staleWake)
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		require.Zero(t, attempts)
		require.Equal(t, 1, releases)
		require.False(t, recovery.Has("pending"))
		require.False(t, recovery.Schedule("new", nil, func(context.Context) (bool, error) {
			t.Fatal("accepted an operation after ForceClose")
			return true, nil
		}, nil, nil))
		require.ErrorIs(t, recovery.ForceClose(), shutdown.ErrForcedClose)
	})
}

func TestLogFileProcessingRecoveryForceCloseRetainsProtectionUntilAcceptedIOReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recovery LogFileProcessingRecovery[string]
		entered := make(chan context.Context, 1)
		finishIO := make(chan struct{})
		// 途中で失敗しても、受理済みのI/Oを解除してから終了処理を待ちます。
		releaseIO := sync.OnceFunc(func() { close(finishIO) })
		closeRecoveryOnCleanup(t, &recovery, releaseIO)
		releases, queuedAttempts := 0, 0
		require.True(t, recovery.Schedule("active", nil, func(ctx context.Context) (bool, error) {
			// 予定外の再試行で呼ばれても、最初の通知だけを受け取ります。
			select {
			case entered <- ctx:
			default:
			}
			<-finishIO
			return false, errors.New("unfinished I/O")
		}, nil, func() { releases++ }))
		ctx := boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		require.True(t, recovery.Schedule("queued", nil, func(context.Context) (bool, error) {
			queuedAttempts++
			return true, nil
		}, nil, func() { releases++ }))
		closed := make(chan error, 8)
		for range cap(closed) {
			go func() { closed <- recovery.Close() }()
		}
		synctest.Wait()
		require.ErrorIs(t, recovery.ForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		synctest.Wait()
		require.Empty(t, closed, "primitive Close must protect dependencies until accepted I/O returns")
		select {
		case <-recovery.Done():
			t.Fatal("Done closed while I/O was still running")
		default:
		}
		require.True(t, recovery.Has("active"))
		require.True(t, recovery.Has("queued"))
		require.Zero(t, releases)
		releaseIO()
		for range cap(closed) {
			require.ErrorIs(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"), shutdown.ErrForcedClose)
		}
		boundedwait.ReceiveInBubble(t, recovery.Done(), "receive from recovery.Done()")
		require.Equal(t, 2, releases)
		require.Zero(t, queuedAttempts)
	})
}

func TestLogFileProcessingRecoveryForceCloseCancelsOwnerLockWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recovery LogFileProcessingRecovery[string]
		var ownerMu infrafsstore.ContextMutex
		ownerMu.Lock()
		defer ownerMu.Unlock()
		entered := make(chan struct{})
		releases := 0
		require.True(t, recovery.Schedule("locked", nil, func(ctx context.Context) (bool, error) {
			close(entered)
			if err := ownerMu.LockContext(ctx); err != nil {
				return false, err
			}
			ownerMu.Unlock()
			t.Error("operation acquired owner lock after force")
			return true, nil
		}, nil, func() { releases++ }))
		boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		require.ErrorIs(t, recovery.ForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "recovery.Close()", recovery.Close, recovery.ForceClose), shutdown.ErrForcedClose)
		require.Equal(t, 1, releases)
	})
}

func TestLogFileProcessingRecoveryForceCloseWaitsForProtectionRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var recovery LogFileProcessingRecovery[string]
		releaseStarted, finishRelease := make(chan struct{}), make(chan struct{})
		// 途中で失敗しても、保護の解放を許可してから終了処理を待ちます。
		notifyReleaseStarted := sync.OnceFunc(func() { close(releaseStarted) })
		allowRelease := sync.OnceFunc(func() { close(finishRelease) })
		closeRecoveryOnCleanup(t, &recovery, allowRelease)
		require.True(t, recovery.Schedule("release", nil, func(context.Context) (bool, error) {
			return true, nil
		}, nil, func() {
			notifyReleaseStarted()
			<-finishRelease
		}))
		boundedwait.ReceiveInBubble(t, releaseStarted, "receive from releaseStarted")
		require.ErrorIs(t, recovery.ForceClose(), shutdown.ErrForcedClose)
		closed := make(chan error, 1)
		go func() { closed <- recovery.Close() }()
		synctest.Wait()
		require.Empty(t, closed)
		allowRelease()
		require.ErrorIs(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"), shutdown.ErrForcedClose)
		boundedwait.ReceiveInBubble(t, recovery.Done(), "receive from recovery.Done()")
	})
}
