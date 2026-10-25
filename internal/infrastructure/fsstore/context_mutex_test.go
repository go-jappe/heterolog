package fsstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestContextMutexDeadlineDoesNotRetainLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu ContextMutex
		mu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := mu.LockContext(ctx)
		mu.Unlock()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked lock = %v, want deadline exceeded", err)
		}
		// nilは従来どおりBackground相当です。中止した待機がlockを取得し直さないことも確認します。
		if err := mu.LockContext(nil); err != nil {
			t.Fatal(err)
		}
		mu.Unlock()
	})
}

func TestContextMutexAlreadyCanceledDoesNotAcquire(t *testing.T) {
	var mu ContextMutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mu.LockContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("available lock with canceled context = %v", err)
	}
	mu.Lock()
	mu.Unlock()
}

func TestContextMutexUnlockWakesQueuedWaitersInOrderWithoutTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu ContextMutex
		mu.Lock()
		unlockInitial := sync.OnceFunc(mu.Unlock)
		defer unlockInitial()
		acquired := make(chan int, 2)
		releaseFirst := make(chan struct{})
		unblockFirst := sync.OnceFunc(func() { close(releaseFirst) })
		defer unblockFirst()
		go func() {
			mu.Lock()
			defer mu.Unlock()
			acquired <- 1
			<-releaseFirst
		}()
		synctest.Wait() // 一件目が待ち列に入ってから、二件目を開始します。
		go func() {
			mu.Lock()
			defer mu.Unlock()
			acquired <- 2
		}()
		synctest.Wait()
		select {
		case id := <-acquired:
			t.Fatalf("waiter %d acquired a held lock", id)
		default:
		}
		started := time.Now()
		unlockInitial()
		if id := <-acquired; id != 1 {
			t.Fatalf("first acquisition = %d, want earlier waiter 1", id)
		}
		synctest.Wait()
		select {
		case id := <-acquired:
			t.Fatalf("waiter %d acquired before the first waiter unlocked", id)
		default:
		}
		unblockFirst()
		if id := <-acquired; id != 2 {
			t.Fatalf("second acquisition = %d", id)
		}
		if elapsed := time.Since(started); elapsed != 0 {
			t.Fatalf("unlock notifications waited for a timer: %s", elapsed)
		}
	})
}

func TestContextMutexCanceledWaiterDoesNotBlockNextWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu ContextMutex
		mu.Lock()
		unlockInitial := sync.OnceFunc(mu.Unlock)
		defer unlockInitial()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		first := make(chan error, 1)
		go func() { first <- mu.LockContext(ctx) }()
		synctest.Wait()
		second := make(chan struct{})
		go func() {
			mu.Lock()
			mu.Unlock()
			close(second)
		}()
		synctest.Wait()
		cancel()
		if err := <-first; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter = %v", err)
		}
		unlockInitial()
		<-second
	})
}

func TestContextMutexCancellationRacingWithUnlockDoesNotLeakLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 100 {
			var mu ContextMutex
			mu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- mu.LockContext(ctx) }()
			synctest.Wait()
			go cancel()
			mu.Unlock()
			if err := <-result; err == nil {
				// 取得完了とキャンセルの順番によっては取得成功も正しい結果です。
				mu.Unlock()
			} else if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			cancel()
			checkCtx, checkCancel := context.WithTimeout(context.Background(), time.Second)
			err := mu.LockContext(checkCtx)
			checkCancel()
			if err != nil {
				t.Fatalf("lock leaked after cancellation/acquisition race: %v", err)
			}
			mu.Unlock()
		}
	})
}
