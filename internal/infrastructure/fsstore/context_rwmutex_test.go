package fsstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestContextRWMutexPreservesSharedAccessAndPendingWriterOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu ContextRWMutex
		for range 2 {
			if err := mu.RLockContext(nil); err != nil {
				t.Fatal(err)
			}
		}
		writerAcquired := make(chan struct{})
		releaseWriter := make(chan struct{})
		unblockWriter := sync.OnceFunc(func() { close(releaseWriter) })
		defer unblockWriter()
		go func() {
			mu.Lock()
			defer mu.Unlock()
			close(writerAcquired)
			<-releaseWriter
		}()
		synctest.Wait()
		readerAcquired := make(chan error, 1)
		go func() {
			err := mu.RLockContext(context.Background())
			if err == nil {
				mu.RUnlock()
			}
			readerAcquired <- err
		}()
		synctest.Wait()
		mu.RUnlock()
		synctest.Wait()
		select {
		case <-writerAcquired:
			t.Fatal("writer acquired while a reader still held the lock")
		default:
		}
		select {
		case err := <-readerAcquired:
			t.Fatalf("later reader overtook the queued writer: %v", err)
		default:
		}
		started := time.Now()
		mu.RUnlock()
		<-writerAcquired
		synctest.Wait()
		select {
		case err := <-readerAcquired:
			t.Fatalf("reader acquired while writer held the lock: %v", err)
		default:
		}
		unblockWriter()
		if err := <-readerAcquired; err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed != 0 {
			t.Fatalf("unlock notifications waited for a timer: %s", elapsed)
		}
	})
}

func TestContextRWMutexCanceledReaderDoesNotRetainSharedLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu ContextRWMutex
		mu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := mu.RLockContext(ctx)
		mu.Unlock()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked reader = %v, want deadline exceeded", err)
		}
		if err := mu.RLockContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		mu.RUnlock()
		mu.Lock()
		mu.Unlock()
	})
}

func TestContextRWMutexAlreadyCanceledReaderDoesNotAcquire(t *testing.T) {
	var mu ContextRWMutex
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mu.RLockContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("available shared lock with canceled context = %v", err)
	}
	mu.Lock()
	mu.Unlock()
}

func TestContextRWMutexCanceledWriterUnblocksQueuedReaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu ContextRWMutex
		if err := mu.RLockContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer mu.RUnlock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		writer := make(chan error, 1)
		go func() { writer <- mu.LockContext(ctx) }()
		synctest.Wait()
		reader := make(chan error, 1)
		go func() {
			err := mu.RLockContext(context.Background())
			if err == nil {
				mu.RUnlock()
			}
			reader <- err
		}()
		synctest.Wait()
		select {
		case err := <-reader:
			t.Fatalf("reader overtook waiting writer: %v", err)
		default:
		}
		cancel()
		if err := <-writer; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting writer = %v", err)
		}
		// 最初のreaderを解放しなくても、取消済みwriterの後ろのreaderは進めます。
		if err := <-reader; err != nil {
			t.Fatal(err)
		}
	})
}

func TestContextRWMutexCanceledWriterDoesNotAcquireOrLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu ContextRWMutex
		mu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := mu.LockContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked writer = %v", err)
		}
		mu.Unlock()
		// 空いているlockでも、期限切れcontextでは取得しません。
		if err := mu.LockContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired writer = %v", err)
		}
		mu.Lock()
		mu.Unlock()
	})
}

func TestContextRWMutexWriterCancellationRacingWithUnlockDoesNotLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 100 {
			var mu ContextRWMutex
			mu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- mu.LockContext(ctx) }()
			synctest.Wait()
			go cancel()
			mu.Unlock()
			if err := <-result; err == nil {
				mu.Unlock()
			} else if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			cancel()
			mu.Lock()
			mu.Unlock()
		}
	})
}

func TestContextRWMutexCancellationRacingWithUnlockDoesNotLeakSharedLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 100 {
			var mu ContextRWMutex
			mu.Lock()
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- mu.RLockContext(ctx) }()
			synctest.Wait()
			go cancel()
			mu.Unlock()
			if err := <-result; err == nil {
				mu.RUnlock()
			} else if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			cancel()
			// 共有枠が一つでも残れば、次の排他取得は完了できません。
			mu.Lock()
			mu.Unlock()
		}
	})
}
