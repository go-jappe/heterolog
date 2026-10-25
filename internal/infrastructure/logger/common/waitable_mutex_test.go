package common

import (
	"testing"
	"testing/synctest"
	"time"
)

// WaitableMutexはsync.Mutexと同じく一つのgoroutineだけに保持され、取得を待つgoroutineは
// bubble内で停止状態として扱われます。
func TestWaitableMutexExcludesConcurrentHolders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu WaitableMutex
		mu.Lock()
		if mu.TryLock() {
			t.Fatal("TryLock acquired a held mutex")
		}

		acquired := make(chan struct{})
		go func() {
			mu.Lock()
			close(acquired)
			mu.Unlock()
		}()
		synctest.Wait()
		select {
		case <-acquired:
			t.Fatal("Lock acquired a held mutex")
		default:
		}

		mu.Unlock()
		select {
		case <-acquired:
		case <-time.After(time.Second):
			t.Fatal("Lock did not acquire the released mutex")
		}
		synctest.Wait()
		if !mu.TryLock() {
			t.Fatal("TryLock did not acquire a free mutex")
		}
		mu.Unlock()
	})
}

func TestWaitableMutexUnlockOfUnlockedMutexPanics(t *testing.T) {
	var mu WaitableMutex
	defer func() {
		if recover() == nil {
			t.Fatal("Unlock of an unlocked mutex did not panic")
		}
	}()
	mu.Unlock()
}
