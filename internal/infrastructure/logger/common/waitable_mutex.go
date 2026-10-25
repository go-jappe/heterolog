package common

import "sync"

// WaitableMutexは、sync.Mutexと同じ意味の排他lockです。容量1のchannelへの送受信で取得と
// 解放を表すため、取得を待つgoroutineはtesting/synctestのbubble内でも停止状態として扱われ、
// testから待機を確定的に観測できます。zero値で使用できます。最初の使用時にchannelを作るため、
// 一つのlockをbubbleの内側と外側の両方から使用しないでください。
type WaitableMutex struct {
	once sync.Once
	ch   chan struct{}
}

func (m *WaitableMutex) channel() chan struct{} {
	m.once.Do(func() { m.ch = make(chan struct{}, 1) })
	return m.ch
}

// Lockはlockを取得するまで待ちます。
func (m *WaitableMutex) Lock() {
	m.channel() <- struct{}{}
}

// TryLockは待たずにlockの取得を試み、取得できた場合にtrueを返します。
func (m *WaitableMutex) TryLock() bool {
	select {
	case m.channel() <- struct{}{}:
		return true
	default:
		return false
	}
}

// Unlockはlockを解放します。保持されていないlockの解放はsync.Mutexと同じく誤用のため、
// panicします。
func (m *WaitableMutex) Unlock() {
	select {
	case <-m.channel():
	default:
		panic("common: unlock of unlocked mutex")
	}
}
