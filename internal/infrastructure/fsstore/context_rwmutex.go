package fsstore

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// 共有取得は1枠、排他取得は全枠を使います。ファイル数や保存件数の上限ではなく、
// 同時に共有lockを保持できる処理数の上限です。
// 2の30乗という値自体に特別な意味はありません。
const contextRWMutexReaderCapacity int64 = 1 << 30

// ContextRWMutexは、複数の保存処理を共有側、purge・終了処理を排他側として保護します。
// 共有・排他どちらの待機もキャンセル可能です。排他側がsemaphoreの待ち列に入ると、
// 後から来た共有側の取得はそれを追い越しません。
// zero値から使えます。使用開始後はコピーしないでください。
type ContextRWMutex struct {
	once      sync.Once
	semaphore *semaphore.Weighted
}

func (m *ContextRWMutex) init() {
	m.once.Do(func() { m.semaphore = semaphore.NewWeighted(contextRWMutexReaderCapacity) })
}

// RLockContextは共有lockを取得します。成功時だけ呼び出し元がRUnlockします。
func (m *ContextRWMutex) RLockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.init()
	return m.semaphore.Acquire(ctx, 1)
}

func (m *ContextRWMutex) RUnlock() {
	m.semaphore.Release(1)
}

// LockContextは排他lockを取得します。成功時だけ呼び出し元がUnlockします。
func (m *ContextRWMutex) LockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.init()
	return m.semaphore.Acquire(ctx, contextRWMutexReaderCapacity)
}

// Lockは終了時の完了待ちなど、待機をキャンセルしない処理向けです。
func (m *ContextRWMutex) Lock() {
	_ = m.LockContext(context.Background())
}

func (m *ContextRWMutex) Unlock() {
	m.semaphore.Release(contextRWMutexReaderCapacity)
}
