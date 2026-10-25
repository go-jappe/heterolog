package fsstore

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// ContextMutexは、待機をキャンセルできるfilesystem操作用の排他lockです。
// 待機と解放時の通知はsemaphoreが管理し、定期的な再試行や待機専用goroutineは使いません。
// zero値から使えます。使用開始後はコピーしないでください。
type ContextMutex struct {
	once      sync.Once
	semaphore *semaphore.Weighted
}

// LockContextは取得成功ならnil、待機中止ならctxのerrorを返します。
// 成功時だけ呼び出し元がUnlockします。取得後の操作を強制中断するものではありません。
func (m *ContextMutex) LockContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.once.Do(func() { m.semaphore = semaphore.NewWeighted(1) })
	return m.semaphore.Acquire(ctx, 1)
}

// Lockは終了時の完了待ちなど、待機をキャンセルしない処理向けです。
func (m *ContextMutex) Lock() {
	_ = m.LockContext(context.Background())
}

func (m *ContextMutex) Unlock() {
	m.semaphore.Release(1)
}
