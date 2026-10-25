package memory

import "context"

// 各モジュールからMemory逼迫率を取得するのに用いる
type MemoryCriticalChecker interface {
	IsCritical(ctx context.Context) (bool, error)
}
