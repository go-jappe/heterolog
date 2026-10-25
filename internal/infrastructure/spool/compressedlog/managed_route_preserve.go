package compressedlog

import (
	"context"

	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
)

// PreserveFileはsourceを通常stateとは別の保全領域へコピーします。
// sourceと処理中のspool fileは移動せず、保全copyは再送・purgeから除外するため、
// purge用lockを追加取得しません。通常handoffのlock保持中にも呼び出せます。
func (r *ManagedRoute) PreserveFile(ctx context.Context, sourcePath string) (string, error) {
	if r == nil || r.rootRuntime == nil {
		return "", compressedlogworker.ErrNilStore
	}
	if r.rootRuntime.isShutdownStarted() {
		return "", ErrRootRuntimeClosed
	}
	return r.store.PreserveFile(ctx, sourcePath)
}
