package worker

import (
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

// Optionは再送workerと保存先runtimeの連携を構築時に設定します。
type Option func(*Worker)

// WithClaimLockは保存処理とfileの取得処理に同じロックを使用します。
// 実行開始後に変更しません。nilの場合はworker専用ロックを使用します。
func WithClaimLock(lock *infrafsstore.ContextMutex) Option {
	return func(w *Worker) {
		if lock != nil {
			w.claimMu = lock
		}
	}
}

// WithRouteSnapshotは、現在の宛先一覧を取得する関数を設定します。
// 回復前の宛先有無の確認と、file取得後の送信先選択で呼び出します。
// 実行開始後に関数自体を変更しません。返り値は独立した一覧にしてください。
func WithRouteSnapshot(snapshot func() RouteRegistry) Option {
	return func(w *Worker) { w.routeSnapshot = snapshot }
}

// WithMaxRetryAfterは、upload先がRetry-Afterで指定した宛先別の待機の上限を設定します。
// より長い指定はこの時間に短縮し、RunReportで通知します。0は既定の1時間として扱い、
// 負値はWorkerの構築errorにします。
func WithMaxRetryAfter(maxRetryAfter time.Duration) Option {
	return func(w *Worker) { w.retryAfter.maxWait = maxRetryAfter }
}

// WithRetryAfterSaveRetryIntervalは、保存に失敗した宛先の待機期限を最初に再び保存するまでの
// 間隔を設定します。runnerの周期を渡します。失敗が続くたびに間隔を倍にし、1分を上限とします。
// 0以下なら既定の1秒を使います。
func WithRetryAfterSaveRetryInterval(interval time.Duration) Option {
	return func(w *Worker) {
		if interval > 0 {
			w.retryAfter.retryInterval = interval
		}
	}
}
