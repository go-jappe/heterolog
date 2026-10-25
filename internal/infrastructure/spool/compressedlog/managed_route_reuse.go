package compressedlog

import (
	"context"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
)

// ConfirmAndSyncSavedFileWhilePurgeBlockedは、purgeを止めた状態で、
// この宛先のspoolから元ファイルと同名・同内容のファイルを探します。
// 見つかったファイルの更新時刻を現在時刻へ変更し、ファイルと親ディレクトリを同期します。
// 確認できたファイルをfound=trueで返し、見つからなければfound=falseを返します。
// 新規コピーは作りません。競合・重複ファイルは.preservedへ保全し、移動結果を
// 戻り値のConflictRenamesで返します。processing内の通常ファイルは移動せず、別内容ならエラーを返します。
// 呼び出し元は、呼び出す前にBlockPurgeでpurgeを止めるロックを取得し、
// 確認結果を使う処理が終わってから解除してください。
// このメソッドはそのロックの取得・解除も、purgeの実行もしません。
func (r *ManagedRoute) ConfirmAndSyncSavedFileWhilePurgeBlocked(ctx context.Context, sourcePath string) (file objectstoragespool.CompressedLogFile, found bool, err error) {
	if r == nil || r.rootRuntime == nil {
		return objectstoragespool.CompressedLogFile{}, false, compressedlogworker.ErrNilStore
	}
	if r.rootRuntime.isShutdownStarted() {
		return objectstoragespool.CompressedLogFile{}, false, ErrRootRuntimeClosed
	}
	if err := r.rootRuntime.saveOrClaimMu.LockContext(ctx); err != nil {
		return objectstoragespool.CompressedLogFile{}, false, err
	}
	defer r.rootRuntime.saveOrClaimMu.Unlock()
	return r.store.ConfirmSavedFile(ctx, sourcePath)
}
