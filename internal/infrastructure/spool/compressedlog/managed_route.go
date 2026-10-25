package compressedlog

import (
	"context"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// ManagedRouteConfigは、一つの宛先配下で扱うログ系列のファイル名判定を登録します。
type ManagedRouteConfig struct {
	// DisableMaintenanceは手動復旧用に、保存だけを行うrouteを登録します。
	// 再送・purge・起動時の残骸掃除は行いません。同じspoolを使う他のprocessを
	// 停止してから使用し、このruntimeの全routeで同じ値を指定してください。
	DisableMaintenance bool
	// FileNameはspoolへコピーした圧縮ログの命名規則です。
	// 元のDirや現在のDate・Indexは照合に使いません。
	FileName    logpattern.FileNamePattern
	RunObserver ManagedRunObserver
	// OnWarnは再送・purge対象外へ保全したファイルの管理者確認を通知します。
	// 保存・走査のロックを解放した後に呼びます。nilなら警告を通知しません。
	OnWarn func(context.Context, error)
}

type ManagedRoute struct {
	rootRuntime *RootRuntime
	store       *objectstoragespool.Store

	// closingはrootRuntime.muで保護します。受理済み周期は登録を維持したまま完了します。
	closing         bool
	closeController shutdown.Controller
	notifications   sync.WaitGroup
	saves           sync.WaitGroup
	forceCtx        context.Context
	forceCancel     context.CancelFunc
	// classifyRetryは、このrouteの宛先への送信エラーからRetry-Afterを判定します。
	// 再送workerと同じ宛先の分類を使い、構築後は変更しません。
	classifyRetry func(context.Context, error) retryable.RetryDecision
}

// notificationContextは実行中の通知へ、発行元とこのrouteの中止を伝えます。
// 終了時はhookだけを外し、後段へ渡されたcontextを通常終了で中止しません。
func (r *ManagedRoute) notificationContext(parent context.Context) (context.Context, func()) {
	if r.forceCtx == nil {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	stopParent := context.AfterFunc(parent, cancel)
	stopRoute := context.AfterFunc(r.forceCtx, cancel)
	if parent.Err() != nil || r.forceCtx.Err() != nil {
		cancel()
	}
	return ctx, func() {
		stopParent()
		stopRoute()
	}
}

// routeUploaderは同じrootの他routeへ影響させず、このrouteのuploadだけを中止します。
type routeUploader struct {
	compressedlogworker.Uploader
	forceCtx context.Context
}

func (u routeUploader) UploadDestinations(ctx context.Context, request uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	workCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(u.forceCtx, cancel)
	defer stop()
	defer cancel()
	if u.forceCtx.Err() != nil {
		cancel()
	}
	return u.Uploader.UploadDestinations(workCtx, request, destinations)
}

// SaveRetryableFailureはpurgeを待たせるロックを取得して、再送可能なupload失敗fileを
// spoolへコピー保存します。保存後にロックを解除し、元ファイルは残します。
func (r *ManagedRoute) SaveRetryableFailure(
	ctx context.Context,
	sourcePath string,
) (objectstoragespool.CompressedLogFile, error) {
	unlockPurge, err := r.BlockPurge(ctx)
	if err != nil {
		return objectstoragespool.CompressedLogFile{}, err
	}
	defer unlockPurge()
	return r.SaveRetryableFailureWithPurgeLockHeld(ctx, sourcePath)
}

// SaveRetryableFailureWithPurgeLockHeldは、BlockPurgeで取得したロックを
// 呼び出し元が保持している間に、再送可能なupload失敗fileをspoolへコピー保存します。
// 元ファイルは削除しません。
func (r *ManagedRoute) SaveRetryableFailureWithPurgeLockHeld(
	ctx context.Context,
	sourcePath string,
) (objectstoragespool.CompressedLogFile, error) {
	if r == nil || r.rootRuntime == nil {
		return objectstoragespool.CompressedLogFile{}, compressedlogworker.ErrNilStore
	}
	if err := r.rootRuntime.saveOrClaimMu.LockContext(ctx); err != nil {
		return objectstoragespool.CompressedLogFile{}, err
	}
	defer r.rootRuntime.saveOrClaimMu.Unlock()
	// 再送可能なupload失敗ファイルを保存します。再送は次のrunner tickで開始し、
	// 保存をきっかけに即座の再送は行いません。
	return r.store.SaveRetryableFailure(ctx, sourcePath)
}

// SavePermanentFailureはpurgeを待たせるロックを取得して、恒久的なupload失敗fileを
// spoolへコピー保存します。保存後にロックを解除し、元ファイルは残します。
func (r *ManagedRoute) SavePermanentFailure(
	ctx context.Context,
	sourcePath string,
) (objectstoragespool.CompressedLogFile, error) {
	unlockPurge, err := r.BlockPurge(ctx)
	if err != nil {
		return objectstoragespool.CompressedLogFile{}, err
	}
	defer unlockPurge()
	return r.SavePermanentFailureWithPurgeLockHeld(ctx, sourcePath)
}

// SavePermanentFailureWithPurgeLockHeldは、BlockPurgeで取得したロックを
// 呼び出し元が保持している間に、恒久的なupload失敗fileをspoolへコピー保存します。
// 元ファイルは削除しません。
func (r *ManagedRoute) SavePermanentFailureWithPurgeLockHeld(
	ctx context.Context,
	sourcePath string,
) (objectstoragespool.CompressedLogFile, error) {
	if r == nil || r.rootRuntime == nil {
		return objectstoragespool.CompressedLogFile{}, compressedlogworker.ErrNilStore
	}
	if err := r.rootRuntime.saveOrClaimMu.LockContext(ctx); err != nil {
		return objectstoragespool.CompressedLogFile{}, err
	}
	defer r.rootRuntime.saveOrClaimMu.Unlock()
	return r.store.SavePermanentFailure(ctx, sourcePath)
}

// Rootはこのrouteの宛先専用ディレクトリを返します。
func (r *ManagedRoute) Root() objectstoragespool.Root { return r.store.Root() }

// RecordUploadRetryAfterは、このrouteの宛先への通常のupload（spoolへ保存する前の直接upload）が
// 一時的な失敗とともにRetry-Afterを返した場合に、その期限を宛先の待機期限として記録し、宛先別spool
// のmarkerへ保存します。再送workerと共有するため、稼働中のworkerも次の周期から期限までその宛先の
// ファイルを送信せず、再起動後も読み込みます。より後の期限を記録済みなら短縮せず、上限を超える
// 指定は上限までに短縮して警告を通知します。Retry-Afterを含まないerrorでは何もしません。
// 判定は呼び出し元のcontextの終了に影響されず、送信先が応答で指定した待機を記録します。
func (r *ManagedRoute) RecordUploadRetryAfter(ctx context.Context, uploadErr error) {
	if r == nil || uploadErr == nil || r.classifyRetry == nil || r.rootRuntime == nil || r.rootRuntime.runner == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	decision := r.classifyRetry(context.WithoutCancel(ctx), uploadErr)
	if !decision.Retry || decision.RetryAfter <= 0 {
		return
	}
	r.rootRuntime.runner.worker.RecordRetryAfter(r.store.Root(), decision.RetryAfter)
}

// BlockPurgeは、戻り値の関数でロックを解除するまで、purgeを実行させないためのロックを取得します。
// 保存・確認・同期など、purgeから保護する処理が終わった後に、呼び出し元が解除してください。
// 実行中のpurgeは中断せず、その完了を待ってからロックを取得します。
// ロック取得待ちはctxのキャンセル・期限切れで中止できます。取得後の自動解除は行いません。
func (r *ManagedRoute) BlockPurge(ctx context.Context) (func(), error) {
	if r == nil || r.rootRuntime == nil {
		return func() {}, nil
	}
	r.rootRuntime.mu.Lock()
	if r.closing || r.rootRuntime.state == rootRuntimeShutdownStarted {
		r.rootRuntime.mu.Unlock()
		return nil, ErrRootRuntimeClosed
	}
	r.saves.Add(1)
	r.rootRuntime.mu.Unlock()
	if err := r.rootRuntime.storeOrPurgeMu.RLockContext(ctx); err != nil {
		r.saves.Done()
		return nil, err
	}
	return sync.OnceFunc(func() {
		r.rootRuntime.storeOrPurgeMu.RUnlock()
		r.saves.Done()
	}), nil
}

func (r *ManagedRoute) Close() error {
	if r == nil || r.rootRuntime == nil {
		return nil
	}
	return r.closeController.Close("compressed log file spool route", func() (bool, error) {
		runtime := r.rootRuntime
		rootRuntimeRegistry.Lock()
		runtime.mu.Lock()
		r.closing = true
		runtime.stopIfRoutesClosingLocked()
		runtime.mu.Unlock()
		rootRuntimeRegistry.Unlock()
		r.saves.Wait()
		// 回復・upload・状態遷移だけでなく、今回の通知の登録が終わるまで待ちます。
		runtime.runner.runMu.Lock()
		rootRuntimeRegistry.Lock()
		runtime.mu.Lock()
		delete(runtime.routes, r)
		finish := runtime.stopIfUnusedLocked()
		runtime.mu.Unlock()
		rootRuntimeRegistry.Unlock()
		runtime.runner.runMu.Unlock()
		// 手動ReportSpoolRunnerRunが先にsnapshotした通知も完了まで待ちます。
		r.notifications.Wait()
		if finish {
			_ = runtime.waitShutdown()
			<-runtime.shutdownComplete
		}
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return true, runtime.runErr
	})
}

// RequestCloseは通知callbackから、callback自身の完了を待たずに終了を要求します。
func (r *ManagedRoute) RequestClose() {
	if r != nil && r.rootRuntime != nil {
		go r.Close()
	}
}

// RequestForceCloseはこのrouteの待機を解き、uploadへ中止を通知します。
// 共有する他routeの処理は中止しません。
// 登録と資源の解放は受理済み周期・通知の終了後に継続します。
func (r *ManagedRoute) RequestForceClose() error {
	if r == nil || r.rootRuntime == nil {
		return nil
	}
	return r.closeController.ForceClose(r.forceCancel)
}

func (r *ManagedRoute) ForceClose() error {
	err := r.RequestForceClose()
	r.RequestClose()
	return err
}

func (r *ManagedRoute) ShutdownDone() <-chan struct{} { return r.closeController.Done() }

func (r *ManagedRoute) ShutdownComplete() bool { return r.closeController.IsComplete() }
