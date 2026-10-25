package checkpoint

import (
	"context"
	"sync"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

// SaveRecoveryIntervalは保存失敗後の再試行間隔です。purgeの周期とは独立しています。
const SaveRecoveryInterval = 5 * time.Second

// PendingSaveErrorは、更新をメモリへ受理したものの、ファイル保存が未完了であることを
// 表します。利用側は失敗を通知し、メモリの更新を取り消さずに後続処理を続けられます。
// 入力検証など、更新を受理する前の失敗には使用しません。
type PendingSaveError struct{ Err error }

func (e *PendingSaveError) Error() string { return "checkpoint save pending: " + e.Err.Error() }
func (e *PendingSaveError) Unwrap() error { return e.Err }

// IsPendingSaveOnlyは、errが受理済み更新の保存待ちだけを表す場合にtrueを返します。
// ラップやerrors.Joinも調べ、別の失敗が含まれる場合やerrがnilの場合はfalseです。
// PendingSaveError内の原因は保存失敗として扱います。trueなら最新のメモリ状態を使って
// 後続処理を進め、ファイル保存の再試行はStoreへ任せられます。
func IsPendingSaveOnly(err error) bool {
	switch err := err.(type) {
	case *PendingSaveError:
		return true
	case interface{ Unwrap() []error }:
		children := err.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !IsPendingSaveOnly(child) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return IsPendingSaveOnly(err.Unwrap())
	default:
		return false
	}
}

// SaveRecoveryは、一つのStoreでcheckpointのファイル保存に失敗したときの再試行を管理します。
// 更新処理と同じmuで排他し、retryLockedでその時点の最新stateだけを保存します。
// 元の更新操作は繰り返しません。
// 保存エラーと保存対象の管理はStoreが行い、この部品は待機・二重起動防止・完了待ちを担当します。
type SaveRecovery struct {
	mu          *infrafsstore.ContextMutex
	retryLocked func(context.Context) (bool, error)
	// onErrorは構築時に固定し、再試行goroutineからmuの解放後に呼びます。
	// callbackは別goroutineへ通知を登録するだけにします。
	onError func(error)
	// runningは再試行ループが登録済みかを表します。
	// 読み書きはmuで保護し、登録解除後に旧ループが再び登録を変更することはありません。
	running bool
	loops   sync.WaitGroup
	// lifecycleMuはStoreのI/O用muと独立し、I/O中も強制終了を要求できるようにします。
	lifecycleMu sync.Mutex
	stopping    bool
	forceDone   chan struct{}
	// retryCancelは最新loopの中止だけを所有します。旧loopの通知終了時に消しません。
	retryCancel context.CancelFunc
	done        chan struct{}
}

// NewSaveRecoveryのretryLockedはmu取得中に呼び、保存待ちがなくなればtrueを返します。
// 再試行に失敗した場合は、その原因も返します。
// retryLocked内で同じmuを再取得しないでください。保存用Contextは通常終了では中止しません。
// onErrorは構築時に固定し、再試行goroutineからmuの解放後に呼びます。nilなら通知しません。
// onErrorは別goroutineへの通知登録だけを行い、通知先を同期実行しないでください。
// 通知先がStopやStoreのCloseを呼んでも、保存ループ自身の終了待ちにならないようにするためです。
// 構築だけではgoroutineを起動しません。
func NewSaveRecovery(mu *infrafsstore.ContextMutex, retryLocked func(context.Context) (bool, error), onError func(error)) *SaveRecovery {
	return &SaveRecovery{mu: mu, retryLocked: retryLocked, onError: onError,
		forceDone: make(chan struct{}), done: make(chan struct{})}
}

// StartLockedは保存失敗時に呼びます。呼び出し元はmuを保持してください。
// 既に再試行中なら何も起動せず、既存の処理が最新stateを保存します。
func (r *SaveRecovery) StartLocked() {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.running || r.stopping {
		return
	}
	r.running = true
	r.loops.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	r.retryCancel = cancel
	go r.run(ctx, cancel)
}

func (r *SaveRecovery) run(ctx context.Context, cancel context.CancelFunc) {
	defer r.loops.Done()
	defer cancel()
	timer := time.NewTimer(SaveRecoveryInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := r.mu.LockContext(ctx); err != nil {
			return
		}
		// timerと中止が同時に通知された場合にも新たな保存は始めません。
		if ctx.Err() != nil {
			r.mu.Unlock()
			return
		}
		done, err := r.retryLocked(ctx)
		if done {
			// 終了判定と登録解除を更新と同じlock内で行います。この直後に新たな
			// 保存失敗が起きても、終了するループへ渡して取り落とすことはありません。
			r.running = false
		}
		r.mu.Unlock()
		if err != nil && r.onError != nil {
			r.onError(err)
		}
		if done || ctx.Err() != nil {
			return
		}
		timer.Reset(SaveRecoveryInterval)
	}
}

// Stopは受理済みの保存とエラー通知登録が完了するまで期限を設けずに待ちます。
// 保存待ちの再試行は中止せず、待機中であることを定期的に標準エラーへ表示します。
// 呼び出し元は先に新しいStore操作を止め、muを保持せずに呼んでください。
// ForceClose後も実行中callbackの終了を待ち、その後ErrForcedCloseを返します。
func (r *SaveRecovery) Stop() error {
	r.lifecycleMu.Lock()
	r.beginStopLocked()
	r.lifecycleMu.Unlock()
	shutdown.Wait("checkpoint saves", r.done)
	select {
	case <-r.forceDone:
		return shutdown.ErrForcedClose
	default:
		return nil
	}
}

func (r *SaveRecovery) beginStopLocked() {
	if r.stopping {
		return
	}
	r.stopping = true
	go func() {
		r.loops.Wait()
		close(r.done)
	}()
}

// ForceCloseは保存の再試行とlock待機を中止します。保存済みのstateへ巻き戻しません。
// 実行中のOSのI/Oは終了するまで残り得るため、ここでは待たずDoneで終了を知らせます。
func (r *SaveRecovery) ForceClose() error {
	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	select {
	case <-r.done:
		select {
		case <-r.forceDone:
			return shutdown.ErrForcedClose
		default:
			return nil
		}
	default:
	}
	select {
	case <-r.forceDone:
	default:
		close(r.forceDone)
		if r.retryCancel != nil {
			r.retryCancel()
		}
	}
	r.beginStopLocked()
	return shutdown.ErrForcedClose
}

// Doneは終了要求後に保存callbackとエラー通知登録が実際に戻ると閉じます。
func (r *SaveRecovery) Done() <-chan struct{} {
	return r.done
}
