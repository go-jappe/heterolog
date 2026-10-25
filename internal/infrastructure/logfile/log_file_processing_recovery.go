package logfile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

// DefaultLogFileProcessingRecoveryIntervalは、未完了のログファイル処理を再試行する間隔です。
// checkpoint更新の受理前の失敗や、送信完了後のファイル削除失敗などを対象とします。
// purgeの概算cache TTLや通常runnerの走査周期とは独立しています。
const DefaultLogFileProcessingRecoveryInterval = 5 * time.Second

var (
	// ErrLogFileProcessingRecoveryPanickedは、再開処理の一回の試行がpanicしたことを表します。
	// 登録は残り、次の再試行で同じ処理を再開します。
	ErrLogFileProcessingRecoveryPanicked = errors.New("log file processing recovery panic")
	// ErrLogFileProcessingRecoveryReleasePanickedは、完了した処理のファイル保護の解除がpanicしたことを表します。
	ErrLogFileProcessingRecoveryReleasePanicked = errors.New("log file processing recovery protection release panic")
)

// LogFileProcessingRecoveryPanicErrorは、再開処理の試行または保護解除のpanicを、recoverした値とともに
// 表します。Opは上のどちらかのsentinelで、errors.Isで判定できます。Valueがerrorなら、
// その原因もerrors.Is/Asで判定できます。
type LogFileProcessingRecoveryPanicError struct {
	Op    error
	Value any
}

func (e *LogFileProcessingRecoveryPanicError) Error() string {
	return fmt.Sprintf("%v: %v", e.Op, e.Value)
}

func (e *LogFileProcessingRecoveryPanicError) Unwrap() []error {
	if err, ok := e.Value.(error); ok {
		return []error{e.Op, err}
	}
	return []error{e.Op}
}

// logFileProcessingRecoveryOperationは、未完了のログファイル処理を再開するための一件の登録です。
// 利用側ごとのログファイル処理・通知・保護解除を関数で受け取り、待機・重複登録防止・停止を共通化します。
// Kは利用側が決めるキー型で、世代・圧縮の有無・分類などを構造体のまま比較できます。
type logFileProcessingRecoveryOperation[K comparable] struct {
	// keyは処理の識別子です。同じkeyの処理が登録中なら、追加登録を拒否します。
	key K
	// attemptは登録した処理を一回実行する関数です。boolは、その処理が完了したかを表します。
	// trueなら保護を解除して一覧から除き、falseなら次の再試行を予約します。
	// 完了判定はerrorの有無とは独立しており、trueとerrorを同時に返す場合もあります。
	attempt func(context.Context) (bool, error)
	// snapshotは試行結果の通知を、その試行の状態を保持する関数として取得します。
	// 操作と完了した保護解除の後、次の試行を許可する前に呼びます。
	snapshot func(error) func()
	// releaseは、この登録のために取得したファイル保護を解除する関数です。
	// 完了または強制終了の破棄時に一度だけ呼びます。実行中のcallbackが戻るまで保護を保持します。
	release func()
	// retryAtはattemptを実行してよい時刻です。登録中の最も早い時刻に合わせてtimerを予約します。
	retryAt time.Time
}

// logFileProcessingRecoveryWakeは、登録済みの処理を次に実行する時刻と、そのためのタイマーです。
type logFileProcessingRecoveryWake struct {
	timer *time.Timer
	at    time.Time
}

// LogFileProcessingRecoveryは、ファイルごとの未完了処理を登録し、一定間隔で再開します。
// checkpoint更新の受理前の失敗や、送信完了後のファイル削除失敗などを対象とします。
// メモリへ反映済みのcheckpointの保存失敗は、Store内部のSaveRecoveryが再試行します。
// ゼロ値から使用でき、同じkeyは重複登録しません。成功時はdone=trueを返します。
// keyの型は利用側で決めます。世代と圧縮の有無などを文字列へ変換せず、そのまま比較します。
// callbackは圧縮・送信・退避・checkpoint更新・削除など、ログファイル処理の実作業を担当します。
// この部品は処理の再試行・通知・終了を管理し、callbackの実行を直列化します。
// 使用開始後はコピーせず、所有者の終了時にCloseしてください。
type LogFileProcessingRecovery[K comparable] struct {
	mu       sync.Mutex
	interval time.Duration
	// registeredOperationsは、登録済みで未完了の処理一覧です。待機中・実行中を含みます。
	registeredOperations []*logFileProcessingRecoveryOperation[K]
	wake                 *logFileProcessingRecoveryWake
	// notificationsは通知をFIFOで排出します。通知がCloseを待っている間も、
	// 後続の試行は通知をここへ預けて次の再試行に進めます。
	notifications *notification.Dispatcher
	// roundActiveは、runRoundが処理の実行を開始してから通知のsnapshot取得までtrueです。
	// trueの間は次のタイマーを予約せず、別のrunRoundが同時に実行されることを防ぎます。
	// 通知自体は対象外なので、通知からCloseしても残りの再試行を進められます。
	roundActive   bool
	closing       bool
	closed        bool
	closeComplete chan struct{}
	closeErr      error
	// forceCancelは実行中roundだけを中止します。Context自体はroundの引数へ渡します。
	forceCancel context.CancelFunc
	forced      bool
}

// NewLogFileProcessingRecoveryは明示した正の再開間隔を使用します。
// 設定を指定しない所有者ではゼロ値とDefaultLogFileProcessingRecoveryIntervalを使えます。
func NewLogFileProcessingRecovery[K comparable](interval time.Duration) (*LogFileProcessingRecovery[K], error) {
	if interval <= 0 {
		return nil, errors.New("log file processing recovery interval must be positive")
	}
	return &LogFileProcessingRecovery[K]{interval: interval}, nil
}

// Scheduleはkeyの処理を登録します。falseの場合、渡したcallbackは一切実行しません。
// callerはその場合に自身で取得した保護を解除します。登録成功後のreleaseは
// 完了または強制終了の破棄時に一度だけ実行します。
// failureが非nilなら再開間隔後にattemptを呼びます。
// nilは失敗のない後続処理をただちに実行する指定です。
// snapshotはattemptと完了時のreleaseの後、後続のattemptが始まる前に呼びます。
// snapshot自身は通知を送らず、保持した状態を使う通知関数を返してください。
// 返された関数は後続の試行と並行して実行でき、Closeを呼ぶこともできます。
// Closeは返された通知関数の完了を待ちません。
func (r *LogFileProcessingRecovery[K]) Schedule(key K, failure error, attempt func(context.Context) (bool, error), snapshot func(error) func(), release func()) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing || attempt == nil {
		return false
	}
	for _, operation := range r.registeredOperations {
		if operation.key == key {
			return false
		}
	}
	operation := &logFileProcessingRecoveryOperation[K]{key: key, attempt: attempt, snapshot: snapshot, release: release}
	r.registeredOperations = append(r.registeredOperations, operation)
	if failure == nil {
		operation.retryAt = time.Now()
	} else {
		r.waitForRetryLocked(operation)
	}
	r.armLocked()
	return true
}

func (r *LogFileProcessingRecovery[K]) Has(key K) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, operation := range r.registeredOperations {
		if operation.key == key {
			return true
		}
	}
	return false
}

func (r *LogFileProcessingRecovery[K]) armLocked() {
	if r.roundActive || r.forced {
		return
	}
	var earliest time.Time
	for _, operation := range r.registeredOperations {
		if earliest.IsZero() || operation.retryAt.Before(earliest) {
			earliest = operation.retryAt
		}
	}
	if earliest.IsZero() || (r.wake != nil && !earliest.Before(r.wake.at)) {
		return
	}
	if r.wake != nil {
		r.wake.timer.Stop()
	}
	wake := &logFileProcessingRecoveryWake{at: earliest}
	r.wake = wake
	wake.timer = time.AfterFunc(time.Until(earliest), func() { r.runRound(wake) })
}

func (r *LogFileProcessingRecovery[K]) waitForRetryLocked(operation *logFileProcessingRecoveryOperation[K]) {
	interval := r.interval
	if interval == 0 {
		interval = DefaultLogFileProcessingRecoveryInterval
	}
	operation.retryAt = time.Now().Add(interval)
}

func (r *LogFileProcessingRecovery[K]) runRound(wake *logFileProcessingRecoveryWake) {
	r.mu.Lock()
	// Stopに間に合わなかった古いtimerは、予約し直した起床と重ねません。
	if r.wake != wake {
		r.mu.Unlock()
		return
	}
	r.wake = nil
	r.roundActive = true
	r.initializeLifecycleLocked()
	ctx, cancel := context.WithCancel(context.Background())
	r.forceCancel = cancel
	now := time.Now()
	var operations []*logFileProcessingRecoveryOperation[K]
	for _, operation := range r.registeredOperations {
		if !operation.retryAt.After(now) {
			operations = append(operations, operation)
		}
	}
	r.mu.Unlock()

	var notifications []func()
	for _, operation := range operations {
		r.mu.Lock()
		forced := r.forced
		r.mu.Unlock()
		if forced {
			break
		}
		done, err := attemptLogFileProcessingRecovery(ctx, operation.attempt)
		if done {
			// releaseが終わるまでkeyを登録中に保ち、同名の新しい処理と重ねません。
			releaseErr := releaseLogFileProcessingRecovery(operation.release)
			err = errors.Join(err, releaseErr)
			r.mu.Lock()
			if releaseErr != nil {
				r.closeErr = errors.Join(r.closeErr, releaseErr)
			}
			for index, current := range r.registeredOperations {
				if current == operation {
					r.registeredOperations = append(r.registeredOperations[:index], r.registeredOperations[index+1:]...)
					break
				}
			}
			r.mu.Unlock()
		} else {
			r.mu.Lock()
			r.waitForRetryLocked(operation)
			r.mu.Unlock()
		}
		if operation.snapshot != nil {
			if notify := snapshotLogFileProcessingRecovery(operation.snapshot, err); notify != nil {
				notifications = append(notifications, notify)
			}
		}
	}

	r.mu.Lock()
	if len(notifications) > 0 && r.notifications == nil {
		r.notifications = notification.NewDispatcher()
	}
	for _, notify := range notifications {
		r.notifications.Enqueue(notify)
	}
	if r.forced {
		// 受理済みcallbackが戻ってから残りの保護を解除します。強制終了を
		// 要求したgoroutineではreleaseを実行せず、I/Oとの重なりを防ぎます。
		r.mu.Unlock()
		r.abandonOperations()
		return
	}
	r.roundActive = false
	r.armLocked()
	r.finishCloseLocked()
	r.mu.Unlock()
}

func attemptLogFileProcessingRecovery(ctx context.Context, attempt func(context.Context) (bool, error)) (done bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			done = false
			err = &LogFileProcessingRecoveryPanicError{Op: ErrLogFileProcessingRecoveryPanicked, Value: recovered}
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	return attempt(ctx)
}

func releaseLogFileProcessingRecovery(release func()) (err error) {
	if release == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &LogFileProcessingRecoveryPanicError{Op: ErrLogFileProcessingRecoveryReleasePanicked, Value: recovered}
		}
	}()
	release()
	return nil
}

func snapshotLogFileProcessingRecovery(snapshot func(error) func(), err error) (notify func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			fmt.Fprintf(os.Stderr, "log file processing recovery notification snapshot panic: %v\n", recovered)
		}
	}()
	return snapshot(err)
}

func (r *LogFileProcessingRecovery[K]) finishCloseLocked() {
	if r.closing && !r.closed && !r.roundActive && len(r.registeredOperations) == 0 {
		r.closed = true
		r.notifications.CloseAfterDrain()
		close(r.closeComplete)
	}
}

func (r *LogFileProcessingRecovery[K]) initializeLifecycleLocked() {
	if r.closeComplete == nil {
		r.closeComplete = make(chan struct{})
	}
}

// abandonOperationsは実行中のcallbackがない状態で呼び、保護解除まで直列化します。
// roundActiveを保持している間は他のtimerや強制終了要求から再実行しません。
func (r *LogFileProcessingRecovery[K]) abandonOperations() {
	r.mu.Lock()
	operations := r.registeredOperations
	r.mu.Unlock()
	var releaseErr error
	for _, operation := range operations {
		releaseErr = errors.Join(releaseErr, releaseLogFileProcessingRecovery(operation.release))
	}
	r.mu.Lock()
	r.registeredOperations = nil
	r.closeErr = errors.Join(r.closeErr, releaseErr)
	r.roundActive = false
	r.finishCloseLocked()
	r.mu.Unlock()
}

// Closeは追加登録を止め、受理済みの操作が完了して保護が解除されるまで待ちます。
// 再試行timerや操作のcontextを中止せず、期限なしで待機状況を定期表示します。
// 通知の排出は所有者がNotificationsDoneで待ちます。
// 所有者の終了も通知完了を待つため、通知内からの終了要求は非同期に行ってください。
// ForceCloseされた場合も実行中callbackと保護解除の終了を待ってErrForcedCloseを返します。
func (r *LogFileProcessingRecovery[K]) Close() error {
	r.mu.Lock()
	r.initializeLifecycleLocked()
	if !r.closing {
		r.closing = true
		r.armLocked()
		r.finishCloseLocked()
	}
	complete := r.closeComplete
	description := fmt.Sprintf("log file processing recovery (%d operations pending when Close began)", len(r.registeredOperations))
	r.mu.Unlock()
	shutdown.Wait(description, complete)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closeErr
}

// ForceCloseは追加登録と再試行を止め、実行中の操作のContextを中止します。
// OSのI/Oは強制中断できないため、保護解除はcallbackが戻った後に行います。
// ここでは完了を待たずErrForcedCloseを返します。CloseとDoneは実際の終了を待ちます。
func (r *LogFileProcessingRecovery[K]) ForceClose() error {
	r.mu.Lock()
	r.initializeLifecycleLocked()
	if r.closed {
		err := r.closeErr
		r.mu.Unlock()
		return err
	}
	if !r.forced {
		r.forced = true
		r.closing = true
		r.closeErr = errors.Join(r.closeErr, shutdown.ErrForcedClose)
		if r.wake != nil {
			r.wake.timer.Stop()
			r.wake = nil
		}
		if !r.roundActive {
			r.roundActive = true
			go r.abandonOperations()
		}
	}
	cancel := r.forceCancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return shutdown.ErrForcedClose
}

// Doneは通常終了・強制終了のどちらでも、操作と保護解除が実際に終了すると閉じます。
// 通知callbackの完了はNotificationsDoneで別に確認してください。
func (r *LogFileProcessingRecovery[K]) Done() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.initializeLifecycleLocked()
	return r.closeComplete
}

// NotificationsDoneはCloseが返った後、受理済みの通知がすべて排出されるのを待つための通知です。
// 呼び出しはCloseの後に行ってください。通知callback自身からこのchannelを待ってはいけません。
// 後段の通知dispatcherを閉じる場合は、別goroutineでこのchannelを待ってから閉じてください。
func (r *LogFileProcessingRecovery[K]) NotificationsDone() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.notifications != nil {
		return r.notifications.Done()
	}
	done := make(chan struct{})
	close(done)
	return done
}
