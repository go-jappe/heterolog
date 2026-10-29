package worker

import (
	"fmt"
	"time"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/spool"
)

const defaultProcessingFileTransitionRetryMaxEntries = 4098

// defaultMaxRetryAfterは、設定を渡さずに構築したWorkerが使うRetry-After待機の上限です。
const defaultMaxRetryAfter = time.Hour

type Worker struct {
	TargetBackendService string
	// Storeは、processing fileの候補の列挙、active fileの取得、recordの読み込み、
	// done / dead / quarantineへの移動とrecordの保存、削除に使います。
	Store WorkFileStore

	// ActiveSpoolWriterは、再送が必要なrecordをactive spoolへ戻す書き込み口です。
	// dead / quarantine / doneへの遷移はStoreが担当します。
	ActiveSpoolWriter ActiveSpoolWriter

	Sender Sender

	// ProcessingPathBackoffは、送信前の読み込み・隔離に失敗したパスを上限付きで保持します。
	// 期限切れや上限到達でも、探索位置はprocessingScanAfterから継続します。
	// backend送信後の後処理待ちはProcessingFileTransitionRetryで管理します。
	ProcessingPathBackoff *ProcessingPathBackoff

	// processingScanAfterは、最後に確認した候補のprocessing配下の相対パスです。
	// ファイルの有無を調べる対象ではなく、次の候補を探す順序の境界として使います。
	// 待機記録の期限や件数とは独立に保持し、末尾に到達したら先頭へ戻ります。
	// 送信完了の記録ではなく、再起動時には先頭から探索して構いません。
	processingScanAfter string

	// retryAfterは、backendがRetry-Afterで指定した、この送信先へ次に送信してよい時刻です。
	// Retry-Afterはrecordではなく送信先に対する指定であり、Workerは一つのbackend送信先だけを
	// 担当するため、期限まではprocessing / activeのどのfileも読み込み・送信しません。
	// 他のbackendのWorkerと、このWorkerの状態遷移retry・lifecycleの削除は止めません。
	// 待機はmaxRetryAfterまでに短縮し、runnerのIntervalや各backoffより短い指定は待機を延ばしません。
	//
	// 同じspool rootへ書き込む通常送信の保存処理とも共有し、通常送信で受け取った期限も次の
	// 周期から適用します。retryAfterStoreがあれば期限を保存し、最初の送信より前に読み込むため、
	// 再起動後もrecordを読み込む順序にかかわらず、保存した期限まで送信しません。
	// 再送待ちへ戻すrecordにもSpoolRecord.NotBeforeとして保存し、保存に失敗した場合も
	// そのrecordを読み込んだ時点で期限を復元します。
	retryAfter *RetryAfterDeadline

	// maxRetryAfterは、backendが指定したRetry-Afterと、保存済みの再送下限時刻に適用する
	// 待機の上限です。0の場合はdefaultMaxRetryAfterを使い、無制限にはしません。
	maxRetryAfter time.Duration

	// retryAfterStoreは、Newがretryの保存先として使う設定値です。nilなら期限をmemoryだけに保持します。
	retryAfterStore RetryAfterStore
	// retryAfterSaveRetryIntervalは、保存に失敗した期限を最初に再び保存するまでの間隔の設定値です。
	retryAfterSaveRetryInterval time.Duration

	// ProcessingFileTransitionRetryは、backend send後に遷移先が確定した
	// processing fileのstore / move / removeをretryするためのruntime stateです。
	//
	// ここに登録された path は、通常の read -> send 処理に流してはいけない。
	// backend sendはすでに実行済みであり、通常処理に戻すと
	// 二重送信の危険があるためである。
	//
	// entry は、対象の状態遷移操作が成功するか、
	// retry 対象 file を失った entry として明示的に破棄されるまで保持する。
	// ProcessingPathBackoff のように、時間経過だけで自然 clear するものではない。
	//
	// この state は、永続 ledger ではなく in-memory の runtime guard である。
	//
	// 状態遷移の失敗は、processing fileのmove / remove / split writeなど、
	// spool 上の file 操作で発生する。
	// その失敗経路で、同じ spool / filesystem へ retry state をさらに永続化できる
	// という前提は置かない。
	//
	// そのため、この worker は process が継続している範囲でのみ、
	// spool上の後処理先が確定したfileを通常のread -> send処理に戻さず、
	// 後処理だけを retry する。
	//
	// process restart でこの state が失われることは許容する。
	// restart 後に processing file だけが残った場合、通常処理に戻り、
	// 同じ record が再送される可能性がある。
	// ただしfilesystem storeにquarantineの保存結果が残っている場合は、
	// 送信前に照合し、隔離済みrecordの再送を防ぐ。部分一致や矛盾は送信せずerrorにする。
	//
	// spool の責務は、at-least-once 前提で可能な範囲の送信と状態管理を行うことである。
	//
	// 重複送信を安全に扱うには、spool payload を作成する上流が
	// 安定した idempotency key を payload に含め、
	// backend 側がその key に基づいて冪等に書き込める必要がある。
	//
	// 例えば OpenSearch では、上流が document の _id を一意かつ安定して決め、
	// 再送時も同じ _id で書き込むことで、重複送信が起きても
	// backend 上の結果を同一 document に収束させる。
	ProcessingFileTransitionRetry *ProcessingFileTransitionRetry

	// 処理結果とエラーはRunOnce系の戻り値で渡します。Loggerの内部通知先への
	// 振り分けはrunnerのReporterが担当し、worker内から重複通知しません。
	// TracerとMeterは、送り直しのspanと、送り直したrecordの数の記録に使います。
	Tracer pkgobserver.Tracer
	Meter  pkgobserver.Meter
}

type Option func(*Worker)

func WithTargetBackendService(backend string) Option {
	return func(w *Worker) { w.TargetBackendService = backend }
}

// WithObserversは、送り直しを計装するTracerとMeterを設定します。nilは記録しないものとして
// 扱います。
func WithObservers(tracer pkgobserver.Tracer, meter pkgobserver.Meter) Option {
	return func(w *Worker) {
		w.Tracer = tracer
		w.Meter = meter
	}
}

// WithMaxRetryAfterは、backendがRetry-Afterで指定した待機と、保存済みの再送下限時刻に
// 適用する上限を設定します。0は既定の1時間として扱い、負値はNewがerrorにします。
func WithMaxRetryAfter(maxRetryAfter time.Duration) Option {
	return func(w *Worker) { w.maxRetryAfter = maxRetryAfter }
}

// WithProcessingFileTransitionRetryは検証済みの再試行部品を構築時に渡します。
// nilの場合だけNewが既定の部品を作成します。
func WithProcessingFileTransitionRetry(retry *ProcessingFileTransitionRetry) Option {
	return func(w *Worker) { w.ProcessingFileTransitionRetry = retry }
}

func New(
	store WorkFileStore,
	activeSpoolWriter ActiveSpoolWriter,
	sender Sender,
	options ...Option,
) (*Worker, error) {
	if store == nil {
		return nil, ErrWorkerStoreNil
	}
	if activeSpoolWriter == nil {
		return nil, ErrWorkerActiveSpoolWriterNil
	}
	if sender == nil {
		return nil, ErrWorkerSenderNil
	}

	w := &Worker{
		Store:             store,
		ActiveSpoolWriter: activeSpoolWriter,
		Sender:            sender,

		ProcessingPathBackoff: NewProcessingPathBackoff(ProcessingPathBackoffConfig{}),
		maxRetryAfter:         defaultMaxRetryAfter,
	}
	for _, option := range options {
		option(w)
	}
	if w.maxRetryAfter < 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalidMaxRetryAfter, w.maxRetryAfter)
	}
	w.retryAfter = newRetryAfterDeadline(w.retryAfterStore, w.retryAfterLimit(), w.retryAfterSaveRetryInterval)
	w.processingFileTransitionRetry()
	return w, nil
}

// RetryAfterDeadlineは、この送信先のRetry-Afterによる待機期限を返します。同じspool rootへ
// 書き込む通常送信の保存処理へ渡し、通常送信で受け取った期限を再送中のWorkerにも適用します。
func (w *Worker) RetryAfterDeadline() *RetryAfterDeadline {
	return w.retryAfterDeadline()
}

// retryAfterDeadlineは、Workerが使う待機期限を返します。Newを使わずに構築したWorkerでは、
// 期限をmemoryだけに保持する部品を遅延初期化します。
func (w *Worker) retryAfterDeadline() *RetryAfterDeadline {
	if w.retryAfter == nil {
		w.retryAfter = newRetryAfterDeadline(w.retryAfterStore, w.retryAfterLimit(), w.retryAfterSaveRetryInterval)
	}
	return w.retryAfter
}

// retryAfterLimitは、Retry-Afterによる待機に適用する上限を返します。
func (w *Worker) retryAfterLimit() time.Duration {
	if w.maxRetryAfter <= 0 {
		return defaultMaxRetryAfter
	}
	return w.maxRetryAfter
}

// processingFileTransitionRetry は、Worker が使う ProcessingFileTransitionRetry を返す。
//
// ProcessingFileTransitionRetry が nil の場合は、デフォルト設定で初期化してから返す。
// そのため、この method は単なる getter ではなく lazy initialization を伴う。
func (w *Worker) processingFileTransitionRetry() *ProcessingFileTransitionRetry {
	if w.ProcessingFileTransitionRetry == nil {
		w.ProcessingFileTransitionRetry = newProcessingFileTransitionRetry(
			NewProcessingFileTransitionRetryConfigAdapter(ProcessingFileTransitionRetryConfig{
				RetryDelay: defaultProcessingFileTransitionRetryDelay,
				MaxEntries: defaultProcessingFileTransitionRetryMaxEntries,
			}),
		)
	}

	return w.ProcessingFileTransitionRetry
}

// unblockProcessingFileTransitionRetry は、指定pathの状態遷移retry entryを削除します。
//
// 対象の後処理が成功した、または孤児 entry として破棄すると判断した場合に呼ぶ。
func (w *Worker) unblockProcessingFileTransitionRetry(path string) {
	w.processingFileTransitionRetry().Remove(path)
}

// blockProcessingFileTransitionRetry は、processing fileの状態遷移失敗を
// retry listに登録または更新します。
//
// backend send は再実行せず、op に対応する後処理だけを後続の RunOnce で再試行する。
func (w *Worker) blockProcessingFileTransitionRetry(
	path string,
	op ProcessingFileTransitionRetryOp,
	err error,
) {
	retry := w.processingFileTransitionRetry()
	retry.Upsert(path, op, err, time.Now())
}

func (w *Worker) blockProcessingFileTransitionRetryForQuarantine(
	path string,
	reason spool.QuarantineReason,
	err error,
) {
	w.processingFileTransitionRetry().UpsertQuarantine(path, reason, err, time.Now())
}

// blockProcessingFileTransitionRetryWithRedistribution は、recordの再分配を伴う
// processing fileの状態遷移失敗をretry listに登録または更新します。
//
// plan には再分配に必要な record 群が保持される。
// backend sendは再実行せず、失敗した再分配または元file削除だけを再試行します。
func (w *Worker) blockProcessingFileTransitionRetryWithRedistribution(
	path string,
	op ProcessingFileTransitionRetryOp,
	plan *ProcessingRedistributionPlan,
	err error,
) {
	retry := w.processingFileTransitionRetry()
	retry.UpsertRedistribution(path, op, plan, err, time.Now())
}
