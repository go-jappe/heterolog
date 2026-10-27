package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
)

const defaultProcessingFileTransitionRetryDelay = 30 * time.Second

// ProcessingFileTransitionRetryConfigは、送信結果確定後のprocessing fileについて、
// active / done / dead / quarantineへの状態遷移または元fileの削除を一度試みて失敗し、
// retry entryへ登録された後の再試行方法を定義します。
// 初回の状態遷移には適用しません。
type ProcessingFileTransitionRetryConfig struct {
	// RetryDelayは、初回の状態遷移またはその再試行に失敗してから、
	// 次に同じpathの後処理を再試行可能にするまでの待機時間です。
	//
	// これは entry の削除期限ではない。
	// entry は成功するまで保持する。
	//
	// ただし、この retry state は in-memory runtime state である。
	// process restart をまたいだ厳密な再送防止は行わない。
	RetryDelay time.Duration

	// MaxEntries は、状態遷移の再試行entryを保持できる最大件数。
	//
	// 正の値が必須です。
	//
	// これは eviction 用ではない。
	// entryを捨てると、spool上の後処理先が確定済みのprocessing fileが
	// 通常の read -> send 処理に戻り、二重送信される危険があるためである。
	//
	// 件数が上限に達している間、worker は新しい backend send を進めず、
	// 既存のprocessing file transition retryの回収を優先する。
	MaxEntries int
}

// ProcessingFileTransitionRetryOp は、次に再試行するprocessing fileの状態遷移操作です。
type ProcessingFileTransitionRetryOp string

const (
	// ProcessingFileTransitionRetryOpMoveProcessingToDone は、
	// processing file の全 record が success と確定済みで、
	// processing から done への移動に失敗した場合に使う。
	//
	// 次回 retry では backend への send は行わず、
	// MoveProcessingToDone だけを再試行する。
	//
	// Redistribution は使わない。
	ProcessingFileTransitionRetryOpMoveProcessingToDone ProcessingFileTransitionRetryOp = "move_processing_to_done"

	// ProcessingFileTransitionRetryOpMoveProcessingToDead は、
	// processing file の全recordが、表現を信頼できるterminal failureと確定済みで、
	// processing から dead への移動に失敗した場合に使う。
	//
	// 次回 retry では backend への send は行わず、
	// MoveProcessingToDead だけを再試行する。
	//
	// Redistribution は使わない。
	ProcessingFileTransitionRetryOpMoveProcessingToDead ProcessingFileTransitionRetryOp = "move_processing_to_dead"

	// ProcessingFileTransitionRetryOpMoveProcessingToQuarantine は、
	// processing file 全体を隔離すると決定した後、processing から
	// quarantine への移動に失敗した場合に使う。
	// 全recordが修復・確認を要するinvalidの場合に加え、Senderの分類結果が構造的に不完全で
	// record単位へ安全に分割できない場合も含む。
	//
	// 次回 retry では backend への send は行わず、
	// MoveProcessingToQuarantine だけを再試行する。
	//
	// Redistribution は使わない。
	ProcessingFileTransitionRetryOpMoveProcessingToQuarantine ProcessingFileTransitionRetryOp = "move_processing_to_quarantine"

	// ProcessingFileTransitionRetryOpStoreRecordsToActive は、
	// Sender.Send 後に再送が必要なrecordsをactiveへ戻す段階で失敗した場合に使う。
	//
	// 次回 retry では backend への send は行わず、
	// Redistributionに保持されたactiveへの保存対象recordから再開する。
	//
	// mixed result の場合、Redistribution にはこの step 以降に必要な record 群だけを保持する。
	// すでに保存済みの record は二重保存を避けるため保持しない。
	ProcessingFileTransitionRetryOpStoreRecordsToActive ProcessingFileTransitionRetryOp = "store_records_to_active"

	// ProcessingFileTransitionRetryOpStoreProcessingRecordsToDead は、
	// Sender.Send後にterminal failure recordをdeadへ分配する段階で失敗した場合に使う。
	//
	// 次回 retry では backend への send は行わず、
	// Redistributionに保持されたdeadへの保存対象recordから再開する。
	//
	// このopが登録される時点でactiveへの保存が完了している場合、
	// その保存対象recordはRedistributionに含めない。
	ProcessingFileTransitionRetryOpStoreProcessingRecordsToDead ProcessingFileTransitionRetryOp = "store_processing_records_to_dead"

	// ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine は、
	// Sender.Send後に修復・確認を要するrecordをquarantineへ分配する段階で失敗した場合に使う。
	//
	// 次回 retry では backend への send は行わず、
	// Redistributionに保持されたquarantineへの保存対象recordから再開する。
	//
	// このopが登録される時点でactive / deadへの保存が完了している場合、
	// それらの保存対象recordはRedistributionに含めない。
	ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine ProcessingFileTransitionRetryOp = "store_processing_records_to_quarantine"

	// ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone は、
	// Sender.Send後にsuccess recordをdoneへ分配する段階で失敗した場合に使う。
	//
	// 次回 retry では backend への send は行わず、
	// Redistributionに保持されたdoneへの保存対象recordから再開する。
	//
	// このopが登録される時点でactive / dead / quarantineへの保存が完了している場合、
	// それらの保存対象recordはRedistributionに含めない。
	ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone ProcessingFileTransitionRetryOp = "store_processing_records_to_done"

	// ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution は、
	// active / dead / quarantine / doneなどへの再分配が完了した後、
	// 元の processing file の削除だけに失敗した場合に使う。
	//
	// 再分配済み record を二重処理しないよう、
	// 次回 retry では RemoveProcessingFile だけを再試行する。
	//
	// Redistribution は使わない。
	ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution ProcessingFileTransitionRetryOp = "remove_processing_file_after_redistribution"
)

// ProcessingFileTransitionRetryは、backend send後に配送結果が確定した
// processing fileをspool上の次の状態へ遷移させる処理の再試行状態です。
// file全体をdone / dead / quarantineへ移動する場合と、mixed resultを
// record単位でactive / done / dead / quarantineへ再分配して元fileを削除する場合を扱います。
//
// この spool worker は at-least-once delivery を前提とする。
// つまり、障害や再起動のタイミングによって同じ record が複数回送信される可能性を許容する。
// exactly-once delivery、外部 backend との分散 transaction、
// process restart をまたいだ完全な重複送信防止はこの worker の責務ではない。
//
// OpenSearch など送信先側で deterministic な _id を使う場合、
// 重複送信は backend 側の idempotency によって吸収される想定である。
//
// ProcessingFileTransitionRetry は ProcessingPathBackoff とは別物である。
// ProcessingPathBackoff は未送信 path の一時 skip 用であり、時間で clear されてよい。
// 一方、このretryはprocess稼働中に、遷移先が確定済みのpathを通常send処理へ
// すぐ戻さないための runtime guard である。
//
// entry は時間で削除しない。
// 対象 operation が成功した場合のみ Remove する。
// ただし in-memory state なので、process restart で失われることは許容する。
//
// ProcessingFileTransitionRetry は goroutine-safe ではない。
// 単一 worker loop からの利用を前提とする。
type ProcessingFileTransitionRetry struct {
	cfg ProcessingFileTransitionRetryConfigProvider

	// key は processing file path。
	//
	// key が存在する path は Sender.Send の結果がすでに確定済み、
	// または再分配済みの後処理中として扱う。
	//
	// 通常の read -> send 処理には流してはいけない。
	items map[string]ProcessingFileTransitionRetryEntry
}

// Requeue は、既存のprocessing file transition retry entryを保持したまま、
// 次回 retry 時刻と最後の error 診断だけを更新する。
//
// Operation や Redistribution は変更しない。
// ProcessingFileExists の一時失敗など、retry 状態そのものを進めない失敗で使う。
func (r *ProcessingFileTransitionRetry) Requeue(
	path string,
	entry ProcessingFileTransitionRetryEntry,
	err error,
	now time.Time,
) {
	if r == nil {
		return
	}
	if path == "" {
		return
	}
	if r.items == nil {
		r.items = make(map[string]ProcessingFileTransitionRetryEntry)
	}

	entry.NextRetryAt = now.Add(r.retryDelay())

	if err != nil {
		entry.ErrorText = err.Error()
	} else {
		entry.ErrorText = ""
	}

	r.items[path] = entry
}

// ProcessingFileTransitionRetryEntry は、1つのprocessing fileについて
// 次に再試行する状態遷移と、その再開に必要な情報を表します。
//
// path はこの struct に持たせず、map の key として管理する。
type ProcessingFileTransitionRetryEntry struct {
	// Operation は、次回 retry するstore / move / remove操作。
	//
	// send は再実行せず、失敗した後処理だけを再試行するために保持する。
	Operation ProcessingFileTransitionRetryOp

	// Redistribution は、mixed result の再分配 retry に必要な record 群。
	//
	// move/remove のように path だけで retry できる operation では nil。
	Redistribution *ProcessingRedistributionPlan

	// QuarantineReasonはfile全体をquarantineへ移すretryで、移動先file名へ
	// 保存する隔離理由です。それ以外のoperationではzero valueです。
	QuarantineReason spool.QuarantineReason

	// NextRetryAt は、この path を次に retry してよい時刻。
	//
	// retry に再失敗した場合はこの時刻を更新し、
	// RunOnce ごとに同じ path を踏み続けることを防ぐ。
	NextRetryAt time.Time

	// ErrorText は、最後に失敗した error の診断用文字列。
	//
	// retry 判定には使わず、ログ・メトリクス・デバッグ用に保持する。
	ErrorText string
}

// ProcessingRedistributionPlanは、Sender.Send後に確定した
// 各recordのspool上の後処理内容を表します。
//
// retry 時刻や error 診断は ProcessingFileTransitionRetryEntry 側で管理する。
// path も retry map の key として管理する。
type ProcessingRedistributionPlan struct {
	// ToActiveRecordsは、次回送信のためactiveへ戻すrecordです。
	// backendからretryableと判定されたrecordと、結果未確定のrecordの両方を含みます。
	ToActiveRecords []spool.SpoolRecord

	// ToDeadRecordsは、表現を信頼できるterminal failureと確定し、deadへ保存するrecordです。
	ToDeadRecords []spool.SpoolRecord

	// ToQuarantineRecordsは、そのままの自動再送が安全でないと確定し、quarantineへ保存するrecordです。
	ToQuarantineRecords []spool.SpoolRecord

	// ToDoneRecordsは、backend送信に成功し、doneへ保存するrecordです。
	ToDoneRecords []spool.SpoolRecord
}

// NewProcessingFileTransitionRetryは、processing fileの状態遷移retryを初期化します。
// RetryDelayとMaxEntriesは正の値が必須です。
func NewProcessingFileTransitionRetry(
	cfg ProcessingFileTransitionRetryConfigProvider,
) (*ProcessingFileTransitionRetry, error) {
	if cfg == nil {
		return nil, ErrNilProcessingFileTransitionRetryConfig
	}
	if retryDelay := cfg.RetryDelay(); retryDelay <= 0 {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrInvalidProcessingFileTransitionRetryDelay,
			retryDelay,
		)
	}
	if maxEntries := cfg.MaxEntries(); maxEntries <= 0 {
		return nil, fmt.Errorf(
			"%w: %d",
			ErrInvalidProcessingFileTransitionRetryMaxEntries,
			maxEntries,
		)
	}
	return newProcessingFileTransitionRetry(cfg), nil
}

// newProcessingFileTransitionRetryは、Worker自身が持つ検証済み既定値からretry状態を
// 初期化します。外部設定を受け取る境界ではNewProcessingFileTransitionRetryを使用します。
func newProcessingFileTransitionRetry(
	cfg ProcessingFileTransitionRetryConfigProvider,
) *ProcessingFileTransitionRetry {
	return &ProcessingFileTransitionRetry{
		cfg:   cfg,
		items: make(map[string]ProcessingFileTransitionRetryEntry),
	}
}

// Upsert は、processing fileの状態遷移retryを登録または更新する。
//
// path だけで再試行できる move / remove 系 operation で使う。
// Redistribution が必要な operation には UpsertRedistribution を使う。
//
// 既存 entry の Operation / Redistribution を変えずに retry 時刻だけ更新する場合は、
// Upsert ではなく Requeue を使う。
func (r *ProcessingFileTransitionRetry) Upsert(
	path string,
	op ProcessingFileTransitionRetryOp,
	err error,
	now time.Time,
) {
	r.upsert(path, op, nil, "", err, now)
}

// UpsertQuarantineは、file全体のquarantine移動だけを再試行するentryを登録します。
// reasonを保持することで、初回移動とretryで同じ調査情報をfile名へ保存します。
func (r *ProcessingFileTransitionRetry) UpsertQuarantine(
	path string,
	reason spool.QuarantineReason,
	err error,
	now time.Time,
) {
	r.upsert(
		path,
		ProcessingFileTransitionRetryOpMoveProcessingToQuarantine,
		nil,
		reason,
		err,
		now,
	)
}

// UpsertRedistribution は、mixed result の再分配 retry を登録または更新する。
//
// retry が途中まで進んだ場合、すでに保存済みの record 群を plan から外し、
// 次に再開すべき operation と残りの record 群だけを保持する。
func (r *ProcessingFileTransitionRetry) UpsertRedistribution(
	path string,
	op ProcessingFileTransitionRetryOp,
	plan *ProcessingRedistributionPlan,
	err error,
	now time.Time,
) {
	r.upsert(path, op, plan, "", err, now)
}

func (r *ProcessingFileTransitionRetry) upsert(
	path string,
	op ProcessingFileTransitionRetryOp,
	plan *ProcessingRedistributionPlan,
	quarantineReason spool.QuarantineReason,
	err error,
	now time.Time,
) {
	if r == nil {
		return
	}
	if path == "" {
		return
	}
	if r.items == nil {
		r.items = make(map[string]ProcessingFileTransitionRetryEntry)
	}

	entry := r.items[path]
	entry.Operation = op
	entry.Redistribution = plan
	entry.QuarantineReason = quarantineReason
	entry.NextRetryAt = now.Add(r.retryDelay())

	if err != nil {
		entry.ErrorText = err.Error()
	} else {
		entry.ErrorText = ""
	}

	r.items[path] = entry
}

// Remove は、指定pathのprocessing file transition retryを解除する。
//
// 対象 operation が成功した場合のみ呼び出す。
func (r *ProcessingFileTransitionRetry) Remove(path string) {
	if r == nil {
		return
	}
	if path == "" {
		return
	}

	delete(r.items, path)
}

// Contains は、指定pathがprocessing file transition retry対象ならtrueを返す。
//
// true の path は通常の read -> send に流してはいけない。
func (r *ProcessingFileTransitionRetry) Contains(path string) bool {
	if r == nil {
		return false
	}
	if path == "" {
		return false
	}

	_, ok := r.items[path]
	return ok
}

// FindReady は、状態遷移を再試行可能なentryを1件返す。
//
// 第3戻り値は、retry 可能な entry が見つかったかを表す。
// false の場合、path と entry はゼロ値を返す。
//
// retry 順序の厳密性は保証しない。
// processing file transition retryはbackend sendではなくspool状態遷移の回収なので、
// 最古 entry を選ぶために全件比較するより、ready な entry を見つけ次第返す。
func (r *ProcessingFileTransitionRetry) FindReady(
	now time.Time,
) (string, ProcessingFileTransitionRetryEntry, bool) {
	if r == nil {
		return "", ProcessingFileTransitionRetryEntry{}, false
	}

	for path, entry := range r.items {
		if !entry.NextRetryAt.IsZero() && now.Before(entry.NextRetryAt) {
			continue
		}

		return path, entry, true
	}

	return "", ProcessingFileTransitionRetryEntry{}, false
}

// Count は、現在保持しているprocessing file transition retry entry数を返す。
func (r *ProcessingFileTransitionRetry) Count() int {
	if r == nil {
		return 0
	}

	return len(r.items)
}

func (r *ProcessingFileTransitionRetry) retryDelay() time.Duration {
	if r == nil || r.cfg == nil {
		return 0
	}
	return r.cfg.RetryDelay()
}

// IsFull は、新しいprocessing file transition retry entryの追加上限に達しているかを返す。
//
// full の場合でも既存 entry の retry / 更新 / 削除は継続する。
func (r *ProcessingFileTransitionRetry) IsFull() bool {
	if r == nil {
		return false
	}
	if r.cfg == nil {
		return true
	}
	return len(r.items) >= r.cfg.MaxEntries()
}

// RunOnceProcessingFileTransitionRetryは、backend send後に遷移先が確定した
// processing fileの状態遷移retryを1件だけ実行します。
//
// backend send は再実行せず、ProcessingFileTransitionRetryOp に対応する
// store / move / remove操作だけを再試行します。
//
// redistribution retry の場合は、entry.Redistribution に保持された record 群を使い、
// 失敗した step 以降の再分配だけを再試行する。
//
// 戻り値の bool は、retry 対象が見つかり、処理を試みたかを表す。
// true は成功を意味しない。
// 成否は error と report.Err で判断する。
//
// 戻り値の意味:
//   - handled == false, err == nil
//     retry 対象がなかった。
//   - handled == true, err == nil
//     retry 対象があり、後処理 retry に成功した。
//   - handled == true, err != nil
//     retry 対象があり、後処理 retry に失敗した。
func (w *Worker) RunOnceProcessingFileTransitionRetry(
	ctx context.Context,
) (ProcessingFileTransitionRetryReport, bool, error) {
	report := ProcessingFileTransitionRetryReport{
		TargetBackendService: w.TargetBackendService,
		Status:               ProcessingFileTransitionRetryStatusNoTarget,
	}

	retry := w.processingFileTransitionRetry()

	// readinessの判定時刻を、後続I/Oの失敗時刻として使い回さない。
	// 再登録時に時刻を取り直し、遅いI/Oでも失敗後のRetryDelayを確保する。
	path, entry, found := retry.FindReady(time.Now())
	if !found {
		return report, false, nil
	}

	report.ProcessingPath = path
	report.Operation = entry.Operation

	exists, err := w.Store.ProcessingFileExists(ctx, path)
	if err != nil {
		report.Status = ProcessingFileTransitionRetryStatusFailed
		report.Err = err
		retry.Requeue(path, entry, err, time.Now())
		return report, true, err
	}

	if !exists && entry.Operation != ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution {
		_, isRedistributionOperation := processingRedistributionStartStep(entry.Operation)
		if !isRedistributionOperation || entry.Redistribution == nil {
			err := ErrProcessingFileTransitionRetryPathNotFound

			report.Status = ProcessingFileTransitionRetryStatusFailed
			report.Err = err

			// pathだけを使うmove操作や、record本体を持たない不完全な再分配entryは、
			// 元fileがなければ回復できない。alert / report に残したうえで、
			// 孤児entryとしてretry listから外す。
			w.unblockProcessingFileTransitionRetry(path)

			return report, true, err
		}

		// 再分配計画には未保存のrecord本体が残っているため、元のprocessing fileが
		// 先に消えていても計画を破棄してはいけない。各保存先への書き込みを続ける。
		// 最後のRemoveProcessingFileは実装契約上idempotentなので、そのまま実行できる。
	}

	switch entry.Operation {
	case ProcessingFileTransitionRetryOpMoveProcessingToDone:
		err = w.Store.MoveProcessingToDone(ctx, path)
		if err != nil {
			report.Status = ProcessingFileTransitionRetryStatusFailed
			report.Err = err
			retry.Requeue(path, entry, err, time.Now())
			return report, true, err
		}

	case ProcessingFileTransitionRetryOpMoveProcessingToDead:
		err = w.Store.MoveProcessingToDead(ctx, path)
		if err != nil {
			report.Status = ProcessingFileTransitionRetryStatusFailed
			report.Err = err
			retry.Requeue(path, entry, err, time.Now())
			return report, true, err
		}

	case ProcessingFileTransitionRetryOpMoveProcessingToQuarantine:
		err = w.Store.MoveProcessingToQuarantine(ctx, path, entry.QuarantineReason)
		if err != nil {
			report.Status = ProcessingFileTransitionRetryStatusFailed
			report.Err = err
			retry.Requeue(path, entry, err, time.Now())
			return report, true, err
		}

	case ProcessingFileTransitionRetryOpStoreRecordsToActive,
		ProcessingFileTransitionRetryOpStoreProcessingRecordsToDead,
		ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine,
		ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone:

		err = w.retryProcessingRedistribution(ctx, retry, path, entry, &report)
		if err != nil {
			report.Status = ProcessingFileTransitionRetryStatusFailed
			return report, true, err
		}

	case ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution:
		// 以前のremoveが成功してdurabilityの確認だけが失敗した場合は、fileがすでになくても
		// 親directoryのsyncが必要なことがあります。
		err = w.Store.RemoveProcessingFile(ctx, path)
		if err != nil {
			report.Status = ProcessingFileTransitionRetryStatusFailed
			report.Err = err
			retry.Requeue(path, entry, err, time.Now())
			return report, true, err
		}

	default:
		err = ErrUnknownProcessingFileTransitionRetryOp

		report.Status = ProcessingFileTransitionRetryStatusFailed
		report.Err = err

		// unknown op は worker 実装側の不整合であり、自動回復できない。
		// entry を消すと原因を追えなくなるため、retry entry を残して alert 対象にする。
		retry.Requeue(path, entry, err, time.Now())
		return report, true, err
	}

	report.Status = ProcessingFileTransitionRetryStatusSucceeded

	w.unblockProcessingFileTransitionRetry(path)
	return report, true, nil
}

func (w *Worker) retryProcessingRedistribution(
	ctx context.Context,
	retry *ProcessingFileTransitionRetry,
	path string,
	entry ProcessingFileTransitionRetryEntry,
	report *ProcessingFileTransitionRetryReport,
) error {
	plan := entry.Redistribution
	if plan == nil {
		err := ErrMissingProcessingRedistributionPlan
		report.Err = err

		// Store 系 operation なのに redistribution plan がないのは、
		// retry state の不整合であり、自動 retry では復旧できない。
		//
		// entryを削除するだけだと、後処理先が確定済みのprocessing fileが
		// 通常処理へ戻り、backend send が再実行される可能性がある。
		// そのため processing file を quarantine に隔離する。
		moveErr := w.Store.MoveProcessingToQuarantine(
			ctx,
			path,
			spool.QuarantineReasonInvalidTransitionState,
		)
		if moveErr != nil {
			// 元の不整合 error と、隔離失敗 error の両方を呼び出し元へ返す。
			report.Err = errors.Join(err, moveErr)

			// 隔離にも失敗した場合は、通常処理へ戻さないため entry を残す。
			// Operation / Redistribution は変えず、次回も隔離を試みる。
			// TODO: alert / metrics を追加する。
			retry.Requeue(path, entry, report.Err, time.Now())

			return report.Err
		}

		// 隔離できたので retry entry は削除する。
		w.unblockProcessingFileTransitionRetry(path)

		return err
	}

	startStep, ok := processingRedistributionStartStep(entry.Operation)
	if !ok {
		err := ErrUnknownProcessingFileTransitionRetryOp
		report.Err = err

		// redistribution plan は存在するが、operation が不明なため、
		// どの step から再分配を再開すべきか判断できない。
		//
		// このまま retry entry を残しても自動復旧できない。
		// 一方でentryを削除するだけだと、後処理先が確定済みのprocessing fileが
		// 通常処理へ戻り、backend send が再実行される可能性がある。
		// そのため processing file を quarantine に隔離する。
		moveErr := w.Store.MoveProcessingToQuarantine(
			ctx,
			path,
			spool.QuarantineReasonInvalidTransitionState,
		)
		if moveErr != nil {
			// 元の operation 不明 error と、隔離失敗 error の両方を返す。
			report.Err = errors.Join(err, moveErr)

			// 隔離にも失敗した場合は、通常処理へ戻さないため entry を残す。
			// Operation / Redistribution は変えず、次回も隔離を試みる。
			// TODO: alert / metrics を追加する。
			retry.Requeue(path, entry, report.Err, time.Now())

			return report.Err
		}

		// 隔離できたので retry entry は削除する。
		w.unblockProcessingFileTransitionRetry(path)

		return err
	}

	// active -> dead -> quarantine -> done -> remove の順に再分配を進める。
	//
	// startStep は、前回失敗した step を表す。
	// すでに成功済みの step は再実行しないことで、同じ record の二重保存を避ける。
	if startStep <= processingRedistributionStepActive {
		op := ProcessingFileTransitionRetryOpStoreRecordsToActive
		if len(plan.ToActiveRecords) > 0 {
			// report.Operation は、今回実際に試みている step を表す。
			// retry entry の元 operation から処理が進んだ場合でも、
			// 失敗位置を report で確認できるように更新する。
			report.Operation = op

			err := w.ActiveSpoolWriter.StoreActive(ctx, plan.ToActiveRecords)
			if err != nil {
				report.Err = err

				// activeへの保存に失敗したため、このstep以降をretry対象として残す。
				// この時点では後続の dead / quarantine / done も未処理なので plan に含める。
				retry.UpsertRedistribution(
					path,
					op,
					&ProcessingRedistributionPlan{
						ToActiveRecords:     plan.ToActiveRecords,
						ToDeadRecords:       plan.ToDeadRecords,
						ToQuarantineRecords: plan.ToQuarantineRecords,
						ToDoneRecords:       plan.ToDoneRecords,
					},
					err,
					time.Now(),
				)

				return err
			}
		}
	}

	if startStep <= processingRedistributionStepDead {
		op := ProcessingFileTransitionRetryOpStoreProcessingRecordsToDead
		if len(plan.ToDeadRecords) > 0 {
			report.Operation = op

			err := w.Store.StoreProcessingRecordsToDead(ctx, path, plan.ToDeadRecords)
			if err != nil {
				report.Err = err

				// activeへの保存対象recordは保存済みなのでplanから外す。
				// 次回は dead への保存から再開する。
				retry.UpsertRedistribution(
					path,
					op,
					&ProcessingRedistributionPlan{
						ToDeadRecords:       plan.ToDeadRecords,
						ToQuarantineRecords: plan.ToQuarantineRecords,
						ToDoneRecords:       plan.ToDoneRecords,
					},
					err,
					time.Now(),
				)

				return err
			}
		}
	}

	if startStep <= processingRedistributionStepQuarantine {
		op := ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine
		if len(plan.ToQuarantineRecords) > 0 {
			report.Operation = op

			err := w.Store.StoreProcessingRecordsToQuarantine(ctx, path, plan.ToQuarantineRecords)
			if err != nil {
				report.Err = err

				// active / deadへの保存対象recordは保存済みなのでplanから外す。
				// 次回は quarantine への保存から再開する。
				retry.UpsertRedistribution(
					path,
					op,
					&ProcessingRedistributionPlan{
						ToQuarantineRecords: plan.ToQuarantineRecords,
						ToDoneRecords:       plan.ToDoneRecords,
					},
					err,
					time.Now(),
				)

				return err
			}
		}
	}

	if startStep <= processingRedistributionStepDone {
		op := ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone
		if len(plan.ToDoneRecords) > 0 {
			report.Operation = op

			err := w.Store.StoreProcessingRecordsToDone(ctx, path, plan.ToDoneRecords)
			if err != nil {
				report.Err = err

				// active / dead / quarantineへの保存対象recordは保存済みなのでplanから外す。
				// 次回はdoneへの保存対象recordだけを再試行する。
				retry.UpsertRedistribution(
					path,
					op,
					&ProcessingRedistributionPlan{
						ToDoneRecords: plan.ToDoneRecords,
					},
					err,
					time.Now(),
				)

				return err
			}
		}
	}

	// ここまで到達した時点で、必要な再分配はすべて完了している。
	// 以後は元の processing file を削除するだけなので、
	// 次回 retry に records を持ち越す必要はない。
	report.Operation = ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution

	err := w.Store.RemoveProcessingFile(ctx, path)
	if err != nil {
		report.Err = err

		// 再分配済み records を二重保存しないよう、
		// 次回は RemoveProcessingFile だけを retry する。
		retry.Upsert(
			path,
			ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution,
			err,
			time.Now(),
		)

		return err
	}

	return nil
}

type processingRedistributionStep int

const (
	processingRedistributionStepActive processingRedistributionStep = iota
	processingRedistributionStepDead
	processingRedistributionStepQuarantine
	processingRedistributionStepDone
)

func processingRedistributionStartStep(
	op ProcessingFileTransitionRetryOp,
) (processingRedistributionStep, bool) {
	switch op {
	case ProcessingFileTransitionRetryOpStoreRecordsToActive:
		return processingRedistributionStepActive, true

	case ProcessingFileTransitionRetryOpStoreProcessingRecordsToDead:
		return processingRedistributionStepDead, true

	case ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine:
		return processingRedistributionStepQuarantine, true

	case ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone:
		return processingRedistributionStepDone, true

	default:
		return 0, false
	}
}
