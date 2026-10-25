package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/pkg/spool"
)

type ProcessStatus string

const (
	// ProcessStatusNoTargetは、今回のprocessing探索範囲で処理対象を選べず、
	// activeからもファイルを取得できなかったことを表します。
	// 探索件数の上限で中断した場合も含むため、滞留ファイルがないとは限りません。
	ProcessStatusNoTarget ProcessStatus = "no_target"

	// ProcessStatusSkipped は、
	// worker の制御上の理由で通常の read -> send を開始しなかったことを表す。
	// Retry-Afterの期限を保存したfileを読み込み、送信だけを控えた場合も含む。
	//
	// 詳細理由は SkipReason に入れる。
	ProcessStatusSkipped ProcessStatus = "skipped"

	// ProcessStatusCompleted は、
	// processing file を読み込み、backend send し、
	// send 結果に応じた後処理まで完了したことを表す。
	//
	// 全 record が success だったという意味ではない。
	// dead相当のpermanent、quarantine相当のinvalid、retryableを含む結果でも、
	// spool 上の分配処理まで完了していれば completed とする。
	// Sender が部分結果と error を返した場合は、Status が completed でも
	// Err と関数の戻り値にはその send error を残す。
	ProcessStatusCompleted ProcessStatus = "completed"

	// ProcessStatusQuarantined は、
	// processing file全体をquarantineへ隔離して処理を終えたことを表す。
	// send前のdecode不能だけでなく、send後にSenderの分類結果が構造的に不完全で
	// record単位へ安全に分割できなかった場合も含む。
	// 後者では隔離に成功していても、Errと戻り値に契約違反errorを残す。
	ProcessStatusQuarantined ProcessStatus = "quarantined"

	// ProcessStatusFailed は、
	// 通常処理中に error が発生し、今回の処理を完了できなかったことを表す。
	ProcessStatusFailed ProcessStatus = "failed"
)

const (
	// 一度に保持する候補数と、一周期で待機対象か確認する候補数の上限です。
	// 待機対象が多くても探索を一周期で占有せず、続きは次の周期へ引き継ぎます。
	// Store内のディレクトリ列挙件数そのものの上限ではありません。
	defaultProcessingCandidatePageSize      = 64
	defaultProcessingCandidatesPerSelection = 128
)

// RunOnceSpoolFileProcess は、spool file の通常送信処理を 1 回だけ実行する。
//
// この処理は processing file を読み込み、Sender へ送信し、
// 送信結果に応じて done / dead / quarantine / active へ振り分ける。
//
// processing file transition retry の実行はここでは行わない。
// ただし、各recordの後処理先またはfile全体の隔離を決定した後に
// store / move / removeへ失敗した場合は、
// 重複送信を避けるため ProcessingFileTransitionRetry に登録する。
//
// backendがRetry-Afterを返した場合は、その時刻まで送信先へのread -> sendを始めない。
// 同じspool rootへ書き込む通常送信で受け取った期限も、次の周期からすべてのfileに適用する。
// 期限はRetryAfterStoreへ保存し、再起動後は最初の送信より前に読み込む。保存に失敗した期限は
// 期限が残る間、以後の周期で保存を再試行する。
// 再送待ちへ戻すrecordにも期限を保存し、期限前のrecordを読み込んだ場合は
// fileをprocessingに残したまま送信を控える。
func (w *Worker) RunOnceSpoolFileProcess(ctx context.Context) (ProcessReport, error) {
	report, err := w.runOnceSpoolFileProcess(ctx)
	// 期限の読み込み時の短縮と、保存・削除の警告を一度だけこの周期のreportで通知する。
	// 同じ周期でbackendの指定を短縮した場合は、読み込み時の短縮を次の周期で通知する。
	clamp, warnings := w.retryAfterDeadline().takeReport(report.RetryAfterClamp == nil)
	if clamp != nil {
		report.RetryAfterClamp = clamp
	}
	report.RetryAfterMarkerErr = errors.Join(report.RetryAfterMarkerErr, warnings)
	return report, err
}

func (w *Worker) runOnceSpoolFileProcess(ctx context.Context) (ProcessReport, error) {
	report := ProcessReport{
		TargetBackendService: w.TargetBackendService,
		Status:               ProcessStatusNoTarget,
	}

	// 再起動前に保存した送信先の待機期限を、最初の送信より前に読み込む。
	// 読み込む前は、どのfileも読み込み・送信しない。保存できていない期限は保存を再試行する。
	deadline := w.retryAfterDeadline()
	if !deadline.isLoaded() {
		if err := ctx.Err(); err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err
			return report, err
		}
	}
	deadline.beginCycle(time.Now())

	// processing file transition retry が満杯の場合は、新しい backend send を進めない。
	//
	// 送信済みだが spool 上の最終状態へ反映できていない path を
	// これ以上増やさないためである。
	//
	// 既存の processing file transition retry は RunOnceProcessingFileTransitionRetry 側で回収する。
	// この関数では通常の read -> send 処理だけを止める。
	if w.processingFileTransitionRetry().IsFull() {
		report.Status = ProcessStatusSkipped
		report.SkipReason = ProcessSkipReasonProcessingFileTransitionRetryFull
		return report, nil
	}

	// backendがRetry-Afterで指定した時刻までは、この送信先への送信を始めない。
	// processing / activeのどのfileも同じ送信先へ送るため、読み込みやclaimも行わない。
	if notBefore, deferred := deadline.deferredUntil(time.Now()); deferred {
		report.Status = ProcessStatusSkipped
		report.SkipReason = ProcessSkipReasonRetryAfter
		report.NotBefore = notBefore
		return report, nil
	}

	var records []spool.SpoolRecord
	var path string
	var err error

	// まず processing 配下に残っているファイルから、実行可能なものを探す。
	// 失敗直後など backoff 中の path は selectProcessingPath 内でスキップされる。
	path, ok, err := w.selectProcessingPath(ctx)
	if err != nil {
		report.Status = ProcessStatusFailed
		report.Err = err
		return report, err
	}

	if ok {
		// processing に既存の処理候補があった場合は、それを優先して処理する。
		report.ProcessingPath = path

		var readOK bool
		records, readOK, err = w.readProcessingRecordsOrQuarantine(ctx, path, &report)
		if err != nil {
			report.Status = ProcessStatusFailed
			return report, err
		}
		if !readOK {
			// 壊れたファイルを quarantine へ移動した場合など、
			// backend send 対象の record がない場合は正常終了する。
			report.Status = ProcessStatusQuarantined
			return report, nil
		}
	} else {
		// 今回の探索範囲でprocessingの候補を選べなければ、
		// activeから最古のファイルをclaimする。
		// 探索件数の上限で中断した場合は、次の周期にprocessingの続きも確認する。
		//
		// claim 後のファイルは processing として扱われる。
		path, ok, err = w.Store.ClaimOldestActiveFile(ctx)
		if err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err
			return report, err
		}
		if !ok {
			// active にも処理対象がなければ、何もせず終了する。
			return report, nil
		}

		report.ProcessingPath = path

		var readOK bool
		records, readOK, err = w.readProcessingRecordsOrQuarantine(ctx, path, &report)
		if err != nil {
			report.Status = ProcessStatusFailed
			return report, err
		}
		if !readOK {
			// 壊れたファイルを quarantine へ移動した場合など、
			// backend send 対象の record がない場合は正常終了する。
			report.Status = ProcessStatusQuarantined
			return report, nil
		}
	}

	// A previous process may have saved its terminal outcome but failed to
	// remove this processing file. Reconcile that durable decision before send.
	if recovery, ok := w.Store.(ProcessingTerminalReconciler); ok {
		state, err := recovery.ReconcileProcessingTerminal(ctx, path, records)
		if err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err
			if errors.Is(err, spool.ErrRemoveProcessingSpoolFile) {
				// The saved decision was verified and synced. Even if unlink
				// succeeded, its parent sync may need retry after the path vanished.
				w.blockProcessingFileTransitionRetry(path, ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution, err)
			} else if !isContextError(err) {
				w.blockProcessingPath(path, ProcessingPathBackoffOpReconcileProcessingTerminal, err)
			}
			return report, err
		}
		if state != "" {
			switch state {
			case spool.StateDone:
				report.Status = ProcessStatusCompleted
				report.SuccessRecordCount = len(records)
			case spool.StateDead:
				report.Status = ProcessStatusCompleted
				report.DeadRecordCount = len(records)
			case spool.StateQuarantine:
				report.Status = ProcessStatusQuarantined
				report.QuarantineRecordCount = len(records)
			default:
				err := fmt.Errorf("unexpected recovered spool state: %q", state)
				report.Status, report.Err = ProcessStatusFailed, err
				return report, err
			}
			w.unblockProcessingPath(path)
			return report, nil
		}
	}

	// recordに保存された期限は、その時刻まで送信しない。Retry-Afterは送信先に対する指定なので、
	// 送信先全体の待機として記録し、保存する。読み込みの間に通常送信の保存処理が記録した期限も
	// 送信の直前に確認する。fileはprocessingに残し、期限後に通常のprocessing候補として送信する。
	// 上限を超えて先の期限は上限までに短縮し、reportで通知する。
	now := time.Now()
	notBefore, clamp := deadline.foldStored(latestSpoolRecordNotBefore(records), now)
	if notBefore.IsZero() {
		notBefore, _ = deadline.deferredUntil(now)
	}
	if !notBefore.IsZero() {
		report.Status = ProcessStatusSkipped
		report.SkipReason = ProcessSkipReasonRetryAfter
		report.NotBefore = notBefore
		if clamp != nil {
			report.RetryAfterClamp = clamp
		}
		return report, nil
	}

	// 読み込んだrecordをbackendへ送信する。
	// Senderは各recordをsuccess / retryable / permanent（dead）/ invalid（quarantine）に
	// 分類した結果を返す。errorがあっても、分類済みの部分結果は捨てない。
	exportReport, sendErr := w.Sender.Send(ctx, records)

	// Sender reportを信用してprocessing fileを分解・削除する前に、入力と同じ数の
	// outcomeがあり、各indexがいずれか1つの結果categoryへ分類されたことを確認する。
	// 4分類で全件を説明できないreportは安全に分割できないため、
	// 元のprocessing file全体をquarantineへ隔離する。
	if validationErr := exportReport.Validate(len(records)); validationErr != nil {
		err := errors.Join(
			sendErr,
			fmt.Errorf("validate sender export report: %w", validationErr),
		)
		err = w.quarantineInvalidExportReport(ctx, path, &report, err)
		return report, err
	}

	// outcomeが明示するInputIndexから、Workerが保持している元recordへ
	// statusと再送試行時刻を適用して、spool上の後処理先ごとに分配する。
	redistribution := processingRedistributionPlanFromOutcomes(records, exportReport)

	// backendがRetry-Afterを指定した場合は、応答を受け取った後の時刻から数えて
	// この送信先への次回送信を控える。待機は上限までに短縮し、短縮した場合はreportで通知する。
	// 期限はrecordの後処理より先に保存し、再起動後も指定時刻より前に再送しない。
	// 再送待ちへ戻すrecordにも期限を保存する。
	// 送信を試みたfileのrecordが持つ以前の期限は無効なため、新しい期限で置き換える。
	// done / dead / quarantineへ保存するrecordは変更せず、再起動時の保存結果の照合を保つ。
	notBefore = time.Time{}
	if throttled, ok := exportReport.LongestRetryAfter(); ok {
		limit := w.retryAfterLimit()
		wait, capped := spool.CapRetryAfter(throttled.RetryAfter, limit)
		notBefore = deadline.deferUntil(time.Now().Add(wait))
		report.NotBefore = notBefore
		if capped {
			report.RetryAfterClamp = &spool.RetryAfterClamp{
				Requested:     throttled.RetryAfter,
				MaxRetryAfter: limit,
				NotBefore:     notBefore,
			}
		}
	}
	setSpoolRecordsNotBefore(redistribution.ToActiveRecords, notBefore)

	report.SuccessRecordCount = len(redistribution.ToDoneRecords)
	report.RetryRecordCount = len(redistribution.ToActiveRecords)
	report.DeadRecordCount = len(redistribution.ToDeadRecords)
	report.QuarantineRecordCount = len(redistribution.ToQuarantineRecords)

	// retryableは、backendが再送可能と判定したrecordだけでなく、
	// context終了で処理を中断したrecord、未送信recordも含む。
	// 応答の解析失敗や対応付け不能はinvalidとしてquarantineへ分配する。
	toActiveRecords := redistribution.ToActiveRecords

	// processing fileの状態遷移に失敗した場合も、先に発生したsend errorを失わない。
	// 状態遷移が完了した場合はStatusをcompletedのまま保ち、send errorだけを返す。
	finishReport := func(transitionErr error) (ProcessReport, error) {
		combinedErr := errors.Join(sendErr, transitionErr)
		if combinedErr != nil {
			report.Err = combinedErr
		}
		return report, combinedErr
	}

	if len(redistribution.ToDoneRecords) == len(records) && len(records) > 0 {
		if err := w.transitionProcessingRecordsToTerminalState(
			ctx,
			path,
			spool.StateDone,
			redistribution.ToDoneRecords,
			&report,
		); err != nil {
			return finishReport(err)
		}

		return finishReport(nil)
	}

	if len(redistribution.ToDeadRecords) == len(records) && len(records) > 0 {
		if err := w.transitionProcessingRecordsToTerminalState(
			ctx,
			path,
			spool.StateDead,
			redistribution.ToDeadRecords,
			&report,
		); err != nil {
			return finishReport(err)
		}

		return finishReport(nil)
	}

	if len(redistribution.ToQuarantineRecords) == len(records) && len(records) > 0 {
		if err := w.transitionProcessingRecordsToTerminalState(
			ctx,
			path,
			spool.StateQuarantine,
			redistribution.ToQuarantineRecords,
			&report,
		); err != nil {
			return finishReport(err)
		}

		return finishReport(nil)
	}

	if len(toActiveRecords) == len(records) && len(records) > 0 {
		err := w.ActiveSpoolWriter.StoreActive(ctx, toActiveRecords)
		if err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err

			// Sender.Send 後なので、通常の processing path backoff には戻さない。
			// 再送が必要なrecordのactive保存だけをretryする。
			w.blockProcessingFileTransitionRetryWithRedistribution(
				path,
				ProcessingFileTransitionRetryOpStoreRecordsToActive,
				&ProcessingRedistributionPlan{
					ToActiveRecords: toActiveRecords,
				},
				err,
			)

			return finishReport(err)
		}

		err = w.Store.RemoveProcessingFile(ctx, path)
		if err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err

			w.blockProcessingFileTransitionRetry(
				path,
				ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution,
				err,
			)

			return finishReport(err)
		}

		report.Status = ProcessStatusCompleted
		return finishReport(nil)
	}

	// ここからは、success / retryable / permanent（dead）/ invalid（quarantine）が
	// 混在しているケース。
	//
	// この時点で各recordのspool上の後処理先は確定している。
	// 以降の失敗はbackend sendではなく、processing fileの状態遷移の失敗である。
	// そのため ProcessingPathBackoff ではなく ProcessingFileTransitionRetry に積む。
	finalizedAt := time.Now()

	toDeadRecords := finalizeSpoolRecords(redistribution.ToDeadRecords, finalizedAt)
	toQuarantineRecords := finalizeSpoolRecords(redistribution.ToQuarantineRecords, finalizedAt)
	toDoneRecords := finalizeSpoolRecords(redistribution.ToDoneRecords, finalizedAt)

	if len(toActiveRecords) > 0 {
		err := w.ActiveSpoolWriter.StoreActive(ctx, toActiveRecords)
		if err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err

			// retryable の active 保存に失敗したため、
			// この step 以降に必要な record 群を保持する。
			w.blockProcessingFileTransitionRetryWithRedistribution(
				path,
				ProcessingFileTransitionRetryOpStoreRecordsToActive,
				&ProcessingRedistributionPlan{
					ToActiveRecords:     toActiveRecords,
					ToDeadRecords:       toDeadRecords,
					ToQuarantineRecords: toQuarantineRecords,
					ToDoneRecords:       toDoneRecords,
				},
				err,
			)

			return finishReport(err)
		}
	}

	if len(toDeadRecords) > 0 {
		err := w.Store.StoreProcessingRecordsToDead(ctx, path, toDeadRecords)
		if err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err

			// activeへの保存対象recordはすでに保存済み。
			// 二重保存を避けるため、失敗した dead 以降の record だけを保持する。
			w.blockProcessingFileTransitionRetryWithRedistribution(
				path,
				ProcessingFileTransitionRetryOpStoreProcessingRecordsToDead,
				&ProcessingRedistributionPlan{
					ToDeadRecords:       toDeadRecords,
					ToQuarantineRecords: toQuarantineRecords,
					ToDoneRecords:       toDoneRecords,
				},
				err,
			)

			return finishReport(err)
		}
	}

	if len(toQuarantineRecords) > 0 {
		err := w.Store.StoreProcessingRecordsToQuarantine(ctx, path, toQuarantineRecords)
		if err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err

			// active / deadへの保存対象recordはすでに保存済み。
			// 失敗した quarantine 以降だけを retry 対象にする。
			w.blockProcessingFileTransitionRetryWithRedistribution(
				path,
				ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine,
				&ProcessingRedistributionPlan{
					ToQuarantineRecords: toQuarantineRecords,
					ToDoneRecords:       toDoneRecords,
				},
				err,
			)

			return finishReport(err)
		}
	}

	if len(toDoneRecords) > 0 {
		err := w.Store.StoreProcessingRecordsToDone(ctx, path, toDoneRecords)
		if err != nil {
			report.Status = ProcessStatusFailed
			report.Err = err

			// active / dead / quarantineへの保存対象recordはすでに保存済み。
			// doneへの保存対象recordだけをretry対象にする。
			w.blockProcessingFileTransitionRetryWithRedistribution(
				path,
				ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone,
				&ProcessingRedistributionPlan{
					ToDoneRecords: toDoneRecords,
				},
				err,
			)

			return finishReport(err)
		}
	}

	err = w.Store.RemoveProcessingFile(ctx, path)
	if err != nil {
		report.Status = ProcessStatusFailed
		report.Err = err

		w.blockProcessingFileTransitionRetry(
			path,
			ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution,
			err,
		)

		return finishReport(err)
	}

	report.Status = ProcessStatusCompleted
	return finishReport(nil)
}

// processingRedistributionPlanFromOutcomesは、outcomeが明示するInputIndexを使って
// Workerが保持する元recordへ結果を適用し、spool上の後処理先ごとに分配します。
//
// 呼び出し前にExportReport.ValidateでInputIndexの範囲・重複・欠落とstatusを
// 検証済みであることを前提とします。reportの並び順にかかわらず、入力index順へ
// 正規化してから分配するため、各保存先でも元の入力順を維持します。
// Senderはrecord本体を返さず、Workerだけが元recordへ再送試行metadataを反映するため、
// record内容から元入力を推測する照合や、Senderが返したcopyの採用は不要です。
func processingRedistributionPlanFromOutcomes(
	records []spool.SpoolRecord,
	outcomes spool.ExportReport,
) ProcessingRedistributionPlan {
	var plan ProcessingRedistributionPlan
	outcomesByInputIndex := make([]spool.RecordOutcome, len(records))
	for _, outcome := range outcomes {
		outcomesByInputIndex[outcome.InputIndex] = outcome
	}

	for inputIndex, outcome := range outcomesByInputIndex {
		record := applyRecordOutcome(records[inputIndex], outcome)

		switch outcome.Status {
		case spool.RecordOutcomeStatusSuccess:
			plan.ToDoneRecords = append(plan.ToDoneRecords, record)
		case spool.RecordOutcomeStatusRetryable:
			plan.ToActiveRecords = append(plan.ToActiveRecords, record)
		case spool.RecordOutcomeStatusPermanent:
			plan.ToDeadRecords = append(plan.ToDeadRecords, record)
		case spool.RecordOutcomeStatusInvalid:
			plan.ToQuarantineRecords = append(plan.ToQuarantineRecords, record)
		}
	}

	return plan
}

// applyRecordOutcomeは最新の失敗理由を反映し、backendへの再送を実際に試みたrecordだけに
// resend attempt metadataを加算します。zero AttemptedAtは未送信を表すため、失敗理由以外の
// attempt metadataは変更しません。
func applyRecordOutcome(
	record spool.SpoolRecord,
	outcome spool.RecordOutcome,
) spool.SpoolRecord {
	if outcome.Err != nil {
		record.LastFailureReason = outcome.Err.Error()
	}
	if outcome.AttemptedAt.IsZero() {
		return record
	}

	if record.FirstResendAttemptedAt.IsZero() {
		record.FirstResendAttemptedAt = outcome.AttemptedAt
	}
	record.LastResendAttemptedAt = outcome.AttemptedAt
	record.ResendAttemptCount++
	return record
}

// setSpoolRecordsNotBeforeは、再送待ちへ戻すrecordのNotBeforeをnotBeforeに置き換えます。
// Retry-Afterは送信先に対する指定なので、未送信のrecordも同じ期限まで送信しません。
// 送信を試みた時点で以前の期限は到来済みか上限まで短縮済みのため、zeroなら期限を削除します。
// 永続化用の値はmonotonic clockを含めません。
func setSpoolRecordsNotBefore(records []spool.SpoolRecord, notBefore time.Time) {
	notBefore = notBefore.Round(0)
	for i := range records {
		records[i].NotBefore = notBefore
	}
}

// latestSpoolRecordNotBeforeは、recordsのNotBeforeのうち最も遅い時刻を返します。
// 期限を保存したrecordが一件でもあれば、file全体をその時刻まで送信しません。
func latestSpoolRecordNotBefore(records []spool.SpoolRecord) time.Time {
	var latest time.Time
	for _, record := range records {
		if record.NotBefore.After(latest) {
			latest = record.NotBefore
		}
	}
	return latest
}

// quarantineInvalidExportReportは、Sender.Send後に構造不正が判明したreportについて、
// 分類を推測してprocessing fileを分割せず、元file全体をquarantineへ隔離します。
//
// backend sendはすでに実行されているため、quarantine移動に失敗した場合は通常の
// path backoffへ戻しません。移動だけをprocessing file transition retryへ登録し、同じrecordの
// backend送信が再実行されることを防ぎます。
func (w *Worker) quarantineInvalidExportReport(
	ctx context.Context,
	path string,
	report *ProcessReport,
	reportErr error,
) error {
	// 他の分類countと同様に、保存操作の成否ではなく、このRunOnceで決定した
	// spool上の後処理先を表す。移動失敗時もfile全体はquarantine対象のままである。
	report.QuarantineRecordCount = report.RecordCount
	report.QuarantineReason = spool.QuarantineReasonInvalidExportReport

	moveErr := w.Store.MoveProcessingToQuarantine(
		ctx,
		path,
		spool.QuarantineReasonInvalidExportReport,
	)
	if moveErr != nil {
		err := errors.Join(reportErr, moveErr)
		report.Status = ProcessStatusFailed
		report.Err = err

		w.blockProcessingFileTransitionRetryForQuarantine(
			path,
			spool.QuarantineReasonInvalidExportReport,
			err,
		)
		return err
	}

	w.unblockProcessingPath(path)
	report.Status = ProcessStatusQuarantined
	report.Err = reportErr
	return reportErr
}

// selectProcessingPathは、前回確認した相対パスの次から、送信可能な候補を一件選びます。
// 待機対象は候補の実際のパスで照合し、登録件数から先頭の除外範囲を推測しません。
// 確認した位置は待機期限とは独立に保持します。末尾に到達した場合は先頭へ戻り、
// 前回より古いファイルや、新しく置かれた古い名前のファイルも再確認します。
// 一回の選択では確認件数と折り返し回数に上限を設け、未確認の続きは次の周期へ渡します。
func (w *Worker) selectProcessingPath(ctx context.Context) (string, bool, error) {
	// 同じ選択内では待機期限の判定時刻を固定します。
	now := time.Now()
	// 先頭から開始した場合は既に全範囲を探すため、その選択内では折り返しません。
	canWrap := w.processingScanAfter != ""
	checked := 0
	for checked < defaultProcessingCandidatesPerSelection {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		limit := min(defaultProcessingCandidatePageSize, defaultProcessingCandidatesPerSelection-checked)
		files, err := w.Store.ListProcessingFileCandidatesAfter(ctx, w.processingScanAfter, limit)
		if err != nil {
			// 一覧取得に失敗した範囲を未確認のまま飛ばさず、次回も同じ境界から探します。
			return "", false, err
		}
		if len(files) == 0 {
			w.processingScanAfter = ""
			if !canWrap {
				return "", false, nil
			}
			canWrap = false
			continue
		}
		for _, file := range files {
			if err := ctx.Err(); err != nil {
				return "", false, err
			}
			// 読み込みや送信に失敗しても後続へ進めるよう、候補を確認した時点で記録します。
			// 未確認の残りの候補は保持せず、次回この境界より後から取得し直します。
			w.processingScanAfter = file.RelativePath
			checked++
			if w.shouldSkipProcessingPathForNormalSend(file.Path, now) {
				continue
			}
			return file.Path, true, nil
		}
		if len(files) < limit {
			// 取得上限に満たなければ、この一覧取得時点で末尾まで確認済みです。
			// 末尾確認だけの再走査は行わず、必要なら先頭へ折り返します。
			w.processingScanAfter = ""
			if !canWrap {
				return "", false, nil
			}
			canWrap = false
		}
	}
	return "", false, nil
}

// readProcessingRecordsOrQuarantine は、processing ファイルから record を読み込む。
//
// spool file 自体が壊れている、または decode 不能な場合は、
// 同じファイルを何度も読み込まないよう quarantine へ移動する。
//
// それ以外の読み込み失敗は、一時的な障害の可能性があるため
// path を backoff して呼び出し元へ error を返す。
func (w *Worker) readProcessingRecordsOrQuarantine(
	ctx context.Context,
	path string,
	report *ProcessReport,
) ([]spool.SpoolRecord, bool, error) {
	records, err := w.Store.ReadProcessingRecords(ctx, path)
	if err == nil {
		report.RecordCount = len(records)
		return records, true, nil
	}

	report.Err = err

	// context cancellation / timeout は path 固有の詰まりではない。
	// backoff に入れると、キャンセル時に無関係な path を skip 対象にしてしまう。
	if isContextError(err) {
		return nil, false, err
	}

	// spool file 自体が壊れている、または decode 不能な場合は quarantine へ隔離する。
	//
	// ErrDecodeProcessingSpoolFile 単独でも、
	// 「file は読めたが records として復元できない」という意味なので、
	// processing に残して再試行しても基本的に直らない。
	if shouldQuarantineProcessingReadError(err) {
		return w.quarantineUnreadableProcessingFile(ctx, path, report)
	}

	// ここから下は、spool file の内容不正とは断定しない。
	// read / validate / operation / unknown failure は、processing に残したまま path backoff する。
	w.blockProcessingPath(path, ProcessingPathBackoffOpReadProcessingRecords, err)
	return nil, false, err
}

func (w *Worker) quarantineUnreadableProcessingFile(
	ctx context.Context,
	path string,
	report *ProcessReport,
) ([]spool.SpoolRecord, bool, error) {
	report.QuarantineReason = spool.QuarantineReasonInvalidSpoolFile
	moveErr := w.Store.MoveProcessingToQuarantine(
		ctx,
		path,
		spool.QuarantineReasonInvalidSpoolFile,
	)
	if moveErr != nil {
		report.Err = moveErr

		// context cancellation / timeout は path 固有の失敗ではない。
		if isContextError(moveErr) {
			return nil, false, moveErr
		}

		// decode 不能な file が processing に残り続けるため、
		// quarantine 移動に失敗した場合は backoff する。
		w.blockProcessingPath(path, ProcessingPathBackoffOpMoveInvalidProcessingToQuarantine, moveErr)
		return nil, false, moveErr
	}

	// quarantine 済みなので、以後この path を避ける必要はない。
	w.unblockProcessingPath(path)
	return nil, false, nil
}

func shouldQuarantineProcessingReadError(err error) bool {
	return errors.Is(err, spool.ErrInvalidSpoolFile) ||
		errors.Is(err, spool.ErrDecodeProcessingSpoolFile)
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// blockProcessingPath は、指定 path を一時的に処理対象から外す。
//
// 失敗した操作種別と error を backoff に記録しておくことで、
// 次回以降の selectProcessingPath で同じ path を短時間スキップできる。
func (w *Worker) blockProcessingPath(
	path string,
	op ProcessingPathBackoffOp,
	err error,
) {
	w.processingPathBackoff().BlockPath(path, op, err, time.Now())
}

// unblockProcessingPath は、指定 path の backoff 状態を解除する。
//
// 処理完了、または quarantine への隔離成功など、以後その path を避け続ける必要がない場合に呼ぶ。
func (w *Worker) unblockProcessingPath(path string) {
	w.processingPathBackoff().UnblockPath(path)
}

// processingPathBackoff は、Worker が使う ProcessingPathBackoff を返す。
//
// Worker に明示的な ProcessingPathBackoff が設定されていない場合は、
// デフォルト設定で遅延初期化する。
func (w *Worker) processingPathBackoff() *ProcessingPathBackoff {
	if w.ProcessingPathBackoff == nil {
		w.ProcessingPathBackoff = NewProcessingPathBackoff(ProcessingPathBackoffConfig{
			MaxPathCacheEntries:  defaultProcessingBackoffMaxPathCacheEntries,
			PathSkipHoldDuration: defaultProcessingPathSkipHoldDuration,
		})
	}

	return w.ProcessingPathBackoff
}

// shouldSkipProcessingPathForNormalSend は、通常の read -> send 処理に
// 流してはいけない processing path なら true を返す。
func (w *Worker) shouldSkipProcessingPathForNormalSend(path string, now time.Time) bool {
	if path == "" {
		return false
	}

	// processing file transition retry にある path は、Sender.Send の結果がすでに確定済み、
	// または再分配済みの後処理中である。
	// 通常 send に戻してはいけないため、最優先で skip する。
	if w.processingFileTransitionRetry().Contains(path) {
		return true
	}

	// path backoff は未送信 path の一時 skip。
	// 時間で clear されてよい。
	if w.processingPathBackoff().ShouldSkipPath(path, now) {
		return true
	}

	return false
}

// transitionProcessingRecordsToTerminalState は、Sender.Send後に
// terminal state へ確定した records を保存し、元の processing file を削除する。
//
// この関数は backend send を再実行しない。
// Sender.Send の結果はすでに確定済みであり、ここで扱う失敗は
// done / dead / quarantine への保存、または processing file 削除の失敗である。
//
// records には、この時点で terminal state に入れることが確定した record 群を渡す。
// 保存前に FinalizedAt を設定し、保存失敗時の retry plan にも
// FinalizedAt 設定済みの records を保持する。
func (w *Worker) transitionProcessingRecordsToTerminalState(
	ctx context.Context,
	path string,
	toState spool.State,
	records []spool.SpoolRecord,
	report *ProcessReport,
) error {
	if len(records) == 0 {
		err := errors.New("terminal transition records are empty")

		report.Status = ProcessStatusFailed
		report.Err = err

		return err
	}

	// この時点で Worker は record の terminal state を確定している。
	// そのため Store に渡す前に FinalizedAt を設定する。
	finalizedRecords := finalizeSpoolRecords(records, time.Now())

	// 保存に失敗した場合、同じ terminal state 保存だけを retry できるよう、
	// retry operation と retry 対象 records を先に組み立てる。
	op, plan, err := processingTerminalStateRetryPlan(toState, finalizedRecords)
	if err != nil {
		report.Status = ProcessStatusFailed
		report.Err = err
		return err
	}

	// Store は state の意味を判断しない。
	// Worker が決めた terminal state に、FinalizedAt 設定済み records を保存させる。
	switch toState {
	case spool.StateDone:
		err = w.Store.StoreProcessingRecordsToDone(ctx, path, finalizedRecords)

	case spool.StateDead:
		err = w.Store.StoreProcessingRecordsToDead(ctx, path, finalizedRecords)

	case spool.StateQuarantine:
		err = w.Store.StoreProcessingRecordsToQuarantine(ctx, path, finalizedRecords)

	default:
		err = errors.New("invalid terminal state for processing file transition")
	}

	if err != nil {
		report.Status = ProcessStatusFailed
		report.Err = err

		// Sender.Send 後なので、backend send は再実行しない。
		// terminal state への保存だけを processing file transition retry に積む。
		w.blockProcessingFileTransitionRetryWithRedistribution(
			path,
			op,
			plan,
			err,
		)

		return err
	}

	// terminal state への保存は完了している。
	// 以後、元の processing file は再送対象として残す必要がない。
	err = w.Store.RemoveProcessingFile(ctx, path)
	if err != nil {
		report.Status = ProcessStatusFailed
		report.Err = err

		// records の保存は完了しているため、次回 retry では
		// processing file の削除だけを再試行する。
		w.blockProcessingFileTransitionRetry(
			path,
			ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution,
			err,
		)

		return err
	}

	report.Status = ProcessStatusCompleted
	return nil
}

func processingTerminalStateRetryPlan(
	toState spool.State,
	records []spool.SpoolRecord,
) (
	ProcessingFileTransitionRetryOp,
	*ProcessingRedistributionPlan,
	error,
) {
	switch toState {
	case spool.StateDone:
		return ProcessingFileTransitionRetryOpStoreProcessingRecordsToDone,
			&ProcessingRedistributionPlan{
				ToDoneRecords: records,
			},
			nil

	case spool.StateDead:
		return ProcessingFileTransitionRetryOpStoreProcessingRecordsToDead,
			&ProcessingRedistributionPlan{
				ToDeadRecords: records,
			},
			nil

	case spool.StateQuarantine:
		return ProcessingFileTransitionRetryOpStoreProcessingRecordsToQuarantine,
			&ProcessingRedistributionPlan{
				ToQuarantineRecords: records,
			},
			nil

	default:
		return "", nil, errors.New("invalid terminal state for processing file transition")
	}
}

func finalizeSpoolRecords(
	records []spool.SpoolRecord,
	finalizedAt time.Time,
) []spool.SpoolRecord {
	if len(records) == 0 {
		return nil
	}

	out := make([]spool.SpoolRecord, len(records))
	copy(out, records)

	for i := range out {
		out[i].FinalizedAt = finalizedAt
	}

	return out
}
