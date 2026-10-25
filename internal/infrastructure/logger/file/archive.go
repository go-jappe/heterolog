package file

import (
	"context"
	"errors"
	"fmt"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
)

// tryArchiveRotatedFile は通常 FileLogger のローテーション済みファイルを圧縮し、必要に応じてアップロードします。
// oldFilePath は直前の古いファイル名を想定します。
// 圧縮・送信・削除の失敗は戻り値で返さず、OnErrorへ通知して再開処理や再走査の予約へ引き継ぎます。
func (l *FileLogger) tryArchiveRotatedFile(
	ctx context.Context,
	oldFilePath string,
	fileNameSnapshot logpattern.FileNamePattern,
) {
	// rotation task外から直接呼び出す場合は、compressionによる一連の
	// directory mutationを一回のcache無効化にまとめます。rotation taskは
	// 保護解除後のcleanupに無効化を集約するため、下位methodを直接使います。
	if l.inlineLifecycleEnabled() && oldFilePath != "" &&
		l.config.Fsstore().Lifecycle().EnableFileCompress() {
		defer l.inlinePurge.invalidateDirectorySizeEstimate()
	}
	archiveCompleted := false
	if l.shouldScheduleLogFileRescanAfterPanic(oldFilePath) {
		defer func() {
			if !archiveCompleted {
				// 圧縮・送信などがpanicで中断しても、残ったログファイルを
				// 次のログ書き込み後のcleanupで再発見できるよう予約します。
				l.scheduleSeriesUploadRetry()
			}
		}()
	}
	retryRequired := l.archiveRotatedFileWithoutEstimateInvalidation(ctx, oldFilePath, fileNameSnapshot)
	archiveCompleted = true
	if retryRequired {
		l.scheduleSeriesUploadRetry()
	}
}

// shouldScheduleLogFileRescanAfterPanicは、panicで処理が止まった場合に、
// 残ったログファイルを探して処理を再開するための再走査を予約すべきか返します。
// 例えば、送信中のpanicで圧縮ファイルが残った場合に、そのファイルを放置しないための判定です。
//
// 処理を始める前に呼びます。trueで、実際にpanicで中断した場合、呼び出し元が再試行時刻を記録します。
// その時刻を過ぎた後のログ書き込みをきっかけに、ログ保存先を調べて未完了の処理を再開します。
//
// 対象パスが空でなく、inlineで送信を使う場合が対象です。圧縮が有効なら圧縮ファイルを扱い、
// 圧縮が無効なら、未圧縮ファイルの送信完了を確認するために永続checkpointを必要とします。
// この関数は判定だけを行い、再試行時刻の記録や送信は呼び出し元で行います。
func (l *FileLogger) shouldScheduleLogFileRescanAfterPanic(oldFilePath string) bool {
	return l != nil && l.inlinePurge != nil && oldFilePath != "" &&
		l.ArchiveUploader != nil && l.inlineLifecycleEnabled() &&
		(l.config.Fsstore().Lifecycle().EnableFileCompress() ||
			l.usesPersistentArchiveUploadCheckpoint(l.archiveUploadDestinationsSnapshot()))
}

// scheduleSeriesUploadRetryは、残存ログを再走査する次の試行時刻を記録します。
// 待機時間を過ぎた後のログ書き込みで再走査し、未完了の送信・退避・削除を進めます。
func (l *FileLogger) scheduleSeriesUploadRetry() {
	_ = l.scheduleSeriesUploadRetryContext(context.Background())
}

// scheduleSeriesUploadRetryContextは、残存ログを再走査する次の試行時刻を記録します。
// 記録する時刻は現在のClockの時刻＋UsageCacheTTLです。既存の早い予約は維持します。
// UsageCacheTTLを再試行の待機間隔として使います。ログ書き込みのたびに再走査や後続処理を
// 試すことは可能ですが、高頻度の実行による不要なI/O負荷や通知量を抑えるために間隔を空けます。
// 例えば待機時間が15分なら、12:00の失敗後は12:15以降のログ書き込みで再走査します。
// 12:15ちょうどに自動実行するタイマーではなく、ログ書き込み時に確認する待機期限です。
// directoryのlifecycle lock内で記録し、並行する走査による予約解除との順序を揃えます。
// ctxはlock取得待ちの中止に使い、取得できなければ予約を変更せずエラーを返します。
func (l *FileLogger) scheduleSeriesUploadRetryContext(ctx context.Context) error {
	if l == nil || l.inlinePurge == nil {
		return nil
	}
	return l.lifecycleGuard.RunWithLifecycleLockContext(ctx, func() error {
		l.inlinePurge.seriesUploadRetry.scheduleRetry(
			l.Clock.Now(),
			l.inlinePurge.config.UsageCacheTTL(),
		)
		return nil
	})
}

// archiveRotatedFileWithoutEstimateInvalidationは、rotation task中のcompression・upload・
// upload後削除を実行します。directory estimateの無効化はcallerが一回に
// 集約します。post-rotation観測はpending uploadを一切再試行しないため、
// このmethodからupload対象pathを引き継ぎません。
func (l *FileLogger) archiveRotatedFileWithoutEstimateInvalidation(
	ctx context.Context,
	oldFilePath string,
	fileNameSnapshot logpattern.FileNamePattern,
) bool {
	return l.archiveRotatedFileWithNotifications(ctx, oldFilePath, fileNameSnapshot, runNotificationImmediately)
}

// archiveRotatedFileWithNotificationsはrotation後の圧縮・送信・削除を実行し、内部ログ通知をnotifyへ渡します。
// rotation taskの呼び出し元はqueue.enqueueを渡し、task完了後に通知します。
// 未完了の圧縮・送信・保存・削除は再開処理へ引き継ぎます。
// 戻り値は既存のseries観測scheduleも必要であることを表します。
func (l *FileLogger) archiveRotatedFileWithNotifications(
	ctx context.Context,
	oldFilePath string,
	fileNameSnapshot logpattern.FileNamePattern,
	notify taskNotificationDelivery,
) bool {
	if !l.inlineLifecycleEnabled() {
		return false
	}
	// callerはrotation成功時に確定したold pathを渡します。ここで可変なActiveFilePathと
	// 再比較すると、次のrotationによる書き換えとdata raceになるためsnapshotだけを使います。
	if oldFilePath == "" {
		return false
	}

	var operation *acceptedLogFileProcessingOperation
	var uploadPath string
	var compressed bool
	var uploadGroup archiveUploadGroup
	defer func() {
		if recovered := recover(); recovered != nil {
			if operation == nil {
				// 圧縮中のpanicでは送信前なので、ここで宛先を固定して復旧へ渡します。
				operation = newAcceptedLogFileProcessingOperation(l.archiveUploadDestinationsSnapshot())
				operation.uploadState.UploadFilePath = uploadPath
			}
			// 同じoperationに保持した宛先別の成功を、通知や削除のpanic後も引き継ぎます。
			scheduled := false
			// 復旧へ渡したoperationは読み直さず、初回処理で解析した不変のキーを使います。
			if uploadGroup.key.Generation != "" {
				scheduled = l.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: uploadGroup.key})
			}
			if !scheduled {
				l.scheduleAcceptedLogFileProcessingRecovery(oldFilePath, fileNameSnapshot, operation)
			}
			panic(recovered)
		}
	}()
	archiveMutation := func() error {
		uploadPath, compressed = l.tryCompressOldFile(
			ctx, oldFilePath, notify, nil,
		)
		return nil
	}
	_ = l.lifecycleGuard.RunWithLifecycleLock(archiveMutation)
	// 送信に使う宛先は従来どおり圧縮後に確定し、その後は再試行でも取り直しません。
	operation = newAcceptedLogFileProcessingOperation(l.archiveUploadDestinationsSnapshot())
	operation.uploadState.UploadFilePath = uploadPath
	if !compressed {
		operation.uploadState.UploadFilePath = ""
		l.scheduleAcceptedLogFileProcessingRecovery(oldFilePath, fileNameSnapshot, operation)
		return false
	}
	if !operation.hasUploadTargets() {
		return false
	}
	if !l.usesPersistentArchiveUploadCheckpoint(operation.destinationSnapshot()) {
		result, err := operation.runUploadWithoutCheckpoint(ctx, l, notify)
		if err != nil || !result.Complete {
			l.scheduleAcceptedLogFileProcessingRecovery(oldFilePath, fileNameSnapshot, operation)
			return operation.uploadState.UploadFilePath != oldFilePath || len(operation.uploadState.DestinationIDs) > 0
		}
		completion, syncErr := processing.RemoveCompletedFile(ctx, processing.CompletionRequest{
			Path: operation.uploadState.UploadFilePath, Compressed: operation.uploadState.UploadFilePath != oldFilePath,
			DestinationIDs: operation.uploadState.DestinationIDs, Complete: result.Complete,
		}, func(path string) (infrafsstore.FileRemovalResult, error) {
			removal := infrafsstore.FileRemovalResult{File: infrafsstore.FileInfo{Path: path}}
			removal.Err = l.removeCompletedCompressedLogFile(ctx, path, notify)
			return removal, nil
		})
		if err := errors.Join(completion.Removal.Err, syncErr); err != nil {
			if !completion.Attempted {
				l.notifyArchiveUploadError(ctx, operation.uploadState.UploadFilePath, err, notify)
			}
			l.scheduleAcceptedLogFileProcessingRecovery(oldFilePath, fileNameSnapshot, operation)
		}
		return false
	}

	// checkpointを使う一件はここで一度だけ解析し、完了処理とpanic時の登録確認へ渡します。
	group, err := parseArchiveUploadGroup(operation.uploadState.UploadFilePath, fileNameSnapshot)
	if err != nil {
		l.notifyArchiveUploadCheckpointError(ctx, operation.uploadState.UploadFilePath, err, notify)
		return true
	}
	uploadGroup = group
	// checkpoint経路にも同じoperationを渡し、受理前の失敗やpanicで成功宛先を失いません。
	deletePath, retryRequired, checkpoint := l.completeArchiveGroupUpload(
		ctx, uploadGroup, notify,
		operation.destinationSnapshot(), &operation.uploadState,
	)
	if deletePath == "" {
		return retryRequired
	}
	// 全宛先の完了はcheckpointに反映済みです。削除失敗ではその完了記録を参照して再開します。
	if err := l.removeCompletedCompressedLogFile(ctx, deletePath, notify); err != nil {
		if checkpoint != nil {
			l.queueArchiveLogFileProcessingRecovery(archiveFileOperationContinuation{
				group:        uploadGroup,
				destinations: operation.destinationSnapshot(),
				upload:       &operation.uploadState,
			}, err)
		}
		return true
	}
	return false
}

// removeCompletedCompressedLogFileは、処理完了済みの圧縮ログ一件をlifecycle lock内で削除し、
// 親ディレクトリを同期します。退避先archiveやspoolのファイルを削除する処理ではありません。
// 呼び出し元は、対象宛先ごとに送信対象外・upload成功・必要な全退避先への保存成功の
// いずれかを確認し、すべての宛先で再送不要になってから呼び出してください。
// この関数は完了判定・送信・checkpointの参照や更新を行いません。
// 削除・同期に失敗した場合はエラーを返し、notifyへ渡す通知処理からOnErrorへ通知します。
// notifyには、その場で通知する処理か、呼び出し元の処理完了後まで通知を保留する処理を渡します。
// 再試行の登録とディレクトリ容量見積もりの無効化は、呼び出し元が行います。
func (l *FileLogger) removeCompletedCompressedLogFile(
	ctx context.Context,
	compressedPath string,
	notify taskNotificationDelivery,
) error {
	removeErr := l.lifecycleGuard.RunWithLifecycleLock(func() error {
		return infrafsstore.RemoveFileAndSyncDirectory(compressedPath)
	})
	if removeErr != nil {
		message := fmt.Sprintf(ErrorMsgUploadedArchiveCleanup, compressedPath)
		wrappedErr := l.wrapErrorByLogger(removeErr, OperationArchiveCleanup)
		entry := l.InternalErrorEntryWithContext(ctx, message, removeErr)
		notify(func() {
			l.dispatchError(ctx, wrappedErr, entry)
		})
	}
	return removeErr
}
