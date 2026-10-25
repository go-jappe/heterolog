package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

var errInvalidFailureArchiveUploadReport = errors.New(
	"failure archive destination upload report is invalid",
)

// archiveFileOperationは、一世代の送信・退避、完了cursorへの反映、圧縮ファイルの削除を進める状態です。
// 初回と再試行で同じ値を使い、完了記録を受理される前の失敗やpanicでも成功済み宛先を保持します。
type archiveFileOperation struct {
	store    checkpointstore.Store
	key      checkpointstore.CheckpointKey
	path     string
	category failurearchive.FailureCategory
	fileName logpattern.FileNamePattern
	// uploaderの宛先は構築時に固定されます。操作ごとの宛先設定のコピーは保持しません。
	uploader uploader.DestinationUploader
	// uploadedDestinationIDsは送信・退避が完了し、AdvanceCompletionCursorへの反映を待っている宛先です。
	uploadedDestinationIDs []objectstorage.DestinationID
}

func (operation *archiveFileOperation) uploadState() processing.UploadState {
	return processing.UploadState{
		Key: operation.key, UploadFilePath: operation.path, DestinationIDs: operation.uploader.DestinationIDs(),
		UploadedDestinationIDs: operation.uploadedDestinationIDs,
	}
}

// archiveFileOperationResultは、一世代のファイル処理全体の結果です。checkpoint単独の結果ではありません。
// 使用量の概算を無効化するかは、この結果を受け取るlifecycle側が判断します。
type archiveFileOperationResult struct {
	// retryRequiredは、送信・退避、完了cursorへの反映、削除・同期などが未完了で、
	// 再試行が必要なことを表します。checkpointの保存待ちだけの失敗は含めません。
	// falseは今回の呼び出しが再試行を要求しないという意味で、宛先なしや復旧登録済みの場合も含みます。
	retryRequired bool
	// completionは、削除処理を呼び出したかと、ファイルの検査・削除の結果です。
	// 送信・完了記録の段階で止まった場合や、未圧縮ファイルを保持する場合はゼロ値です。
	// 同期やキャンセルのエラーも含む処理全体の成否は、各処理のerrorで返します。
	completion processing.CompletionResult
}

// uploadCheckpointedArchiveGroupは、一世代のfileを、構築時に確定した
// 宛先のうち未完了の宛先へuploadします。upload成功、または失敗時の全指定退避先への
// 保存成功を完了として記録します。有効な宛先が一つ以上あり、すべての完了を
// メモリ上のcheckpointへ反映でき、少なくとも一つへ送信・退避していれば圧縮済みfileだけを
// 削除します。宛先の登録前から存在したため対象外の宛先だけで完了した世代は削除しません。
// checkpointファイルの保存だけの失敗は通知し、Storeの再保存へ任せます。
func (f *FsArchiveHandler) uploadCheckpointedArchiveGroup(
	ctx context.Context,
	filePaths []string,
	failureCategory failurearchive.FailureCategory,
	fileName logpattern.FileNamePattern,
) archiveFileOperationResult {
	var outcome archiveFileOperationResult
	destinationUploader, ok := f.archiveUploader(failureCategory).(uploader.DestinationUploader)
	if !ok || destinationUploader == nil || len(destinationUploader.DestinationIDs()) == 0 {
		return outcome
	}
	store := f.compressedArchiveUploadCheckpointStore(failureCategory)
	if store == nil || len(filePaths) == 0 {
		outcome.retryRequired = true
		return outcome
	}
	representativePath := filePaths[0]

	groups, err := groupFailureArchiveUploadPaths(fileName, filePaths)
	if err != nil {
		f.notifyCompressedArchiveUploadCheckpointError(ctx, representativePath, err)
		outcome.retryRequired = true
		return outcome
	}
	if len(groups) != 1 {
		err = fmt.Errorf("failure archive upload spans %d generations", len(groups))
		f.notifyCompressedArchiveUploadCheckpointError(ctx, representativePath, err)
		outcome.retryRequired = true
		return outcome
	}
	key := groups[0].key
	representativePath = groups[0].path
	if f.archiveGenerationContinuationAccepted(failureCategory, key) {
		// Storeへ未反映の成功宛先は再開処理が保持します。通常の再観測では再uploadしません。
		return outcome
	}
	operation := &archiveFileOperation{
		store: store, key: key, path: representativePath, category: failureCategory,
		uploader: destinationUploader, fileName: fileName,
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			// 初回試行のpanicも同じoperationを保持し、記録待ちの成功宛先を失いません。
			f.scheduleArchiveLogFileProcessingRecovery(operation, fmt.Errorf("failure archive operation panic: %v", recovered))
			panic(recovered)
		}
	}()
	for {
		observed := f.archiveGenerationOrderProgress(failureCategory)
		var pending bool
		outcome, pending, err = f.runArchiveFileOperation(ctx, operation)
		if pending && err == nil {
			// 古い世代を待つだけの処理は再開処理の間隔で待たず、古い世代の完了記録が進んだ時点で再開します。
			switch f.awaitArchiveGenerationOrder(operation, observed, false) {
			case archiveGenerationOrderWaiting:
				return outcome
			case archiveGenerationOrderRetry:
				continue
			}
		}
		if pending {
			f.scheduleArchiveLogFileProcessingRecovery(operation, err)
		}
		return outcome
	}
}

// runArchiveFileOperationは更新の受理前やfile削除の失敗時に再開位置を維持します。
// pendingがtrueなら再試行間隔の経過後に再開します。checkpoint保存だけの失敗は
// 後続処理を止めず、未完了の送信・退避も通常writeなしで継続します。
func (f *FsArchiveHandler) runArchiveFileOperation(
	ctx context.Context,
	operation *archiveFileOperation,
) (outcome archiveFileOperationResult, pending bool, err error) {
	upload := operation.uploadState()
	// AdvanceCompletionCursor中のpanicでも、送信済み宛先を同じ再開処理へ引き継ぎます。
	defer func() { operation.uploadedDestinationIDs = upload.UploadedDestinationIDs }()
	result, err := processing.RunUpload(ctx, &upload, processing.UploadDependencies{
		Checkpoints: operation.store, Uploader: operation.uploader,
		ReadyDestinations: func(ctx context.Context, destinations []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
			return f.readyArchiveDestinationIDs(ctx, operation, destinations)
		},
	})
	if len(result.CommittedDestinationIDs) > 0 {
		// 完了記録が進んだため、この世代を待っていた新しい世代を再開間隔を待たずに再試行します。
		f.resumeArchiveGenerationOrderWaits(operation.category)
	}
	for _, saveErr := range result.PendingSaveErrors {
		f.notifyCompressedArchiveUploadCheckpointError(ctx, operation.path, saveErr)
	}
	if err != nil {
		switch result.FailureStage {
		case processing.UploadFailureReport:
			err = fmt.Errorf("%w: %w", errInvalidFailureArchiveUploadReport, err)
		case processing.UploadFailureCheckpointRead:
			err = fmt.Errorf("restore upload checkpoint before continuing file operation: %w", err)
		case processing.UploadFailureCompletionCursor:
			err = errors.Join(fmt.Errorf("record uploaded destinations for %q: %w", operation.path, err), result.Report.Err())
		}
		f.notifyCompressedArchiveUploadCheckpointError(ctx, operation.path, err)
		outcome.retryRequired = true
		return outcome, true, err
	}
	if len(upload.DestinationIDs) == 0 {
		return outcome, false, nil
	}
	if !result.Complete {
		outcome.retryRequired = true
		return outcome, true, nil
	}
	if !result.Delivered {
		// 対象外の宛先だけで完了した世代はどこにも送信していません。upload完了を理由に
		// 削除せず、再試行も登録しないで通常の保持期間・世代数・容量によるpurgeへ残します。
		return outcome, false, nil
	}
	outcome.completion, err = f.finishArchiveUploadOperation(ctx, operation.path, operation.key.Compressed, upload.DestinationIDs, operation.category)
	outcome.retryRequired = err != nil
	return outcome, outcome.retryRequired, err
}

// finishArchiveUploadOperationは全宛先の完了後、圧縮ファイルの削除と同期を進めます。
// 未圧縮ファイルは通常の保持・purgeへ残します。通知とlockはこの利用側で管理します。
// 検査・削除の結果と、同期を含むエラーを返すだけで、再試行の登録や使用量の概算の変更は行いません。
func (f *FsArchiveHandler) finishArchiveUploadOperation(
	ctx context.Context,
	path string,
	compressed bool,
	destinations []objectstorage.DestinationID,
	category failurearchive.FailureCategory,
) (processing.CompletionResult, error) {
	completion, syncErr := processing.RemoveCompletedFile(ctx, processing.CompletionRequest{
		Path: path, Compressed: compressed,
		DestinationIDs: destinations, Complete: true,
	}, func(path string) (fsstore.FileRemovalResult, error) {
		removal := fsstore.FileRemovalResult{File: fsstore.FileInfo{Path: path}}
		var directorySyncErr error
		remove := func() error {
			removal, directorySyncErr = fsstore.RemoveRegularFileAndSyncDirectory(path)
			return nil
		}
		if state := f.directoryForFailureCategory(category); state != nil && state.guard != nil {
			if err := state.guard.RunWithLifecycleLockContext(ctx, remove); err != nil {
				removal.Err = err
			}
		} else if removal.Err = ctx.Err(); removal.Err == nil {
			_ = remove()
		}
		return removal, directorySyncErr
	})
	err := errors.Join(completion.Removal.Err, syncErr)
	if err != nil {
		f.notifyUploadedCompressedArchiveRemovalError(ctx, path, err)
		// 削除済みでも親directory同期の完了まで保持します。完了cursorを
		// 読み直すためuploadは繰り返さず、不在確認後の同期だけを再実行します。
	}
	return completion, err
}
