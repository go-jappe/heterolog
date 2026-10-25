package processing

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// UploadCheckpointは、一件のファイルの完了記録を参照・更新する契約です。
// 更新受理後の保存失敗はPendingSaveErrorで返し、更新済みの値を維持してください。
type UploadCheckpoint interface {
	Load(context.Context, checkpoint.CheckpointKey) (checkpoint.Checkpoint, error)
	// AdvanceCompletionCursorは指定宛先の完了位置をメモリへ反映し、そこまでの途中経過を整理して保存します。
	// 更新受理後に保存だけ失敗した場合は、更新済みの完了記録とPendingSaveErrorを返してください。
	AdvanceCompletionCursor(context.Context, checkpoint.CheckpointKey, []objectstorage.DestinationID) (checkpoint.Checkpoint, error)
}

// DestinationUploaderは、このファイルの未完了宛先だけを送信・退避します。
type DestinationUploader interface {
	UploadDestinations(context.Context, uploader.UploadRequest, []objectstorage.DestinationID) uploader.UploadReport
}

// DestinationUploadFuncは、利用側の通知処理を伴う送信関数を渡すためのadapterです。
type DestinationUploadFunc func(context.Context, uploader.UploadRequest, []objectstorage.DestinationID) uploader.UploadReport

func (upload DestinationUploadFunc) UploadDestinations(ctx context.Context, request uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	return upload(ctx, request, destinations)
}

// UploadStateは、対象ファイル・対象宛先と、送信・退避を完了した宛先の状態を保持します。
// 呼び出し元は初回と再試行で同じ値を直列に更新し、失敗やpanic後も保持してください。
type UploadState struct {
	Key checkpoint.CheckpointKey
	// UploadFilePathは送信対象のローカルファイルのパスです。圧縮・未圧縮の両方を扱います。
	// 送信対象の準備が未完了の場合は空文字です。
	UploadFilePath string
	DestinationIDs []objectstorage.DestinationID
	// UploadedDestinationIDsは送信・退避を完了し、checkpointへの反映を待つ宛先です。
	// Checkpointsがnilの場合は、この状態内で完了した宛先を保持します。
	UploadedDestinationIDs []objectstorage.DestinationID
}

// UploadDependenciesは、保存・送信と世代順の確認方法を指定します。
// ReadyDestinations内の走査・保護一覧取得・lockの範囲は利用側が管理します。
type UploadDependencies struct {
	Checkpoints       UploadCheckpoint
	Uploader          DestinationUploader
	ReadyDestinations func(context.Context, []objectstorage.DestinationID) ([]objectstorage.DestinationID, error)
	// CommitReadySubsetがtrueなら、記録待ち宛先のうち世代順を満たす宛先だけを反映します。
	// falseなら記録待ち宛先のすべてが反映可能になるまで待ちます。
	CommitReadySubset bool
}

// UploadFailureStageは、今回の処理がエラーになった段階です。通知方法は利用側が決めます。
type UploadFailureStage uint8

const (
	UploadFailureNone UploadFailureStage = iota
	UploadFailureCheckpointRead
	UploadFailureGenerationOrder
	UploadFailureRequest
	UploadFailureReport
	UploadFailureCompletionCursor
	UploadFailureDestination
)

// UploadResultは一回の処理結果です。Completeは現在の全宛先の完了記録が受理済みかを示します。
// PendingSaveErrorsはメモリへ反映済みの更新の保存エラーで、利用側が通知します。
type UploadResult struct {
	Checkpoint checkpoint.Checkpoint
	// Completeは、現在の全宛先でuploadが不要になったことを表します。宛先の登録前から
	// 存在したため対象外とした宛先も含めます。再試行の要否はこの値で判断します。
	Complete bool
	// Deliveredは、Completeであり、現在の有効宛先の少なくとも一つへこの世代を送信、
	// または全指定退避先へ保存したことを完了記録から確認できたかを表します。
	// 対象外の宛先だけで完了した世代はfalseです。元ファイルの削除はこの値で判断し、
	// falseなら通常の保持期間・世代数・容量によるpurgeへ残します。
	Delivered               bool
	Report                  uploader.UploadReport
	RequestedDestinationIDs []objectstorage.DestinationID
	// UploadedDestinationIDsは今回の送信・退避に成功した宛先です。以前の記録待ちは含みません。
	UploadedDestinationIDs []objectstorage.DestinationID
	// CommittedDestinationIDsは今回AdvanceCompletionCursorへ反映できた宛先です。保存待ちの受理も含みます。
	CommittedDestinationIDs []objectstorage.DestinationID
	PendingSaveErrors       []error
	FailureStage            UploadFailureStage
}

// DiscardConflictingCompletionは、このファイル・有効宛先の矛盾だけを記録待ちから外します。
// 次のRunUploadでuploaderへ戻し、保存済みの送信結果を使った保全処理を再開できます。
// 別ファイルや、記録待ち・有効宛先にない宛先の矛盾では情報を変更しません。
func (operation *UploadState) DiscardConflictingCompletion(err error) {
	destination, found := checkpoint.SavedTargetConflictDestination(err, operation.Key, operation.UploadedDestinationIDs)
	if !found || !slices.Contains(operation.DestinationIDs, destination) {
		return
	}
	operation.UploadedDestinationIDs = slices.DeleteFunc(operation.UploadedDestinationIDs, func(id objectstorage.DestinationID) bool {
		return id == destination
	})
}

// CommitPendingUploadsは送信・退避済み宛先の完了記録だけを反映します。送信は行いません。
// 記録待ちがなければ現在のcheckpointを読みます。世代順で待つ宛先はoperationに残します。
// 保存待ちだけなら受理済みの値を返し、同じ成功を反映し直す必要はありません。
func CommitPendingUploads(ctx context.Context, operation *UploadState, dependencies UploadDependencies) (result UploadResult, err error) {
	if err := ctx.Err(); err != nil {
		result.FailureStage = UploadFailureCheckpointRead
		return result, err
	}
	for _, destinations := range [][]objectstorage.DestinationID{operation.DestinationIDs, operation.UploadedDestinationIDs} {
		if err := validateUniqueUploadDestinations(destinations); err != nil {
			result.FailureStage = UploadFailureReport
			return result, err
		}
	}
	if len(operation.DestinationIDs) == 0 {
		return result, nil
	}
	if dependencies.Checkpoints == nil {
		result.Checkpoint = checkpoint.Checkpoint{Key: operation.Key, CompletedDestinationIDs: slices.Clone(operation.UploadedDestinationIDs)}
	} else if len(operation.UploadedDestinationIDs) > 0 {
		ready, orderErr := readyUploadDestinations(ctx, dependencies, operation.UploadedDestinationIDs)
		if orderErr != nil {
			result.FailureStage = uploadReadinessFailureStage(orderErr)
			return result, orderErr
		}
		if !dependencies.CommitReadySubset && len(ready) != len(operation.UploadedDestinationIDs) {
			return result, nil
		}
		if len(ready) > 0 {
			if err := commitUploadedDestinations(ctx, operation, dependencies.Checkpoints, ready, &result); err != nil {
				return result, err
			}
		} else {
			result.Checkpoint, err = dependencies.Checkpoints.Load(ctx, operation.Key)
			if err != nil {
				result.FailureStage = UploadFailureCheckpointRead
				return result, err
			}
		}
	} else {
		result.Checkpoint, err = dependencies.Checkpoints.Load(ctx, operation.Key)
		if err != nil {
			result.FailureStage = UploadFailureCheckpointRead
			return result, err
		}
	}
	setUploadCompletion(&result, operation, dependencies)
	return result, nil
}

// RunUploadは完了記録を復元し、記録待ちの成功を反映してから未完了宛先へ送信します。
// 送信・退避の成功はAdvanceCompletionCursorより前にoperationへ保持し、更新中のpanicでも失いません。
// 保存待ちだけなら最新のcheckpointで続行します。元ログ・圧縮ファイルの削除は行いません。
func RunUpload(ctx context.Context, operation *UploadState, dependencies UploadDependencies) (result UploadResult, err error) {
	result, err = CommitPendingUploads(ctx, operation, dependencies)
	if err != nil || result.Complete || len(operation.DestinationIDs) == 0 {
		return result, err
	}
	if dependencies.Checkpoints != nil && !dependencies.CommitReadySubset && len(operation.UploadedDestinationIDs) > 0 {
		// inlineは保持済み成功の全宛先を反映できるまで、同じ処理の次段へ進みません。
		return result, nil
	}
	// 記録待ちの成功はまだcursorに現れないことがあるため、送信対象から明示的に除きます。
	pending := result.Checkpoint.PendingDestinationIDs(operation.DestinationIDs)
	pending = slices.DeleteFunc(pending, func(destination objectstorage.DestinationID) bool {
		return slices.Contains(operation.UploadedDestinationIDs, destination)
	})
	pending, err = readyUploadDestinations(ctx, dependencies, pending)
	if err != nil {
		result.FailureStage = uploadReadinessFailureStage(err)
		return result, err
	}
	if len(pending) == 0 {
		return result, nil
	}
	request, err := uploader.NewUploadRequest(operation.UploadFilePath)
	if err != nil {
		result.FailureStage = UploadFailureRequest
		return result, err
	}
	if dependencies.Uploader == nil {
		result.FailureStage = UploadFailureRequest
		return result, errors.New("destination-aware uploader is unavailable")
	}
	result.RequestedDestinationIDs = slices.Clone(pending)
	result.Report = dependencies.Uploader.UploadDestinations(ctx, request, pending)
	indexed, err := uploader.IndexUploadReport(pending, result.Report)
	if err != nil {
		result.FailureStage = UploadFailureReport
		return result, err
	}
	for _, destination := range pending {
		if indexed[destination].Succeeded() {
			result.UploadedDestinationIDs = append(result.UploadedDestinationIDs, destination)
		}
	}
	operation.UploadedDestinationIDs = append(operation.UploadedDestinationIDs, result.UploadedDestinationIDs...)
	if len(result.UploadedDestinationIDs) > 0 {
		if dependencies.Checkpoints == nil {
			result.Checkpoint.CompletedDestinationIDs = slices.Clone(operation.UploadedDestinationIDs)
		} else if err := commitUploadedDestinations(ctx, operation, dependencies.Checkpoints, result.UploadedDestinationIDs, &result); err != nil {
			return result, err
		}
	}
	setUploadCompletion(&result, operation, dependencies)
	if err := result.Report.Err(); err != nil {
		result.FailureStage = UploadFailureDestination
		return result, err
	}
	return result, nil
}

// setUploadCompletionは、再試行の要否を表すCompleteと、元ファイルの削除に使うDeliveredを設定します。
// Checkpointsがない場合の完了記録は、この状態内で送信・退避した宛先だけから作るため、
// 完了していれば送信済みです。
func setUploadCompletion(result *UploadResult, operation *UploadState, dependencies UploadDependencies) {
	result.Complete = uploadStateComplete(operation, dependencies, result.Checkpoint)
	result.Delivered = result.Complete && result.Checkpoint.DeliveredToAny(operation.DestinationIDs)
}

func uploadStateComplete(operation *UploadState, dependencies UploadDependencies, state checkpoint.Checkpoint) bool {
	return state.Complete(operation.DestinationIDs) && (dependencies.Checkpoints == nil || len(operation.UploadedDestinationIDs) == 0)
}

func readyUploadDestinations(ctx context.Context, dependencies UploadDependencies, destinations []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
	if dependencies.ReadyDestinations == nil || len(destinations) == 0 {
		return slices.Clone(destinations), nil
	}
	ready, err := dependencies.ReadyDestinations(ctx, slices.Clone(destinations))
	if err == nil {
		err = validateUniqueUploadDestinations(ready)
	}
	return ready, err
}

func validateUniqueUploadDestinations(destinations []objectstorage.DestinationID) error {
	seen := make(map[objectstorage.DestinationID]struct{}, len(destinations))
	for _, destination := range destinations {
		if _, duplicate := seen[destination]; duplicate {
			return fmt.Errorf("%w: %w: destination %q was requested more than once", uploader.ErrInvalidUploadReport, uploader.ErrDuplicateDestinationID, destination)
		}
		seen[destination] = struct{}{}
	}
	return nil
}

func uploadReadinessFailureStage(err error) UploadFailureStage {
	if errors.Is(err, uploader.ErrDuplicateDestinationID) {
		return UploadFailureReport
	}
	return UploadFailureGenerationOrder
}

func commitUploadedDestinations(ctx context.Context, operation *UploadState, store UploadCheckpoint, destinations []objectstorage.DestinationID, result *UploadResult) error {
	updated, err := store.AdvanceCompletionCursor(ctx, operation.Key, destinations)
	if err != nil && !checkpoint.IsPendingSaveOnly(err) {
		operation.DiscardConflictingCompletion(err)
		result.FailureStage = UploadFailureCompletionCursor
		return err
	}
	result.Checkpoint = updated
	result.CommittedDestinationIDs = append(result.CommittedDestinationIDs, destinations...)
	if err != nil {
		result.PendingSaveErrors = append(result.PendingSaveErrors, err)
	}
	operation.UploadedDestinationIDs = slices.DeleteFunc(operation.UploadedDestinationIDs, func(destination objectstorage.DestinationID) bool {
		return slices.Contains(destinations, destination)
	})
	return nil
}
