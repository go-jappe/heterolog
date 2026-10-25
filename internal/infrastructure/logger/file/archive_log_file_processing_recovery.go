package file

import (
	"context"
	"errors"
	"fmt"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// archiveLogFileProcessingRecoveryKeyはファイルごとの再開処理を識別します。
type archiveLogFileProcessingRecoveryKey struct {
	File checkpointstore.CheckpointKey
	// PreparationPathは圧縮や旧uploaderの受理済み処理を、checkpoint操作と分けます。
	PreparationPath string
}

// archiveFileOperationContinuationはcheckpoint更新の受理前やfile削除の失敗で止まった一件の処理です。
// upload後にStoreへ反映できなかった宛先IDをここに残します。
// 再試行はこの段階から進め、成功済みuploadを繰り返しません。
type archiveFileOperationContinuation struct {
	group        archiveUploadGroup
	destinations archiveUploadDestinationSnapshot
	// uploadは初回処理から引き継ぐ宛先別の途中経過です。再試行ごとに作り直しません。
	upload *processing.UploadState
}

// retrySavedTargetConflictは、このファイルの矛盾した宛先だけをcursor更新待ちから
// ManagedUploaderの保全処理へ戻します。他宛先の成功は再送せず先にcursorへ反映します。
// 別世代や未知の宛先の矛盾にはこのpayloadを使わず、元のerrorを保持します。
func (next *archiveFileOperationContinuation) retrySavedTargetConflict(conflict *checkpointstore.SavedTargetConflictError) {
	operation := next.uploadState()
	operation.DiscardConflictingCompletion(conflict)
}

func (next *archiveFileOperationContinuation) uploadState() *processing.UploadState {
	if next.upload == nil {
		next.upload = &processing.UploadState{
			Key: next.group.key, UploadFilePath: next.group.path, DestinationIDs: next.destinations.destinationIDs,
		}
	}
	return next.upload
}

// runArchiveUploadは、FileLoggerの宛先判定と通知を共通の送信・完了記録更新処理へ組み込みます。
// nextには初回処理と再試行で同じ途中経過を渡します。成功宛先はpanic後も保持し、
// 再開時に同じ宛先へ送信し直しません。
func (l *FileLogger) runArchiveUpload(
	ctx context.Context,
	next *archiveFileOperationContinuation,
	store checkpointstore.Store,
	notify taskNotificationDelivery,
) (processing.UploadResult, error) {
	operation := next.uploadState()
	var upload processing.DestinationUploader
	if next.destinations.uploader != nil {
		upload = processing.DestinationUploadFunc(func(ctx context.Context, request uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
			return uploadArchiveDestinationsWithNotifications(ctx, next.destinations.uploader, request, destinations, notify)
		})
	}
	result, err := processing.RunUpload(ctx, operation, processing.UploadDependencies{
		Checkpoints: store, Uploader: upload,
		ReadyDestinations: func(ctx context.Context, destinations []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
			return l.archiveDestinationIDsAllowedByGenerationOrder(ctx, next.group, destinations)
		},
	})
	for _, saveErr := range result.PendingSaveErrors {
		l.notifyArchiveUploadCheckpointError(ctx, next.group.path, saveErr, notify)
	}
	if result.FailureStage != processing.UploadFailureReport {
		if uploadErr := result.Report.Err(); uploadErr != nil {
			l.notifyArchiveUploadError(ctx, next.group.path, uploadErr, notify)
		}
	}
	return result, err
}

// queueArchiveLogFileProcessingRecoveryは再試行間隔の経過後に後続処理を再開します。
// checkpointファイルの保存だけの失敗はStoreが処理し、ここでは待ちません。
// trueなら登録済みであり、呼び出し元は引き継いだuploadを以後変更しません。
func (l *FileLogger) queueArchiveLogFileProcessingRecovery(next archiveFileOperationContinuation, failure error) bool {
	store := l.archiveUploadCheckpoints
	if store == nil {
		return false
	}
	operation := next.uploadState()
	operation.UploadedDestinationIDs = append([]objectstorage.DestinationID(nil), operation.UploadedDestinationIDs...)
	next.destinations.destinationIDs = append([]objectstorage.DestinationID(nil), next.destinations.destinationIDs...)
	var conflict *checkpointstore.SavedTargetConflictError
	if errors.As(failure, &conflict) {
		next.retrySavedTargetConflict(conflict)
	}
	// 成功したuploadのcursor更新またはlocal削除が終わるまで世代を保護します。
	paths := infrafsstore.CompressionProtectionPaths(next.group.path, LogFileExtension)
	owner := appendlog.NewDirectoryGuardToken()
	l.lifecycleGuard.ProtectPaths(owner, paths...)
	release := func() { l.lifecycleGuard.UnprotectPaths(owner, paths...) }
	notifications := &backgroundTaskNotificationQueue{}
	accepted := l.archiveLogFileProcessingRecovery.Schedule(
		archiveLogFileProcessingRecoveryKey{File: next.group.key},
		failure,
		func(ctx context.Context) (bool, error) {
			notifications = &backgroundTaskNotificationQueue{}
			return next.resume(ctx, l, store, notifications.enqueue)
		},
		func(err error) func() {
			// 次の再試行が通知より先に進んでも、今回の通知だけを渡します。
			completedNotifications := notifications
			return func() {
				completedNotifications.runDeferredNotifications()
				if err != nil {
					l.notifyArchiveUploadCheckpointError(context.Background(), next.group.path, err, runNotificationImmediately)
				}
			}
		},
		release,
	)
	if !accepted {
		release()
	}
	return accepted
}

func (next *archiveFileOperationContinuation) resume(
	ctx context.Context,
	l *FileLogger,
	store checkpointstore.Store,
	notify taskNotificationDelivery,
) (bool, error) {
	result, err := l.runArchiveUpload(ctx, next, store, notify)
	if err != nil && result.FailureStage != processing.UploadFailureDestination {
		return false, err
	}
	if len(next.destinations.destinationIDs) == 0 {
		return true, nil
	}
	if !result.Complete {
		// 通常writeが止まっても、受理済みの送信と全必須退避先への保存を完了します。
		return false, nil
	}
	// 待機中はlifecycle lockを持ちません。削除と同期の短い区間だけ取得します。
	// 宛先の登録前から存在したため対象外の宛先だけで完了した世代は削除しません。
	completion, syncErr := processing.RemoveCompletedFile(ctx, processing.CompletionRequest{
		Path: next.group.path, Compressed: next.group.key.Compressed,
		DestinationIDs: next.destinations.destinationIDs, Complete: result.Delivered,
	}, func(path string) (infrafsstore.FileRemovalResult, error) {
		removal := infrafsstore.FileRemovalResult{File: infrafsstore.FileInfo{Path: path}}
		removal.Err = l.lifecycleGuard.RunWithLifecycleLockContext(ctx, func() error {
			return infrafsstore.RemoveFileAndSyncDirectory(path)
		})
		return removal, nil
	})
	if completion.Attempted {
		l.inlinePurge.invalidateDirectorySizeEstimate()
	}
	err = errors.Join(completion.Removal.Err, syncErr)
	if err != nil {
		// 削除済みでも親directoryの同期が完了するまで保持します。
		// 完了cursorによりuploadは繰り返さず、不在確認後に同期だけ再実行します。
		return false, fmt.Errorf("remove uploaded compressed archive %q: %w", next.group.path, err)
	}
	return true, nil
}

func (l *FileLogger) queueArchiveFileCleanup(group archiveUploadGroup, checkpoint *checkpointstore.Checkpoint, failure error) {
	if checkpoint == nil {
		return
	}
	l.queueArchiveLogFileProcessingRecovery(archiveFileOperationContinuation{
		group:        group,
		destinations: l.archiveUploadDestinationsSnapshot(),
	}, failure)
}
