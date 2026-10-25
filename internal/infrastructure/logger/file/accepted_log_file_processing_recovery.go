package file

import (
	"context"
	"errors"
	"fmt"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// acceptedLogFileProcessingOperationは、受理した一件の送信先と送信途中の状態を保持します。
// 初回と再試行で同じuploadStateを使い、完了した宛先を再送しません。
type acceptedLogFileProcessingOperation struct {
	// uploaderは送信開始時の送信実装を固定します。対象宛先はuploadStateから渡し、
	// 再試行で現在の設定から送信実装や宛先一覧を取り直しません。
	uploader processing.DestinationUploader
	// uploadStateは対象ファイル・対象宛先・完了宛先を保持し、送信処理の再開に使います。
	uploadState processing.UploadState
}

func newAcceptedLogFileProcessingOperation(snapshot archiveUploadDestinationSnapshot) *acceptedLogFileProcessingOperation {
	return &acceptedLogFileProcessingOperation{
		uploader: snapshot.uploader,
		uploadState: processing.UploadState{
			DestinationIDs: append([]objectstorage.DestinationID(nil), snapshot.destinationIDs...),
		},
	}
}

// destinationSnapshotは、固定したuploaderとuploadStateの対象宛先から受け渡し用の値を作ります。
// 宛先IDの一覧は共有するため、返却値から変更しません。
func (operation *acceptedLogFileProcessingOperation) destinationSnapshot() archiveUploadDestinationSnapshot {
	return archiveUploadDestinationSnapshot{uploader: operation.uploader, destinationIDs: operation.uploadState.DestinationIDs}
}

func (operation *acceptedLogFileProcessingOperation) hasUploadTargets() bool {
	return operation.uploader != nil && len(operation.uploadState.DestinationIDs) > 0
}

// scheduleAcceptedLogFileProcessingRecoveryは、FileLoggerのinline処理で受理した一件の
// ログファイルについて、未完了の圧縮・送信／退避・削除を再開する処理を登録します。
// operationは初回処理で固定したuploader・対象宛先と、その時点の完了宛先をそのまま引き継ぎます。
// 呼び出し元は登録後にoperationを変更しません。全宛先の完了は再試行時にもIDから判定します。
// 対象はrotationで使用を終えた元ログとその圧縮ファイルです。受理済みは送信成功を意味しません。
// 同じ登録内で次段へ進むため、Closeによる新規受付停止後も処理を引き継げます。
// 起動時の探索やpurgeは行わず、受理した一件のsourceと成果物だけを扱います。
func (l *FileLogger) scheduleAcceptedLogFileProcessingRecovery(sourcePath string, fileName logpattern.FileNamePattern, operation *acceptedLogFileProcessingOperation) {
	owner := appendlog.NewDirectoryGuardToken()
	paths := []string{sourcePath, operation.uploadState.UploadFilePath}
	l.lifecycleGuard.ProtectPaths(owner, paths...)
	release := func() { l.lifecycleGuard.UnprotectPaths(owner, paths...) }
	var next *archiveFileOperationContinuation
	var notifications *backgroundTaskNotificationQueue
	accepted := l.archiveLogFileProcessingRecovery.Schedule(
		archiveLogFileProcessingRecoveryKey{PreparationPath: sourcePath},
		fmt.Errorf("accepted log file processing remains incomplete: %q", sourcePath),
		func(ctx context.Context) (bool, error) {
			notifications = &backgroundTaskNotificationQueue{}
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if operation.uploadState.UploadFilePath == "" {
				var compressed bool
				err := l.lifecycleGuard.RunWithLifecycleLockContext(ctx, func() error {
					operation.uploadState.UploadFilePath, compressed = l.tryCompressOldFile(ctx, sourcePath, notifications.enqueue, nil)
					return nil
				})
				l.inlinePurge.invalidateDirectorySizeEstimate()
				if err != nil || !compressed {
					return false, err
				}
				paths = append(paths, operation.uploadState.UploadFilePath)
				l.lifecycleGuard.ProtectPaths(owner, operation.uploadState.UploadFilePath)
			}
			if operation.uploadState.UploadFilePath != sourcePath {
				err := l.lifecycleGuard.RunWithLifecycleLockContext(ctx, func() error {
					_, err := appendlog.RecoverCompressionSourceDeletePendingPath(logpattern.CompressionSourceDeletePendingPath(sourcePath))
					return err
				})
				if err != nil {
					return false, err
				}
			}
			if !operation.hasUploadTargets() {
				return true, nil
			}
			if l.usesPersistentArchiveUploadCheckpoint(operation.destinationSnapshot()) {
				if next == nil {
					group := archiveUploadGroup{key: operation.uploadState.Key, path: operation.uploadState.UploadFilePath, fileName: fileName}
					if group.key.Generation == "" {
						// 圧縮準備で初めて対象が決まった場合だけ解析し、以後の再試行へ保持します。
						var err error
						group, err = parseArchiveUploadGroup(group.path, fileName)
						if err != nil {
							return false, err
						}
						operation.uploadState.Key, operation.uploadState.UploadFilePath = group.key, group.path
					}
					next = &archiveFileOperationContinuation{group: group, destinations: operation.destinationSnapshot(), upload: &operation.uploadState}
				}
				return next.resume(ctx, l, l.archiveUploadCheckpoints, notifications.enqueue)
			}
			result, err := operation.runUploadWithoutCheckpoint(ctx, l, notifications.enqueue)
			if err != nil || !result.Complete {
				return false, err
			}
			completion, syncErr := processing.RemoveCompletedFile(ctx, processing.CompletionRequest{
				Path: operation.uploadState.UploadFilePath, Compressed: operation.uploadState.UploadFilePath != sourcePath,
				DestinationIDs: operation.uploadState.DestinationIDs, Complete: result.Complete,
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
			return err == nil, err
		},
		func(err error) func() {
			completed := notifications
			return func() {
				completed.runDeferredNotifications()
				if err != nil {
					l.notifyArchiveUploadError(context.Background(), sourcePath, err, runNotificationImmediately)
				}
			}
		}, release,
	)
	if !accepted {
		release()
	}
}

// runUploadWithoutCheckpointは、永続checkpointを使わず、宛先別の完了IDをuploadStateに保持します。
// 初回処理と再試行で同じoperationを使い、完了後の削除失敗では送信を繰り返しません。
func (operation *acceptedLogFileProcessingOperation) runUploadWithoutCheckpoint(ctx context.Context, l *FileLogger, notify taskNotificationDelivery) (processing.UploadResult, error) {
	return l.uploadAcceptedDestinations(ctx, &operation.uploadState, operation.uploader, notify)
}

// uploadAcceptedDestinationsは、初回から引き継いだoperationへ宛先別の完了IDを直接反映します。
// Storeを使わない場合も、通知がpanicして戻り値を返せなくても既知の成功を失いません。
func (l *FileLogger) uploadAcceptedDestinations(ctx context.Context, operation *processing.UploadState, archiveUploader processing.DestinationUploader, notify taskNotificationDelivery) (processing.UploadResult, error) {
	result, err := processing.RunUpload(ctx, operation, processing.UploadDependencies{
		Uploader: processing.DestinationUploadFunc(func(ctx context.Context, request uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
			return uploadArchiveDestinationsWithNotifications(ctx, archiveUploader, request, destinations, notify)
		}),
	})
	if err != nil {
		l.notifyArchiveUploadError(ctx, operation.UploadFilePath, err, notify)
	}
	return result, err
}

// scheduleCompressionSourceCleanupRetryは、圧縮後の元ファイルの削除・親ディレクトリ同期が
// 完了しなかった場合に、削除待ちパスの後片付けを再試行へ登録します。
func (l *FileLogger) scheduleCompressionSourceCleanupRetry(sourcePath string, failure error) {
	path := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	owner := appendlog.NewDirectoryGuardToken()
	l.lifecycleGuard.ProtectPaths(owner, path)
	release := func() { l.lifecycleGuard.UnprotectPaths(owner, path) }
	if !l.archiveLogFileProcessingRecovery.Schedule(archiveLogFileProcessingRecoveryKey{PreparationPath: path}, failure,
		func(ctx context.Context) (bool, error) {
			err := l.lifecycleGuard.RunWithLifecycleLockContext(ctx, func() error {
				_, err := appendlog.RecoverCompressionSourceDeletePendingPath(path)
				return err
			})
			l.inlinePurge.invalidateDirectorySizeEstimate()
			return err == nil, err
		}, nil, release) {
		release()
	}
}
