package fsarchivehandler

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// scheduleArchiveProcessingRetryは、failure archiveの一ファイルについて、開始済みの
// 圧縮・送信・削除が失敗またはpanicで中断した場合に、その続きの再試行を登録します。
// 対象パスをpurgeから保護し、LogFileProcessingRecoveryが待機間隔の経過後に再開します。
// uploadPathが空なら圧縮から、準備済みなら送信・完了記録・削除へ進みます。
// 圧縮からcheckpoint処理までを同じ登録で実行するため、段階ごとの追加登録は不要です。
// 送信は、構築時に宛先付きuploaderとcheckpoint Storeを用意した系列だけで行います。
func (f *FsArchiveHandler) scheduleArchiveProcessingRetry(
	sourcePath,
	uploadPath string,
	category failurearchive.FailureCategory,
	fileName logpattern.FileNamePattern,
) {
	destinationUploader, destinationAware := f.archiveUploader(category).(uploader.DestinationUploader)
	state := f.directoryForFailureCategory(category)
	protection := archivePathProtection{token: appendlog.NewDirectoryGuardToken(), guardPaths: []string{sourcePath, uploadPath}}
	if state != nil && state.guard != nil {
		state.guard.ProtectPaths(protection.token, protection.guardPaths...)
	}
	release := func() { f.releaseArchivePathProtection(category, protection) }
	mutate := func(ctx context.Context, run func() error) error {
		if state != nil && state.guard != nil {
			return state.guard.RunWithLifecycleLockContext(ctx, run)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return run()
	}
	var operation *archiveFileOperation
	var errorReported bool
	accepted := f.logFileProcessingRecovery.Schedule(
		archiveLogFileProcessingRecoveryKey{Category: category, PreparationPath: sourcePath},
		fmt.Errorf("accepted failure archive operation remains incomplete: %q", sourcePath),
		func(ctx context.Context) (bool, error) {
			errorReported = false
			defer func() {
				if recovered := recover(); recovered != nil {
					// 通知先のpanicでも、直前のファイル操作を概算値へ取り落としません。
					f.invalidateArchiveDirectoryEstimate(category)
					panic(recovered)
				}
			}()
			if uploadPath == "" {
				var attempt archiveCompressionAttempt
				err := mutate(ctx, func() error {
					attempt = f.tryCompressArchiveSource(ctx, sourcePath, nil)
					return nil
				})
				f.invalidateArchiveDirectoryEstimate(category)
				if err == nil && attempt.completed {
					// 通知先がpanicしても、完成済みファイルとその保護を次の試行へ渡します。
					uploadPath = attempt.compressedPath
					protection.guardPaths = append(protection.guardPaths, uploadPath)
					if state != nil && state.guard != nil {
						state.guard.ProtectPaths(protection.token, uploadPath)
					}
				}
				f.notifyArchiveCompressionAttempt(ctx, sourcePath, attempt)
				if err != nil || !attempt.completed {
					return false, err
				}
			}
			if uploadPath != sourcePath {
				if err := mutate(ctx, func() error {
					_, err := appendlog.RecoverCompressionSourceDeletePendingPath(logpattern.CompressionSourceDeletePendingPath(sourcePath))
					return err
				}); err != nil {
					return false, err
				}
			}
			if operation == nil {
				store := f.compressedArchiveUploadCheckpointStore(category)
				if store == nil || !destinationAware {
					// 送信を設定していない系列では、圧縮と元ファイルの後片付けで完了です。
					return true, nil
				}
				groups, err := groupFailureArchiveUploadPaths(fileName, []string{uploadPath})
				if err != nil {
					return false, err
				}
				operation = &archiveFileOperation{store: store, key: groups[0].key, path: uploadPath,
					category: category, fileName: fileName, uploader: destinationUploader}
			}
			for {
				observed := f.archiveGenerationOrderProgress(category)
				outcome, pending, err := f.runArchiveFileOperation(ctx, operation)
				errorReported = err != nil
				if outcome.completion.Attempted {
					f.invalidateArchiveDirectoryEstimate(category)
				}
				if !pending || err != nil {
					return !pending, err
				}
				switch f.awaitArchiveGenerationOrder(operation, observed, false) {
				case archiveGenerationOrderWaiting:
					// 圧縮済みの世代は、古い世代の完了記録が進むまで順序待ちが保護して引き継ぎます。
					return true, nil
				case archiveGenerationOrderClosed:
					return false, nil
				}
			}
		},
		func(err error) func() {
			if err != nil && !errorReported {
				f.notifyCompressedArchiveUploadCheckpointError(context.Background(), sourcePath, err)
			}
			return nil
		}, release,
	)
	if !accepted {
		release()
	}
}

// scheduleCompressionSourceCleanupRetryは、圧縮後の元ファイルの削除・親ディレクトリ同期が
// 完了しなかった場合に、削除待ちパスの後片付けを再試行へ登録します。
func (f *FsArchiveHandler) scheduleCompressionSourceCleanupRetry(sourcePath string, category failurearchive.FailureCategory, failure error) {
	path := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	state := f.directoryForFailureCategory(category)
	protection := archivePathProtection{token: appendlog.NewDirectoryGuardToken(), guardPaths: []string{path}}
	if state != nil && state.guard != nil {
		state.guard.ProtectPaths(protection.token, path)
	}
	release := func() { f.releaseArchivePathProtection(category, protection) }
	if !f.logFileProcessingRecovery.Schedule(archiveLogFileProcessingRecoveryKey{Category: category, PreparationPath: path}, failure,
		func(ctx context.Context) (bool, error) {
			cleanup := func() error {
				_, err := appendlog.RecoverCompressionSourceDeletePendingPath(path)
				return err
			}
			var err error
			if state != nil && state.guard != nil {
				err = state.guard.RunWithLifecycleLockContext(ctx, cleanup)
			} else if err = ctx.Err(); err == nil {
				err = cleanup()
			}
			f.invalidateArchiveDirectoryEstimate(category)
			return err == nil, err
		}, nil, release) {
		release()
	}
}
