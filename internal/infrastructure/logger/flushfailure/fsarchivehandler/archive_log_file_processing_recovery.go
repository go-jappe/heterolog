package fsarchivehandler

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

// archiveLogFileProcessingRecoveryKeyはカテゴリとファイルごとの再開処理を識別します。
type archiveLogFileProcessingRecoveryKey struct {
	Category failurearchive.FailureCategory
	File     checkpointstore.CheckpointKey
	// PreparationPathは、圧縮から始まる処理と元ファイルの後片付けを、世代ごとのcheckpoint操作と区別します。
	PreparationPath string
}

func failureArchiveRecoveryKey(category failurearchive.FailureCategory, key checkpointstore.CheckpointKey) archiveLogFileProcessingRecoveryKey {
	return archiveLogFileProcessingRecoveryKey{Category: category, File: key}
}

// scheduleArchiveLogFileProcessingRecoveryは後続処理を受理済みの再開処理へ登録します。
// failureが非nilなら再試行間隔の経過後に、nilなら直ちに再開します。
// checkpointファイルの保存だけの失敗はStoreが処理し、ここでは待ちません。
func (f *FsArchiveHandler) scheduleArchiveLogFileProcessingRecovery(operation *archiveFileOperation, failure error) {
	// 元のtaskの保護が解除される前に再開処理へ引き継ぎ、
	// cursor更新またはlocal削除が完了するまで世代を保持します。
	f.acceptArchiveOperationContinuation(operation, failure, f.protectArchiveOperationPaths(operation))
}

// protectArchiveOperationPathsは、一世代の処理が扱う圧縮fileと元ログをpurgeから保護します。
func (f *FsArchiveHandler) protectArchiveOperationPaths(operation *archiveFileOperation) archivePathProtection {
	protection := archivePathProtection{
		token:      appendlog.NewDirectoryGuardToken(),
		guardPaths: fsstore.CompressionProtectionPaths(operation.path, operation.fileName.Extension),
	}
	if state := f.directoryForFailureCategory(operation.category); state != nil && state.guard != nil {
		state.guard.ProtectPaths(protection.token, protection.guardPaths...)
	}
	return protection
}

// acceptArchiveOperationContinuationは、保護済みの一世代の処理を受理済みの再開処理へ登録します。
// 登録できない場合は保護を解除します。古い世代を待つだけの試行は再開処理から外して順序待ちへ移し、
// 古い世代の完了記録が進んだ時点で再開します。
func (f *FsArchiveHandler) acceptArchiveOperationContinuation(
	operation *archiveFileOperation,
	failure error,
	protection archivePathProtection,
) {
	release := func() { f.releaseArchivePathProtection(operation.category, protection) }
	key := failureArchiveRecoveryKey(operation.category, operation.key)
	var errorReported, handoffPending bool
	accepted := f.logFileProcessingRecovery.Schedule(
		key,
		failure,
		func(ctx context.Context) (bool, error) {
			errorReported = false
			handoffPending = false
			defer func() {
				if recovered := recover(); recovered != nil {
					// 削除後の通知がpanicしても、変更済みの使用量を次の走査で確認します。
					f.invalidateArchiveDirectoryEstimate(operation.category)
					panic(recovered)
				}
			}()
			for {
				observed := f.archiveGenerationOrderProgress(operation.category)
				outcome, pending, err := f.runArchiveFileOperation(ctx, operation)
				errorReported = err != nil
				if outcome.completion.Attempted {
					// 削除・同期を試みた結果から、復旧処理側で使用量の概算を無効化します。
					f.invalidateArchiveDirectoryEstimate(operation.category)
				}
				if !pending || err != nil {
					return !pending, err
				}
				switch f.awaitArchiveGenerationOrder(operation, observed, true) {
				case archiveGenerationOrderWaiting:
					// 古い世代を待つ間は再開処理の間隔で再試行せず、保護は順序待ちが引き継ぎます。
					// 同じkeyの登録が解除されるまでは、順序待ちから再登録できません。
					handoffPending = true
					return true, nil
				case archiveGenerationOrderClosed:
					return false, nil
				}
			}
		},
		func(err error) func() {
			// 完了した処理の登録解除後に呼ばれるため、ここで順序待ちへの引き渡しを完了します。
			if handoffPending {
				handoffPending = false
				f.completeArchiveGenerationOrderHandoff(key)
			}
			// 通常のエラーは発生箇所で通知済みです。panicや保護解除の失敗だけを
			// この処理の終了直後に通知し、次のファイルの処理までは保留しません。
			if err != nil && !errorReported {
				f.notifyCompressedArchiveUploadCheckpointError(context.Background(), operation.path, err)
			}
			return nil
		},
		release,
	)
	if !accepted {
		release()
	}
}
