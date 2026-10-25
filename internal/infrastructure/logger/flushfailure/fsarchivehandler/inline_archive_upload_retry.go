package fsarchivehandler

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
)

// retryScheduledArchiveUploadsは、再試行時刻に達したログ系列の残存ファイルを走査して保護し、送信を再試行します。
// 呼び出し元は再試行時刻に達したことを確認してから呼んでください。
// 走査・保護・再試行予約の取得はlifecycleのロック内で行い、送信はロック解放後に行います。
// 途中で失敗・panicした場合も保護を解除し、未完了の処理を再試行できるようにします。
func (f *FsArchiveHandler) retryScheduledArchiveUploads(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	state *archiveDirectoryRuntime,
	attemptAt time.Time,
	retryInterval time.Duration,
) error {
	var (
		retainedUpload      archivePathProtection
		uploadFileName      logpattern.FileNamePattern
		uploadRetryClaimed  bool
		uploadRetryFinished bool
		targetScanCompleted bool
	)
	defer func() {
		// 走査やuploadがpanicしても保護を解放し、未完了の再試行期限を残します。
		f.releaseArchivePathProtection(failureCategory, retainedUpload)
		if !targetScanCompleted {
			state.uploadRetry.backoffDue(f.Clock.Now(), retryInterval)
		}
		if uploadRetryClaimed && !uploadRetryFinished {
			state.uploadRetry.arm(f.Clock.Now(), retryInterval)
		}
	}()

	observeUploadTarget := func() error {
		observation, fileName, err := f.observeArchiveFileSeriesForUpload(failureCategory)
		if err != nil {
			return err
		}
		// Fsstore.Mu内で確定したpatternを、lock外の再uploadにも引き継ぎます。
		uploadFileName = fileName
		retainedUpload, err = f.retainArchivePathsForUpload(
			ctx,
			failureCategory,
			fileName,
			fsstore.GroupFilesByCompressionSource(observation, fileName.Extension),
		)
		if err != nil {
			return err
		}
		targetScanCompleted = true
		// 保護の成功後にlock内でclaimし、以後の成功で別rotationの失敗期限を消しません。
		uploadRetryClaimed = state.uploadRetry.claimForAttempt(attemptAt, retryInterval)
		return nil
	}
	var err error
	if state.guard != nil {
		// 走査・保護・claimだけをlifecycle lockで直列化します。
		err = state.guard.RunWithLifecycleLock(observeUploadTarget)
	} else {
		err = observeUploadTarget()
	}
	if err != nil {
		return err
	}
	if uploadRetryClaimed {
		// remote I/Oはlock解放後に行い、保護の解放責任をupload側のdeferへ移します。
		retryProtection := retainedUpload
		retainedUpload = archivePathProtection{}
		retryFailed := f.retryRetainedArchiveUploads(
			ctx,
			failureCategory,
			uploadFileName,
			retryProtection,
		)
		uploadRetryFinished = true
		if retryFailed {
			state.uploadRetry.arm(f.Clock.Now(), retryInterval)
		}
	}
	return nil
}
