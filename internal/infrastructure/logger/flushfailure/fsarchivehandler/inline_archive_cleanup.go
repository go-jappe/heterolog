package fsarchivehandler

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorysize"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

// archiveDirectoryObservationConfigは、使用量の概算から走査の要否を判定するTrackerへ渡す設定です。
// failure archiveのMaxDirSize 0は容量上限なしを表しますが、Trackerは上限0のとき強制時以外の
// 観測を開始しないため、uint64の最大値へ置き換えます。容量では削除しない場合も、UsageCacheTTLごとの
// 走査で削除待ちの後片付け、圧縮の再開、送信待ちの保護、保持期限・世代数のpurgeを継続します。
type archiveDirectoryObservationConfig struct {
	directorysize.ConfigProvider
}

func (c archiveDirectoryObservationConfig) MaxDirSize() uint64 {
	if maxDirSize := c.ConfigProvider.MaxDirSize(); maxDirSize > 0 {
		return maxDirSize
	}
	return math.MaxUint64
}

// cleanupArchiveDirectoryは、概算使用量と保持期限から走査の要否を判断し、必要な場合に
// 削除待ちファイルの後片付け、残存ログの圧縮再開、送信待ちファイルの保護、purgeを行います。
// 走査・復旧・purgeはlifecycleのロック内で行い、内部ログへの通知はロック解放後に行います。
// 処理中の保護と使用量の概算は、この関数の終了処理で管理します。
func (f *FsArchiveHandler) cleanupArchiveDirectory(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	state *archiveDirectoryRuntime,
	purgeAt time.Time,
	deferredRawRecoveryPath string,
) (err error) {
	lastPurge := state.lastAgePurgeAt
	// 容量が上限以下でも、保持期限の確認が必要な日は走査します。
	// 保持日数0は期限なしのため、期限の確認を理由に毎回の書き込みで走査しません。
	ageDue := f.Config.MaxFileAge() > 0 && fsstore.ShouldPurgeByAgeToday(purgeAt, lastPurge)
	directoryTracker := f.directorySizeTracker(failureCategory)
	if directoryTracker != nil && !directoryTracker.TryStartObservation(
		purgeAt.UTC(),
		archiveDirectoryObservationConfig{ConfigProvider: f.Config},
		ageDue,
	) {
		return nil
	}

	var (
		retainedRecoveryPaths []string
		retainedUpload        archivePathProtection
		compressionFailures   []archiveCompressionRecoveryFailure
		checkpointErr         error
		purgeResult           archivePurgeResult
		// purgeExecutedはpurgeの実行結果を取得したこと、purgeCompletedは
		// その結果の通知までpanicせずに戻ったことを表します。
		purgeExecuted  bool
		purgeCompleted bool
	)
	// 通知や使用量の概算の更新中にpanicしても、今回の処理の保護を解除します。
	defer func() {
		f.unprotectArchivePaths(failureCategory, retainedRecoveryPaths)
		f.releaseArchivePathProtection(failureCategory, retainedUpload)
	}()
	defer func() {
		if directoryTracker == nil {
			return
		}
		if purgeCompleted {
			directoryTracker.FinishObservation(purgeResult.observedDirectoryBytes)
		} else {
			// 検査・復旧・削除・通知が完了していない場合、古い概算で次の走査を省略しません。
			directoryTracker.Invalidate()
			directoryTracker.AbortObservation()
		}
	}()
	// ロックを取得する処理が戻った後に通知します。途中のpanicでも、ロックが
	// 解放された後に、そこまでの処理で判明したエラーを通知します。
	defer func() {
		if purgeResult.ageResult.Ran {
			// 個別の削除失敗があっても、同じ日の保持期限purgeを繰り返しません。
			state.lastAgePurgeAt = purgeAt
		}
		f.notifyArchiveCompressionRecoveryFailures(ctx, compressionFailures)
		if checkpointErr != nil {
			f.notifyCompressedArchiveUploadCheckpointError(ctx, "", checkpointErr)
		}
		if purgeExecuted {
			purgeErr := f.reportArchivePurge(ctx, purgeResult, purgeAt)
			// 削除エラーが返っても、通知がpanicせず戻れば削除後の概算値を確定できます。
			purgeCompleted = true
			err = errors.Join(err, purgeErr)
		}
	}()

	run := func() (runErr error) {
		inspection, runErr := f.observeArchiveDirectoryForPurge(failureCategory)
		if runErr != nil {
			return runErr
		}
		// 削除待ちの名前も同じ走査結果から回収します。通常ログの容量集計には含まれません。
		if runErr = f.removeFailureArchiveCompressionSourceDeletePendingPaths(failureCategory, inspection); runErr != nil {
			return runErr
		}
		directoryMutated := f.recoverRemainingUncompressedArchives(
			ctx,
			failureCategory,
			inspection,
			&retainedRecoveryPaths,
			deferredRawRecoveryPath,
			&compressionFailures,
		)
		if directoryMutated {
			// 圧縮・名前変更・削除後は古い走査結果でpurgeや容量の確定を行いません。
			// 終了処理で概算を無効にし、次のcleanupで再走査します。
			return nil
		}
		retainedUpload, runErr = f.retainObservedArchivesForUpload(ctx, failureCategory, inspection)
		if runErr != nil {
			if len(retainedUpload.paths) == 0 {
				return runErr
			}
			// 保護できたファイルの処理は続け、checkpointのエラーはロック解放後に通知します。
			checkpointErr = runErr
		}
		if len(retainedUpload.paths) > 0 {
			// 今回は送信待ちとして保護し、送信はこの系列だけを走査する後続の再試行で行います。
			f.scheduleArchiveUploadRescan(failureCategory, retainedUpload.paths[0])
		}
		purgeResult = f.purgeObservedArchiveFiles(
			ctx,
			failureCategory,
			lastPurge,
			purgeAt,
			ageDue,
			inspection,
			retainedUpload.paths,
		)
		purgeExecuted = true
		return nil
	}
	if state.guard != nil {
		return state.guard.RunWithLifecycleLock(run)
	}
	return run()
}
