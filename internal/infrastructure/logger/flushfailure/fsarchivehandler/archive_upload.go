package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func (f *FsArchiveHandler) archiveUploader(failureCategory failurearchive.FailureCategory) uploader.Uploader {
	if f == nil {
		return nil
	}
	f.archiveUploaderMu.RLock()
	defer f.archiveUploaderMu.RUnlock()

	switch failureCategory {
	case failurearchive.FailureCategoryRetryable:
		return f.retryableArchiveUploader
	case failurearchive.FailureCategoryPermanent:
		return f.permanentArchiveUploader
	default:
		return nil
	}
}

// retainObservedArchivesForUploadは、自然なinline観測で見つかったlocal
// 圧縮済みfileと、圧縮無効時の未完了rawを、active writerや別taskの保護を避けて
// 今回の再uploadが完了するまでDirectoryGuardへretainします。local fileと完了cursorから、
// 再起動後もuploadまたは圧縮file削除の再試行を判断します。有効な宛先がない場合と、
// 宛先の登録前から存在したため対象外の宛先だけで完了した世代は、upload成功による
// 削除はせず、通常の保持期間・世代数・容量の削除規則へ戻します。
func (f *FsArchiveHandler) retainObservedArchivesForUpload(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	inspection archiveDirectoryInspection,
) (archivePathProtection, error) {
	protection, err := f.retainArchivePathsForUpload(
		ctx,
		failureCategory,
		inspection.seriesPattern,
		inspection.seriesSources)
	return protection, err
}

// retainArchivePathsForUploadはGlobまたはfull directory観測で得たpathから
// 圧縮済みfileと未完了rawを選び、upload完了までDirectoryGuardへretainします。
// 送信は、構築時に宛先付きuploaderとcheckpoint Storeを用意した系列だけで行います。
func (f *FsArchiveHandler) retainArchivePathsForUpload(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	fileName logpattern.FileNamePattern,
	sourceGroups fsstore.CompressionSourceGroups,
) (archivePathProtection, error) {
	checkpointStore := f.compressedArchiveUploadCheckpointStore(failureCategory)
	destinationUploader, destinationAware := f.archiveUploader(failureCategory).(uploader.DestinationUploader)
	if checkpointStore == nil || !destinationAware {
		return archivePathProtection{}, nil
	}
	destinationIDs := destinationUploader.DestinationIDs()
	rawUploadEnabled := f.Config != nil && !f.Config.EnableFileCompress()
	var candidateErrs []error
	groups := make([]failureArchiveUploadGroup, 0)
	for _, source := range sourceGroups.Sources {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(source.SourcePath, fileName)
		if !canonical {
			// prefixだけが一致する利用者fileの検査errorは、正規世代を中断しません。
			continue
		}
		if source.Err != nil {
			candidateErrs = append(candidateErrs, source.Err)
			continue
		}
		group := failureArchiveUploadGroup{
			key:            checkpointstore.CheckpointKey{Generation: checkpointstore.ArchiveCheckpointGenerationKey(generation.Period, generation.Index), Compressed: true},
			protectedPaths: []string{source.SourcePath},
		}
		unretired := false
		sourceKey := pathidentity.ComparisonKey(source.SourcePath)
		for _, file := range source.RegularFiles {
			if pathidentity.ComparisonKey(file.Path) == sourceKey {
				unretired = f.Config != nil && f.Config.EnableFileCompress()
				if rawUploadEnabled && group.path == "" {
					group.path = file.Path
					group.key.Compressed = false
				}
				continue
			}
			if group.path == "" || !group.key.Compressed {
				group.path = file.Path
				group.key.Compressed = true
			}
			group.protectedPaths = pathidentity.AppendUniquePaths(group.protectedPaths, file.Path)
		}
		if group.path == "" || unretired {
			// rawが残る世代は通常の回復で検証とsource retirementを終えるまで、
			// 全suffixのuploadを延期します。追加のfilesystem観測は行いません。
			continue
		}
		groups = append(groups, group)
	}
	if err := errors.Join(candidateErrs...); err != nil {
		return archivePathProtection{}, err
	}
	if len(destinationIDs) == 0 || len(groups) == 0 {
		// 宛先なしのUploadがnilを返しても、送信成功としてfileを削除しません。
		return archivePathProtection{}, nil
	}
	pendingGroups := groups[:0]
	for _, group := range groups {
		checkpoint, loadErr := checkpointStore.Load(ctx, group.key)
		if loadErr == nil && checkpoint.Complete(destinationIDs) &&
			(!group.key.Compressed || !checkpoint.DeliveredToAny(destinationIDs)) {
			// uploadが不要になった未圧縮fileと、宛先の登録前から存在したため対象外の宛先だけで
			// 完了した圧縮fileは送信も削除もしません。保護せず通常の保持・purgeへ任せます。
			continue
		}
		// 読み取り失敗時は保護を継続し、upload処理で復旧します。
		pendingGroups = append(pendingGroups, group)
	}
	groups = pendingGroups

	state := f.directoryForFailureCategory(failureCategory)
	if state == nil || state.guard == nil {
		return archivePathProtection{paths: failureArchiveUploadGroupPaths(groups)}, nil
	}

	ownerToken := appendlog.NewDirectoryGuardToken()
	retained := make([]string, 0, len(groups))
	var guardPaths []string
	for _, group := range groups {
		if f.archiveGenerationContinuationAccepted(failureCategory, group.key) {
			continue
		}
		registeredPaths := group.protectedPaths
		if !state.guard.ProtectPathsIfNoOtherOwnerConflict(ownerToken, nil, registeredPaths) {
			continue
		}
		retained = pathidentity.AppendUniquePaths(retained, group.path)
		guardPaths = pathidentity.AppendUniquePaths(guardPaths, registeredPaths...)
	}
	if len(retained) == 0 {
		return archivePathProtection{}, nil
	}
	return archivePathProtection{token: ownerToken, paths: retained, guardPaths: guardPaths}, nil
}

// failureArchiveUploadGroupは、一つのlogical generationのfileの観測値です。
// 各世代では観測順の先頭fileだけを処理します。
type failureArchiveUploadGroup struct {
	key  checkpointstore.CheckpointKey
	path string
	// 未選択peerも同じ観測のpurgeから保全します。upload・削除対象には加えません。
	protectedPaths []string
}

func failureArchiveUploadGroupPaths(groups []failureArchiveUploadGroup) []string {
	var paths []string
	for _, group := range groups {
		paths = append(paths, group.path)
	}
	return paths
}

// retryRetainedArchiveUploadsは観測・purgeのlifecycle lock外で再uploadし、
// 全宛先成功時だけlocal compressed archiveを削除します。一件でも
// uploadまたはlocal削除が失敗した場合はtrueを返し、callerがこのfile系列の
// target-only再試行をUsageCacheTTL後へ再armします。
func (f *FsArchiveHandler) retryRetainedArchiveUploads(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	fileName logpattern.FileNamePattern,
	protection archivePathProtection,
) (failed bool) {
	paths := protection.paths
	invalidateDirectoryUsage := false
	defer func() {
		recovered := recover()
		if recovered != nil {
			// 通知先のpanicでも、直前の削除結果を受け取れていない可能性があるため
			// 使用量の概算を無効化してからpanicを呼び出し元へ返します。
			invalidateDirectoryUsage = true
		}
		// upload/delete試行が終わったpathを先にpurge対象へ戻してから
		// directory baselineを無効化します。逆順だと、無効化を見た別cleanupが
		// まだprotectedなsnapshotを確定する余地があります。
		f.releaseArchivePathProtection(failureCategory, protection)
		if invalidateDirectoryUsage {
			// target-only観測のfileが直前のdirectory baselineに含まれたかは
			// 確定できません。複数pathの削除成否を問わず、batch完了時に
			// 一度だけ無効化し、次の通常cleanupで全体をreconcileします。
			f.invalidateArchiveDirectoryEstimate(failureCategory)
		}
		if recovered != nil {
			panic(recovered)
		}
	}()

	if f.compressedArchiveUploadCheckpointStore(failureCategory) == nil {
		// 送信を設定していない系列では、保護の解放だけを行います。
		return false
	}
	groups, err := groupFailureArchiveUploadPaths(fileName, paths)
	if err != nil {
		f.notifyCompressedArchiveUploadCheckpointError(ctx, "", err)
		return true
	}
	for _, group := range groups {
		outcome := f.uploadCheckpointedArchiveGroup(
			ctx,
			[]string{group.path},
			failureCategory,
			fileName,
		)
		invalidateDirectoryUsage = invalidateDirectoryUsage || outcome.completion.Attempted
		failed = failed || outcome.retryRequired
	}
	return failed
}

func groupFailureArchiveUploadPaths(
	fileName logpattern.FileNamePattern,
	paths []string,
) ([]failureArchiveUploadGroup, error) {
	groups := make([]failureArchiveUploadGroup, 0)
	indexes := make(map[string]int)
	for _, path := range paths {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
		if !canonical {
			return nil, fmt.Errorf(
				"resolve failure archive upload checkpoint generation for %q: non-canonical path",
				path,
			)
		}
		key := checkpointstore.ArchiveCheckpointGenerationKey(generation.Period, generation.Index)
		index, exists := indexes[key]
		if !exists {
			index = len(groups)
			indexes[key] = index
			groups = append(groups, failureArchiveUploadGroup{key: checkpointstore.CheckpointKey{Generation: key, Compressed: generation.Kind == logpattern.GenerationFileKindCompressed}})
		}
		if groups[index].path == "" {
			groups[index].path = path
		}
	}
	slices.SortFunc(groups, func(left, right failureArchiveUploadGroup) int {
		return checkpointstore.CompareGenerations(left.key.Generation, right.key.Generation)
	})
	return groups, nil
}

func (f *FsArchiveHandler) notifyUploadedCompressedArchiveRemovalError(
	ctx context.Context,
	filePath string,
	err error,
) {
	message := fmt.Sprintf(ErrorMsgUploadedArchiveDelete, filePath)
	wrappedErr := &FailureArchiveInternalError{Err: err, Op: OperationUploadedArchiveDelete}
	entry := f.internalErrorEntryWithContext(ctx, message, err)
	handler.CallErrorHandlerIfPresent(
		f.OnError,
		ctx,
		wrappedErr,
		entry,
		f.Tracer,
		f.Meter,
	)
}

// invalidateArchiveDirectoryEstimateはstandaloneのcompression・upload後削除では
// 直ちにcacheを無効化します。rotation task内ではold/compressed pathをretainしたまま
// 途中観測を起動しないよう、task完了時の一回の無効化へ委譲します。
func (f *FsArchiveHandler) invalidateArchiveDirectoryEstimate(
	failureCategory failurearchive.FailureCategory,
) {
	if tracker := f.directorySizeTracker(failureCategory); tracker != nil {
		tracker.Invalidate()
	}
}

// dispatchArchiveRotatedFileは構築時など、active transitionの外から確定済みの
// rotated fileをinline lifecycleへ渡す入口です。通常のrotationはtransition前に
// pathを保護し、finishRotatedFileTransitionを直接呼びます。
func (f *FsArchiveHandler) dispatchArchiveRotatedFile(
	ctx context.Context,
	activeFilePath string,
	oldFilePath string,
	failureCategory failurearchive.FailureCategory,
) {
	protection := f.protectRotatedFileBeforeTransition(failureCategory, oldFilePath)
	if err := f.finishRotatedFileTransition(
		ctx,
		activeFilePath,
		oldFilePath,
		failureCategory,
		protection,
		nil,
	); err != nil {
		f.notifyCompressedArchiveUploadCheckpointError(ctx, oldFilePath, err)
	}
}

// finishRotatedFileTransitionはactive transition前から保護していたold pathを、
// modeに応じてinline taskへ引き渡すか直ちに解除します。
//
// runner modeではnew activeの登録後に保護を解除し、専用runnerが次回scanでold fileを
// 処理できるようにします。disable modeも同様に解除しますが、post-rotation lifecycleは
// 起動しません。transition失敗時はold pathをactive候補として保持するため解除します。
// 準備中のエラーは返し、呼び出し元がwriterのlockを解放した直後に通知します。
func (f *FsArchiveHandler) finishRotatedFileTransition(
	ctx context.Context,
	activeFilePath string,
	oldFilePath string,
	failureCategory failurearchive.FailureCategory,
	protection archivePathProtection,
	transitionErr error,
) error {
	if transitionErr != nil {
		f.releaseArchivePathProtection(failureCategory, protection)
		if !f.archiveFileHandlePresent(failureCategory) {
			// active handle自体が存在しない不正状態では、writerが所有していないpathを
			// activeとして保護し続けないようtokenを解除します。
			f.removeDirectoryGuardToken(failureCategory)
		}
		return nil
	}
	if !f.shouldArchiveRotatedFile(activeFilePath, oldFilePath, failureCategory) {
		f.releaseArchivePathProtection(failureCategory, protection)
		// 圧縮・uploadが無効でもrotationによってold pathがactiveから削除候補へ
		// 移ります。呼び出し元の通常cleanupで一回の全体観測を行わせます。
		if tracker := f.directorySizeTracker(failureCategory); tracker != nil && f.inlineLifecycleEnabled() {
			tracker.Invalidate()
		}
		return nil
	}
	bg := contextWithoutCancel(ctx)
	// callerはrotation中のFsstore.Mu保持、または構築中の排他的な状態です。命名情報を
	// ここで確定し、Guardへの保護登録とbackground taskから可変なwriter stateを読み直しません。
	fileName, preparationErr := f.archiveFileNamePattern(failureCategory)
	var compressionCandidate *uncompressedArchiveRecoveryCandidate
	if preparationErr == nil && f.Config.EnableFileCompress() {
		var existingCompressedPath string
		existingCompressedPath, _, preparationErr = f.findExistingCompressedArchivePath(oldFilePath)
		if preparationErr == nil {
			compressionCandidate = &uncompressedArchiveRecoveryCandidate{sourcePath: oldFilePath, existingCompressedPath: existingCompressedPath}
			var compressedPath string
			compressedPath, _, preparationErr = fsstore.ResolveCompressedFilePath(
				oldFilePath, []string{existingCompressedPath}, f.Fsstore.GetCompressFormat(),
			)
			if preparationErr == nil {
				// 旧形式の既存ファイルと新規作成先のどちらも、圧縮・uploadの前から保護します。
				protection.paths = pathidentity.AppendUniquePaths(protection.paths, compressedPath)
			}
		}
	}
	if preparationErr != nil {
		// 命名情報や圧縮先を解決できない場合はrawを残し、次の全体観測へ回します。
		f.releaseArchivePathProtection(failureCategory, protection)
		f.invalidateArchiveDirectoryEstimate(failureCategory)
		return preparationErr
	}
	state := f.directoryForFailureCategory(failureCategory)
	if state != nil && state.guard != nil {
		generationAvailable := state.guard.ProtectPathsIfNoOtherOwnerConflict(
			protection.token,
			nil,
			protection.paths,
		)
		if !generationAvailable {
			// 同じgenerationを別のupload/recovery taskが所有しています。このrotationは
			// rawを変更せず、既存task完了後の自然なfull scanへ回します。
			f.releaseArchivePathProtection(failureCategory, protection)
			f.invalidateArchiveDirectoryEstimate(failureCategory)
			return nil
		}
	}
	armRetryOnAbnormalExit := (f.Fsstore.ShouldCompressFile(
		activeFilePath,
		oldFilePath,
	) || f.compressedArchiveUploadCheckpointStore(failureCategory) != nil) && (f.archiveUploader(failureCategory) != nil ||
		f.compressedArchiveUploadCheckpointStore(failureCategory) != nil)

	panicEntryTemplate := f.internalErrorEntryWithContext(
		bg,
		fmt.Sprintf(ErrorMsgRotationArchivePanic, oldFilePath),
		nil,
	)
	if !f.beginArchiveTask() {
		f.releaseArchivePathProtection(failureCategory, protection)
		return nil
	}
	bg, cancelArchive := f.forceWork.context(bg)
	go func() {
		defer cancelArchive()
		defer f.endArchiveTask()
		var uploadRetryPending bool
		archiveReturned := false
		defer func() {
			if recovered := recover(); recovered != nil {
				f.notifyArchiveTaskPanic(bg, panicEntryTemplate, recovered)
			}
			if armRetryOnAbnormalExit && !archiveReturned {
				// panicで圧縮済みpathの公開有無を確定できない場合も、通常の
				// upload失敗と同じschedule公開・保護解除境界へまとめます。
				uploadRetryPending = true
			}
			// taskはinline modeで受理済みのため、mode providerの再読込み自体が
			// panicした場合も保護解除とcache無効化はinlineとして完了させます。
			inlineLifecycle := true
			state := f.directoryForFailureCategory(failureCategory)
			if modePanic := captureArchiveTaskPanic(func() {
				inlineLifecycle = f.inlineLifecycleEnabled()
			}); modePanic != nil {
				f.notifyArchiveTaskPanic(bg, panicEntryTemplate, modePanic)
			}
			if finalizationPanic := captureArchiveTaskPanic(func() {
				finish := func() error {
					// armがpanicしても保護解除とcache無効化は必ず試行します。
					defer func() {
						if inlineLifecycle {
							if tracker := f.directorySizeTracker(failureCategory); tracker != nil {
								tracker.Invalidate()
							}
						}
					}()
					defer f.releaseArchivePathProtection(failureCategory, protection)
					if inlineLifecycle && state != nil && uploadRetryPending {
						// localに残ったcompressed archiveは、directory全体観測とは
						// 独立したこのfile系列のtarget-only再試行へ予約します。
						state.uploadRetry.arm(f.Clock.Now(), f.Config.UsageCacheTTL())
					}
					return nil
				}
				// retry scheduleの公開とpathの保護解除をdirectory lifecycleに
				// 対してatomicにし、走査が予約前のpathだけを見る順序を防ぎます。
				if state != nil && state.guard != nil {
					_ = state.guard.RunWithLifecycleLock(finish)
				} else {
					_ = finish()
				}
			}); finalizationPanic != nil {
				f.notifyArchiveTaskPanic(bg, panicEntryTemplate, finalizationPanic)
			}
			if inlineLifecycle {
				// cleanup自体もlifecycle lockを取得するため、triggerはlock解除後に行います。
				if cleanupPanic := captureArchiveTaskPanic(func() {
					_ = f.triggerPostRotationArchiveCleanup(
						bg,
						failureCategory,
						oldFilePath,
					)
				}); cleanupPanic != nil {
					f.notifyArchiveTaskPanic(bg, panicEntryTemplate, cleanupPanic)
				}
			}
		}()
		uploadRetryPending, _ = f.processRotatedArchive(
			bg,
			activeFilePath,
			oldFilePath,
			failureCategory,
			fileName,
			compressionCandidate,
		)
		archiveReturned = true
	}()
	return nil
}

func captureArchiveTaskPanic(run func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	run()
	return nil
}

func (f *FsArchiveHandler) notifyArchiveTaskPanic(
	ctx context.Context,
	entryTemplate *logging.LogEntry,
	recovered any,
) {
	// panic通知の構築自体で二次panicが発生してもbackground goroutineから
	// process全体へ伝播させず、残りの後片付けを続けます。
	defer func() {
		_ = recover()
	}()
	panicErr := &ArchiveTaskPanicError{Value: recovered, Stack: debug.Stack()}
	entry := *entryTemplate
	entry.Err = panicErr
	wrappedErr := &FailureArchiveInternalError{Err: panicErr, Op: OperationRotationArchive}
	f.delegateError(
		ctx,
		wrappedErr,
		&entry,
	)
}

func (f *FsArchiveHandler) archiveFileHandlePresent(failureCategory failurearchive.FailureCategory) bool {
	if f == nil || f.Fsstore == nil {
		return false
	}
	switch failureCategory {
	case failurearchive.FailureCategoryRetryable:
		return f.Fsstore.FileRetryable != nil
	case failurearchive.FailureCategoryPermanent:
		return f.Fsstore.FilePermanent != nil
	default:
		return false
	}
}

// beginArchiveTaskは圧縮・upload goroutineをShutdownから追跡できるよう登録します。
// task数が0から増えるたびに、その世代の完了を表すchannelを新しく作ります。
func (f *FsArchiveHandler) beginArchiveTask() bool {
	f.archiveTaskMu.Lock()
	defer f.archiveTaskMu.Unlock()
	if f.archiveTaskClosing {
		return false
	}
	if f.archiveTaskCount == 0 {
		f.archiveTaskGeneration = make(chan struct{})
	}
	f.archiveTaskCount++
	return true
}

// endArchiveTaskは圧縮・upload goroutineの完了を登録し、同じ世代の最後のtaskなら
// Shutdownが待機している完了channelを閉じます。
func (f *FsArchiveHandler) endArchiveTask() {
	f.archiveTaskMu.Lock()
	defer f.archiveTaskMu.Unlock()
	f.archiveTaskCount--
	if f.archiveTaskCount == 0 {
		close(f.archiveTaskGeneration)
	}
}

func (f *FsArchiveHandler) shouldArchiveRotatedFile(
	activeFilePath string,
	oldFilePath string,
	failureCategory failurearchive.FailureCategory,
) bool {
	if f == nil || f.Fsstore == nil || !f.inlineLifecycleEnabled() ||
		oldFilePath == "" || oldFilePath == activeFilePath {
		return false
	}
	return f.Config.EnableFileCompress() || f.archiveUploader(failureCategory) != nil
}

func (f *FsArchiveHandler) archiveProtectedPaths(oldFilePath string) []string {
	paths := []string{oldFilePath}
	if f != nil && f.Fsstore != nil && f.Config.EnableFileCompress() {
		compressedPath := fsstore.GenerateCompressedPath(oldFilePath, f.Fsstore.GetCompressFormat())
		if compressedPath != "" {
			paths = append(paths, compressedPath)
		}
	}
	return paths
}
