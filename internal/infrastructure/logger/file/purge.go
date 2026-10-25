package file

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	commoncheckpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// purgeLogsWithinGuardは、呼び出し元がdirectory共有のcleanup排他を取得した状態で、
// file系列の観測、age・generation purge、directory-size purgeを順に実行します。
// planはClockが返す日付におけるage purge基準日と、日次age、
// directory size、series upload retryの各確認理由を保持します。directory全体を
// 観測する場合はそのFileInfoから対象系列を選び、追加のfilesystem走査なしで
// generation保持数も実fileからbest effortで再評価します。
// series upload retryだけの場合は名前一覧から通常fileを検査してretainし、age・generation
// purgeは次の自然なwriteへ回します。
// lifecycleProtectedPathsは全保護pathのownership、pendingUploadsは解析済みの送信対象を
// 受け取るcallerのsliceであり、nilは許容しません。
// inlinePurgeは構築済み、fileNameはcleanup起動前に確定済みであることを前提とします。
func (l *FileLogger) purgeLogsWithinGuard(
	ctx context.Context,
	fileName logpattern.FileNamePattern,
	plan inlinePurgePlan,
	ownerToken *appendlog.DirectoryGuardToken,
	lifecycleProtectedPaths *[]string,
	pendingUploads *[]archiveUploadGroup,
	notify taskNotificationDelivery,
) error {
	if lifecycleProtectedPaths == nil || pendingUploads == nil {
		return fmt.Errorf("inline lifecycle protected path owner is nil")
	}
	// 以下のpurge中、初回openはcallerが保持するlifecycle lockで排他します。
	// 既存writerは新しい世代へだけ切り替わるため、削除候補を次の書き込み先にしません。
	inlinePurge := l.inlinePurge
	fileSeriesPurgeRequired := plan.fileSeriesPurgeRequired()

	directoryCacheDirty := false
	directorySizeScanCompleted := false
	defer func() {
		if directorySizeScanCompleted {
			return
		}
		// target-only purgeでは、削除対象が最後の全体走査で記録したdirectory合計へ
		// 含まれていたかを確定できません。file単位の削除を試みた場合は、次のcleanupで
		// directory全体を観測するためcacheを無効化します。
		if directoryCacheDirty {
			inlinePurge.directorySizeTracker.Invalidate()
		}
		if plan.directorySizeScanAcquired {
			inlinePurge.directorySizeTracker.AbortObservation()
		}
	}()

	if plan.seriesUploadRetryDue && !plan.directorySizeScanAcquired {
		observation, err := inlinePurge.fileScanner.Scan(
			fileName,
			logpattern.GlobByTarget,
		)
		if err != nil {
			return fmt.Errorf("failed to scan file series paths for upload retry: %w", err)
		}
		retainedFiles, err := l.retainRemainingLogFilesForOwner(
			ctx,
			fsstore.GroupFilesByCompressionSource(observation, fileName.Extension),
			fileName,
			ownerToken,
			lifecycleProtectedPaths,
		)
		if err != nil {
			if !commoncheckpoint.IsPendingSaveOnly(err) {
				return fmt.Errorf("retain pending compressed archive paths: %w", err)
			}
			l.notifyArchiveUploadCheckpointError(ctx, "", err, notify)
		}
		// upload-only retryではpurgeやdirectory estimateを変更しません。
		// retain ownershipはcallerがremote I/O完了後、またはpanic時に解放します。
		*pendingUploads = retainedFiles
		return nil
	}
	var (
		observation fsstore.FileInspection
		targetFiles []fsstore.FileInfo
		allFiles    []fsstore.FileInfo
		err         error
	)
	switch {
	case plan.directorySizeScanAcquired:
		observation, err = inlinePurge.fileScanner.Scan(
			fileName,
			logpattern.GlobAllFiles,
		)
		if err != nil {
			return fmt.Errorf("failed to scan directory for inline purge: %w", err)
		}
		matchName := logpattern.FileNameMatcher(fileName, logpattern.GlobAllFiles)
		for _, file := range observation.RegularFiles {
			if matchName(file.Path) {
				allFiles = append(allFiles, file)
			}
		}
		// generation purgeとpending compressed pathの保護は、この自然なdirectory観測を
		// 再利用します。uploadの開始条件はseries scheduleだけであり、未予約のfull scanは
		// remote retryを起動しません。current physical seriesはmemory上で抽出します。
		targetFiles = selectFileSeriesInfo(allFiles, fileName)
	case fileSeriesPurgeRequired:
		observation, err = inlinePurge.fileScanner.Scan(
			fileName,
			logpattern.GlobByTarget,
		)
		if err != nil {
			return fmt.Errorf("failed to scan file series for inline lifecycle: %w", err)
		}
		targetFiles = selectFileSeriesInfo(observation.RegularFiles, fileName)
	}

	sourceGroups := fsstore.GroupFilesByCompressionSource(observation, fileName.Extension)
	if plan.directorySizeScanAcquired {
		// 削除待ちは通常ログの容量・purge候補に含めません。pendingだけを片付けても
		// 観測済みraw/peerの情報は変わらないため、同じ観測で回復とpurgeを続けます。
		if pendingErr := l.removeCompressionSourceDeletePendingPaths(fileName, sourceGroups); pendingErr != nil {
			return fmt.Errorf("remove compression source delete-pending paths: %w", pendingErr)
		}
		directoryMutated, recoveryErr := l.recoverObservedRawArchivesForOwner(
			ctx,
			sourceGroups,
			fileName,
			plan.deferredRawRecoveryPath,
			lifecycleProtectedPaths,
			ownerToken,
			notify,
		)
		if recoveryErr != nil {
			return fmt.Errorf("recover observed raw archives: %w", recoveryErr)
		}
		// raw候補の選択とretainが正常に完了した全体走査だけを、このphysical
		// seriesの起動時回復完了として記録します。post-rotation観測も同じ回復契約です。
		inlinePurge.rawArchiveRecoveryBootstrapPending.Store(false)
		if directoryMutated {
			// 圧縮・source retirement・invalid peer削除の後は、走査前のFileInfoで
			// purge量やdirectory baselineを確定しません。deferがcacheを無効化して
			// 観測権を解放し、次の自然なwriteで新しい全体snapshotを取得します。
			directoryCacheDirty = true
			return nil
		}
		// 回復を試みたもののfilesystem mutationがなかったrawは、上でretain済みです。
		// 同じ観測のage/generation/directory-size purgeは他の候補について継続し、
		// 残存rawを含むbaselineにより次のretryをTTLでbackoffします。
	}

	retainedFiles, err := l.retainRemainingLogFilesForOwner(
		ctx,
		sourceGroups,
		fileName,
		ownerToken,
		lifecycleProtectedPaths,
	)
	if err != nil {
		if !commoncheckpoint.IsPendingSaveOnly(err) {
			return fmt.Errorf("retain pending compressed archives: %w", err)
		}
		l.notifyArchiveUploadCheckpointError(ctx, "", err, notify)
	}
	// 保護できたファイルの世代・系列情報も後続の送信処理へ渡します。
	// 解除用の全保護パスはretainRemainingLogFilesForOwnerが登録直後に所有者へ追加済みです。
	*pendingUploads = retainedFiles
	if plan.directorySizeScanAcquired && len(retainedFiles) > 0 {
		// local compressed file自体がprocessを跨ぐupload pending状態です。別のphysical
		// loggerが共有directoryのpending rawを先に回収した場合も、owning seriesの
		// 自然なfull scanで再試行scheduleを復元します。同じscanでは期限前なので
		// remote I/Oを開始せず、次のTTL後にtarget-onlyで処理します。
		l.scheduleRecoveredCompressedArchiveUpload(retainedFiles[0].path)
	}

	// 名前の解析結果だけを一回のcleanupで共有します。各purgeはGuardの保護一覧と
	// 別系列の完了情報をその都度取得し、この値には保存しません。
	fileNames := newPurgeFileNames(fileName)
	if fileSeriesPurgeRequired {
		seriesResult := l.purgeObservedFileSeries(
			ctx,
			inlinePurge,
			targetFiles,
			fileNames,
			plan.agePurgeReferenceDate,
			plan.dailyAgePurgeDue,
			&directoryCacheDirty,
			notify,
		)
		remainingTargetFiles := seriesResult.Remaining
		if plan.directorySizeScanAcquired {
			allFiles = mergeRemainingTargetFiles(
				allFiles,
				targetFiles,
				remainingTargetFiles,
			)
		}
		if fsstore.HasFileRemovalFailures(seriesResult.RemovalResults) {
			// 同じ削除失敗pathをdirectory-size purgeで再試行しません。失敗はfile系列の
			// 処理ですでに通知済みです。directory全体を走査済みなら、削除結果を反映した
			// 使用量でTracker更新だけを完了し、directory-size purgeは実行しません。
			if plan.directorySizeScanAcquired {
				inlinePurge.directorySizeTracker.FinishObservation(filesSize(allFiles))
				directorySizeScanCompleted = true
			}
			return nil
		}
	}

	if !plan.directorySizeScanAcquired {
		return nil
	}

	// 概算値は走査開始判定にだけ使います。directory-size purgeは今回一度だけ
	// 取得したFileInfoへ、直前のfile系列purge結果を反映した一覧で判断します。
	remainingAllFiles := l.purgeByDirectorySize(
		ctx,
		inlinePurge,
		allFiles,
		fileNames,
		&directoryCacheDirty,
		notify,
	)
	inlinePurge.directorySizeTracker.FinishObservation(filesSize(remainingAllFiles))
	directorySizeScanCompleted = true

	return nil
}

// selectFileSeriesInfoはdirectory全体から取得済みのFileInfoへGlobByTargetと同じ
// filename patternを適用し、対象系列の未圧縮・圧縮済みfileだけを返します。
// filesystemの再走査は行いません。
func selectFileSeriesInfo(
	files []fsstore.FileInfo,
	fileName logpattern.FileNamePattern,
) []fsstore.FileInfo {
	matchName := logpattern.FileNameMatcher(fileName, logpattern.GlobByTarget)
	var selected []fsstore.FileInfo
	for _, file := range files {
		if matchName(file.Path) {
			selected = append(selected, file)
		}
	}
	return selected
}

// purgeObservedFileSeriesは、取得済みの一つのログ系列の一覧に、保持日数・保持数の順でpurgeを適用します。
// 保持日数によるpurgeはdailyAgePurgeDueがtrueの場合だけ行い、削除に失敗した場合は
// 保持数によるpurgeへ進まず、その時点の削除結果と残存ファイルを返します。
// 各purgeの通知処理はnotifyへ渡します。呼び出し元は即時通知ならrunNotificationImmediately、
// 処理完了後まで通知を保留するならqueue.enqueueを指定します。
func (l *FileLogger) purgeObservedFileSeries(
	ctx context.Context,
	runtime *inlinePurgeRuntime,
	targetFiles []fsstore.FileInfo,
	fileNames *purgeFileNames,
	agePurgeReferenceDate time.Time,
	dailyAgePurgeDue bool,
	directoryCacheDirty *bool,
	notify taskNotificationDelivery,
) fsstore.PurgeResult {
	result := fsstore.PurgeResult{
		Remaining: append([]fsstore.FileInfo(nil), targetFiles...),
	}
	if dailyAgePurgeDue {
		ageResult := l.purgeLogsByAge(
			ctx,
			runtime,
			result.Remaining,
			fileNames,
			agePurgeReferenceDate,
			directoryCacheDirty,
			notify,
		)
		result.Ran = result.Ran || ageResult.Ran
		result.Remaining = ageResult.Remaining
		result.RemovalResults = append(result.RemovalResults, ageResult.RemovalResults...)
		if fsstore.HasFileRemovalFailures(ageResult.RemovalResults) {
			return result
		}
	}

	generationResult := l.purgeLogsByGeneration(
		ctx,
		runtime,
		result.Remaining,
		fileNames,
		directoryCacheDirty,
		notify,
	)
	result.Ran = result.Ran || generationResult.Ran
	result.Remaining = generationResult.Remaining
	result.RemovalResults = append(result.RemovalResults, generationResult.RemovalResults...)
	return result
}

// runPurgeAndTrackDirectoryMutationは削除処理へ入る前にcacheをdirtyにします。
// 正常に戻り削除試行が一件もなければ元の状態へ戻します。削除処理またはその直後の
// reportingがpanicした場合はdirtyのまま残るため、callerのdeferがcacheを無効化できます。
func runPurgeAndTrackDirectoryMutation(
	directoryCacheDirty *bool,
	purge func() fsstore.PurgeResult,
) fsstore.PurgeResult {
	if directoryCacheDirty == nil {
		return purge()
	}

	wasDirty := *directoryCacheDirty
	*directoryCacheDirty = true
	result := purge()
	if len(result.RemovalResults) == 0 {
		*directoryCacheDirty = wasDirty
	}
	return result
}

func filesSize(files []fsstore.FileInfo) uint64 {
	var bytes uint64
	for _, file := range files {
		if math.MaxUint64-bytes < file.Size {
			return math.MaxUint64
		}
		bytes += file.Size
	}
	return bytes
}

// mergeRemainingTargetFilesは、purge対象から外したactive/retained fileを元のfile系列へ戻す
// ときなどに、対象部分だけをpurge後のRemainingへ置き換えます。削除成功または削除前に
// 消失していたpathは除外し、削除失敗pathはRemainingにあるため保持します。
func mergeRemainingTargetFiles(
	allFiles []fsstore.FileInfo,
	targetFiles []fsstore.FileInfo,
	remainingTargetFiles []fsstore.FileInfo,
) []fsstore.FileInfo {
	if len(allFiles) == 0 || len(targetFiles) == 0 {
		return allFiles
	}
	targetPaths := make(map[string]struct{}, len(targetFiles))
	for _, file := range targetFiles {
		targetPaths[pathidentity.ComparisonKey(file.Path)] = struct{}{}
	}
	remainingTargetPaths := make(map[string]struct{}, len(remainingTargetFiles))
	for _, file := range remainingTargetFiles {
		remainingTargetPaths[pathidentity.ComparisonKey(file.Path)] = struct{}{}
	}

	remaining := make([]fsstore.FileInfo, 0, len(allFiles))
	for _, file := range allFiles {
		path := pathidentity.ComparisonKey(file.Path)
		if _, belongsToTarget := targetPaths[path]; !belongsToTarget {
			remaining = append(remaining, file)
			continue
		}
		if _, stillExists := remainingTargetPaths[path]; !stillExists {
			continue
		}
		remaining = append(remaining, file)
	}
	return remaining
}

// purgeLogsByAgeは、使用中・保護中・送信や退避が未完了のファイルを除外し、
// 保持日数による日次purgeを行います。削除しなかったファイルはRemainingに残します。
// 削除失敗やデバッグ情報の通知処理はnotifyへ渡します。呼び出し元は即時通知なら
// runNotificationImmediately、処理完了後まで通知を保留するならqueue.enqueueを指定します。
func (l *FileLogger) purgeLogsByAge(
	ctx context.Context,
	runtime *inlinePurgeRuntime,
	fileInfo []fsstore.FileInfo,
	fileNames *purgeFileNames,
	agePurgeReferenceDate time.Time,
	directoryCacheDirty *bool,
	notify taskNotificationDelivery,
) fsstore.PurgeResult {
	maxAgeDays := runtime.config.MaxFileAge()
	result := fsstore.PurgeResult{Remaining: append([]fsstore.FileInfo(nil), fileInfo...)}
	prepareProtection := func(protectedPaths map[string]struct{}) map[string]struct{} {
		return l.remainingLogFileProtectedPaths(
			fileInfo,
			fileNames,
			protectedPaths,
		)
	}
	runPurge := func(purge *appendlog.Purge) {
		_, span := l.Tracer.StartSpan(ctx, appendlog.SpanFilePurgeByAge)
		defer span.End()
		// 保持日数を条件とするパージを（条件を満たしていれば）実行。一日一回のみ実行。
		result = runPurgeAndTrackDirectoryMutation(
			directoryCacheDirty,
			func() fsstore.PurgeResult {
				return purge.PurgeByAge(
					fileInfo,
					maxAgeDays,
					agePurgeReferenceDate,
					runtime.lastAgePurgeDate,
				)
			},
		)
		if result.Ran {
			// 一部fileの削除に失敗しても、日次age走査を同じ日に繰り返しません。
			runtime.lastAgePurgeDate = agePurgeReferenceDate
		}
	}
	// Guardに登録済みの保護対象は、RunPurgeWithProtectionがPurgeへ設定します。
	// upload待ちなどの追加保護は、prepareProtectionから返し、今回のPurgeだけに設定します。
	l.lifecycleGuard.RunPurgeWithProtection(fileInfo, prepareProtection, runPurge)

	//OnDebug パージ実行しました
	if result.Ran && l.config.Runtime().IsDebug() {
		entry := l.InternalCallerDebugEntry(DebugMsgPurgedFileAge)
		notify(func() {
			l.dispatchDebug(ctx, entry)
		})
	}

	// デバッグモードであれば削除対象ファイルを記録
	if l.config.Runtime().IsDebug() {
		for _, removal := range result.RemovalResults {
			if removal.Err != nil {
				continue
			}
			deleted := removal.File
			entry := l.InternalCallerDebugEntry(fmt.Sprintf(MsgOldLogDeleted, deleted.Path))
			entry.Fields = map[string]any{
				"reason":     fsstore.MsgRotationAges,
				"maxAgeDays": maxAgeDays,
				"modTime":    deleted.ModTime,
				"periodDate": agePurgeReferenceDate.
					AddDate(0, 0, -maxAgeDays).
					Format(fsstore.DateFormat),
			}
			// 構築時に選択されたdebug用internal-log handlerへ委譲する。
			notify(func() {
				l.dispatchDebug(ctx, entry)
			})
		}
	}

	// 削除失敗fileがあった場合の処理
	if err := fsstore.JoinFileRemovalFailures(result.RemovalResults); err != nil {
		// purge失敗を記録
		entry := l.InternalCallerErrorEntry(ErrorMsgPurgeLogsByAge, err)
		notify(func() {
			l.wrapAndDispatchError(ctx, err, entry)
		})
	}

	return result
}

// purgeLogsByGenerationは、使用中・保護中・送信や退避が未完了のファイルを除外し、
// 保持数の上限を超えた世代を名前の日付・番号が古い順に削除します。
// 上限は未圧縮・圧縮形式を問わず世代単位で適用し、超過世代のファイルはまとめて削除します。
// 削除しなかったファイルはRemainingに残します。
// 削除失敗やデバッグ情報の通知処理はnotifyへ渡します。呼び出し元は即時通知なら
// runNotificationImmediately、処理完了後まで通知を保留するならqueue.enqueueを指定します。
func (l *FileLogger) purgeLogsByGeneration(
	ctx context.Context,
	runtime *inlinePurgeRuntime,
	files []fsstore.FileInfo,
	fileNames *purgeFileNames,
	directoryCacheDirty *bool,
	notify taskNotificationDelivery,
) fsstore.PurgeResult {
	baseExtension := fileNames.currentSeries.Extension
	maxBackups := runtime.config.MaxFileBackups()
	result := fsstore.PurgeResult{Remaining: append([]fsstore.FileInfo(nil), files...)}
	prepareProtection := func(protectedPaths map[string]struct{}) map[string]struct{} {
		return l.remainingLogFileProtectedPaths(
			files,
			fileNames,
			protectedPaths,
		)
	}
	runPurge := func(purge *appendlog.Purge) {
		candidates := purge.DeletionCandidates(files)
		if logpattern.CountGenerations(candidates, baseExtension) <= uint64(maxBackups) {
			return
		}
		_, span := l.Tracer.StartSpan(ctx, appendlog.SpanFilePurgeByGen)
		defer span.End()
		result = runPurgeAndTrackDirectoryMutation(
			directoryCacheDirty,
			func() fsstore.PurgeResult {
				return purge.PurgeGenerationSeries(
					files,
					baseExtension,
					maxBackups,
				)
			},
		)
	}
	l.lifecycleGuard.RunPurgeWithProtection(files, prepareProtection, runPurge)

	// パージ実行しました
	if result.Ran && l.config.Runtime().IsDebug() {
		entry := l.InternalCallerDebugEntry(DebugMsgPurgedFileGenerations)
		notify(func() {
			l.dispatchDebug(ctx, entry)
		})
	}

	// デバッグモードであれば、削除対象ファイルを記録
	if l.config.Runtime().IsDebug() {
		for _, removal := range result.RemovalResults {
			if removal.Err != nil {
				continue
			}
			deleted := removal.File
			entry := l.InternalCallerDebugEntry(fmt.Sprintf(MsgOldLogDeleted, deleted.Path))
			entry.Fields = map[string]any{
				"reason":      fsstore.MsgRotationExceeded,
				"existFiles":  logpattern.CountGenerations(result.Remaining, baseExtension),
				"maxLogFiles": maxBackups,
				"orderBy":     "file name date and index asc",
				"path":        deleted.Path,
				"modTime":     deleted.ModTime,
			}
			notify(func() {
				l.dispatchDebug(ctx, entry)
			})
		}
	}

	if err := fsstore.JoinFileRemovalFailures(result.RemovalResults); err != nil {
		entry := l.InternalCallerErrorEntry(
			"failed to purge logs by generations",
			err,
		)
		notify(func() {
			l.wrapAndDispatchError(ctx, err, entry)
		})
	}
	return result
}

// purgeByDirectorySizeは、filesの合計サイズが上限を超えた場合に容量purgeを行い、
// 削除しなかったファイルの一覧を返します。使用中・保護中・送信や退避が未完了の
// ファイルも容量計算には含め、実際の削除候補からだけ除外します。
// 削除失敗・容量超過の警告・デバッグ情報の通知処理はnotifyへ渡します。呼び出し元は
// 即時通知ならrunNotificationImmediately、処理完了後まで保留するならqueue.enqueueを指定します。
func (l *FileLogger) purgeByDirectorySize(
	ctx context.Context,
	runtime *inlinePurgeRuntime,
	files []fsstore.FileInfo,
	fileNames *purgeFileNames,
	directoryCacheDirty *bool,
	notify taskNotificationDelivery,
) []fsstore.FileInfo {
	maxDirectoryBytes := runtime.config.MaxDirSize()
	var (
		isPurged                       bool
		remainingFiles                 = files
		removalResults                 []fsstore.FileRemovalResult
		activeFileBytes                uint64
		activeFileCount                int
		remainingDirectoryBytes        uint64
		remainingFileCount             int
		remainingEligibleForPurgeCount int
	)
	prepareProtection := func(protectedPaths map[string]struct{}) map[string]struct{} {
		// 同じdirectoryを共有するlazy未生成のerror/debug roleも、config上で
		// upload有効ならそのcompressed fileを削除候補から除外します。
		return l.remainingLogFileProtectedPaths(
			files,
			fileNames,
			protectedPaths,
		)
	}
	runPurge := func(purge *appendlog.Purge) {
		activeFileBytes, activeFileCount = fileUsageForPaths(files, purge.ActivePaths())
		// sizeは観測した通常file（activeも含む）で計算し、protected pathは
		// 実際の削除候補からだけ除外します。リンク等は走査時点で除外済みです。
		if !fsstore.ShouldPurgeByDirectorySize(files, maxDirectoryBytes) {
			remainingDirectoryBytes = filesSize(files)
			remainingFileCount = len(files)
			return
		}
		_, span := l.Tracer.StartSpan(ctx, appendlog.SpanFilePurgeByDirSize)
		defer span.End()
		result := runPurgeAndTrackDirectoryMutation(
			directoryCacheDirty,
			func() fsstore.PurgeResult {
				return purge.PurgeByDirectorySize(
					maxDirectoryBytes,
				)
			},
		)
		isPurged = result.Ran
		remainingFiles = result.Remaining
		removalResults = result.RemovalResults
		remainingDirectoryBytes = filesSize(remainingFiles)
		remainingFileCount = len(remainingFiles)
		remainingEligibleForPurgeCount = len(purge.DeletionCandidates(remainingFiles))
	}
	l.lifecycleGuard.RunPurgeWithProtection(files, prepareProtection, runPurge)

	activeFilesExceedLimit := activeFileBytes > maxDirectoryBytes
	if activeFilesExceedLimit {
		entry := l.InternalWarnEntry(WarnMsgActiveLogFilesExceedMaxDirSize)
		entry.Fields = map[string]any{
			"directoryPath":  fileNames.currentSeries.Dir,
			"directorySize":  remainingDirectoryBytes,
			"maxDirSize":     maxDirectoryBytes,
			"activeFileSize": activeFileBytes,
			"activeFiles":    activeFileCount,
		}
		notify(func() {
			l.dispatchWarn(ctx, entry)
		})
	} else if remainingDirectoryBytes > maxDirectoryBytes &&
		remainingEligibleForPurgeCount == 0 {
		entry := l.InternalWarnEntry(WarnMsgNoFilesEligibleForDirSizePurge)
		entry.Fields = map[string]any{
			"directoryPath":  fileNames.currentSeries.Dir,
			"directorySize":  remainingDirectoryBytes,
			"maxDirSize":     maxDirectoryBytes,
			"remainingFiles": remainingFileCount,
		}
		notify(func() {
			l.dispatchWarn(ctx, entry)
		})
	}

	//OnDebug パージ開始します
	if isPurged && l.config.Runtime().IsDebug() {
		entry := l.InternalCallerDebugEntry(DebugMsgPurgedFileDirSize)
		notify(func() {
			l.dispatchDebug(ctx, entry)
		})
	}

	// デバッグモードであれば削除対象ファイルを記録
	if l.config.Runtime().IsDebug() {
		for _, removal := range removalResults {
			if removal.Err != nil {
				continue
			}
			deleted := removal.File
			entry := l.InternalCallerDebugEntry(fmt.Sprintf(MsgOldLogDeleted, deleted.Path))
			entry.Fields = map[string]any{
				"reason":     fsstore.MsgRotationDirSize,
				"maxDirSize": maxDirectoryBytes,
				"modTime":    deleted.ModTime,
				"fileSize":   deleted.Size,
			}
			// 構築時に選択されたdebug用internal-log handlerへ委譲する。
			notify(func() {
				l.dispatchDebug(ctx, entry)
			})
		}
	}

	// 削除失敗fileがあった場合の処理
	if err := fsstore.JoinFileRemovalFailures(removalResults); err != nil {
		// purge失敗を記録
		entry := l.InternalCallerErrorEntry(ErrorMsgPurgeLogsByDirectorySize, err)
		notify(func() {
			l.wrapAndDispatchError(ctx, err, entry)
		})
	}
	return remainingFiles
}

// fileUsageForPathsは観測fileのうちpathsに含まれるもののbyte数とfile数を返します。
// byte合計はdirectory全体の観測値と同じくuint64最大値で飽和させます。
func fileUsageForPaths(
	files []fsstore.FileInfo,
	paths map[string]struct{},
) (uint64, int) {
	if len(files) == 0 || len(paths) == 0 {
		return 0, 0
	}
	var (
		bytes uint64
		count int
	)
	for _, file := range files {
		if !pathidentity.Contains(paths, file.Path) {
			continue
		}
		count++
		if math.MaxUint64-bytes < file.Size {
			bytes = math.MaxUint64
			continue
		}
		bytes += file.Size
	}
	return bytes, count
}
