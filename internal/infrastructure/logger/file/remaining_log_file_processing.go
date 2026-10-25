package file

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// retainRemainingLogFilesForOwnerは、selectRemainingLogFileProcessingCandidatesが選んだ
// 残存ログを、今回のcleanupが処理する間DirectoryGuardで保護します。
// 対象ファイルと対応する未圧縮ログのパスをまとめて登録し、書き込み中または
// 別処理が保護中の世代は取得しません。戻り値には保護できたファイルのパス・世代・
// 系列の命名規則を保持し、後続処理へ解析済みの情報をそのまま渡します。
//
// 呼び出し元は、候補選択から保護登録までdirectoryのlifecycle lockを保持してください。
// 保護はcleanup専用のownerTokenへ登録し、登録直後に全保護パスをretainedPathsOwnerへ
// 追加します。呼び出し元は後続処理の完了後に、失敗・panic時も含めて解除してください。
func (l *FileLogger) retainRemainingLogFilesForOwner(
	ctx context.Context,
	sourceGroups infrafsstore.CompressionSourceGroups,
	fileNameSnapshot logpattern.FileNamePattern,
	ownerToken *appendlog.DirectoryGuardToken,
	retainedPathsOwner *[]string,
) ([]archiveUploadGroup, error) {
	if l == nil {
		return nil, nil
	}
	if ownerToken == nil || retainedPathsOwner == nil {
		return nil, fmt.Errorf("pending archive protection owner is nil")
	}
	groups, err := l.selectRemainingLogFileProcessingCandidates(ctx, sourceGroups, fileNameSnapshot, *retainedPathsOwner)
	if err != nil {
		return nil, err
	}
	var retained []archiveUploadGroup
	for _, group := range groups {
		// 元ログが消えていても共通の保護キーを使い、別の圧縮形式を扱う処理との競合を防ぎます。
		protection := infrafsstore.CompressionProtectionPaths(group.path, group.fileName.Extension)
		if !l.lifecycleGuard.ProtectPathsIfNoOtherOwnerConflict(ownerToken, nil, protection) {
			continue
		}
		// 後続処理がpanicしても全保護パスを解除できるよう、登録の直後に所有者へ渡します。
		*retainedPathsOwner = append(*retainedPathsOwner, protection...)
		retained = append(retained, group)
	}
	return retained, nil
}

// selectRemainingLogFileProcessingCandidatesは、取得済みのファイル情報と完了情報から、
// 現在のFileLoggerが送信・退避・削除を進める残存ログを選びます。
// 戻り値にパス・解析済みの世代・系列の命名規則を保持し、後続処理での再解析を不要にします。
// 圧縮無効時の未圧縮ログはcheckpointのメモリ上の完了情報を参照し、完了済みなら
// 除外して通常のpurgeへ戻します。参照に失敗した場合は、未確認として候補に残します。
// 圧縮ログは完了済みでも削除が必要なので候補に残し、ここではcheckpointを参照しません。
//
// 名前一覧は圧縮元の残存確認に使い、候補は同じ走査で確認した通常ファイルに限定します。
// 圧縮有効時に元ログが残る世代、復旧処理が所有する世代、retainedPathsにある元ログは
// 除外します。圧縮ログに対応するファイルの検査エラーは、同じcleanupで保護済みの世代を除き返します。
// checkpointの更新・保存、保護登録、追加のファイル走査は行いません。
// 呼び出し元はlifecycle lockを保持し、選択後の保護登録まで解放しないでください。
func (l *FileLogger) selectRemainingLogFileProcessingCandidates(
	ctx context.Context,
	sourceGroups infrafsstore.CompressionSourceGroups,
	fileNameSnapshot logpattern.FileNamePattern,
	retainedPaths []string,
) ([]archiveUploadGroup, error) {
	paths := sourceGroups.Files.Paths
	uploadSnapshot := l.archiveUploadDestinationsSnapshot()
	persistentCheckpoint := l.usesPersistentArchiveUploadCheckpoint(uploadSnapshot)
	compressionEnabled := l.config.Fsstore().Lifecycle().EnableFileCompress()
	previouslyRetained := make(map[string]struct{}, len(retainedPaths))
	for _, path := range retainedPaths {
		previouslyRetained[pathidentity.ComparisonKey(path)] = struct{}{}
	}
	var inspectionErr error
	for _, source := range sourceGroups.Sources {
		if source.Err == nil {
			continue
		}
		if _, retained := previouslyRetained[pathidentity.ComparisonKey(source.SourcePath)]; retained {
			// 同じ走査結果から元ログの復旧処理が保護した世代は、送信も見送ります。
			// 残る世代のpurgeとcheckpoint確認は同じ走査結果で継続できます。
			continue
		}
		for _, path := range source.Paths {
			if generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileNameSnapshot); canonical &&
				generation.Kind == logpattern.GenerationFileKindCompressed {
				inspectionErr = errors.Join(inspectionErr, source.Err)
				break
			}
		}
	}
	if inspectionErr != nil {
		return nil, fmt.Errorf("inspect pending compressed archives: %w", inspectionErr)
	}
	unretiredGenerations := make(map[string]struct{})
	for _, path := range paths {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(
			path,
			fileNameSnapshot,
		)
		if !canonical {
			continue
		}
		if generation.Kind == logpattern.GenerationFileKindUncompressed && compressionEnabled {
			// 圧縮元を削除待ちの名前へ変更する前の世代は、送信しません。
			unretiredGenerations[checkpointstore.ArchiveCheckpointGenerationKey(
				generation.Period,
				generation.Index,
			)] = struct{}{}
		}
	}

	// 通常ファイルとして確認済みの情報から候補を作ります。名前だけの一覧は上の
	// 圧縮元の残存確認に使い、送信候補へは戻しません。
	var groups []archiveUploadGroup
	groupIndexes := make(map[string]int)
	for _, file := range sourceGroups.Files.RegularFiles {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(file.Path, fileNameSnapshot)
		if !canonical || (generation.Kind != logpattern.GenerationFileKindCompressed &&
			(compressionEnabled || !persistentCheckpoint)) {
			continue
		}
		key := checkpointstore.ArchiveCheckpointGenerationKey(generation.Period, generation.Index)
		group := archiveUploadGroup{
			key:      checkpointstore.CheckpointKey{Generation: key, Compressed: generation.Kind == logpattern.GenerationFileKindCompressed},
			path:     pathidentity.AbsoluteOrCleanPath(file.Path),
			fileName: fileNameSnapshot,
		}
		if index, exists := groupIndexes[key]; exists {
			if !groups[index].key.Compressed && group.key.Compressed {
				groups[index] = group
			}
			continue
		}
		groupIndexes[key] = len(groups)
		groups = append(groups, group)
	}
	if uploadSnapshot.uploader == nil || len(uploadSnapshot.destinationIDs) == 0 {
		return nil, nil
	}
	if len(groups) == 0 {
		return nil, nil
	}

	var selected []archiveUploadGroup
	for _, group := range groups {
		if l.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: group.key}) {
			continue
		}
		if persistentCheckpoint {
			checkpoint, loadErr := l.archiveUploadCheckpoints.Load(ctx, group.key)
			if loadErr == nil && checkpoint.Complete(uploadSnapshot.destinationIDs) &&
				(!group.key.Compressed || !checkpoint.DeliveredToAny(uploadSnapshot.destinationIDs)) {
				// uploadが不要になった未圧縮ログと、宛先の登録前から存在したため対象外の宛先だけで
				// 完了した圧縮ファイルは送信も削除もしません。保護せず通常の保持・purgeへ任せます。
				continue
			}
			// 完了を確認できない場合は候補に残して保護し、後続の送信処理で再確認します。
		}
		if _, unretired := unretiredGenerations[group.key.Generation]; unretired {
			continue
		}
		sourcePath := group.path
		if group.key.Compressed {
			var ok bool
			sourcePath, ok = infrafsstore.CompressionSourcePath(group.path, fileNameSnapshot.Extension)
			if !ok {
				return nil, fmt.Errorf("resolve compression source protection path: %q", group.path)
			}
		}
		if _, retained := previouslyRetained[pathidentity.ComparisonKey(sourcePath)]; retained {
			// 同じcleanupの復旧処理が保護した世代を重ねて取得し、送信しないようにします。
			continue
		}
		selected = append(selected, group)
	}
	return selected, nil
}

// processRemainingLogFilesは、通常のinline cleanupで選択・保護した残存ログを
// 世代順に処理します。lifecycle lockの外で必要な送信・退避を進め、全宛先の完了後は
// 圧縮ファイルだけを短いlifecycle lock内で削除します。未圧縮ログは通常の保持対象へ戻します。
// 未完了の受理済み処理は専用のLogFileProcessingRecoveryへ引き継ぎます。
// checkpointファイルの保存だけの失敗では後続処理を止めません。
// groupsには候補選択時のパス・世代・系列情報を渡します。保持された世代で並べ替え、
// 対象パスから世代を解析し直さず、各ファイルの送信と再試行へ引き継ぎます。
// 通知はnotifyへ渡し、戻り値は今回の処理に再試行が必要だったかを表します。
// 呼び出し元は即時通知ならrunNotificationImmediately、処理完了後まで通知を保留するなら
// queue.enqueueを指定します。
func (l *FileLogger) processRemainingLogFiles(
	ctx context.Context,
	groups []archiveUploadGroup,
	notify taskNotificationDelivery,
) bool {
	var (
		retryRequired              bool
		directoryMutationAttempted bool
	)
	defer func() {
		// 一回のseries retryで複数pathを削除しても、directory cacheの
		// 無効化はbatch完了時の一回に集約します。
		if directoryMutationAttempted {
			l.inlinePurge.invalidateDirectorySizeEstimate()
		}
	}()
	slices.SortFunc(groups, func(left, right archiveUploadGroup) int {
		return checkpointstore.CompareGenerations(left.key.Generation, right.key.Generation)
	})
	for _, group := range groups {
		deletePath, retry, checkpoint := l.completeArchiveGroupUpload(
			ctx,
			group,
			notify,
			l.archiveUploadDestinationsSnapshot(),
			nil,
		)
		if deletePath == "" {
			retryRequired = retryRequired || retry
			continue
		}
		directoryMutationAttempted = true
		if err := l.removeCompletedCompressedLogFile(
			ctx,
			deletePath,
			notify,
		); err != nil {
			l.queueArchiveFileCleanup(group, checkpoint, err)
			retryRequired = true
			continue
		}
	}
	return retryRequired
}

// remainingLogFileProtectedPathsは、残存ログの処理が完了する前にpurgeされないよう、
// 現在の保護一覧へ対象パスを追加して返します。圧縮元の削除前の未圧縮ログと同世代の
// 圧縮ファイル、共有ディレクトリ内で送信が有効な別ログ系列のファイルも考慮します。
// 残存している圧縮ファイルには、送信・退避が未完了のものと、完了後の削除が未完了のものが
// 含まれます。ファイルが存在することだけで、送信失敗や未送信とは判断しません。
// namesには今回のcleanupで共有する名前の解析結果を渡します。保護一覧は毎回作り直し、
// 別のpurge後に保護が登録・解除された場合や、別系列の完了位置が進んだ場合も反映します。
func (l *FileLogger) remainingLogFileProtectedPaths(
	files []infrafsstore.FileInfo,
	names *purgeFileNames,
	protected map[string]struct{},
) map[string]struct{} {
	if protected == nil {
		protected = make(map[string]struct{})
	}
	persistentCheckpoint := l.usesPersistentArchiveUploadCheckpoint(
		l.archiveUploadDestinationsSnapshot(),
	)
	compressionEnabled := l.config.Fsstore().Lifecycle().EnableFileCompress()
	fileNameSnapshot := names.currentSeries
	if compressionEnabled {
		for _, file := range files {
			generation, canonical := names.generation(file.Path, fileNameSnapshot)
			if canonical && generation.Kind == logpattern.GenerationFileKindUncompressed {
				// age・generationだけの観測ではraw recoveryを実行しません。
				// 圧縮やretirementの失敗で残るsourceも、次のfull scanまでは
				// 同世代peerとともに保護し、purgeで回復元を失わないようにします。
				protected[pathidentity.AbsoluteOrCleanPath(file.Path)] = struct{}{}
			}
		}
	}
	if persistentCheckpoint || compressionEnabled {
		// rawが残る世代、または一pathでも別taskが保護中の世代はgroup全体を
		// 処理対象から外します。purge側でも同じgroup境界を再現し、別suffixを
		// source retirement前やrotation/upload中に削除しないようにします。
		protectedGenerationKeys := make(map[string]struct{})
		for path := range protected {
			generation, canonical := names.generation(
				path,
				fileNameSnapshot,
			)
			if !canonical {
				continue
			}
			protectedGenerationKeys[checkpointstore.ArchiveCheckpointGenerationKey(
				generation.Period,
				generation.Index,
			)] = struct{}{}
		}
		for _, file := range files {
			generation, canonical := names.generation(
				file.Path,
				fileNameSnapshot,
			)
			if !canonical || generation.Kind != logpattern.GenerationFileKindCompressed {
				continue
			}
			key := checkpointstore.ArchiveCheckpointGenerationKey(
				generation.Period,
				generation.Index,
			)
			if _, groupProtected := protectedGenerationKeys[key]; groupProtected {
				protected[pathidentity.AbsoluteOrCleanPath(file.Path)] = struct{}{}
			}
		}
	}
	for _, file := range files {
		if persistentCheckpoint {
			_, canonical := names.generation(
				file.Path,
				fileNameSnapshot,
			)
			if canonical {
				// current seriesは現在の宛先がある場合だけretain済みです。
				// 宛先がない間は、成功記録の有無にかかわらず通常retentionへ戻します。
				continue
			}
		}
		if !l.requiresUploadProtection(file.Path, names) {
			continue
		}
		protected[pathidentity.AbsoluteOrCleanPath(file.Path)] = struct{}{}
	}
	return protected
}

// requiresUploadProtectionは、送信設定とログ系列の完了情報から、指定パスを
// 送信・退避のためにpurgeから保護すべきかを返します。保護対象への追加は呼び出し元が行います。
// 現在の系列は送信先があれば保護し、共有ディレクトリの別系列は、その系列の完了情報も参照します。
// 完了情報を確認できない場合も保護するため、trueは送信失敗や未送信を確定する値ではありません。
func (l *FileLogger) requiresUploadProtection(
	path string,
	names *purgeFileNames,
) bool {
	if l == nil {
		return false
	}
	matchedGeneration := func(pattern logpattern.FileNamePattern, allowRaw bool) (logpattern.CanonicalGenerationFile, bool) {
		if pattern.Dir == "" || pathidentity.ComparisonKey(filepath.Dir(path)) !=
			pathidentity.ComparisonKey(pattern.Dir) {
			return logpattern.CanonicalGenerationFile{}, false
		}
		generation, canonical := names.generation(path, pattern)
		return generation, canonical && (generation.Kind == logpattern.GenerationFileKindCompressed || allowRaw)
	}

	// 現在のログ系列は、実際のUploaderに有効な宛先があれば保護します。
	rawTracked := l.config != nil && !l.config.Fsstore().Lifecycle().EnableFileCompress() &&
		l.usesPersistentArchiveUploadCheckpoint(l.archiveUploadDestinationsSnapshot())
	if l.ArchiveUploader != nil {
		if _, matched := matchedGeneration(names.currentSeries, rawTracked); matched {
			return len(l.ArchiveUploader.DestinationIDs()) > 0
		}
	}
	if l.config == nil {
		return false
	}

	uploadConfig := l.config.ObjectStorageUpload()
	if !uploadConfig.Enabled() {
		return false
	}
	layout := l.config.Fsstore().Layout()
	lazyRawTracked := !l.config.Fsstore().Lifecycle().EnableFileCompress() && l.archiveGenerationPending != nil
	configuredPattern := names.currentSeries
	configuredPattern.Target = layout.Target()
	pendingForRole := func(role loggerType, generation logpattern.CanonicalGenerationFile) bool {
		if l.archiveGenerationPending == nil {
			return true
		}
		key := checkpointstore.CheckpointKey{Generation: checkpointstore.ArchiveCheckpointGenerationKey(generation.Period, generation.Index)}
		return l.archiveGenerationPending(role, key)
	}
	configuredPattern.Dir = l.directoryPaths[ErrorLogger]
	configuredPattern.Prefix = layout.SeparateErrorLogsPrefix()
	if layout.IsSeparateErrorLogsEnabled() {
		if generation, matched := matchedGeneration(configuredPattern, lazyRawTracked); matched {
			return uploadConfig.Error().Enabled() && pendingForRole(ErrorLogger, generation)
		}
	}
	configuredPattern.Dir = l.directoryPaths[DebugLogger]
	configuredPattern.Prefix = layout.SeparateDebugLogsPrefix()
	if layout.IsSeparateDebugLogsEnabled() {
		if generation, matched := matchedGeneration(configuredPattern, lazyRawTracked); matched {
			return uploadConfig.Debug().Enabled() && pendingForRole(DebugLogger, generation)
		}
	}
	configuredPattern.Dir = l.directoryPaths[DefaultLogger]
	configuredPattern.Prefix = ""
	if !uploadConfig.Default().Enabled() {
		return false
	}
	generation, matched := matchedGeneration(configuredPattern, lazyRawTracked)
	return matched && pendingForRole(DefaultLogger, generation)
}
