package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// uncompressedArchiveRecoveryCandidateは、検査結果から選んだ未圧縮のfailure archiveと、
// 対応する圧縮ファイル・検査エラー・保護対象を保持します。
type uncompressedArchiveRecoveryCandidate struct {
	sourcePath string
	// existingCompressedPathは検査結果にある圧縮ファイルのパスです。未発見なら空です。
	existingCompressedPath string
	preparationErr         error
	protectedPaths         []string
}

// archiveCompressionRecoveryFailureは、lifecycle lock内で行った復旧処理の失敗です。
// 呼び出し元がlockを解放した直後に、対象パスと失敗した工程を使って通知します。
type archiveCompressionRecoveryFailure struct {
	sourcePath    string
	err           error
	sourceCleanup bool
}

// recoverRemainingUncompressedArchivesは、全体走査の検査結果から、命名規則に一致する
// 書き込み終了済みの未圧縮failure archiveを復旧します。
// 圧縮ファイルがなければ圧縮し、既存の圧縮ファイルはその形式で検証してから元ファイルを削除します。
// 組み込み形式の検証では、展開した内容が元ファイルと一致することも確認します。
// 破損した圧縮ファイルは元ファイルを残して削除し、次の走査で再生成できるようにします。
// 完全でも内容の異なる圧縮ファイルは残し、元ファイルを削除せず.preservedへ退避して通知します。
// 戻り値は、ファイルの作成・削除などにより、受け取ったFileInfoをpurgeや容量計算へ
// 使い続けられなくなったかを返します。
// 追加のディレクトリ走査は行いません。
//
// retainedPathsOwnerには、ファイル操作の前に今回取得した保護パスを追加します。
// 呼び出し元は対象ディレクトリのlifecycle lockを保持し、panicを含む全終了経路で
// unprotectArchivePathsを呼んでください。deferredSourcePathの元ファイルは保護だけを行い、
// 今回の復旧処理から外します。failuresには復旧できなかった対象・工程を追加します。
// 後続ファイルのpanicでも記録が残るよう、呼び出し元が所有するスライスを渡し、
// lifecycle lockの解放直後にその結果を通知してください。
func (f *FsArchiveHandler) recoverRemainingUncompressedArchives(
	ctx context.Context,
	failureCategory failurearchive.FailureCategory,
	inspection archiveDirectoryInspection,
	retainedPathsOwner *[]string,
	deferredSourcePath string,
	failures *[]archiveCompressionRecoveryFailure,
) (directoryMutated bool) {
	if f == nil || f.Fsstore == nil || f.Config == nil || retainedPathsOwner == nil ||
		!f.Config.EnableFileCompress() {
		return false
	}

	candidates := selectUncompressedArchiveRecoveryCandidates(
		inspection.seriesSources,
		inspection.seriesPattern,
	)
	if len(candidates) == 0 {
		return false
	}
	for index := range candidates {
		candidate := &candidates[index]
		if candidate.preparationErr != nil {
			continue
		}
		compressedPath, exists, err := fsstore.ResolveCompressedFilePath(
			candidate.sourcePath, []string{candidate.existingCompressedPath}, f.Fsstore.GetCompressFormat(),
		)
		if err != nil {
			candidate.preparationErr = err
		} else if !exists {
			candidate.protectedPaths = pathidentity.AppendUniquePaths(candidate.protectedPaths, compressedPath)
		}
	}

	selected := make([]uncompressedArchiveRecoveryCandidate, 0, len(candidates))
	retained := make([]string, 0, len(candidates)*2)
	deferredSource := pathidentity.ComparisonKey(deferredSourcePath)
	selectCandidates := func(protected map[string]struct{}) []string {
		for _, candidate := range candidates {
			source := pathidentity.AbsoluteOrCleanPath(candidate.sourcePath)
			conflicts := false
			for _, path := range candidate.protectedPaths {
				if pathidentity.Contains(protected, path) {
					conflicts = true
					break
				}
			}
			if conflicts {
				continue
			}
			// 圧縮パスが通常ファイルでない場合も関連パスを保護し、今回のpurgeから除外します。
			retained = pathidentity.AppendUniquePaths(retained, candidate.protectedPaths...)
			if deferredSource != "" && pathidentity.ComparisonKey(source) == deferredSource {
				// rotation直後に圧縮に失敗した元ファイルは保護だけを行います。
				// ほかの残存ファイルの復旧は続け、延期した元ファイルは次の全体走査で扱います。
				continue
			}
			selected = append(selected, candidate)
		}
		return retained
	}

	state := f.directoryForFailureCategory(failureCategory)
	if state != nil && state.guard != nil {
		if err := state.guard.WithProtectedPathsAndRetain(state.token, selectCandidates); err != nil {
			*failures = append(*failures, archiveCompressionRecoveryFailure{
				err: fmt.Errorf("retain failure archive raw recovery paths: %w", err),
			})
			return false
		}
	} else {
		_ = selectCandidates(nil)
	}
	// 保護したパスを先に呼び出し元へ渡し、以降のpanicでも外側のdeferが解除できるようにします。
	*retainedPathsOwner = append(*retainedPathsOwner, retained...)
	if len(selected) == 0 {
		return false
	}

	for _, candidate := range selected {
		if candidate.preparationErr != nil {
			*failures = append(*failures, archiveCompressionRecoveryFailure{
				sourcePath: candidate.sourcePath, err: candidate.preparationErr,
			})
			continue
		}
		switch {
		case candidate.existingCompressedPath == "":
			attempt := f.tryCompressArchiveSource(
				ctx,
				candidate.sourcePath,
				&candidate,
			)
			directoryMutated = directoryMutated || attempt.directoryMutated
			if attempt.err != nil {
				*failures = append(*failures, archiveCompressionRecoveryFailure{
					sourcePath: candidate.sourcePath, err: attempt.err,
					sourceCleanup: errors.Is(attempt.err, appendlog.ErrCompressionSourceCleanup),
				})
			}
			if attempt.completed {
				f.scheduleArchiveUploadRescan(failureCategory, attempt.compressedPath)
			}

		default:
			result, processingErr := processing.ProcessCompression(ctx, processing.CompressionRequest{
				SourcePath: candidate.sourcePath, CompressedPath: candidate.existingCompressedPath, ExistingFileObserved: true,
				RemoveCorruptPeer: true, PreserveMismatchedSource: true,
			}, processing.CompressionOperations{
				VerifyExisting: func(sourcePath, compressedPath string) (bool, error) {
					return processing.VerifyCompressionPeer(sourcePath, compressedPath, f.verifyCompressedPeerContents)
				},
			})
			directoryMutated = directoryMutated || result.FilesMayHaveChanged
			if processingErr != nil {
				*failures = append(*failures, archiveCompressionRecoveryFailure{
					sourcePath: candidate.sourcePath, err: processingErr,
					sourceCleanup: result.FailureStage == processing.CompressionFailureSourceRemoval,
				})
			}
			if result.ReadyForUpload {
				f.scheduleArchiveUploadRescan(failureCategory, result.CompressedPath)
			}
		}
	}
	return directoryMutated
}

// verifyCompressedPeerContentsは、元ファイルと共存する既存の圧縮ファイルを、その拡張子の
// codecで検証します。組み込み形式では最後まで展開しながら元ファイルとの内容一致も確認し、
// 完全でも内容の異なる圧縮ファイルを、元ファイルを削除する根拠にしません。
// 拡張子が一致する現在の独自codecはFsstoreの設定から取得し、その完全性検証を優先します。
func (f *FsArchiveHandler) verifyCompressedPeerContents(sourcePath, compressedPath string) error {
	var custom pkgfsstore.CompressionCodec
	if provider, ok := f.Fsstore.Config.(failurearchive.CompressionCodecProvider); ok {
		custom = provider.CompressionCodec()
	}
	return fsstore.VerifyExistingCompressedFileMatchesSource(sourcePath, compressedPath, custom)
}

// removeFailureArchiveCompressionSourceDeletePendingPathsは、検査結果にある元ファイルの
// 削除待ちパスを片付け、親ディレクトリを同期します。プロセスの停止や削除失敗で残った
// ファイルが対象です。削除待ちの名前を元へ戻さないため、再圧縮の対象にはなりません。
func (f *FsArchiveHandler) removeFailureArchiveCompressionSourceDeletePendingPaths(
	failureCategory failurearchive.FailureCategory,
	inspection archiveDirectoryInspection,
) error {
	fileName := inspection.seriesPattern
	matchName := logpattern.FileNameMatcher(fileName, logpattern.GlobByTarget)
	var paths []string
	for _, path := range inspection.directoryFiles.Paths {
		source, pending := logpattern.CompressionSourcePathFromDeletePendingPath(path)
		if pending && matchName(source) &&
			!logpattern.IsCompressedGenerationFile(source, fileName.Extension) {
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return nil
	}
	var errs []error
	for _, path := range paths {
		if pathErr := inspection.directoryFiles.PathErrors[path]; pathErr != nil {
			if !errors.Is(pathErr, os.ErrNotExist) {
				errs = append(errs, pathErr)
			}
			continue
		}
		sourcePath, ok := logpattern.CompressionSourcePathFromDeletePendingPath(path)
		if !ok {
			errs = append(errs, fmt.Errorf(
				"resolve failure archive compression source from delete-pending path %q",
				path,
			))
			continue
		}
		existingCompressedPath, found, candidateErr := inspection.seriesSources.FindExistingCompressedFile(sourcePath)
		if candidateErr != nil {
			errs = append(errs, candidateErr)
			continue
		}

		state := f.directoryForFailureCategory(failureCategory)
		owner := appendlog.NewDirectoryGuardToken()
		protectedPaths := []string{sourcePath, path}
		if found {
			protectedPaths = append(protectedPaths, existingCompressedPath)
		}
		if state != nil && state.guard != nil &&
			!state.guard.ProtectPathsIfNoOtherOwnerConflict(owner, nil, protectedPaths) {
			continue
		}
		retirementDurable, retirementErr := func() (bool, error) {
			if state != nil && state.guard != nil {
				defer state.guard.UnprotectPaths(owner, protectedPaths...)
			}
			return appendlog.RecoverCompressionSourceDeletePendingPath(path)
		}()
		if retirementDurable && found {
			// 元ファイルの後片付けを確定できたら、未登録だった送信対象の再走査を予約します。
			// 実際の送信は、待機期間後の対象系列の走査を経て行います。
			f.scheduleArchiveUploadRescan(failureCategory, existingCompressedPath)
		}
		if retirementErr != nil {
			errs = append(errs, retirementErr)
		}
	}
	return errors.Join(errs...)
}

// selectUncompressedArchiveRecoveryCandidatesは、取得済みの検査結果から、正規の命名規則に
// 一致する未圧縮ファイルと、対応する圧縮ファイルの情報を選びます。書き込み中などの保護判定は呼び出し元が行います。
func selectUncompressedArchiveRecoveryCandidates(
	sourceGroups fsstore.CompressionSourceGroups,
	fileName logpattern.FileNamePattern,
) []uncompressedArchiveRecoveryCandidate {
	archives := make([]uncompressedArchiveRecoveryCandidate, 0)
	for _, source := range sourceGroups.Sources {
		if !isCanonicalUncompressedArchivePath(source.SourcePath, fileName) {
			continue
		}
		existingPath, _, err := sourceGroups.FindExistingCompressedFile(source.SourcePath)
		sourceKey := pathidentity.ComparisonKey(source.SourcePath)
		for _, file := range source.RegularFiles {
			if pathidentity.ComparisonKey(file.Path) == sourceKey {
				archives = append(archives, uncompressedArchiveRecoveryCandidate{
					sourcePath:             file.Path,
					existingCompressedPath: existingPath,
					preparationErr:         err,
					protectedPaths:         pathidentity.AppendUniquePaths([]string{file.Path}, source.Paths...),
				})
			}
		}
	}
	return archives
}

// isCanonicalUncompressedArchivePathは、パスのファイル名が対象系列の正規の命名規則に一致し、
// 未圧縮ファイルを表すかを返します。ファイルの存在・内容・親ディレクトリは検査しません。
func isCanonicalUncompressedArchivePath(
	path string,
	fileName logpattern.FileNamePattern,
) bool {
	generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
	return canonical && generation.Kind == logpattern.GenerationFileKindUncompressed
}

// scheduleArchiveUploadRescanは、送信対象が見つかった系列について、次の再走査時刻を予約します。
// 送信が有効で未予約の場合だけ、現在時刻からUsageCacheTTL後を設定します。
// 期限後のログ書き込みをきっかけに対象系列を走査します。この呼び出しでは走査・送信を行いません。
// 呼び出し元は対象ディレクトリのlifecycle lockを保持してください。
func (f *FsArchiveHandler) scheduleArchiveUploadRescan(
	failureCategory failurearchive.FailureCategory,
	uploadPath string,
) {
	if f == nil || uploadPath == "" || f.archiveUploader(failureCategory) == nil {
		return
	}
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil {
		return
	}
	state.uploadRetry.armIfUnscheduled(f.Clock.Now(), f.Config.UsageCacheTTL())
}

// notifyArchiveCompressionRecoveryFailuresは、残存ファイルの復旧結果のエラーを通知します。
// 呼び出し元は、検査・復旧に使ったlifecycle lockを解放してから呼んでください。
func (f *FsArchiveHandler) notifyArchiveCompressionRecoveryFailures(
	ctx context.Context,
	failures []archiveCompressionRecoveryFailure,
) {
	for _, failure := range failures {
		if failure.sourceCleanup {
			f.notifyCompressionSourceCleanupError(ctx, failure.sourcePath, failure.err)
		} else {
			f.notifyArchiveCompressionRecoveryError(ctx, failure.sourcePath, failure.err)
		}
	}
}

// notifyArchiveCompressionRecoveryErrorは、残存archiveの圧縮復旧エラーをOnErrorへ通知します。
func (f *FsArchiveHandler) notifyArchiveCompressionRecoveryError(
	ctx context.Context,
	sourcePath string,
	err error,
) {
	message := fmt.Sprintf(ErrorMsgRawArchiveRecovery, sourcePath)
	wrappedErr := &FailureArchiveInternalError{Err: err, Op: OperationCompress}
	entry := f.internalErrorEntryWithContext(ctx, message, err)
	handler.CallErrorHandlerIfPresent(f.OnError, ctx, wrappedErr, entry, f.Tracer, f.Meter)
}

// notifyCompressionSourceCleanupErrorは、圧縮後の元ファイルの後片付けエラーをOnErrorへ通知します。
func (f *FsArchiveHandler) notifyCompressionSourceCleanupError(
	ctx context.Context,
	sourcePath string,
	err error,
) {
	message := fmt.Sprintf(ErrorMsgCompressionSourceCleanup, sourcePath)
	wrappedErr := &FailureArchiveInternalError{Err: err, Op: OperationSourceCleanup}
	entry := f.internalErrorEntryWithContext(ctx, message, err)
	handler.CallErrorHandlerIfPresent(f.OnError, ctx, wrappedErr, entry, f.Tracer, f.Meter)
}
