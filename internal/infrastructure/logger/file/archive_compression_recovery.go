package file

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

type observedRawArchive struct {
	sourcePath string
	// existingCompressedPathは観測済みの圧縮ファイルです。未発見なら空です。
	existingCompressedPath string
	// outputPathは既存ファイルがない場合だけ設定する新規圧縮先です。
	outputPath          string
	uploadScheduleOwned bool
}

// recoverObservedRawArchivesForOwnerは、ディレクトリ全体の観測で見つかった未圧縮ログを回復します。
// 対象は、設定上このディレクトリに保存する各ログ系列（通常・error・debug）のうち、
// アプリの正規の命名規則に一致し、現在書き込み中ではないファイルです。
// 新規圧縮には現在のcodecを使い、既存の圧縮ファイルはその形式で検証して再利用します。
// 別の処理が保護している元ログ・圧縮ファイルは処理しません。
// 別のログ系列の圧縮ファイルは、このloggerの送信再試行には登録しません。
// 設定によるpurge保護を維持し、担当loggerの初期化時の走査へ引き継ぎます。
// まだloggerを生成していないerror/debugの系列も対象です。
//
// final compressed peerが存在しないsourceは再圧縮します。peerが存在する場合はその形式のcodecで
// 完全性を検証し、組み込み形式では展開した内容がsourceと一致することも確認します。
// validなpeerは再生成せずsourceだけをretireします。これにより、一度commitした圧縮成果物を
// 上書きせず、source cleanup失敗も次の全体観測で回復できます。
// 破損を確認したpeerだけはsourceを残して削除し、次の全体観測で再生成します。
// 完全でも内容の異なるpeerは残し、sourceを削除せず.preservedへ退避してerrorで通知します。
// 未知codecやI/O障害で検証できない場合はsourceとpeerの両方を保全します。
//
// retainedPathsOwnerにはretain完了直後にownershipを公開します。callerはpanicを含む
// 全終了経路でUnprotectPathsしなければなりません。呼び出し元はdirectory lifecycle
// lockを取得済みでなければなりません。
// 回復中の元ログ・圧縮ファイルの保護は、今回のcleanupのownerTokenへ登録します。
// 書き込み中のファイルや、並行して実行するファイル処理のtokenとは共有しません。
func (l *FileLogger) recoverObservedRawArchivesForOwner(
	ctx context.Context,
	sourceGroups fsstore.CompressionSourceGroups,
	fileName logpattern.FileNamePattern,
	deferredSourcePath string,
	retainedPathsOwner *[]string,
	ownerToken *appendlog.DirectoryGuardToken,
	notify taskNotificationDelivery,
) (directoryMutated bool, err error) {
	if l == nil || l.inlinePurge == nil || retainedPathsOwner == nil || ownerToken == nil {
		return false, errors.New("raw archive recovery owner is nil")
	}

	if !l.config.Fsstore().Lifecycle().EnableFileCompress() {
		return false, nil
	}

	candidates := observedCanonicalRawArchives(
		sourceGroups.Files.RegularFiles,
		fileName,
	)
	foreignCandidates := observedConfiguredForeignRawArchives(
		sourceGroups.Files.RegularFiles,
		fileName,
		l.configuredPhysicalRawArchivePatterns(fileName),
	)
	candidates = append(candidates, foreignCandidates...)
	// rawがない世代も同じ観測の拒否pathから検査し、衝突した通常peerを保全します。
	patterns := l.configuredPhysicalRawArchivePatterns(fileName)
	for _, source := range sourceGroups.Sources {
		if source.Err == nil {
			continue
		}
		sourcePath := source.SourcePath
		owned := false
		for _, pattern := range patterns {
			if generation, canonical := logpattern.ParseCanonicalGenerationFilePath(sourcePath, pattern); canonical &&
				generation.Kind == logpattern.GenerationFileKindUncompressed {
				owned = true
				break
			}
		}
		if !owned {
			continue
		}
		protection := append([]string{sourcePath}, source.Paths...)
		if l.lifecycleGuard.ProtectPathsIfNoOtherOwnerConflict(ownerToken, nil, protection) {
			*retainedPathsOwner = append(*retainedPathsOwner, protection...)
			l.reportRawArchiveRecoveryError(ctx, sourcePath, source.Err, notify)
		}
	}
	selected := make([]observedRawArchive, 0, len(candidates))
	deferredSource := pathidentity.ComparisonKey(deferredSourcePath)
	for _, candidate := range candidates {
		existingPath, _, inspectErr := sourceGroups.FindExistingCompressedFile(candidate.sourcePath)
		if inspectErr != nil {
			// この元ログの検査エラーは、上で関連パスを保護して通知済みです。
			continue
		}
		// 検査エラーのない観測で選んだ既存パスを渡し、再走査せず圧縮先を決めます。
		compressedPath, exists, err := fsstore.ResolveCompressedFilePath(
			candidate.sourcePath, []string{existingPath}, l.Fsstore.GetCompressFormat(),
		)
		if err != nil {
			return false, err
		}
		if exists {
			candidate.existingCompressedPath = compressedPath
		} else {
			candidate.outputPath = compressedPath
		}
		// 圧縮形式に依存しないraw pathを、rotation・回復・upload共通の保護キーにします。
		paths := []string{candidate.sourcePath}
		if candidate.existingCompressedPath != "" {
			paths = append(paths, candidate.existingCompressedPath)
		} else {
			paths = append(paths, candidate.outputPath)
		}
		if !l.lifecycleGuard.ProtectPathsIfNoOtherOwnerConflict(ownerToken, nil, paths) {
			continue
		}
		// 圧縮・検証がpanicしても解除できるよう、取得直後にownershipを公開します。
		*retainedPathsOwner = append(*retainedPathsOwner, paths...)
		if deferredSource != "" && pathidentity.ComparisonKey(candidate.sourcePath) == deferredSource {
			// rotation直後は同じsourceを再試行せず、次の自然なfull scanへ戻します。
			continue
		}
		selected = append(selected, candidate)
	}
	if len(selected) == 0 {
		return false, nil
	}
	for _, candidate := range selected {
		switch {
		case candidate.existingCompressedPath == "":
			compressedPath, compressed := l.tryCompressOldFile(
				ctx,
				candidate.sourcePath,
				notify,
				&sourceGroups,
			)
			if !compressed {
				// 圧縮済みfinalの公開後、canonical sourceのretirement前に失敗することが
				// あります。圧縮前snapshotにはこのpeerが含まれないため破棄し、次の
				// full scanでpeerを検証して、再圧縮せずsourceをretireします。
				// retirement完了前のpeerだけでは成功証明にならないため、まだuploadは
				// scheduleしません。
				if _, peerErr := os.Lstat(candidate.outputPath); peerErr == nil {
					directoryMutated = true
				} else if !errors.Is(peerErr, os.ErrNotExist) {
					l.reportRawArchiveRecoveryError(
						ctx,
						candidate.sourcePath,
						fmt.Errorf("lstat compressed peer after compression failure %s: %w", candidate.outputPath, peerErr),
						notify,
					)
				}
				continue
			}
			directoryMutated = true
			l.scheduleRecoveredCompressedArchiveUploadIfOwned(candidate, compressedPath)

		default:
			result, processingErr := processing.ProcessCompression(ctx, processing.CompressionRequest{
				SourcePath: candidate.sourcePath, CompressedPath: candidate.existingCompressedPath, ExistingFileObserved: true,
				RemoveCorruptPeer: true, PreserveMismatchedSource: true,
			}, processing.CompressionOperations{
				VerifyExisting: func(sourcePath, compressedPath string) (bool, error) {
					return processing.VerifyCompressionPeer(sourcePath, compressedPath, l.verifyCompressedPeerContents)
				},
			})
			processingErr = withPreservedSource(processingErr, result.PreservedSourcePath)
			directoryMutated = directoryMutated || result.FilesMayHaveChanged
			if processingErr != nil {
				if result.FailureStage == processing.CompressionFailureSourceRemoval {
					l.reportCompressionSourceCleanupError(ctx, candidate.sourcePath, processingErr, notify)
				} else {
					l.reportRawArchiveRecoveryError(ctx, candidate.sourcePath, processingErr, notify)
				}
			}
			if result.ReadyForUpload {
				l.scheduleRecoveredCompressedArchiveUploadIfOwned(candidate, result.CompressedPath)
			}
		}
	}
	return directoryMutated, nil
}

// removeCompressionSourceDeletePendingPathsは、source retirementのrename後にprocessが
// 終了した場合などに残ったreserved pathの削除を再試行します。directory full scanで
// 得た観測を使います。変更するのは通常ログの容量・purge候補に含めないpending名だけで、
// raw/peerの観測情報は変更しません。削除・比較・退避・同期の失敗はcallerへ返します。
// pending pathはcanonical rawへ戻さず、compressed peerが外部削除されていても
// 再圧縮候補にはしません。
func (l *FileLogger) removeCompressionSourceDeletePendingPaths(
	fileName logpattern.FileNamePattern,
	sourceGroups fsstore.CompressionSourceGroups,
) error {
	var errs []error
	for _, path := range sourceGroups.Files.Paths {
		sourcePath, ok := logpattern.CompressionSourcePathFromDeletePendingPath(path)
		if !ok {
			continue
		}
		existingCompressedPath, found, inspectErr := sourceGroups.FindExistingCompressedFile(sourcePath)
		if inspectErr != nil {
			errs = append(errs, inspectErr)
			continue
		}
		// 削除待ちの回復では新規圧縮しません。圧縮ファイルが消えていても後片付けできます。
		retirementDurable, retirementErr := appendlog.RecoverCompressionSourceDeletePendingPath(path)
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(
			existingCompressedPath,
			fileName,
		)
		if found && retirementDurable && canonical &&
			generation.Kind == logpattern.GenerationFileKindCompressed {
			// outer errorで失われたupload予約を、raw retirementが永続化された時点で
			// owning seriesへ戻します。今回のplanは既に確定済みなので、remote I/Oは
			// 次のTTL後のtarget-only観測で開始します。
			l.scheduleRecoveredCompressedArchiveUpload(existingCompressedPath)
		}
		if retirementErr != nil {
			errs = append(errs, retirementErr)
		}
	}
	return errors.Join(errs...)
}

func observedCanonicalRawArchives(
	files []fsstore.FileInfo,
	fileName logpattern.FileNamePattern,
) []observedRawArchive {
	archives := make([]observedRawArchive, 0)
	for _, file := range files {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(file.Path, fileName)
		if !canonical || generation.Kind != logpattern.GenerationFileKindUncompressed {
			continue
		}
		archives = append(archives, observedRawArchive{
			sourcePath:          file.Path,
			uploadScheduleOwned: true,
		})
	}
	return archives
}

// configuredPhysicalRawArchivePatternsは、現在のディレクトリに保存する各ログ系列
// （通常・error・debug）の命名規則を返します。まだloggerを生成していない系列も、
// 構築時に確定した実体ディレクトリで照合します。ディレクトリの作成やリンクの再解決は行いません。
func (l *FileLogger) configuredPhysicalRawArchivePatterns(
	current logpattern.FileNamePattern,
) []logpattern.FileNamePattern {
	if l == nil || l.config == nil {
		return []logpattern.FileNamePattern{current}
	}

	layout := l.config.Fsstore().Layout()
	currentDirectory := pathidentity.ComparisonKey(current.Dir)
	patterns := make([]logpattern.FileNamePattern, 0, 3)
	seen := make(map[string]struct{}, 3)
	add := func(prefix string, configuredDirectory string) {
		if configuredDirectory == "" || pathidentity.ComparisonKey(configuredDirectory) != currentDirectory {
			return
		}
		key := prefix + "\x00" + layout.Target()
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		pattern := current
		pattern.Dir = current.Dir
		pattern.Prefix = prefix
		pattern.Target = layout.Target()
		patterns = append(patterns, pattern)
	}

	// current patternはstandalone constructorなどの非標準構成でも必ず含めます。
	add(current.Prefix, current.Dir)
	add("", l.directoryPaths[DefaultLogger])
	// enabled状態を問わず既知prefixを含めます。前processまたは設定変更前に作られた
	// rawが現在も同じconfigured directoryへ残っている場合も回復対象です。
	add(
		layout.SeparateErrorLogsPrefix(),
		l.directoryPaths[ErrorLogger],
	)
	add(
		layout.SeparateDebugLogsPrefix(),
		l.directoryPaths[DebugLogger],
	)
	return patterns
}

// observedConfiguredForeignRawArchivesは、同じdirectoryの既知physical系列のうち、
// current系列ではないcanonical rawを返します。新規圧縮には現在のcodecを使いますが、
// current loggerがforeign seriesのupload scheduleを所有することはありません。
func observedConfiguredForeignRawArchives(
	files []fsstore.FileInfo,
	current logpattern.FileNamePattern,
	configured []logpattern.FileNamePattern,
) []observedRawArchive {
	archives := make([]observedRawArchive, 0)
	for _, file := range files {
		generation, canonical := logpattern.ParseCanonicalGenerationFilePath(file.Path, current)
		if canonical && generation.Kind == logpattern.GenerationFileKindUncompressed {
			continue
		}
		for _, candidate := range configured {
			generation, canonical = logpattern.ParseCanonicalGenerationFilePath(
				file.Path,
				candidate,
			)
			if canonical && generation.Kind == logpattern.GenerationFileKindUncompressed {
				archives = append(archives, observedRawArchive{
					sourcePath: file.Path,
				})
				break
			}
		}
	}
	return archives
}

func (l *FileLogger) scheduleRecoveredCompressedArchiveUploadIfOwned(
	archive observedRawArchive,
	compressedPath string,
) {
	if !archive.uploadScheduleOwned {
		return
	}
	l.scheduleRecoveredCompressedArchiveUpload(compressedPath)
}

func (l *FileLogger) scheduleRecoveredCompressedArchiveUpload(compressedPath string) {
	if l == nil || l.inlinePurge == nil || l.ArchiveUploader == nil || compressedPath == "" {
		return
	}
	l.inlinePurge.seriesUploadRetry.scheduleRetryIfUnscheduled(
		l.Clock.Now(),
		l.inlinePurge.config.UsageCacheTTL(),
	)
}

func (l *FileLogger) reportRawArchiveRecoveryError(
	ctx context.Context,
	sourcePath string,
	err error,
	notify taskNotificationDelivery,
) {
	message := fmt.Sprintf(ErrorMsgRawArchiveRecovery, sourcePath)
	wrappedErr := l.wrapErrorByLogger(err, OperationCompress)
	entry := l.InternalErrorEntryWithContext(ctx, message, err)
	notify(func() {
		l.dispatchError(ctx, wrappedErr, entry)
	})
}

func (l *FileLogger) reportCompressionSourceCleanupError(
	ctx context.Context,
	sourcePath string,
	err error,
	notify taskNotificationDelivery,
) {
	message := fmt.Sprintf(ErrorMsgCompressionSourceCleanup, sourcePath)
	wrappedErr := l.wrapErrorByLogger(err, OperationSourceCleanup)
	entry := l.InternalErrorEntryWithContext(ctx, message, err)
	notify(func() {
		l.dispatchError(ctx, wrappedErr, entry)
	})
}
