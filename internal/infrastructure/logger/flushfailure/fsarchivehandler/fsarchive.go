package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// ログファイルを開く、または作成
func (fh *FsArchiveHandler) openRetryableFile(ctx context.Context, at time.Time) error {
	previousPath := fh.Fsstore.ActiveFilePathRetryable
	protectedPaths := fh.protectRotatedFileBeforeTransition(
		failurearchive.FailureCategoryRetryable,
		previousPath,
	)
	var oldFilePath string
	err := fh.retryableDirectory.guard.TransitionActive(
		fh.retryableDirectory.token,
		func() (string, error) {
			var err error
			oldFilePath, err = fh.Fsstore.UpdateFileRetryableFilePathAndGetOldpath(at)
			if err != nil {
				return "", err
			}
			path := fh.Fsstore.ActiveFilePathRetryable
			file, err := failurearchive.OpenFile(path)
			if err != nil {
				fh.Fsstore.ActiveFilePathRetryable = previousPath
				return "", err
			}
			fh.Fsstore.FileRetryable = file
			return path, nil
		},
	)
	if preparationErr := fh.finishRotatedFileTransition(
		ctx,
		fh.Fsstore.ActiveFilePathRetryable,
		oldFilePath,
		failurearchive.FailureCategoryRetryable,
		protectedPaths,
		err,
	); preparationErr != nil {
		// 構築中の入口なのでwriter lockは保持していません。既存ログの処理を
		// 開始できなかった異常を通知し、開けたactive fileはそのまま使用します。
		fh.notifyCompressedArchiveUploadCheckpointError(ctx, oldFilePath, preparationErr)
	}
	return err
}

func (fh *FsArchiveHandler) openPermanentFile(ctx context.Context, at time.Time) error {
	previousPath := fh.Fsstore.ActiveFilePathPermanent
	protectedPaths := fh.protectRotatedFileBeforeTransition(
		failurearchive.FailureCategoryPermanent,
		previousPath,
	)
	var oldFilePath string
	err := fh.permanentDirectory.guard.TransitionActive(
		fh.permanentDirectory.token,
		func() (string, error) {
			var err error
			oldFilePath, err = fh.Fsstore.UpdateFilePermanentFilePathAndGetOldpath(at)
			if err != nil {
				return "", err
			}
			path := fh.Fsstore.ActiveFilePathPermanent
			file, err := failurearchive.OpenFile(path)
			if err != nil {
				fh.Fsstore.ActiveFilePathPermanent = previousPath
				return "", err
			}
			fh.Fsstore.FilePermanent = file
			return path, nil
		},
	)
	if preparationErr := fh.finishRotatedFileTransition(
		ctx,
		fh.Fsstore.ActiveFilePathPermanent,
		oldFilePath,
		failurearchive.FailureCategoryPermanent,
		protectedPaths,
		err,
	); preparationErr != nil {
		fh.notifyCompressedArchiveUploadCheckpointError(ctx, oldFilePath, preparationErr)
	}
	return err
}

// processRotatedArchiveは、ローテーションで書き込み対象から外れたfailure archiveを処理します。
// 設定に応じて圧縮、送信・失敗時の退避、checkpoint更新を行い、全宛先の処理が
// 完了した圧縮ファイルを削除します。途中で完了できなかった処理は再試行へ登録します。
//
// fileNameには、呼び出し元がwriterのlock内で確定した対象系列の命名規則を渡してください。
// compressionCandidateは事前に確認した圧縮先の情報で、nilなら処理内で既存ファイルを探します。
// ファイルの作成・名前変更・削除に伴うディレクトリ使用量の概算の無効化は、呼び出し元が行います。
// rotation taskでは、一連の処理を終えた後の完了処理で無効化します。
func (fh *FsArchiveHandler) processRotatedArchive(
	ctx context.Context,
	activeFilePath string,
	oldFilePath string,
	failureCategory failurearchive.FailureCategory,
	fileName logpattern.FileNamePattern,
	compressionCandidate *uncompressedArchiveRecoveryCandidate,
) (uploadRetryPending bool, err error) {
	var (
		uploadPath  string
		ok          bool
		compression archiveCompressionAttempt
	)
	defer func() {
		if recovered := recover(); recovered != nil {
			queued := false
			if groups, groupErr := groupFailureArchiveUploadPaths(fileName, []string{uploadPath}); groupErr == nil && len(groups) == 1 {
				queued = fh.archiveGenerationContinuationAccepted(failureCategory, groups[0].key)
			}
			// 既存のpanic通知を保ち、既知の圧縮結果を引き継いで再開します。
			// 送信を始めた世代は、送信側が受理済みの再開処理へ登録済みです。
			if !queued {
				fh.scheduleArchiveProcessingRetry(oldFilePath, uploadPath, failureCategory, fileName)
			}
			panic(recovered)
		}
	}()
	archiveMutation := func() error {
		if oldFilePath == "" || oldFilePath == activeFilePath {
			return nil
		}
		if !fh.Fsstore.ShouldCompressFile(activeFilePath, oldFilePath) {
			uploadPath, ok = oldFilePath, true
			return nil
		}
		compression = fh.tryCompressArchiveSource(ctx, oldFilePath, compressionCandidate)
		uploadPath, ok = compression.compressedPath, compression.completed
		return nil
	}
	if state := fh.directoryForFailureCategory(failureCategory); state != nil && state.guard != nil {
		_ = state.guard.RunWithLifecycleLock(archiveMutation)
	} else {
		_ = archiveMutation()
	}
	// 圧縮の排他を解放した時点で通知し、送信・削除の完了までは待ちません。
	fh.notifyArchiveCompressionAttempt(ctx, oldFilePath, compression)
	if !ok {
		if oldFilePath != "" && oldFilePath != activeFilePath {
			fh.scheduleArchiveProcessingRetry(oldFilePath, "", failureCategory, fileName)
		}
		return false, nil
	}
	compressed := uploadPath != oldFilePath
	if compressed {
		pendingPath := logpattern.CompressionSourceDeletePendingPath(oldFilePath)
		if _, pendingErr := os.Lstat(pendingPath); errors.Is(compression.err, appendlog.ErrCompressionSourceCleanup) || pendingErr == nil || !errors.Is(pendingErr, os.ErrNotExist) {
			fh.scheduleCompressionSourceCleanupRetry(oldFilePath, failureCategory,
				fmt.Errorf("compression source cleanup remains incomplete for %q", oldFilePath))
		}
	}
	// 送信は、構築時に宛先付きuploaderとcheckpoint Storeを用意した系列だけで行います。
	// 送信を設定していない系列では、圧縮fileを通常の保持・purgeへ残します。
	if fh.compressedArchiveUploadCheckpointStore(failureCategory) == nil {
		return false, nil
	}
	outcome := fh.uploadCheckpointedArchiveGroup(ctx, []string{uploadPath}, failureCategory, fileName)
	return outcome.retryRequired, nil
}

// archiveCompressionAttemptは、圧縮成果物を後続処理へ渡せるかと、観測済みdirectoryが
// 変化したかを分離します。final path公開後にsource retirement前で失敗した場合は、
// completed=falseでもdirectoryMutated=trueです。
type archiveCompressionAttempt struct {
	compressedPath   string
	completed        bool
	directoryMutated bool
	err              error
}

// tryCompressArchiveSourceは圧縮・元ファイルの後片付けを試み、その結果を返します。
// 呼び出し元がlifecycle lockを保持し、解放後に結果のエラーを通知してください。
func (fh *FsArchiveHandler) tryCompressArchiveSource(
	ctx context.Context,
	sourcePath string,
	compressionCandidate *uncompressedArchiveRecoveryCandidate,
) archiveCompressionAttempt {
	var attempt archiveCompressionAttempt
	// ファイル圧縮処理に対して span を切る
	ctx, span := fh.Tracer.StartSpan(ctx, SpanFileCompress)
	format := fh.Fsstore.GetCompressFormat()
	span.SetAttributes(
		pkgobserver.Attr{
			Key:   "compress.format",
			Value: string(format),
		},
	)
	defer span.End()
	// 圧縮処理
	result, err := fh.compressArchiveSourceOrReuseExisting(
		ctx,
		sourcePath,
		compressionCandidate,
	)
	attempt.directoryMutated = result.FilesMayHaveChanged
	attempt.err = err
	if err != nil && !errors.Is(err, appendlog.ErrCompressionSourceCleanup) {
		return attempt
	}
	if !result.ReadyForUpload {
		return attempt
	}
	attempt.compressedPath = result.CompressedPath
	attempt.completed = true
	return attempt
}

// notifyArchiveCompressionAttemptは、圧縮または元ファイルの後片付けの失敗を通知します。
// 呼び出し元は圧縮に使ったlifecycle lockを解放してから呼んでください。
func (fh *FsArchiveHandler) notifyArchiveCompressionAttempt(
	ctx context.Context,
	sourcePath string,
	attempt archiveCompressionAttempt,
) {
	if attempt.err == nil {
		return
	}
	if errors.Is(attempt.err, appendlog.ErrCompressionSourceCleanup) {
		fh.notifyCompressionSourceCleanupError(ctx, sourcePath, attempt.err)
		return
	}
	message := fmt.Sprintf(ErrorMsgFileCompress, sourcePath)
	wrappedErr := &FailureArchiveInternalError{Err: attempt.err, Op: OperationCompress}
	entry := fh.internalErrorEntryWithContext(ctx, message, attempt.err)
	handler.CallErrorHandlerIfPresent(fh.OnError, ctx, wrappedErr, entry, fh.Tracer, fh.Meter)
}

// compressArchiveSourceOrReuseExistingは、元ログを新しく圧縮するか、既存の圧縮ファイルを検証して再利用します。
// 既存の圧縮ファイルが複数あれば、取得した一覧で最初に見つかったものを使い、
// その拡張子に対応する圧縮方式で内容を検証します。検証に成功したら、
// 元ログを削除待ちの名前に変更してから削除します。既存ファイルがなければ、現在の設定で圧縮します。
// 既存候補が通常ファイルでない、検証できない圧縮形式である、検証中にI/O障害が起きたなどの
// 場合は、元ログと既存候補を削除・上書きせずにエラーを返します。この場合、新たな圧縮も行いません。
// 既存候補の破損を確認した場合は元ログを残して既存候補を削除し、次の試行で再圧縮します。
// 既存候補が完全でも元ログと内容が異なる場合は、既存候補を残して元ログを.preservedへ退避し、
// エラーを返します。受理済みの再開処理が元ログを保護したまま再試行し続けないためです。
// 呼び出し元は元ログと圧縮先のパスを保護し、対象ディレクトリのlifecycle lockを保持してください。
func (fh *FsArchiveHandler) compressArchiveSourceOrReuseExisting(
	ctx context.Context,
	sourcePath string,
	compressionCandidate *uncompressedArchiveRecoveryCandidate,
) (processing.CompressionResult, error) {
	var existingCompressedPath string
	var found bool
	var err error
	if candidate := compressionCandidate; candidate != nil {
		existingCompressedPath, err = candidate.existingCompressedPath, candidate.preparationErr
		found = existingCompressedPath != ""
	} else {
		existingCompressedPath, found, err = fh.findExistingCompressedArchivePath(sourcePath)
	}
	if err != nil {
		return processing.CompressionResult{}, err
	}
	return processing.ProcessCompression(ctx, processing.CompressionRequest{
		SourcePath: sourcePath, CompressedPath: existingCompressedPath, ExistingFileObserved: found,
		RemoveCorruptPeer: true, PreserveMismatchedSource: true,
	}, processing.CompressionOperations{
		VerifyExisting: func(sourcePath, compressedPath string) (bool, error) {
			return processing.VerifyCompressionPeer(sourcePath, compressedPath, fh.verifyCompressedPeerContents)
		},
		Compress: fh.Fsstore.CompressOldFileWithResult,
	})
}

func (fh *FsArchiveHandler) findExistingCompressedArchivePath(sourcePath string) (string, bool, error) {
	directory := filepath.Dir(sourcePath)
	entries, err := fsstore.ReadDirectoryEntries(directory)
	if err != nil {
		return "", false, fmt.Errorf("find existing compressed failure archive: %w", err)
	}
	observedPaths := make([]string, 0, len(entries))
	baseExtension := filepath.Ext(sourcePath)
	sourceKey := pathidentity.ComparisonKey(sourcePath)
	for _, entry := range entries {
		path := filepath.Join(directory, entry.Name())
		if source, ok := fsstore.CompressionSourcePath(path, baseExtension); ok &&
			pathidentity.ComparisonKey(source) == sourceKey {
			observedPaths = append(observedPaths, path)
		}
	}
	// rotationではこの元ログに対応する名前だけを検査し、他世代や一時fileをLstatしません。
	sourceGroups := fsstore.GroupFilesByCompressionSource(fh.inspectArchivePaths(observedPaths), baseExtension)
	return sourceGroups.FindExistingCompressedFile(sourcePath)
}

// FailureArchiveInternalError はfile archive lifecycle内の操作失敗を表します。
type FailureArchiveInternalError struct {
	Op  string
	Err error
}

func (e *FailureArchiveInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *FailureArchiveInternalError) Unwrap() error {
	return e.Err
}

// ArchiveTaskPanicErrorは、rotation後の圧縮・送信taskで回復したpanicを表します。
// Valueは回復した値、Stackは回復した時点のgoroutineのstack traceです。
type ArchiveTaskPanicError struct {
	Value any
	Stack []byte
}

func (e *ArchiveTaskPanicError) Error() string {
	return fmt.Sprintf("failure archive rotation task panic: %v\n%s", e.Value, e.Stack)
}
