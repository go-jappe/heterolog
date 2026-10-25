package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// compression_operation_recovery.goは
// runnerで開始した圧縮処理が途中で失敗した場合に、処理を再開するコードです。

// scheduleCompressionOperationRecoveryは、開始済みの圧縮処理が失敗した場合に、再試行を登録します。
// 元ログ・圧縮先・元ログの削除待ちファイルをpurgeから保護し、
// 圧縮の完了後は必要な送信・退避・削除処理も再開します。
// 破損・非通常ファイル・元ログと削除待ちファイルの競合・元ログと内容の異なる圧縮ファイルは、
// 再試行に登録しません。
// 再試行では新しいファイルを処理対象に加えず、purgeも実行しません。
func (m *FileSeriesManager) scheduleCompressionOperationRecovery(source, compressedPath string, failure error) {
	if m.manualRecovery {
		return
	}
	if errors.Is(failure, pkgfsstore.ErrCorruptCompressedFile) ||
		errors.Is(failure, appendlog.ErrCompressedPeerNotRegular) ||
		errors.Is(failure, fsstore.ErrNotRegularFile) ||
		errors.Is(failure, appendlog.ErrCompressionSourceDeletePendingConflict) ||
		errors.Is(failure, fsstore.ErrCompressedContentMismatch) {
		return
	}
	generation, ok := m.parseFileGeneration(source)
	if !ok || compressedPath == "" {
		return
	}
	key := logFileProcessingRecoveryKey{File: fileCheckpointKey(generation, true), Compression: true}
	if m.logFileProcessingRecovery.Has(key) {
		return
	}
	token := appendlog.NewDirectoryGuardToken()
	paths := []string{source, compressedPath, logpattern.CompressionSourceDeletePendingPath(source)}
	if !m.directoryGuard.ProtectPathsIfNoOtherOwnerConflict(token, m.directoryToken, paths) {
		return
	}
	release := func() { m.directoryGuard.UnprotectPaths(token, paths...) }
	var report RunReport
	var prepared bool
	resumeUpload := true
	var successes []objectstorage.DestinationID
	if !m.logFileProcessingRecovery.Schedule(key, failure, func(ctx context.Context) (done bool, err error) {
		report = RunReport{DirectoryPath: m.fileName.Dir}
		err = m.directoryGuard.RunWithLifecycleLockContext(ctx, func() error {
			if !prepared {
				var preparationErr error
				prepared, preparationErr = m.resumeAcceptedCompression(ctx, source, compressedPath, &report)
				if preparationErr != nil || !prepared {
					return preparationErr
				}
			}
			var uploadErr error
			done, uploadErr = m.recoverLogFileProcessing(ctx, compressedPath, generation, true, &resumeUpload, &successes, &report)
			return uploadErr
		})
		return done, err
	}, func(err error) func() {
		report := report
		return func() { m.reportLogFileProcessingRecovery(report, err) }
	}, release) {
		release()
	}
}

// resumeAcceptedCompressionは固定した元ログ・圧縮先・削除待ちだけを操作します。
// 公開済みpeerがある場合は検証・同期後に元ログを退避し、再圧縮しません。
// この登録が両fileを保護しているため、peerの異常は通常周期へ委ねずにここで解消します。
// 破損したpeerは元ログを残して削除し、次回の試行で元ログから再圧縮します。
// peerが完全でも元ログと内容が異なる場合は、元ログを.preservedへ移してerrorを返し、
// 次回の試行でpeerの処理を再開します。
func (m *FileSeriesManager) resumeAcceptedCompression(ctx context.Context, source, compressedPath string, report *RunReport) (bool, error) {
	_, sourceErr := fsstore.InspectRegularFile(source)
	if sourceErr != nil && !errors.Is(sourceErr, os.ErrNotExist) {
		return false, sourceErr
	}
	pending := logpattern.CompressionSourceDeletePendingPath(source)
	_, pendingErr := fsstore.InspectRegularFile(pending)
	if pendingErr == nil {
		if _, err := appendlog.RecoverCompressionSourceDeletePendingPath(pending); err != nil {
			return false, err
		}
		report.RecoveredCompressionSourceDeletePendingPaths = append(report.RecoveredCompressionSourceDeletePendingPaths, pending)
	} else if !errors.Is(pendingErr, os.ErrNotExist) {
		return false, pendingErr
	}
	if errors.Is(sourceErr, os.ErrNotExist) {
		valid, err := m.existingCompressedFileIsValid(source, compressedPath)
		if err != nil {
			return false, err
		}
		if valid {
			return true, fsstore.SyncDirectory(filepath.Dir(compressedPath))
		}
		// 受理後に管理者が削除した元ログを新しく生成することはありません。
		return true, fsstore.SyncDirectory(filepath.Dir(source))
	}
	compression, err := processing.ProcessCompression(ctx, processing.CompressionRequest{
		SourcePath: source, CompressedPath: compressedPath,
		RemoveCorruptPeer: true, PreserveMismatchedSource: true,
	}, processing.CompressionOperations{
		VerifyExisting: func(sourcePath, peerPath string) (bool, error) {
			valid, err := m.existingCompressedFileIsValid(sourcePath, peerPath)
			if err != nil || !valid {
				return valid, err
			}
			// 公開直後の同期失敗からの復旧では、元ログを削除する前に同期を完了します。
			return true, fsstore.SyncDirectory(filepath.Dir(peerPath))
		},
		Compress: m.compressFile, RemoveSource: m.removeSourceAfterCompression,
		RemoveInvalid: m.removeInvalidCompressedPeer,
	})
	if err != nil {
		return false, err
	}
	if !compression.ExistingFileVerified {
		report.Compressed = append(report.Compressed, CompressionReport{SourcePath: source, CompressedPath: compression.CompressedPath})
	}
	return compression.ReadyForUpload, nil
}

// 圧縮commit後に失敗した削除待ちの後片付けを、uploadとは独立して完了まで待ちます。
// 通常のpurge候補や起動時の残骸を新たに登録するためには使用しません。
func (m *FileSeriesManager) scheduleCompressionSourceCleanup(source string, failure error) {
	if m.manualRecovery {
		return
	}
	path := logpattern.CompressionSourceDeletePendingPath(source)
	key := logFileProcessingRecoveryKey{CleanupPath: path}
	if m.logFileProcessingRecovery.Has(key) {
		return
	}
	var report RunReport
	m.logFileProcessingRecovery.Schedule(key, failure, func(ctx context.Context) (done bool, err error) {
		report = RunReport{DirectoryPath: m.fileName.Dir}
		err = m.directoryGuard.RunWithLifecycleLockContext(ctx, func() error {
			_, err := fsstore.InspectRegularFile(path)
			if errors.Is(err, os.ErrNotExist) {
				err = fsstore.SyncDirectory(filepath.Dir(path))
			} else if err == nil {
				_, err = appendlog.RecoverCompressionSourceDeletePendingPath(path)
			}
			if err == nil {
				done = true
				report.RecoveredCompressionSourceDeletePendingPaths = append(report.RecoveredCompressionSourceDeletePendingPaths, path)
			}
			return err
		})
		return done, err
	}, func(err error) func() {
		report := report
		return func() { m.reportLogFileProcessingRecovery(report, err) }
	}, nil)
}
