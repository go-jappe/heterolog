package file

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// tryCompressOldFileは、設定に応じて元ログを圧縮し、後続処理へ渡すファイルパスを返します。
// 圧縮が無効なら元のパスを返します。後続処理へ進める場合はtrue、検査・圧縮に失敗した場合は
// 空のパスとfalseを返します。既存の圧縮ファイルが破損していれば元ログを残して削除し、
// 次の試行で再圧縮します。完全でも元ログと内容が異なる場合は、元ログを削除せず
// .preservedへ退避し、退避先を持つPreservedSourceErrorで失敗を通知します。
// 圧縮中の遅延・失敗は、通知用の関数をnotifyへ渡して通知します。
// 通知を保留する呼び出し元はqueue.enqueue、即時通知する呼び出し元はrunNotificationImmediatelyを渡します。
// sourceGroupsには元ファイルごとにまとめた候補と検査エラーを渡します。
// sourceGroupsがnilなら、この入口で名前一覧とファイル種別を検査します。
func (l *FileLogger) tryCompressOldFile(
	ctx context.Context,
	oldFilePath string,
	notify taskNotificationDelivery,
	sourceGroups *fsstore.CompressionSourceGroups,
) (string, bool) {
	uploadPath := oldFilePath

	// oldFilePathはrotation成功時のsnapshotです。圧縮可否はimmutableな設定だけで判定し、
	// 次のrotationが更新し得るFsstore.ActiveFilePathを非同期goroutineから読みません。
	if oldFilePath != "" && l.config.Fsstore().Lifecycle().EnableFileCompress() {
		ctx, span := l.Tracer.StartSpan(ctx, SpanFileCompress)
		format := l.Fsstore.GetCompressFormat()
		span.SetAttributes(
			observer.Attr{
				Key:   "compress.format",
				Value: string(format),
			},
		)
		defer span.End()

		// 圧縮処理
		delayLog := func(eventCtx context.Context, elapsed time.Duration, meta any) {
			notify(l.compressDelayNotification(eventCtx, elapsed, meta))
		}
		var existingCompressedPath string
		var exists bool
		var err error
		if sourceGroups == nil {
			existingCompressedPath, exists, err = l.findExistingCompressedArchivePath(oldFilePath)
		} else {
			existingCompressedPath, exists, err = sourceGroups.FindExistingCompressedFile(oldFilePath)
		}
		var result processing.CompressionResult
		if err == nil {
			// 受理済みの再開処理はsourceを保護して再試行するため、破損したpeerや内容の
			// 異なるpeerを通常走査へ委ねず、この場で破損peerを削除するか、sourceを
			// .preservedへ退避します。破損peerを削除したsourceは次の試行で再圧縮します。
			result, err = processing.ProcessCompression(ctx, processing.CompressionRequest{
				SourcePath: oldFilePath, CompressedPath: existingCompressedPath, ExistingFileObserved: exists,
				RemoveCorruptPeer: true, PreserveMismatchedSource: true,
			}, processing.CompressionOperations{
				VerifyExisting: func(sourcePath, compressedPath string) (bool, error) {
					return processing.VerifyCompressionPeer(sourcePath, compressedPath, l.verifyCompressedPeerContents)
				},
				Compress: func(compressCtx context.Context, sourcePath string) (string, error) {
					return l.Fsstore.CompressOldFileAndRemoveSource(compressCtx, sourcePath, delayLog)
				},
			})
			err = withPreservedSource(err, result.PreservedSourcePath)
		}
		if err != nil && !errors.Is(err, appendlog.ErrCompressionSourceCleanup) {
			// ファイル圧縮失敗しても他には支障がないため、エラーログを出力して続行
			message := fmt.Sprintf(ErrorMsgFileCompress, oldFilePath)
			wrappedErr := l.wrapErrorByLogger(err, OperationCompress)
			entry := l.InternalErrorEntryWithContext(ctx, message, err)
			notify(func() {
				l.dispatchError(ctx, wrappedErr, entry)
			})
			return "", false
		}
		if errors.Is(err, appendlog.ErrCompressionSourceCleanup) {
			l.scheduleCompressionSourceCleanupRetry(oldFilePath, err)
			message := fmt.Sprintf(ErrorMsgCompressionSourceCleanup, oldFilePath)
			wrappedErr := l.wrapErrorByLogger(err, OperationSourceCleanup)
			entry := l.InternalErrorEntryWithContext(ctx, message, err)
			notify(func() {
				l.dispatchError(ctx, wrappedErr, entry)
			})
		}
		uploadPath = result.CompressedPath
	}

	return uploadPath, true
}

// PreservedSourceErrorは、完全でも内容の異なる圧縮済みfileと共存した元ログを、削除せず
// .preservedへ退避した失敗を表します。PreservedPathは退避先で、運用者が元ログの内容を確認できる
// 場所です。Errは内容の不一致と退避の結果を表し、errors.Isで原因を判別できます。
type PreservedSourceError struct {
	PreservedPath string
	Err           error
}

func (e *PreservedSourceError) Error() string { return e.Err.Error() }

func (e *PreservedSourceError) Unwrap() error { return e.Err }

// withPreservedSourceは、圧縮の検証で元ログを退避した場合に、その退避先をerrへ付けます。
func withPreservedSource(err error, preservedPath string) error {
	if err == nil || preservedPath == "" {
		return err
	}
	return &PreservedSourceError{PreservedPath: preservedPath, Err: err}
}

// verifyCompressedPeerContentsは、元ログと共存する既存の圧縮ファイルを、その拡張子のcodecで
// 検証します。組み込み形式では最後まで展開しながら元ログとの内容一致も確認し、完全でも
// 内容の異なる圧縮ファイルを、元ログを削除する根拠にしません。
// 拡張子が一致する現在の独自codecはFsstoreの設定から取得し、その完全性検証を優先します。
func (l *FileLogger) verifyCompressedPeerContents(sourcePath, compressedPath string) error {
	var custom pkgfsstore.CompressionCodec
	if provider, ok := l.Fsstore.Config.(appendlog.CompressionCodecProvider); ok {
		custom = provider.CompressionCodec()
	}
	return fsstore.VerifyExistingCompressedFileMatchesSource(sourcePath, compressedPath, custom)
}

// findExistingCompressedArchivePathは、元ログと同じディレクトリから、対応する圧縮ファイルを探します。
// 例えばsourcePathがapp_20261015_3.logなら、app_20261015_3.log.gzやapp_20261015_3.log.zstを探します。
// 取得済みのファイル一覧がない場合に使い、この関数内で一覧取得とファイル種別の検査を行います。
// 現在の圧縮設定と異なる形式も対象にし、通常ファイルと確認できた最初の一件のパスとtrueを返します。
// 見つからなければ空文字・false・nilを返します。圧縮内容の検証は後続処理で行います。
// 一覧取得・候補の情報取得に失敗した場合や、元ログまたは対応する圧縮パスがリンク・ディレクトリなどの
// 通常ファイル以外だった場合はエラーを返します。一覧取得後に消えたファイルは無視します。
//
// 呼び出し元はDirectoryGuardのlifecycle lockを保持するか、対象をDirectoryGuardで保護してから呼びます。
// ファイル一覧の取得・検査中は、保護情報を管理するDirectoryGuardのstatesMuを保持しないでください。
func (l *FileLogger) findExistingCompressedArchivePath(sourcePath string) (string, bool, error) {
	directory := filepath.Dir(sourcePath)
	entries, err := fsstore.ReadDirectoryEntries(directory)
	if err != nil {
		return "", false, fmt.Errorf("read compressed archive directory: %w", err)
	}
	paths := make([]string, 0, len(entries))
	baseExtension := filepath.Ext(sourcePath)
	sourceKey := pathidentity.ComparisonKey(sourcePath)
	for _, entry := range entries {
		path := filepath.Join(directory, entry.Name())
		if source, ok := fsstore.CompressionSourcePath(path, baseExtension); ok &&
			pathidentity.ComparisonKey(source) == sourceKey {
			paths = append(paths, path)
		}
	}
	// 元ログと対応する圧縮ファイルの名前だけを検査し、他世代や一時ファイルの情報は取得しません。
	sourceGroups := fsstore.GroupFilesByCompressionSource(fsstore.InspectFilePaths(paths), baseExtension)
	return sourceGroups.FindExistingCompressedFile(sourcePath)
}
