package failurearchive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// ErrCompressionSourcePathEmptyは、圧縮入口へ元fileのpathが渡されなかったことを表します。
var ErrCompressionSourcePathEmpty = errors.New("source file path is empty")

// ファイル圧縮を実行するか判定
func (fs *FileStore) ShouldCompressFile(activeFilePath, oldFilePath string) bool {
	// 圧縮対象である条件を明確に書く
	isNotEmpty := oldFilePath != ""
	isDifferentFile := oldFilePath != activeFilePath
	isCompressEnabled := fs.Config.EnableFileCompress()

	return isNotEmpty && isDifferentFile && isCompressEnabled
}

// CompressOldFileは古いfailure archiveを圧縮し、公開後にsourceを削除します。
// 呼び出し元は対象directoryのDirectoryGuard.RunWithLifecycleLock内で実行します。
func (fs *FileStore) CompressOldFile(
	ctx context.Context,
	filePath string,
) error {
	_, err := fs.CompressOldFileWithResult(ctx, filePath)
	return err
}

// CompressOldFileWithResultは圧縮途中のfileをtmp pathへ作成し、同じcodecで完全性を
// 検証できた場合だけfinal pathへ公開します。組み込みcodecでは、展開した内容が元fileと
// 一致することも確認します。公開後はcanonical sourceを専用の
// delete-pending pathへ退避してから削除します。
//
// compressedPathはfinal pathへのrenameが成功した場合だけ返し、その後のエラー時も保持します。
// uploadへ進めるのは、errがnilかappendlog.ErrCompressionSourceCleanupに一致する場合だけです。
// 後者はsourceの退避・同期まで完了し、削除待ちsourceの後片付けだけが失敗した状態です。
func (fs *FileStore) CompressOldFileWithResult(
	ctx context.Context,
	filePath string,
) (compressedPath string, err error) {
	return fs.compressOldFileWithResultAndDurability(
		ctx,
		filePath,
		(*os.File).Sync,
		fsstore.SyncDirectory,
	)
}

// compressOldFileWithResultAndDurabilityはproductionと同じ圧縮commitを、明示された同期
// 処理で実行します。同期failure後のfilesystem状態を決定的に検証するtest seamです。
func (fs *FileStore) compressOldFileWithResultAndDurability(
	ctx context.Context,
	filePath string,
	syncTemporaryFile func(*os.File) error,
	syncDirectory func(string) error,
) (string, error) {
	if filePath == "" {
		return "", ErrCompressionSourcePathEmpty
	}
	if syncTemporaryFile == nil {
		return "", errors.New("compressed temporary file sync function is nil")
	}
	if syncDirectory == nil {
		return "", errors.New("compression parent directory sync function is nil")
	}
	// 生成先を確定し、有効な拡張子がなければfileを変更せず拒否する。
	format := fs.GetCompressFormat()
	compressedPath := fsstore.GenerateCompressedPath(filePath, format)
	if compressedPath == "" {
		return "", &fsstore.CompressionFormatExtensionError{Format: format}
	}
	codec, err := fs.compressionCodec()
	if err != nil {
		return "", err
	}

	// 同じディレクトリに tmp を作る
	tmpPath := compressedPath + ".tmp"
	// 念のため古い残骸があれば消す
	_ = os.Remove(tmpPath)

	// 圧縮元の存在と通常ファイルであることは、開く際に共通処理で確認します。
	inFile, outFile, err := fsstore.OpenFilesForCompression(filePath, tmpPath)
	if err != nil {
		return "", fmt.Errorf("failed to open file for compression: %w", err)
	}
	cleanupTmp := true
	defer func() {
		if inFile != nil {
			_ = inFile.Close()
		}
		if outFile != nil {
			_ = outFile.Close()
		}
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := codec.Compress(inFile, outFile); err != nil {
		return "", err
	}
	// codecが書き終えたtmp本体を、final pathの公開より先に永続化します。
	if err := syncTemporaryFile(outFile); err != nil {
		return "", fmt.Errorf("%w: failed to sync temp compressed file: %w", pkgfsstore.ErrDurabilityUnconfirmed, err)
	}
	// Sync済みのoutFileを閉じ、以降の検証で読み直せる状態にします。
	if err := outFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp compressed file: %w", err)
	}
	outFile = nil
	if err := inFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close source file: %w", err)
	}
	inFile = nil

	// codecがnilを返しても壊れた出力や元fileと異なる内容を生成する実装はあり得ます。
	// sourceを完了済みと扱う前にtmp fileを最後までdecodeし、組み込みcodecでは展開した
	// 内容が元fileと一致することも確認して、誤ったskip証明を作らないようにします。
	if err := fsstore.VerifyCompressedFileMatchesSource(tmpPath, codec, filePath); err != nil {
		return "", fmt.Errorf("failed to verify temp compressed file: %w", err)
	}

	// 成功時だけ tmp -> 本番名へ切り替え
	if err := os.Rename(tmpPath, compressedPath); err != nil {
		return "", fmt.Errorf("failed to rename temp compressed file: %w", err)
	}

	cleanupTmp = false
	// final fileの公開をdirectoryへ同期してから、canonical sourceのretirementへ進みます。
	if err := syncDirectory(filepath.Dir(compressedPath)); err != nil {
		return compressedPath, fmt.Errorf(
			"failed to sync directory after publishing compressed file %q: %w",
			compressedPath,
			err,
		)
	}

	// canonical sourceの退避をdirectoryへ同期できた時点を「一度正常に圧縮済み」の
	// 永続的な境界にします。同期後の削除または削除後同期だけが失敗した場合は、圧縮成果物を
	// 後続uploadへ渡せます。
	if err := appendlog.RemoveSourceAfterCompression(filePath, compressedPath); err != nil {
		return compressedPath, err
	}

	return compressedPath, nil
}

// VerifyCompressedFileIntegrityはFileStoreが圧縮に使用するcodecで既存fileを最後まで
// decodeできることを確認します。再起動後のraw recoveryで、存在するだけの壊れたpeerを
// 圧縮完了済みと誤認しないために使用します。
func (fs *FileStore) VerifyCompressedFileIntegrity(path string) error {
	codec, err := fs.compressionCodec()
	if err != nil {
		return err
	}
	return fsstore.VerifyCompressedFileIntegrity(path, codec)
}

// VerifyExistingCompressedFileIntegrityは、設定変更前の圧縮結果もその形式で検証します。
// 現在利用できない独自形式は、検証を省略せずエラーにします。
func (fs *FileStore) VerifyExistingCompressedFileIntegrity(path string) error {
	var custom pkgfsstore.CompressionCodec
	if provider, ok := fs.Config.(CompressionCodecProvider); ok {
		custom = provider.CompressionCodec()
	}
	return appendlog.VerifyExistingCompressedFileIntegrity(path, custom)
}

func (fs *FileStore) Compress(in *os.File, out *os.File) error {
	codec, err := fs.compressionCodec()
	if err != nil {
		return err
	}
	return codec.Compress(in, out)
}

func (fs *FileStore) GetCompressFormat() pkgfsstore.CompressFormat {
	if provider, ok := fs.Config.(CompressionCodecProvider); ok {
		if codec := provider.CompressionCodec(); codec != nil {
			return codec.Format()
		}
	}
	return fs.Config.CompressFormat()
}

func (fs *FileStore) compressionCodec() (pkgfsstore.CompressionCodec, error) {
	var custom pkgfsstore.CompressionCodec
	if provider, ok := fs.Config.(CompressionCodecProvider); ok {
		custom = provider.CompressionCodec()
	}
	return fsstore.ResolveCompressionCodec(fs.Config.CompressFormat(), custom)
}
