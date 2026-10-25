package appendlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// ErrCompressionSourcePathEmptyは、圧縮入口へ元fileのpathが渡されなかったことを表します。
// tmp fileの作成やsourceのretirementを始める前に返し、filesystemを変更しません。
var ErrCompressionSourcePathEmpty = errors.New("appendlog compression source path is empty")

func (fs *FileStore) GetCompressFormat() pkgfsstore.CompressFormat {
	if provider, ok := fs.Config.(CompressionCodecProvider); ok {
		if codec := provider.CompressionCodec(); codec != nil {
			return codec.Format()
		}
	}
	return fs.Config.Lifecycle().CompressFormat()
}

func (fs *FileStore) Compress(in *os.File, out *os.File) error {
	codec, err := fs.compressionCodec()
	if err != nil {
		return err
	}
	return codec.Compress(in, out)
}

// VerifyCompressedFileIntegrityは、このFileStoreが圧縮に使うのと同じcodecで
// 既存の圧縮済みfileを検証します。runnerが再起動後に圧縮の完了を
// 回復する際も、独自codecを組み込みcodecと同じ境界で扱えます。
func (fs *FileStore) VerifyCompressedFileIntegrity(path string) error {
	codec, err := fs.compressionCodec()
	if err != nil {
		return err
	}
	return fsstore.VerifyCompressedFileIntegrity(path, codec)
}

// compressionCodecForExtensionは、圧縮fileの保存先directoryの規則nameCaseで拡張子を比較し、
// 検証に使うcodecを選びます。選択規則はfsstore.ResolveExistingCompressionCodecと共有し、
// file pathやobject keyの表記は変えず、この選択に使うextensionだけを揃えます。
func compressionCodecForExtension(
	extension string,
	custom pkgfsstore.CompressionCodec,
	nameCase pathidentity.NameCase,
) (pkgfsstore.CompressionCodec, bool, error) {
	return fsstore.ResolveCompressionCodecForExtension(extension, custom, nameCase)
}

func (fs *FileStore) compressionCodec() (pkgfsstore.CompressionCodec, error) {
	var custom pkgfsstore.CompressionCodec
	if provider, ok := fs.Config.(CompressionCodecProvider); ok {
		custom = provider.CompressionCodec()
	}
	return fsstore.ResolveCompressionCodec(fs.Config.Lifecycle().CompressFormat(), custom)
}

// CompressOldFileAndRemoveSourceは古いappendlog fileを圧縮し、圧縮済みfileの公開後に
// sourceを専用pending-delete pathへ移してから削除するproduction用の公開境界です。
// 組み込みcodecでは公開前にtmp fileを展開して元fileと内容が一致することを確認し、
// 一致しなければ公開もsourceの退避もしません。
// inline lifecycleとrunner lifecycleは、同じFileStore設定と圧縮処理を使用します。
// source削除にはos.Removeを固定し、呼び出し側へ削除処理の差し替えを公開しません。
//
// compressedPathはfinal pathへのrenameが成功した場合だけ返し、その後のエラー時も保持します。
// archive/uploadへ進めるのは、errがnilかErrCompressionSourceCleanupに一致する場合だけです。
// 後者はsourceの退避・同期まで完了し、削除待ちsourceの後片付けだけが失敗した状態です。
//
// 前提:
//   - 呼び出し元がDirectoryGuard.RunWithLifecycleLockで対象directoryのlifecycle排他を取得する
//   - final公開後でもsource retirement前のerrorは返り得る。callerは再走査時にvalidな
//     compressed peerを確認した場合、再圧縮せずRemoveSourceAfterCompressionを再試行する
//   - compressedPathが既に存在する状態は通常想定せず、caller側で重複を判定する
//   - 別プロセスとの競合は考慮しない
func (fs *FileStore) CompressOldFileAndRemoveSource(
	ctx context.Context,
	filePath string,
	latencyHook LatencyHook,
) (compressedPath string, err error) {
	return fs.compressOldFile(
		ctx,
		filePath,
		latencyHook,
		os.Remove,
		(*os.File).Sync,
		fsstore.SyncDirectory,
	)
}

// verifyCompressedOutputMatchesSourceは、圧縮に使うのと同じcodecでpathを最後まで展開します。
// 組み込みcodecでは展開した内容がsourcePathと一致することも同じ走査で確認し、
// 元ログを表さない圧縮結果を公開しません。独自codecは完全性だけを検証します。
func (fs *FileStore) verifyCompressedOutputMatchesSource(path, sourcePath string) error {
	codec, err := fs.compressionCodec()
	if err != nil {
		return err
	}
	return fsstore.VerifyCompressedFileMatchesSource(path, codec, sourcePath)
}

// compressOldFileはcompressed file公開前の圧縮失敗、source retirement前の失敗、
// retirement後のpending source削除失敗を分離する実装本体です。末尾の3関数は
// productionでは固定し、testだけが各filesystem操作の失敗を決定的に再現するために
// 差し替えます。
func (fs *FileStore) compressOldFile(
	ctx context.Context,
	filePath string,
	latencyHook LatencyHook,
	removePendingSource func(string) error,
	syncTemporaryFile func(*os.File) error,
	syncDirectory syncCompressionDirectoryFunc,
) (string, error) {
	// 元pathは圧縮入口で検証し、後段のsource retirementでは再検証しません。
	if filePath == "" {
		return "", ErrCompressionSourcePathEmpty
	}
	if syncTemporaryFile == nil {
		return "", errors.New("compressed temporary file sync function is nil")
	}
	if syncDirectory == nil {
		return "", errors.New("compression parent directory sync function is nil")
	}

	format := fs.GetCompressFormat()
	compressedPath := fsstore.GenerateCompressedPath(filePath, format)
	// 圧縮先は生成直後に検証し、tmp作成やsource retirementへ空pathを渡しません。
	if compressedPath == "" {
		return "", &fsstore.CompressionFormatExtensionError{Format: format}
	}

	// 圧縮途中のfileをfinal pathへ公開しないよう、同じdirectoryの一時fileへ書き込む。
	tmpPath := compressedPath + ".tmp"
	_ = os.Remove(tmpPath)

	// 圧縮元の存在と通常ファイルであることは、開く際に共通処理で確認します。
	inFile, outFile, err := fsstore.OpenFilesForCompression(filePath, tmpPath)
	if err != nil {
		return "", fmt.Errorf("failed to open files for compression: %w", err)
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

	if err := measureLatency(ctx, "compressing", filePath, func() error {
		return fs.Compress(inFile, outFile)
	}, latencyHook); err != nil {
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

	// codecが正常終了しても、壊れた出力や元ログと異なる内容を生成している可能性は残ります。
	// sourceを削除するcommitへ進む前に、同じcodecでtmp fileを最後まで読み取れることを
	// 確認します。組み込みcodecでは、展開した内容が元ログと一致することも確認します。
	if _, err := InspectCompressedFile(filePath, tmpPath); err != nil {
		return "", fmt.Errorf("failed to inspect temp compressed file: %w", err)
	}
	if err := fs.verifyCompressedOutputMatchesSource(tmpPath, filePath); err != nil {
		return "", fmt.Errorf("failed to verify temp compressed file: %w", err)
	}

	// 検証に成功した場合だけtmpからfinal pathへ切り替える。
	if err := os.Rename(tmpPath, compressedPath); err != nil {
		return "", fmt.Errorf("failed to rename temp compressed file: %w", err)
	}

	cleanupTmp = false
	// file本体のSyncだけではrenameされたdirectory entryの永続化を保証できません。
	// final公開をdirectoryへ同期してからcanonical sourceのretirementへ進みます。
	if err := syncDirectory(filepath.Dir(compressedPath)); err != nil {
		return compressedPath, fmt.Errorf(
			"failed to sync directory after publishing compressed file %q: %w",
			compressedPath,
			err,
		)
	}

	// compressed file公開後、canonical sourceを専用pending-delete名へ移します。rename後の
	// directory同期まで成功したsourceは、compressed fileが後から失われても再圧縮対象へ
	// 戻しません。
	retirementErr := removeSourceAfterCompression(
		filePath,
		compressedPath,
		os.Rename,
		removePendingSource,
		syncDirectory,
	)
	if retirementErr != nil {
		return compressedPath, retirementErr
	}

	return compressedPath, nil
}
