package appendlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// ErrCompressedPeerNotRegularは圧縮先と同名のpathが通常file以外で占有されていることを表します。
var ErrCompressedPeerNotRegular = errors.New("appendlog compressed peer is not a regular file")

// InspectCompressedFileは、元ログに対応する圧縮先をLstatで調べます。
// 不存在、または元ログと同じパスならnilを返し、リンク・ディレクトリなどは拒否します。
// 非空の元ログに対する0 byteの圧縮先は、元ログを表現できないため破損として拒否します。
// 元ログが空、または退避済みで存在しない場合は、空の圧縮先もcodec検証へ渡せます。
// 戻り値は存在する通常ファイルの情報です。圧縮内容の完全性はここでは確認しません。
// compressedPathには呼び出し元で解決済みの空でないパスを渡してください。
func InspectCompressedFile(sourcePath, compressedPath string) (os.FileInfo, error) {
	if pathidentity.ComparisonKey(compressedPath) == pathidentity.ComparisonKey(sourcePath) {
		return nil, nil
	}
	info, err := fsstore.InspectRegularFile(compressedPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if errors.Is(err, fsstore.ErrNotRegularFile) {
		return nil, fmt.Errorf(
			"%w: source=%s: %w",
			ErrCompressedPeerNotRegular,
			sourcePath,
			err,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("inspect compressed peer %s for source %s: %w", compressedPath, sourcePath, err)
	}
	if info.Size() == 0 {
		sourceInfo, sourceErr := fsstore.InspectRegularFile(sourcePath)
		if sourceErr != nil && !errors.Is(sourceErr, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect source %s for empty compressed peer %s: %w", sourcePath, compressedPath, sourceErr)
		}
		if sourceErr == nil && sourceInfo.Size() > 0 {
			return nil, fmt.Errorf("%w: empty compressed peer %s cannot represent nonempty source %s", pkgfsstore.ErrCorruptCompressedFile, compressedPath, sourcePath)
		}
	}
	return info, nil
}

// RemoveInvalidCompressedFileは、内容検証に失敗した圧縮ファイルを削除します。
// 元ログ自身の削除は拒否し、すでに削除されていれば成功とします。
// 呼び出し元は通常ファイルの内容破損を確認し、元ログを次回の再圧縮用に保持します。
// compressedPathには空でないパスを渡してください。親ディレクトリの同期は行いません。
func RemoveInvalidCompressedFile(sourcePath, compressedPath string) error {
	if pathidentity.ComparisonKey(compressedPath) == pathidentity.ComparisonKey(sourcePath) {
		return fmt.Errorf("compressed peer path is the same as source: %s", sourcePath)
	}
	if err := os.Remove(compressedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf(
			"remove invalid compressed peer %s for source %s: %w",
			compressedPath,
			sourcePath,
			err,
		)
	}
	return nil
}

// VerifyExistingCompressedFileIntegrityは、既存圧縮ファイルの拡張子に対応するcodecで検証します。
// 組み込みcodecでは最後まで展開し、途中切れやチェックサム不一致などの破損が
// 検出されないことを確認します。展開した内容は保存しません。
// 拡張子が一致する現在の独自codecがあれば、その検証処理を優先します。
// 設定変更前の組み込み形式も対象ですが、対応するcodecがない形式はエラーです。
// 拡張子の大小文字は、pathの保存先directoryの規則（pathidentity.DirectoryNameCase）で比較します。
// 元ログとの内容一致や、fsyncによる永続化を確認する処理ではありません。
func VerifyExistingCompressedFileIntegrity(path string, custom pkgfsstore.CompressionCodec) error {
	return verifyExistingCompressedFileIntegrity(path, custom, pathidentity.DirectoryNameCase(filepath.Dir(path)))
}

// verifyExistingCompressedFileIntegrityは、判定済みの規則nameCaseで拡張子を比較し、
// VerifyExistingCompressedFileIntegrityと同じ検証を行います。
func verifyExistingCompressedFileIntegrity(
	path string,
	custom pkgfsstore.CompressionCodec,
	nameCase pathidentity.NameCase,
) error {
	codec, supported, err := compressionCodecForExtension(filepath.Ext(path), custom, nameCase)
	if err != nil {
		return err
	}
	if !supported {
		return fmt.Errorf("%w: cannot verify existing compressed file %q", fsstore.ErrUnsupportedCompressionFormat, path)
	}
	return fsstore.VerifyCompressedFileIntegrity(path, codec)
}

// VerifyExistingCompressedFileIntegrityは、設定変更前の圧縮結果もその形式で検証します。
func (fs *FileStore) VerifyExistingCompressedFileIntegrity(path string) error {
	var custom pkgfsstore.CompressionCodec
	if provider, ok := fs.Config.(CompressionCodecProvider); ok {
		custom = provider.CompressionCodec()
	}
	return VerifyExistingCompressedFileIntegrity(path, custom)
}
