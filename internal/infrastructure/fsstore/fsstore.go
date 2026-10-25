package fsstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// ensureLogDir は、ログディレクトリがなければ作成する
func ensureLogDir(logDir string) error {
	if err := os.MkdirAll(logDir, 0750); err != nil {
		return fmt.Errorf("could not create log directory: %w", err)
	}

	return nil
}

// getLogDirectory は、ファイルストレージディレクトリのパス（ファイル名を含まない）を取得し、なければ作成する
func GetFileSystemStoreDirectoryPath(storeDir, target, specificDir string) (string, error) {

	// 設定からログディレクトリのパスを取得し、実行ディレクトリと結合
	logDirPath, err := pathidentity.ResolveDirectory(filepath.Join(storeDir, target, specificDir))
	if err != nil {
		return "", err
	}

	// ログディレクトリが存在しなければ作成
	if err := ensureLogDir(logDirPath); err != nil {
		return "", err // 追記する情報は特にない
	}

	return logDirPath, nil
}

// PathExistsは、path自体の存在をLstatで確認します。symlinkの参照先は辿らないため、
// 参照先が失われたsymlinkも存在すると判定します。不存在だけをfalse, nilとし、
// 権限不足やsymlinkの循環などの確認不能なerrorは呼び出し元へ返します。file種別の検証は行いません。
func PathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		// Windowsのos.Lstatは名前を解決できないpathも不存在として返すため、OSごとに確かめます。
		return confirmPathNotExist(path)
	}
	return false, err
}

// 指定されたファイルを削除
func DeleteFile(filePath string) error {
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete original file %v: %w", filePath, err)
	}
	return nil
}
