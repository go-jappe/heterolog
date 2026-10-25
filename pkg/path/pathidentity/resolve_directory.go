package pathidentity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrEmptyStorageDirectoryは、保存先のpathが空文字または空白だけであることを表します。
// ResolveDirectoryとResolveAndValidateManagedDirectoryが返し、errors.Isで判別できます。
var ErrEmptyStorageDirectory = errors.New("storage directory path must not be empty")

// ResolveDirectoryは、保存先の初期化時にリンクを解決し、実体の絶対パスを返します。
// 未作成の部分は、存在する親を解決してから結合します。ディレクトリは作成しません。
// リンク切れ・循環・権限不足・ディレクトリ以外のパスはエラーとし、未解決のパスへ戻しません。
// 呼び出し側は結果を保持して保存先と比較キーの両方に使い、通常処理では再解決しません。
// 初期化と並行する外部からのパス置換や、稼働中のリンク変更への追従は対象外です。
func ResolveDirectory(directory string) (string, error) {
	if strings.TrimSpace(directory) == "" {
		return "", ErrEmptyStorageDirectory
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("resolve storage directory %q: %w", directory, err)
	}
	var missing []string
	for current := absolute; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err == nil {
			resolved, err := resolveExistingDirectory(current, info)
			if err != nil {
				return "", err
			}
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) || filepath.Dir(current) == current {
			return "", fmt.Errorf("inspect storage directory %q: %w", current, err)
		}
		missing = append(missing, filepath.Base(current))
	}
}
