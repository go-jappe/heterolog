package pathidentity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrManagedDirectoryNotChildは、管理用ディレクトリの相対パスが保存先の配下を指さないことを表します。
// 空・"."・絶対パス・保存先の外へ出るパスで返し、errors.Isで判別できます。
var ErrManagedDirectoryNotChild = errors.New("managed directory must be a child path")

// ManagedDirectoryObstructionErrorは、ディレクトリになるべき管理パスが
// 通常ファイルやリンク等で塞がれていることを表します。権限・I/Oエラーとは区別します。
// Infoはリンク先をたどらず取得した情報です。隔離する場合は、呼び出し側で
// 親ディレクトリと現在のエントリを再確認してください。この型自体は変更を行いません。
type ManagedDirectoryObstructionError struct {
	Path string
	Info os.FileInfo
}

func (e *ManagedDirectoryObstructionError) Error() string {
	return fmt.Sprintf("managed directory %q is not a directory or is a link", e.Path)
}

// ResolveAndValidateManagedDirectoryは、利用者が指定する保存先と、その配下のアプリ管理用
// ディレクトリを分けて解決します。baseDirectoryのリンクは実体へ解決しますが、
// relativeDirectoryの各階層はリンクを拒否します。Windowsのjunctionも拒否します。
// relativeDirectoryはbaseDirectory内に収まる、管理用ディレクトリの相対パスです。
// 未作成の部分はそのまま返し、ディレクトリの作成・権限変更は行いません。
// 初期化時や管理用ディレクトリを用意する際に使用し、稼働中の外部変更は監視しません。
func ResolveAndValidateManagedDirectory(baseDirectory, relativeDirectory string) (string, error) {
	if !filepath.IsLocal(relativeDirectory) || filepath.Clean(relativeDirectory) == "." {
		return "", fmt.Errorf("%w: %q", ErrManagedDirectoryNotChild, relativeDirectory)
	}
	base, err := ResolveDirectory(baseDirectory)
	if err != nil {
		return "", err
	}
	relative := filepath.Clean(relativeDirectory)
	resolved := filepath.Join(base, relative)
	current := base
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			// この階層が未作成なら、その下も未作成です。作成は呼び出し元に委ねます。
			return resolved, nil
		}
		if err != nil {
			return "", fmt.Errorf("inspect managed directory %q: %w", current, err)
		}
		if !info.IsDir() || isDirectoryLink(info) {
			return "", &ManagedDirectoryObstructionError{Path: current, Info: info}
		}
	}
	return resolved, nil
}
