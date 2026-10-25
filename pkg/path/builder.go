package path

import (
	"path/filepath"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// BuildApplicationDataDirectoryPath は、基準パス配下にアプリ用ディレクトリ名を付与したパスを返します。
// すでに path のいずれかの階層に appName が含まれている場合は、二重に付与せず path をそのまま返します。
// 各階層の名前は、その親ディレクトリのfilesystemの規則（pathidentity.DirectoryNameCase）で照合し、
// 大小文字を区別しないディレクトリでは大小文字だけが異なる名前も同じディレクトリとして扱います。
// 返却pathの元表記は変更しません。ディレクトリの作成や存在確認は行いません。
func BuildApplicationDataDirectoryPath(path, appName string) string {
	return buildApplicationDataDirectoryPath(path, appName, pathidentity.DirectoryNameCase)
}

// buildApplicationDataDirectoryPathは、親ディレクトリごとの大小文字の規則directoryNameCaseを指定して
// BuildApplicationDataDirectoryPathと同じ判定を行います。試験では規則を差し替えます。
func buildApplicationDataDirectoryPath(
	path string,
	appName string,
	directoryNameCase func(directory string) pathidentity.NameCase,
) string {
	// 比較前にパス表現を正規化します。
	cleanedPath := filepath.Clean(path)
	cleanedAppName := filepath.Clean(appName)

	// path の各階層を末尾から確認し、すでに appName が含まれていればそのまま返します。
	for current := cleanedPath; ; {
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		nameCase := directoryNameCase(parent)
		if nameCase.Key(filepath.Base(current)) == nameCase.Key(cleanedAppName) {
			return cleanedPath
		}
		current = parent
	}

	// appName が含まれていない場合は、path 配下に appName を追加します。
	return filepath.Join(cleanedPath, cleanedAppName)
}
