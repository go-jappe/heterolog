package path

import (
	"path/filepath"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// BuildApplicationDataDirectoryPath returns the path made by appending the
// application directory name under the base path. If a level of path is
// already named appName, it returns path, cleaned by filepath.Clean, instead
// of appending appName twice. The name at each level is matched by the
// filesystem rule of its parent directory (pathidentity.DirectoryNameCase),
// and in a case-insensitive directory a name that differs only in case is also
// treated as the same directory. Apart from filepath.Clean, the returned path
// keeps its original spelling. It neither creates directories nor checks that
// they exist.
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
