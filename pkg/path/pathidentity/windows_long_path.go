package pathidentity

import "strings"

// AddWindowsLongPathPrefixは、長いWindows絶対パスへWin32の拡張prefixを付けます。
// 呼び出し元は先に絶対化とパスの整理を済ませてください。元の大小文字は変更しません。
// 既存処理の248バイト境界を使い、それより短いパスと既存のdevice・拡張パス表記はそのまま返します。
// UNCには専用のprefixを付けます。存在確認やリンク解決、UTF-16への変換は行いません。
func AddWindowsLongPathPrefix(path string) string {
	if len(path) < 248 || strings.HasPrefix(path, `\\?\`) ||
		strings.HasPrefix(path, `\\.\`) || strings.HasPrefix(path, `\??\`) {
		return path
	}
	if strings.HasPrefix(path, `\\`) {
		return `\\?\UNC\` + path[2:]
	}
	return `\\?\` + path
}
