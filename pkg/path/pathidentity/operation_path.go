package pathidentity

import "path/filepath"

// AbsoluteOrCleanPathは、元の大小文字を保ったままパスを整理して絶対化します。
// 空のパスは空のまま返します。絶対化に失敗した場合は、整理した元のパスを返します。
// 対象ファイルの存在確認やリンク解決、比較用の小文字化は行いません。
func AbsoluteOrCleanPath(path string) string {
	if path == "" {
		return ""
	}
	cleaned := filepath.Clean(path)
	if absolute, err := filepath.Abs(cleaned); err == nil {
		return filepath.Clean(absolute)
	}
	return cleaned
}
