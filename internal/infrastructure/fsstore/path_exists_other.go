//go:build !windows

package fsstore

// confirmPathNotExistは、Lstatが不存在と返したpathの判定を確定します。
// Windows以外のLstatは、symlinkの循環などの名前を解決できないpathを不存在と区別して返します。
func confirmPathNotExist(string) (bool, error) {
	return false, nil
}
