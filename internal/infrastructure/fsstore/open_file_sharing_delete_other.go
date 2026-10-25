//go:build !windows

package fsstore

import "os"

// openFileSharingDeleteは、Windows以外ではos.OpenFileと同じです。
// 開いているfileのrename・削除はOSが妨げません。
func openFileSharingDelete(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}
