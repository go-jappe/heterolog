//go:build linux

package fsstore

import (
	"io/fs"

	"golang.org/x/sys/unix"
)

func renameFileNoReplace(sourcePath, destinationPath string) error {
	err := unix.Renameat2(unix.AT_FDCWD, sourcePath, unix.AT_FDCWD, destinationPath, unix.RENAME_NOREPLACE)
	switch err {
	case unix.EEXIST, unix.ENOTEMPTY, unix.EISDIR:
		return fs.ErrExist
	default:
		return err
	}
}
