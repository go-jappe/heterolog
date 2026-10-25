//go:build darwin

package fsstore

import (
	"io/fs"

	"golang.org/x/sys/unix"
)

func renameFileNoReplace(sourcePath, destinationPath string) error {
	err := unix.RenamexNp(sourcePath, destinationPath, unix.RENAME_EXCL)
	switch err {
	case unix.EEXIST, unix.ENOTEMPTY, unix.EISDIR:
		return fs.ErrExist
	default:
		return err
	}
}
