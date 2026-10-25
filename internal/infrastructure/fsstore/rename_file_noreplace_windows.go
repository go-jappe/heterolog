//go:build windows

package fsstore

import (
	"io/fs"
	"os"

	"golang.org/x/sys/windows"
)

func renameFileNoReplace(sourcePath, destinationPath string) error {
	source, err := privateWindowsPath(sourcePath)
	if err != nil {
		return err
	}
	destination, err := privateWindowsPath(destinationPath)
	if err != nil {
		return err
	}
	// flagsを0にして、既存fileの置換と異なるvolumeへのcopy fallbackを両方禁止します。
	err = windows.MoveFileEx(source, destination, 0)
	switch err {
	case windows.ERROR_ALREADY_EXISTS, windows.ERROR_FILE_EXISTS:
		return fs.ErrExist
	case windows.ERROR_ACCESS_DENIED:
		// 移動先がdirectoryの場合、Windowsはaccess deniedを返すことがあります。
		// native移動が失敗した後だけ調べ、directoryやsymlinkとの衝突をErrExistへ揃えます。
		if info, statErr := os.Lstat(destinationPath); statErr == nil &&
			(info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return fs.ErrExist
		}
	}
	return err
}
