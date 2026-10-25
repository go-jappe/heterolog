package fsstore

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// confirmPathNotExistは、Lstatが不存在と返したpathを、reparse pointを辿らないopenで確かめます。
// os.LstatはGetFileAttributesExのerrorを返し、GetFileAttributesExは親directoryのsymlinkの循環など
// 名前を解決できないpathもERROR_FILE_NOT_FOUNDとして返します。同じpathのCreateFileは
// ERROR_CANT_RESOLVE_FILENAMEを返すため、不存在のerrorだけを不存在とし、それ以外は返します。
func confirmPathNotExist(path string) (bool, error) {
	name, err := privateWindowsPath(path)
	if err != nil {
		return false, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}
	handle, err := windows.CreateFile(name, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, &os.PathError{Op: "CreateFile", Path: path, Err: err}
	}
	// Lstatの後に作成されたpathは、確認した時点で存在します。
	if err := windows.CloseHandle(handle); err != nil {
		return false, &os.PathError{Op: "CloseHandle", Path: path, Err: err}
	}
	return true, nil
}
