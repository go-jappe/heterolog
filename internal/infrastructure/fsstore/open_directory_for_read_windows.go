//go:build windows

package fsstore

import (
	"os"

	"golang.org/x/sys/windows"
)

// OpenDirectoryForReadはReadDir(n)による分割読込用にdirectoryを開きます。
// 戻り値のfileは呼出側でCloseする必要があります。
// Windowsでは、同じdirectoryの権限設定用handleが開いていても一覧を取得できる
// 共有modeを使用します。FILE_SHARE_DELETEは他handleのDELETE accessと共存する
// 指定であり、このreaderに削除権限を付与したりACLを緩めたりするものではありません。
// directory以外はERROR_DIRECTORYで拒否します。FILE_FLAG_BACKUP_SEMANTICSは通常fileも
// 開けるうえ、そのhandleのReadDirが返すENOTDIRはWindowsではERROR_PATH_NOT_FOUNDと
// 同じ値です。呼出側が通常fileをos.ErrNotExist（未作成のdirectory）と誤認しないようにします。
func OpenDirectoryForRead(path string) (*os.File, error) {
	name, err := privateWindowsPath(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "open", Path: path, Err: windows.ERROR_DIRECTORY}
	}
	return os.NewFile(uintptr(handle), path), nil
}
