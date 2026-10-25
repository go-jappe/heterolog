package fsstore

import (
	"os"

	"golang.org/x/sys/windows"
)

// openFileSharingDeleteは、既存fileをos.OpenFileと同じ読み書きの共有に加えて
// FILE_SHARE_DELETEも共有して開きます。MoveFileExなどでrename中のhandleはDELETE accessを
// 持つため、os.OpenFileの共有modeでは、その間のopenがERROR_SHARING_VIOLATIONで失敗します。
// flagはos.O_RDONLYまたはos.O_RDWRだけを受け付け、作成・切り詰めは行いません。
func openFileSharingDelete(path string, flag int, _ os.FileMode) (*os.File, error) {
	var access uint32
	switch flag {
	case os.O_RDONLY:
		access = windows.GENERIC_READ
	case os.O_RDWR:
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
	default:
		return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrInvalid}
	}
	name, err := privateWindowsPath(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	handle, err := windows.CreateFile(name, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(handle), path), nil
}
