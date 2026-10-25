package testutil

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// IsClosedFileErrorは、errが閉じた*os.Fileへの操作の失敗かを返します。
// Windowsの(*os.File).Statは閉じたhandleを検査せずGetFileTypeへ渡すため、
// os.ErrClosedではなくERROR_INVALID_HANDLEを返します。どちらも同じ原因として扱います。
func IsClosedFileError(err error) bool {
	return errors.Is(err, os.ErrClosed) || errors.Is(err, windows.ERROR_INVALID_HANDLE)
}
