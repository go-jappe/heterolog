//go:build !windows

package testutil

import (
	"errors"
	"os"
)

// IsClosedFileErrorは、errが閉じた*os.Fileへの操作の失敗かを返します。
// Windows以外では閉じたfileへの操作がos.ErrClosedを返します。
func IsClosedFileError(err error) bool {
	return errors.Is(err, os.ErrClosed)
}
