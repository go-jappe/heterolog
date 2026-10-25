//go:build !linux && !darwin && !windows

package fsstore

import "errors"

func renameFileNoReplace(sourcePath, destinationPath string) error {
	return errors.ErrUnsupported
}
