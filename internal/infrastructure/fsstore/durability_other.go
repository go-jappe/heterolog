//go:build !windows

package fsstore

import "os"

func openDirectoryForSync(directoryPath string) (*os.File, error) {
	return os.Open(directoryPath)
}
