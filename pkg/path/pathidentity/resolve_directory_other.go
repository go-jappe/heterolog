//go:build !windows

package pathidentity

import (
	"fmt"
	"os"
	"path/filepath"
)

func isDirectoryLink(info os.FileInfo) bool {
	return info.Mode()&os.ModeSymlink != 0
}

func resolveExistingDirectory(directory string, info os.FileInfo) (string, error) {
	if !info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		return "", fmt.Errorf("storage directory path %q is not a directory", directory)
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return "", fmt.Errorf("resolve storage directory link %q: %w", directory, err)
	}
	info, err = os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect resolved storage directory %q: %w", resolved, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("resolved storage directory path %q is not a directory", resolved)
	}
	return resolved, nil
}
