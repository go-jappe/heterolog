package pathidentity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrEmptyStorageDirectory reports that the path of a storage location is an
// empty string or only white space. ResolveDirectory and
// ResolveAndValidateManagedDirectory return it, and errors.Is can identify it.
var ErrEmptyStorageDirectory = errors.New("storage directory path must not be empty")

// ResolveDirectory resolves links when a storage location is initialized and
// returns the absolute path of the real directory. Parts that do not exist yet
// are joined after the existing parent is resolved. It does not create
// directories. A broken link, a loop, insufficient permissions, or a path that
// is not a directory is an error, and it never falls back to the unresolved
// path. The caller keeps the result and uses it both as the storage location
// and for comparison keys, and does not resolve it again during normal
// processing. Following external path replacements made concurrently with
// initialization, or link changes made while running, is out of scope.
func ResolveDirectory(directory string) (string, error) {
	if strings.TrimSpace(directory) == "" {
		return "", ErrEmptyStorageDirectory
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("resolve storage directory %q: %w", directory, err)
	}
	var missing []string
	for current := absolute; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err == nil {
			resolved, err := resolveExistingDirectory(current, info)
			if err != nil {
				return "", err
			}
			for index := len(missing) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, missing[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) || filepath.Dir(current) == current {
			return "", fmt.Errorf("inspect storage directory %q: %w", current, err)
		}
		missing = append(missing, filepath.Base(current))
	}
}
