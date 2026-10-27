package pathidentity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrManagedDirectoryNotChild reports that the relative path of a managed
// directory does not point inside the storage location. It is returned for an
// empty path, ".", an absolute path, and a path that leaves the storage
// location, and errors.Is can identify it.
var ErrManagedDirectoryNotChild = errors.New("managed directory must be a child path")

// ManagedDirectoryObstructionError reports that a managed path that should be a
// directory is blocked by a regular file, a link or something similar. It is
// kept distinct from permission and I/O errors. Info is the information
// obtained without following links. To quarantine the entry, the caller must
// check the parent directory and the current entry again. This type itself
// makes no changes.
type ManagedDirectoryObstructionError struct {
	Path string
	Info os.FileInfo
}

func (e *ManagedDirectoryObstructionError) Error() string {
	return fmt.Sprintf("managed directory %q is not a directory or is a link", e.Path)
}

// ResolveAndValidateManagedDirectory resolves, separately, the storage location
// that the user specifies and the application's managed directory under it.
// Links in baseDirectory are resolved to their targets, but a link at any level
// of relativeDirectory is rejected. Windows junctions are rejected too.
// relativeDirectory is the relative path of the managed directory, contained
// in baseDirectory. Parts that do not exist yet are returned as they are, and
// it neither creates directories nor changes permissions. Use it at
// initialization or when preparing a managed directory; it does not watch for
// external changes while running.
func ResolveAndValidateManagedDirectory(baseDirectory, relativeDirectory string) (string, error) {
	if !filepath.IsLocal(relativeDirectory) || filepath.Clean(relativeDirectory) == "." {
		return "", fmt.Errorf("%w: %q", ErrManagedDirectoryNotChild, relativeDirectory)
	}
	base, err := ResolveDirectory(baseDirectory)
	if err != nil {
		return "", err
	}
	relative := filepath.Clean(relativeDirectory)
	resolved := filepath.Join(base, relative)
	current := base
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			// この階層が未作成なら、その下も未作成です。作成は呼び出し元に委ねます。
			return resolved, nil
		}
		if err != nil {
			return "", fmt.Errorf("inspect managed directory %q: %w", current, err)
		}
		if !info.IsDir() || isDirectoryLink(info) {
			return "", &ManagedDirectoryObstructionError{Path: current, Info: info}
		}
	}
	return resolved, nil
}
