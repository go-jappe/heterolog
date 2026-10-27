package pathidentity

import "path/filepath"

// AbsoluteOrCleanPath cleans the path and makes it absolute while keeping its
// original case. An empty path is returned empty. If making the path absolute
// fails, it returns the cleaned original path. It does not check whether the
// target file exists, resolve links, or lowercase the path for comparison.
func AbsoluteOrCleanPath(path string) string {
	if path == "" {
		return ""
	}
	cleaned := filepath.Clean(path)
	if absolute, err := filepath.Abs(cleaned); err == nil {
		return filepath.Clean(absolute)
	}
	return cleaned
}
