package pathidentity

import "strings"

// AddWindowsLongPathPrefix adds the Win32 extended prefix to a long absolute
// Windows path. The caller must make the path absolute and clean it first. The
// original case is not changed. Paths shorter than 248 bytes, the boundary that
// the os package also uses (MAX_PATH minus the 12 bytes of an 8.3 file name),
// and paths already in device or extended notation are returned as they are.
// A UNC path gets its own prefix. It does not check existence, resolve links,
// or convert to UTF-16.
func AddWindowsLongPathPrefix(path string) string {
	if len(path) < 248 || strings.HasPrefix(path, `\\?\`) ||
		strings.HasPrefix(path, `\\.\`) || strings.HasPrefix(path, `\??\`) {
		return path
	}
	if strings.HasPrefix(path, `\\`) {
		return `\\?\UNC\` + path[2:]
	}
	return `\\?\` + path
}
