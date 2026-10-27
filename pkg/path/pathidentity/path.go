// Package pathidentity resolves the real path of a storage location when the
// location is initialized, and generates comparison keys for paths. The result
// of resolving links with ResolveDirectory is kept, and normal processing
// compares paths with ComparisonKey. Comparison does not resolve links or 8.3
// short names. On Darwin and Linux, the filesystem characteristics of the
// existing ancestors are read. Comparison on Windows targets standard
// case-insensitive directories. The case rule for comparing the file names
// directly under a directory is obtained from the same decision with
// DirectoryNameCase. Do not pass identity keys to actual file operations; keep
// the original path to operate on separately.
package pathidentity

import (
	"path/filepath"
	"runtime"
	"strings"

	"github.com/go-jappe/heterolog/internal/pathcaserule"
)

// AbsoluteKey returns a comparison key made by making the path absolute and
// normalizing it. For ordinary directories on Windows, it unifies the case and
// the extended path notation. On Darwin and Linux, it uses the case
// characteristics of the target filesystem. It does not resolve links.
func AbsoluteKey(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return platformKey(absolute), nil
}

// ComparisonKey returns the key used to compare paths with each other. It makes
// the path absolute and cleans it, and on Windows it evens out differences in
// case and in the ordinary extended path notation. An empty path is returned
// empty. If making the path absolute fails, the key is built from the cleaned
// path. On Darwin and Linux, it reads the existing ancestors and applies the
// same case characteristics to children that do not exist yet. Do not use the
// returned value for actual file operations.
func ComparisonKey(path string) string {
	if path == "" {
		return ""
	}
	key, err := AbsoluteKey(path)
	if err != nil {
		return platformKey(filepath.Clean(path))
	}
	return key
}

// Contains matches a path against a snapshot whose keys are original
// spellings already processed by Clean and Abs. It prefers an exact match, and
// it also matches case aliases on the filesystem and the Windows extended
// notation. It does not rewrite the keys of the map, so the caller can keep
// using the original spellings for file operations.
func Contains(paths map[string]struct{}, path string) bool {
	if len(paths) == 0 {
		return false
	}
	if _, exists := paths[path]; exists {
		return true
	}
	// purgeの除外集合はClean済みの相対pathも受け取るため、その契約も維持します。
	if _, exists := paths[filepath.Clean(path)]; exists {
		return true
	}
	key := ComparisonKey(path)
	if _, exists := paths[key]; exists {
		return true
	}
	// Linuxでは、大小文字を区別しないdirectoryを通るpathだけ別表記を探します。
	if runtime.GOOS == "windows" || runtime.GOOS == "darwin" || pathcaserule.Active() ||
		filesystemFoldsCase(AbsoluteOrCleanPath(path)) {
		for candidate := range paths {
			if ComparisonKey(candidate) == key {
				return true
			}
		}
	}
	return false
}

func platformKey(path string) string {
	if pathcaserule.Active() && filepath.IsAbs(path) {
		return overriddenPathKey(path)
	}
	if runtime.GOOS == "windows" {
		return windowsPathKey(path)
	}
	return filesystemPathKey(path)
}

// overriddenPathKeyは、試験用の規則（internal/pathcaserule）が設定されている間の比較keyです。
// 各階層の名前を、規則を差し替えたdirectoryではその規則、それ以外ではOSの既定の判定で揃えます。
// Windowsでは通常のextended表記とdriveの大小文字だけを先に揃えます。
func overriddenPathKey(path string) string {
	if runtime.GOOS == "windows" {
		path = windowsNormalPath(path)
		volume := filepath.VolumeName(path)
		path = strings.ToLower(volume) + path[len(volume):]
	}
	return caseKey(path, func(parent string) (bool, error) {
		if sensitive, ok := pathcaserule.Lookup(parent); ok {
			return sensitive, nil
		}
		return filesystemCaseSensitive(parent)
	})
}

// windowsNormalPathは、通常のdrive/UNC pathのextended表記を通常表記へ戻します。
// windowsPathKeyと同じ表記を対象にし、大小文字は変更しません。
func windowsNormalPath(path string) string {
	if len(path) >= 8 && strings.EqualFold(path[:8], `\\?\UNC\`) {
		return `\\` + path[8:]
	}
	if len(path) >= 7 && strings.HasPrefix(path, `\\?\`) &&
		((path[4] >= 'a' && path[4] <= 'z') || (path[4] >= 'A' && path[4] <= 'Z')) &&
		path[5] == ':' && path[6] == '\\' {
		return path[4:]
	}
	return path
}

// windowsPathKeyは通常のdrive/UNC pathだけをextended表記と統合します。
// volume GUIDなど別のdevice namespaceを通常drive pathとして解釈し直しません。
func windowsPathKey(path string) string {
	key := strings.ToLower(path)
	if strings.HasPrefix(key, `\\?\unc\`) {
		return `\\` + key[len(`\\?\unc\`):]
	}
	if len(key) >= 7 && strings.HasPrefix(key, `\\?\`) &&
		key[4] >= 'a' && key[4] <= 'z' && key[5] == ':' && key[6] == '\\' {
		return key[4:]
	}
	return key
}
