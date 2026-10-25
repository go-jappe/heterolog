package pathidentity

import (
	"path/filepath"
	"strings"
)

// caseKey follows the rules of each existing parent, including mount boundaries.
// Missing descendants inherit their nearest existing ancestor's rules. Inspecting
// directory metadata never creates probe files or directories.
// directoryNameCase applies the same walk to report the rule for a directory's entries.
func caseKey(path string, caseSensitive func(string) (bool, error)) string {
	if isCleanSlashAbsolute(path) {
		return cleanSlashCaseKey(path, caseSensitive)
	}
	return joinedCaseKey(path, caseSensitive)
}

// joinedCaseKeyは、volume rootから各階層をfilepath.Joinで組み立てながらcaseKeyを求めます。
// Windowsのpathや整理されていないpathに使います。
func joinedCaseKey(path string, caseSensitive func(string) (bool, error)) string {
	volume := filepath.VolumeName(path)
	root := volume + string(filepath.Separator)
	relative := strings.TrimPrefix(path[len(volume):], string(filepath.Separator))
	original, key := root, root
	sensitive := true
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		if current, err := caseSensitive(original); err == nil {
			sensitive = current
		}
		name := part
		if !sensitive {
			name = strings.ToLower(name)
		}
		key = filepath.Join(key, name)
		original = filepath.Join(original, part)
	}
	return key
}

// isCleanSlashAbsoluteは、区切りが"/"のOSで、整理済みの絶対path（filepath.Cleanで変わらないpath）かを
// 返します。このpathでは各階層の親が入力の部分文字列になるため、filepath.Joinで組み立て直す必要がありません。
// 空・"."・".."の要素と末尾の区切りがないことを、pathを複製せずに確かめます。
func isCleanSlashAbsolute(path string) bool {
	if filepath.Separator != '/' || !strings.HasPrefix(path, "/") {
		return false
	}
	for start := 1; start < len(path); {
		end := strings.IndexByte(path[start:], '/')
		if end < 0 {
			end = len(path)
		} else {
			end += start
		}
		if element := path[start:end]; element == "" || element == "." || element == ".." || end == len(path)-1 {
			return false
		}
		start = end + 1
	}
	return true
}

// cleanSlashCaseKeyは、isCleanSlashAbsoluteのpathについてcaseKeyと同じkeyを返します。
// 親directoryは入力の部分文字列で問い合わせ、名前を揃えて表記が変わるまでは入力をそのまま返します。
func cleanSlashCaseKey(path string, caseSensitive func(string) (bool, error)) string {
	var key []byte
	sensitive := true
	for start := 1; start < len(path); {
		end := strings.IndexByte(path[start:], '/')
		if end < 0 {
			end = len(path)
		} else {
			end += start
		}
		parent := "/"
		if start > 1 {
			parent = path[:start-1]
		}
		if current, err := caseSensitive(parent); err == nil {
			sensitive = current
		}
		name := path[start:end]
		if !sensitive {
			if lower := strings.ToLower(name); lower != name {
				if key == nil {
					key = append(make([]byte, 0, len(path)), path[:start]...)
				}
				name = lower
			}
		}
		if key != nil {
			key = append(key, name...)
			if end < len(path) {
				key = append(key, '/')
			}
		}
		start = end + 1
	}
	if key == nil {
		return path
	}
	return string(key)
}

// filesystemFoldsCaseは、ComparisonKeyと同じ問い合わせで、pathのいずれかの階層の名前を
// 大小文字を区別しないdirectoryの規則で揃えるかを返します。Containsが大小文字だけ異なる
// 元表記を探す必要があるかの判定に使います。絶対化できなかったpathはfalseです。
func filesystemFoldsCase(path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	folds := false
	caseKey(path, func(directory string) (bool, error) {
		sensitive, err := filesystemCaseSensitive(directory)
		if err == nil && !sensitive {
			folds = true
		}
		return sensitive, err
	})
	return folds
}
