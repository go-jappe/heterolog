package pathidentity

import (
	"path/filepath"
	"runtime"
	"strings"

	"github.com/go-jappe/heterolog/internal/pathcaserule"
)

// NameCase is how case is treated when file names directly under one directory
// are compared. The zero value, NameCaseUnspecified, means that the rule has
// not been determined yet; ForDirectory determines it from the target
// directory. It is a rule only for comparison and does not change the spelling
// of the paths actually opened or of object storage keys.
type NameCase uint8

const (
	// NameCaseUnspecified is the state in which the rule has not yet been
	// determined from the target directory.
	NameCaseUnspecified NameCase = iota
	// NameCaseSensitive treats names that differ only in case as different
	// entries.
	NameCaseSensitive
	// NameCaseInsensitive treats names that differ only in case as the same
	// entry.
	NameCaseInsensitive
)

// DirectoryNameCase returns the case rule applied to entry names directly under
// directory. It shares the decision with ComparisonKey: Windows uses standard
// case-insensitive directories, Darwin uses the filesystem characteristics of
// the existing ancestors, Linux uses the casefold attribute of the existing
// ancestors and the type of the filesystem, and other operating systems use a
// case-sensitive rule. On Linux, the only directories judged case-insensitive
// are directories with a casefold attribute that can be confirmed by reading
// it (ext4, F2FS, tmpfs) and directories on FAT (vfat, msdos) and exFAT whose
// mount options, as read from /proc/self/mountinfo, keep names
// case-insensitive. Other filesystems whose behavior depends on the server or
// on mount options, such as SMB/CIFS, NFS, NTFS and FUSE, are judged
// case-sensitive. A directory that does not exist yet gets the rule of its
// nearest existing ancestor, and no probe file is created. An empty directory
// string is treated as the current directory. The tests of this module can
// override the rule; code outside the module cannot set such an override.
func DirectoryNameCase(directory string) NameCase {
	if directory == "" {
		directory = "."
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		absolute = filepath.Clean(directory)
	}
	if sensitive, ok := pathcaserule.Lookup(absolute); ok {
		if sensitive {
			return NameCaseSensitive
		}
		return NameCaseInsensitive
	}
	if runtime.GOOS == "windows" {
		return NameCaseInsensitive
	}
	return filesystemDirectoryNameCase(absolute)
}

// ForDirectory returns c as is when the rule is already specified, and
// otherwise determines the rule from directory. Callers that pass a rule for
// tests or an already determined rule can use the same processing without
// querying the filesystem.
func (c NameCase) ForDirectory(directory string) NameCase {
	if c == NameCaseSensitive || c == NameCaseInsensitive {
		return c
	}
	return DirectoryNameCase(directory)
}

// Key returns the string used to compare name under this rule. With
// NameCaseInsensitive it converts name to lowercase; otherwise it returns the
// input as is. An undetermined rule is treated as case-sensitive, so determine
// the rule with ForDirectory first.
func (c NameCase) Key(name string) string {
	if c == NameCaseInsensitive {
		return strings.ToLower(name)
	}
	return name
}

// EntryNameCases builds a function that returns the rule for each path. When
// nameCase is already specified, the function returns it as is; otherwise the
// function determines the rule of each path's parent directory only once and
// reuses it within the same function. Use the function only during a short
// operation, such as classifying the results of one scan, and do not call it
// concurrently from multiple goroutines.
func EntryNameCases(nameCase NameCase) func(path string) NameCase {
	if nameCase == NameCaseSensitive || nameCase == NameCaseInsensitive {
		return func(string) NameCase { return nameCase }
	}
	directories := make(map[string]NameCase)
	return func(path string) NameCase {
		directory := filepath.Dir(path)
		resolved, exists := directories[directory]
		if !exists {
			resolved = DirectoryNameCase(directory)
			directories[directory] = resolved
		}
		return resolved
	}
}

// EntryComparisonKey returns a comparison key in which the parent directory is
// normalized with ComparisonKey and the final entry name with nameCase. When
// nameCase is unspecified, it is the same as ComparisonKey(path). When the
// nameCase passed was determined from the rule of the parent directory, the
// key is also the same as the one from ComparisonKey. Do not use the returned
// value for actual file operations.
func EntryComparisonKey(path string, nameCase NameCase) string {
	if path == "" || (nameCase != NameCaseSensitive && nameCase != NameCaseInsensitive) {
		return ComparisonKey(path)
	}
	absolute := AbsoluteOrCleanPath(path)
	directory, name := filepath.Dir(absolute), filepath.Base(absolute)
	if directory == absolute {
		return ComparisonKey(absolute)
	}
	return filepath.Join(ComparisonKey(directory), nameCase.Key(name))
}

// directoryNameCaseは、caseKeyがdirectory直下の名前へ適用する規則を返します。
// caseKeyと同じくvolume rootから各階層を問い合わせ、失敗した階層では親の規則を引き継ぎます。
func directoryNameCase(directory string, caseSensitive func(string) (bool, error)) NameCase {
	volume := filepath.VolumeName(directory)
	current := volume + string(filepath.Separator)
	relative := strings.TrimPrefix(directory[len(volume):], string(filepath.Separator))
	sensitive := true
	if value, err := caseSensitive(current); err == nil {
		sensitive = value
	}
	if isCleanSlashAbsolute(directory) {
		// 各階層は入力の部分文字列なので、組み立て直さずに同じ順で問い合わせます。
		for index := 2; index <= len(directory); index++ {
			if index < len(directory) && directory[index] != '/' {
				continue
			}
			if value, err := caseSensitive(directory[:index]); err == nil {
				sensitive = value
			}
		}
		relative = ""
	}
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		if value, err := caseSensitive(current); err == nil {
			sensitive = value
		}
	}
	if sensitive {
		return NameCaseSensitive
	}
	return NameCaseInsensitive
}
