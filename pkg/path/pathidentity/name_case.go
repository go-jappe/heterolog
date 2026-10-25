package pathidentity

import (
	"path/filepath"
	"runtime"
	"strings"

	"github.com/go-jappe/heterolog/internal/pathcaserule"
)

// NameCaseは、一つのdirectory直下でfile名を比較するときの大小文字の扱いです。
// ゼロ値のNameCaseUnspecifiedは未判定を表し、ForDirectoryで対象directoryから判定します。
// 比較専用の規則であり、実際に開くpathやobject storage keyの表記は変更しません。
type NameCase uint8

const (
	// NameCaseUnspecifiedは、まだ対象directoryから判定していない状態です。
	NameCaseUnspecified NameCase = iota
	// NameCaseSensitiveは、大小文字だけが異なる名前を別のentryとして扱います。
	NameCaseSensitive
	// NameCaseInsensitiveは、大小文字だけが異なる名前を同じentryとして扱います。
	NameCaseInsensitive
)

// DirectoryNameCaseは、directory直下のentry名に適用する大小文字の規則を返します。
// ComparisonKeyと同じ判定を共用し、Windowsは標準のcase-insensitive directory、
// Darwinは既存祖先のfilesystem特性、Linuxは既存祖先のcasefold属性とfilesystemの種類、
// その他のOSは大小文字を区別する規則です。Linuxで区別しないと判定するのは、読み取りで確かめられる
// casefold属性（ext4・F2FS・tmpfs）を持つdirectoryと、FAT（vfat・msdos）・exFATのdirectoryだけです。
// SMB/CIFS・NFS・NTFS・FUSEなど、serverやmount optionで挙動が変わるfilesystemは区別すると判定します。
// 未作成のdirectoryには最も近い既存祖先の規則を適用し、probe用のfileは作成しません。
// 空のdirectoryは現在のdirectoryとして扱います。試験用の規則が設定されている間は、
// 対象directoryでその規則を使います（internal/pathcaserule）。
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

// ForDirectoryは、指定済みの規則はそのまま返し、未指定ならdirectoryから判定します。
// 試験や判定済みの規則を渡す呼び出し元は、filesystemを問い合わせずに同じ処理を使えます。
func (c NameCase) ForDirectory(directory string) NameCase {
	if c == NameCaseSensitive || c == NameCaseInsensitive {
		return c
	}
	return DirectoryNameCase(directory)
}

// Keyは、この規則でnameを比較するための文字列を返します。
// NameCaseInsensitiveでは小文字に揃え、それ以外では入力をそのまま返します。
// 未判定の規則は大小文字を区別して扱うため、先にForDirectoryで判定してください。
func (c NameCase) Key(name string) string {
	if c == NameCaseInsensitive {
		return strings.ToLower(name)
	}
	return name
}

// EntryNameCasesは、pathごとの規則を返す関数を作ります。指定済みの規則はそのまま返し、
// 未指定なら各pathの親directoryを一度だけ判定して同じ関数内で再利用します。
// 一回の走査結果の分類など、短い処理の間だけ使用し、複数goroutineから並行して呼び出さないでください。
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

// EntryComparisonKeyは、親directoryをComparisonKeyで、末尾のentry名をnameCaseで揃えた
// 比較キーを返します。nameCaseが未指定ならComparisonKey(path)と同じです。
// 親directoryの規則から判定したnameCaseを渡した場合も、ComparisonKeyと同じキーになります。
// 戻り値は実際のファイル操作には使わないでください。
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
