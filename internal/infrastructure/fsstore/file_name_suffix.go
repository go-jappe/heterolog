package fsstore

import (
	"runtime"
	"strings"
	"unicode/utf8"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// TrimFileNameSuffixは、directoryを参照しないOSの既定規則に従って末尾のsuffixを取り除きます。
// Windowsでは大小文字を区別せずに照合します。返す名前は元の表記を保持します。
// 保存先directoryのfilesystemの規則で照合する場合はTrimFileNameSuffixForNameCaseを使います。
func TrimFileNameSuffix(name, suffix string) (string, bool) {
	return TrimFileNameSuffixForNameCase(name, suffix, pathidentity.NameCaseUnspecified)
}

// TrimFileNameSuffixForNameCaseは、file名を保存するdirectoryの規則nameCaseに従って
// 末尾のsuffixを取り除きます。返す名前は元の表記を保持します。
// nameCaseが未指定なら、TrimFileNameSuffixと同じOSの既定規則を使います。
func TrimFileNameSuffixForNameCase(name, suffix string, nameCase pathidentity.NameCase) (string, bool) {
	if fileNameCaseOrDefault(nameCase) != pathidentity.NameCaseInsensitive {
		return strings.CutSuffix(name, suffix)
	}
	if !strings.HasSuffix(strings.ToLower(name), strings.ToLower(suffix)) {
		return "", false
	}
	// Unicodeのcase変換でUTF-8のbyte長が変わっても、元のstemをそのまま返します。
	index := len(name)
	for range suffix {
		_, size := utf8.DecodeLastRuneInString(name[:index])
		index -= size
	}
	return name[:index], true
}

// fileNameCaseOrDefaultは、名前だけを受け取る処理で未指定の規則をOSの既定規則へ置き換えます。
// directoryを参照できないため、Windows以外では従来どおり大小文字を区別します。
func fileNameCaseOrDefault(nameCase pathidentity.NameCase) pathidentity.NameCase {
	if nameCase != pathidentity.NameCaseUnspecified {
		return nameCase
	}
	if runtime.GOOS == "windows" {
		return pathidentity.NameCaseInsensitive
	}
	return pathidentity.NameCaseSensitive
}
