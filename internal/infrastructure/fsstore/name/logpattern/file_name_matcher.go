package logpattern

import (
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// FileNameMatcherは、未圧縮・圧縮済みログの名前がpatternとmodeに一致するかを判定する関数を返します。
// GlobByTargetでは接頭辞・対象名・拡張子を、GlobAllFilesでは拡張子だけを照合します。
// 圧縮拡張子は安全な一段のsuffixを許容し、圧縮途中の.tmpなどは対象外です。
// 設定値は文字どおり扱い、大小文字はpatternのEffectiveNameCase（未指定ならDirの判定結果）で比較します。
//
// 入力はファイル名でもパスでもよく、ディレクトリ・patternのDateとIndexは照合しません。
// 日付・世代番号の正当性も検証しません。厳密な世代の解析にはParseCanonicalGenerationFilePathを使ってください。
// ファイルの存在・種類・内容は調べず、不明なmodeでは常にfalseを返します。
func FileNameMatcher(pattern FileNamePattern, mode GlobMode) func(string) bool {
	return fileNameMatcher(pattern, mode, pattern.EffectiveNameCase())
}

// fileNameMatcherは、判定済みの規則nameCaseでFileNameMatcherと同じ照合を行います。
func fileNameMatcher(pattern FileNamePattern, mode GlobMode, nameCase pathidentity.NameCase) func(string) bool {
	var prefix string
	switch mode {
	case GlobByTarget:
		prefix = nameCase.Key(pattern.Prefix + pattern.Target + "_")
	case GlobAllFiles:
	default:
		return func(string) bool { return false }
	}
	return func(path string) bool {
		stem, ok := trimBaseAndOptionalCompressionExtensions(filepath.Base(path), pattern.Extension, nameCase)
		return ok && strings.HasPrefix(nameCase.Key(stem), prefix)
	}
}
