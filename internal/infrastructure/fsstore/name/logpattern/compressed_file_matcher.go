package logpattern

import (
	"path/filepath"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// CompressedFilePathMatcherは、保存先ディレクトリとログ系列の命名規則に合う
// 圧縮ファイルのパスを判定する関数を返します。別ディレクトリの同名ファイル、
// 元ログ、圧縮途中のtmpは対象外です。ファイルの存在や内容は調べません。
// ファイル名の大小文字は、保存先ディレクトリの規則（patternのEffectiveNameCase）で比較します。
func CompressedFilePathMatcher(pattern FileNamePattern) func(string) bool {
	wantDirectory := pathidentity.ComparisonKey(pattern.Dir)
	matchName := CompressedFileNameMatcher(pattern)
	return func(path string) bool {
		return pathidentity.ComparisonKey(filepath.Dir(path)) == wantDirectory && matchName(path)
	}
}

// CompressedFileNameMatcherは、ディレクトリを問わず、ファイル名がログ系列の
// 圧縮ファイルの命名規則に合うかを判定する関数を返します。元のディレクトリから
// spoolへコピーされたファイルの判定に使います。ファイルの存在や内容は調べません。
// 大小文字はpatternのEffectiveNameCaseで比較し、関数の作成時に一度だけ判定します。
func CompressedFileNameMatcher(pattern FileNamePattern) func(string) bool {
	nameCase := pattern.EffectiveNameCase()
	return func(path string) bool {
		generation, canonical := parseCanonicalGenerationFilePath(path, pattern, nameCase)
		return canonical && generation.Kind == GenerationFileKindCompressed
	}
}
