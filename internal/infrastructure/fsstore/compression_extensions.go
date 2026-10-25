package fsstore

import (
	"strings"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// SplitBaseAndOptionalCompressionExtensionsは、ファイル名を拡張子の前の部分と、
// 基底拡張子・圧縮拡張子を合わせた拡張子へ分割します。
//
// baseExtensionが`log`の場合、`app_1.log`は(`app_1`, `.log`)、
// `app_1.log.gz`は(`app_1`, `.log.gz`)を返します。この完全なextensionを使うことで、
// 未圧縮ファイルと圧縮形式が異なるファイルを区別できます。
// 圧縮拡張子は安全な一段のsuffixだけを許可し、予約された.tmpや複数段の拡張子は拒否します。
// base extensionの大小文字はdirectoryを参照しないOSの既定規則で照合します。保存先directoryの
// 規則で照合する場合はSplitBaseAndOptionalCompressionExtensionsForNameCaseを使います。
func SplitBaseAndOptionalCompressionExtensions(
	name string,
	baseExtension string,
) (stem string, extension string, ok bool) {
	return SplitBaseAndOptionalCompressionExtensionsForNameCase(name, baseExtension, pathidentity.NameCaseUnspecified)
}

// SplitBaseAndOptionalCompressionExtensionsForNameCaseは、file名を保存するdirectoryの
// 規則nameCaseでbase extensionを照合し、SplitBaseAndOptionalCompressionExtensionsと
// 同じ分割を行います。返すstemと圧縮suffixは元の表記を保持し、base extension部分は
// "."とbaseExtensionを連結した値を返します。nameCaseが未指定ならOSの既定規則を使います。
func SplitBaseAndOptionalCompressionExtensionsForNameCase(
	name string,
	baseExtension string,
	nameCase pathidentity.NameCase,
) (stem string, extension string, ok bool) {
	baseExtension = strings.TrimPrefix(baseExtension, ".")
	if baseExtension == "" {
		return "", "", false
	}
	plainSuffix := "." + baseExtension
	if stem, matched := TrimFileNameSuffixForNameCase(name, plainSuffix, nameCase); matched {
		return stem, plainSuffix, true
	}

	compressionSeparator := strings.LastIndexByte(name, '.')
	if compressionSeparator < 0 {
		return "", "", false
	}
	stem, matched := TrimFileNameSuffixForNameCase(name[:compressionSeparator], plainSuffix, nameCase)
	if !matched {
		return "", "", false
	}
	compressionSuffix := name[compressionSeparator+1:]
	if !pkgfsstore.IsValidCompressionFileExtension("." + compressionSuffix) {
		return "", "", false
	}
	return stem, plainSuffix + "." + compressionSuffix, true
}
