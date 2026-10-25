package fsstore

import (
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// CompressionSourcePathは、未圧縮fileならそのpath、圧縮fileなら末尾の圧縮拡張子を
// 外したpathを返します。元fileの存在は確認しません。圧縮後もこのpathを共通の
// 保護キーにすることで、同じ元ファイルを別の処理が同時に扱うことを防げます。
// baseExtensionに一致しない名前や、圧縮途中の.tmpは受け付けません。
// base extensionの大小文字は、pathの親directoryの規則（pathidentity.DirectoryNameCase）で照合します。
func CompressionSourcePath(path, baseExtension string) (string, bool) {
	return CompressionSourcePathForNameCase(path, baseExtension, pathidentity.NameCaseUnspecified)
}

// CompressionSourcePathForNameCaseは、file名の規則nameCaseを指定してCompressionSourcePathと
// 同じ判定を行います。nameCaseが未指定ならpathの親directoryから判定します。
func CompressionSourcePathForNameCase(path, baseExtension string, nameCase pathidentity.NameCase) (string, bool) {
	nameCase = nameCase.ForDirectory(filepath.Dir(path))
	_, extension, ok := SplitBaseAndOptionalCompressionExtensionsForNameCase(filepath.Base(path), baseExtension, nameCase)
	if !ok {
		return "", false
	}
	if extension == "."+strings.TrimPrefix(baseExtension, ".") {
		return path, true
	}
	return strings.TrimSuffix(path, filepath.Ext(path)), true
}

// CompressionProtectionPathsは、処理対象のパスと、対応する未圧縮ファイルのパスを返します。
// 圧縮形式が異なっても同じ元ファイルのパスを保護することで、処理の競合を防げます。
// 空パスは含めず、未圧縮ファイルは同じパスを一度だけ返します。
// 名前を解析できない場合は、処理対象のパスだけを返します。
// ファイルの存在確認や保護登録は行いません。登録と解除は呼び出し元が行います。
func CompressionProtectionPaths(path, baseExtension string) []string {
	if path == "" {
		return nil
	}
	source, valid := CompressionSourcePath(path, baseExtension)
	if !valid || source == path {
		return []string{path}
	}
	return []string{path, source}
}
