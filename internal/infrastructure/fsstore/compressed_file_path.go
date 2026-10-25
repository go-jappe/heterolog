package fsstore

import (
	"path/filepath"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// ResolveCompressedFilePathは、元ファイルに対応する圧縮ファイルのパスを決めます。
// 一覧に対応する既存ファイルがあれば、旧形式も含めてそのパスとtrueを返します。
// なければformatで作成予定のパスを生成し、falseを返します。ファイルは作成しません。
// observedPathsには通常ファイルと確認済みの候補を渡してください。
// 観測時に検査エラーがあったファイルの圧縮・再利用は、呼び出し側で中止してください。
// 戻り値のexistsは一覧で見つかったことを示し、使用時の存在・完全性は別途確認します。
func ResolveCompressedFilePath(
	sourcePath string,
	observedPaths []string,
	format pkgfsstore.CompressFormat,
) (compressedPath string, exists bool, err error) {
	if existingPath, found := FindExistingCompressedFilePath(sourcePath, observedPaths); found {
		return existingPath, true, nil
	}
	compressedPath = GenerateCompressedPath(sourcePath, format)
	if compressedPath == "" {
		return "", false, &CompressionFormatExtensionError{Format: format}
	}
	return compressedPath, false, nil
}

// FindExistingCompressedFilePathは、sourcePathに対応する既存の圧縮ファイルを探します。
// 元ファイル1つにつき圧縮ファイルは1つを前提とし、一覧順で最初の一致を返します。
// 現在の圧縮設定に依存せず、以前の形式も対象です。未発見なら空文字とfalseを返し、
// 作成予定のパスは生成しません。
// sourcePathには、元の拡張子を持つファイルのパスを渡してください。
// observedPathsには呼び出し元で通常ファイルと確認した候補を渡してください。
// file名の大小文字は、各pathの親directoryの規則（pathidentity.DirectoryNameCase）で比較します。
// この関数は一覧だけを検索します。使用時の存在・完全性の確認は呼び出し側で行います。
func FindExistingCompressedFilePath(sourcePath string, observedPaths []string) (path string, found bool) {
	return FindExistingCompressedFilePathForNameCase(sourcePath, observedPaths, pathidentity.NameCaseUnspecified)
}

// FindExistingCompressedFilePathForNameCaseは、file名の規則nameCaseを指定して
// FindExistingCompressedFilePathと同じ検索を行います。directory部分はpathidentityの
// ComparisonKeyで比較します。nameCaseが未指定なら各pathの親directoryから判定します。
func FindExistingCompressedFilePathForNameCase(
	sourcePath string,
	observedPaths []string,
	nameCase pathidentity.NameCase,
) (path string, found bool) {
	baseExtension := filepath.Ext(sourcePath)
	if baseExtension == "" || baseExtension == "." {
		return "", false
	}
	entryNameCase := pathidentity.EntryNameCases(nameCase)
	sourceKey := pathidentity.EntryComparisonKey(sourcePath, nameCase)
	for _, candidate := range observedPaths {
		// 元ファイルと圧縮結果の対応は、観測の分類と同じ解析規則で確認します。
		source, valid := CompressionSourcePathForNameCase(candidate, baseExtension, entryNameCase(candidate))
		if !valid || source == candidate {
			continue
		}
		if pathidentity.EntryComparisonKey(source, nameCase) == sourceKey {
			return candidate, true
		}
	}
	return "", false
}
