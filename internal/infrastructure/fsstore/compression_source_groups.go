package fsstore

import (
	"errors"
	"os"
	"slices"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// CompressionSourceGroupsは、検査済みのファイル情報を元ファイルごとにまとめた結果です。
// Filesは分類前の検査結果全体を保持し、Sourcesから各組のパス・通常ファイル・検査エラーを取得できます。
// Filesと各組のslice/mapは読み取り専用です。元のFileInspectionを含めて変更せず、
// ファイルの状態が変わった後は再検査して分類し直してください。
type CompressionSourceGroups struct {
	Files         FileInspection
	Sources       []CompressionSourceGroup
	sourceIndexes map[string]int
	// nameCaseは分類と検索で共通に使うfile名の規則です。未指定なら各pathの親directoryで判定します。
	nameCase pathidentity.NameCase
}

// CompressionSourceGroupは、一つの元ファイルと対応する圧縮ファイルの情報をまとめた組です。
// SourcePathとPathsは入力時のパス表記を保持し、Pathsには非通常ファイルや検査失敗も残します。
// RegularFilesは通常ファイルだけを入力順で保持します。元ファイル自体も含まれます。
// Errがある元ファイルでは、通常ファイルの圧縮結果があっても処理を中止してください。
type CompressionSourceGroup struct {
	SourcePath   string
	Paths        []string
	RegularFiles []FileInfo
	Err          error
}

// GroupFilesByCompressionSourceは、取得済みの検査結果を、対応する元ファイルのパスごとに分類します。
// 例えばbaseExtensionが"log"なら、app.log・app.log.gz・app.log.zstを同じ組にまとめます。
// パスの比較にはpathidentityを使います。元ファイル自身が存在しなくても分類できます。
// file名の大小文字は、各pathの親directoryの規則（pathidentity.DirectoryNameCase）で比較します。
// 一時ファイル・元ログの削除待ちファイル・別の拡張子はSourcesから除外し、Filesには入力全体を残します。
// 日付・番号の解析や処理対象のファイル名の選別は呼び出し側が行います。
// この関数は再走査・Lstat・圧縮・内容検証を行いません。
func GroupFilesByCompressionSource(files FileInspection, baseExtension string) CompressionSourceGroups {
	return GroupFilesByCompressionSourceForNameCase(files, baseExtension, pathidentity.NameCaseUnspecified)
}

// GroupFilesByCompressionSourceForNameCaseは、file名の規則nameCaseを指定して
// GroupFilesByCompressionSourceと同じ分類を行います。結果のSource・FindExistingCompressedFileも
// 同じ規則で検索します。directory部分はpathidentityのComparisonKeyで比較します。
// nameCaseが未指定なら各pathの親directoryから判定します。
func GroupFilesByCompressionSourceForNameCase(
	files FileInspection,
	baseExtension string,
	nameCase pathidentity.NameCase,
) CompressionSourceGroups {
	sourceGroups := CompressionSourceGroups{Files: files, nameCase: nameCase}
	entryNameCase := pathidentity.EntryNameCases(nameCase)
	pathIndexes := make(map[string]int)
	sourceIndex := func(path string) (int, bool) {
		if index, classified := pathIndexes[path]; classified {
			return index, index >= 0
		}
		sourcePath, ok := CompressionSourcePathForNameCase(path, baseExtension, entryNameCase(path))
		if !ok {
			pathIndexes[path] = -1
			return -1, false
		}
		key := pathidentity.EntryComparisonKey(sourcePath, nameCase)
		index, exists := sourceGroups.sourceIndexes[key]
		if !exists {
			index = len(sourceGroups.Sources)
			if sourceGroups.sourceIndexes == nil {
				sourceGroups.sourceIndexes = make(map[string]int)
			}
			sourceGroups.sourceIndexes[key] = index
			sourceGroups.Sources = append(sourceGroups.Sources, CompressionSourceGroup{SourcePath: sourcePath})
		}
		pathIndexes[path] = index
		sourceGroups.Sources[index].Paths = append(sourceGroups.Sources[index].Paths, path)
		return index, true
	}
	for _, path := range files.Paths {
		sourceIndex(path)
	}
	for _, file := range files.RegularFiles {
		if index, ok := sourceIndex(file.Path); ok {
			sourceGroups.Sources[index].RegularFiles = append(sourceGroups.Sources[index].RegularFiles, file)
		}
	}
	// 入力でPathsが省略されていても、検査エラーに含まれるパスを一定の順で分類します。
	errorPaths := make([]string, 0, len(files.PathErrors))
	for path := range files.PathErrors {
		errorPaths = append(errorPaths, path)
	}
	slices.Sort(errorPaths)
	for _, path := range errorPaths {
		index, ok := sourceIndex(path)
		err := files.PathErrors[path]
		if !ok || err == nil || errors.Is(err, os.ErrNotExist) {
			continue
		}
		source := &sourceGroups.Sources[index]
		source.Err = errors.Join(source.Err, &CompressionCandidateError{Path: path, Err: err})
	}
	return sourceGroups
}

// Sourceは、指定した元ファイルのパスに対応する読み取り専用の組を返します。
func (g CompressionSourceGroups) Source(sourcePath string) (CompressionSourceGroup, bool) {
	index, exists := g.sourceIndexes[pathidentity.EntryComparisonKey(sourcePath, g.nameCase)]
	if !exists {
		return CompressionSourceGroup{}, false
	}
	return g.Sources[index], true
}

// FindExistingCompressedFileは、同じ元ファイルの検査エラーがなければ、最初の通常圧縮ファイルを返します。
// 現在の圧縮形式に依存せずRegularFilesの入力順で選び、未発見でも作成予定のパスは生成しません。
// 分類済みの情報だけを参照し、使用時の存在・完全性の確認は呼び出し側が行います。
func (g CompressionSourceGroups) FindExistingCompressedFile(sourcePath string) (string, bool, error) {
	source, exists := g.Source(sourcePath)
	if !exists {
		return "", false, nil
	}
	if source.Err != nil {
		return "", false, source.Err
	}
	sourceKey := pathidentity.EntryComparisonKey(sourcePath, g.nameCase)
	for _, file := range source.RegularFiles {
		if pathidentity.EntryComparisonKey(file.Path, g.nameCase) != sourceKey {
			return file.Path, true, nil
		}
	}
	return "", false, nil
}
