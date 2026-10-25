package logpattern

import (
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// CountGenerationsは、base extensionの未圧縮fileと一段の圧縮fileを、日付・数値indexで決まる
// 世代ごとに数えます。同じ世代の未圧縮fileと各圧縮形式のfileは一世代として数え、
// base extensionへ一致しないfileや、圧縮途中のtemporary fileは数えません。
// 日付・indexを解析できない名前も、圧縮suffixを除いた名前ごとに一つとして数えます。
//
// candidatesは、呼び出し側で一つのlogger targetへ絞り込み、active・retainedなど
// generation purgeから保護するpathを除外してから渡します。
// 名前の大小文字は各fileの保存先ディレクトリの規則で比較し、同じ名前になるfileは同じ世代です。
func CountGenerations(candidates []fsstore.FileInfo, baseExtension string) uint64 {
	return countGenerations(candidates, baseExtension, pathidentity.NameCaseUnspecified)
}

// countGenerationsは、名前の規則nameCaseを指定してCountGenerationsと同じ数え方をします。
func countGenerations(candidates []fsstore.FileInfo, baseExtension string, nameCase pathidentity.NameCase) uint64 {
	generationKey := generationKeys(baseExtension, nameCase)
	generations := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if key, ok := generationKey(candidate); ok {
			generations[key] = struct{}{}
		}
	}
	return uint64(len(generations))
}

// PurgeGenerationsByFileNameIfNeededは、世代数がmaxBackupsを超えた場合に、
// ファイル名の日付・数値indexが古い超過世代の未圧縮・圧縮fileをまとめて削除します。
// 世代の数え方はCountGenerationsと同じで、圧縮形式ごとに上限を適用しません。更新日時は使用しません。
// candidatesは、呼び出し側で一つのlogger targetへ絞り、削除禁止のファイルを除いて渡します。
// 削除禁止のfileは数えず削除もしないため、同じ世代でも保護中のfileは残ります。
// この関数自身はGuardの保護情報を取得しません。Remainingは入力順を保持します。
func PurgeGenerationsByFileNameIfNeeded(
	candidates []fsstore.FileInfo,
	baseExtension string,
	maxBackups int,
) fsstore.PurgeResult {
	return purgeGenerations(candidates, baseExtension, maxBackups, pathidentity.NameCaseUnspecified)
}

// purgeGenerationsは、名前の規則nameCaseを指定してPurgeGenerationsByFileNameIfNeededと同じ削除を行います。
func purgeGenerations(
	candidates []fsstore.FileInfo,
	baseExtension string,
	maxBackups int,
	nameCase pathidentity.NameCase,
) fsstore.PurgeResult {
	return fsstore.PurgeGenerationGroupsFromFileNameIfNeeded(
		candidates,
		maxBackups,
		generationKeys(baseExtension, nameCase),
	)
}

// generationKeysは、fileの世代を表すkeyを返す関数を作ります。keyは保存先ディレクトリの
// 比較keyと、未圧縮・圧縮suffixを除いた名前をそのディレクトリの規則で揃えた値です。
// base extensionへ一致しないfileと圧縮途中のtemporary fileはfalseを返します。
// nameCaseが未指定なら、各fileの親ディレクトリの規則を一度だけ判定して再利用します。
func generationKeys(
	baseExtension string,
	nameCase pathidentity.NameCase,
) func(fsstore.FileInfo) (string, bool) {
	entryNameCase := pathidentity.EntryNameCases(nameCase)
	directoryKeys := make(map[string]string)
	return func(file fsstore.FileInfo) (string, bool) {
		candidateCase := entryNameCase(file.Path)
		stem, _, ok := fsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(
			filepath.Base(file.Path),
			baseExtension,
			candidateCase,
		)
		if !ok {
			return "", false
		}
		directory := filepath.Dir(file.Path)
		directoryKey, exists := directoryKeys[directory]
		if !exists {
			directoryKey = pathidentity.ComparisonKey(directory)
			directoryKeys[directory] = directoryKey
		}
		return directoryKey + "\x00" + candidateCase.Key(stem), true
	}
}
