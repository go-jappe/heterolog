package fsstore

import (
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// PurgeByFileNameDateIfNeededは、nowの日付で未実行の場合だけ、ファイル名の日付で
// 保持期限を判定します。nowと同じタイムゾーンのmaxAgeDays日前を期限日とし、
// 期限日より古いファイルを日付・数値indexの順に削除します。期限当日は保持します。
// 名前を解析できないファイルは削除せず、そのエラーをRemovalResultsへ記録します。
// この関数は書き込み中・upload中かを判定しません。削除禁止のファイルは、呼び出し側でfilesから除いてください。
func PurgeByFileNameDateIfNeeded(
	files []FileInfo,
	maxAgeDays int,
	now time.Time,
	lastPurgeAt time.Time,
) PurgeResult {
	if !ShouldPurgeByAgeToday(now, lastPurgeAt) {
		return purgeNotRun(files)
	}
	result := PurgeResult{Ran: true, Remaining: append([]FileInfo(nil), files...)}
	confirmedAbsent := make([]bool, len(files))
	indexes := validFileNameIndexes(files, nil, &result)
	if !sortValidatedFileIndexesByName(files, indexes, &result) {
		return result
	}
	cutoff := now.AddDate(0, 0, -maxAgeDays)
	// 共通処理のエラーは各callbackでRemovalResultsにも記録します。
	PurgeExpiredFiles(indexes, func(index *int) (bool, error) {
		file := files[*index]
		order, err := CompareFileNameDate(file.Path, cutoff)
		if err != nil {
			recordPurgeFileNameError(&result, file, err)
			return false, err
		}
		return order < 0, nil
	}, func(index *int) error {
		return removeFileAndRecordResult(&result, confirmedAbsent, *index, files[*index])
	})
	result.Remaining = collectRemainingFiles(files, confirmedAbsent)
	return result
}

// PurgeByGenerationsFromFileNameIfNeededは、名前を解析できるファイルがmaxBackups件を
// 超えたとき、日付・数値indexが古い超過分を削除します。選んだファイルの削除に失敗しても、
// 代わりに新しいファイルを削除しません。解析できない名前は件数に含めず、エラーとともに保持します。
// この関数は書き込み中・upload中かを判定しません。削除禁止のファイルは、呼び出し側でfilesから除いてください。
func PurgeByGenerationsFromFileNameIfNeeded(files []FileInfo, maxBackups int) PurgeResult {
	if len(files) <= maxBackups {
		return purgeNotRun(files)
	}
	result := PurgeResult{Ran: true, Remaining: append([]FileInfo(nil), files...)}
	confirmedAbsent := make([]bool, len(files))
	indexes := validFileNameIndexes(files, nil, &result)
	excess := len(indexes) - maxBackups
	if excess > len(indexes) {
		excess = len(indexes)
	}
	if excess > 0 {
		if !sortValidatedFileIndexesByName(files, indexes, &result) {
			return result
		}
		for _, index := range indexes[:excess] {
			removeFileAndRecordResult(&result, confirmedAbsent, index, files[index])
		}
	} else if len(result.RemovalResults) == 0 {
		result.Ran = false
	}
	result.Remaining = collectRemainingFiles(files, confirmedAbsent)
	return result
}

// PurgeGenerationGroupsFromFileNameIfNeededは、generationKeyが同じ値を返すファイルを一つの世代として
// 数え、世代数がmaxBackupsを超えたとき、ファイル名の日付・数値indexが古い超過世代のファイルを
// すべて削除します。未圧縮・圧縮形式の違いで世代数を増やさないために使います。
// generationKeyがfalseを返すファイルは対象外として数えず、削除もしません。
// 日付・indexを解析できない対象ファイルは、実行要否を決める世代数にはkeyごとに含めますが、
// 削除する世代の選択には含めず、エラーとともに保持します。
// 選んだ世代のファイルの削除に失敗しても、代わりに新しい世代を削除しません。
// 同じ世代の削除結果は入力順に記録し、世代の間は日付・indexの古い順です。
// この関数は書き込み中・upload中かを判定しません。削除禁止のファイルは、呼び出し側でfilesから除いてください。
func PurgeGenerationGroupsFromFileNameIfNeeded(
	files []FileInfo,
	maxBackups int,
	generationKey func(FileInfo) (string, bool),
) PurgeResult {
	type generation struct {
		// pathは日付・indexの並べ替えに使う、名前を解析できた最初のファイルです。
		path    string
		indexes []int
	}
	keys := make([]string, len(files))
	targets := make([]bool, len(files))
	distinct := make(map[string]struct{}, len(files))
	for index, file := range files {
		key, ok := generationKey(file)
		if !ok {
			continue
		}
		keys[index], targets[index] = key, true
		distinct[key] = struct{}{}
	}
	if len(distinct) <= maxBackups {
		return purgeNotRun(files)
	}

	result := PurgeResult{Ran: true, Remaining: append([]FileInfo(nil), files...)}
	generationIndexes := make(map[string]int, len(distinct))
	var generations []generation
	for index, file := range files {
		if !targets[index] {
			continue
		}
		if _, _, err := parseFileNameDateAndIndex(file.Path); err != nil {
			recordPurgeFileNameError(&result, file, err)
			continue
		}
		position, exists := generationIndexes[keys[index]]
		if !exists {
			position = len(generations)
			generationIndexes[keys[index]] = position
			generations = append(generations, generation{path: file.Path})
		}
		generations[position].indexes = append(generations[position].indexes, index)
	}
	excess := len(generations) - maxBackups
	if excess <= 0 {
		if len(result.RemovalResults) == 0 {
			result.Ran = false
		}
		return result
	}
	if err := SortFilesByNameDateAndIndexAsc(generations, func(item generation) string { return item.path }); err != nil {
		result.RemovalResults = append(result.RemovalResults, FileRemovalResult{
			Err: fmt.Errorf("sort file generations by name for removal: %w", err),
		})
		return result
	}
	confirmedAbsent := make([]bool, len(files))
	removedPaths := make(map[string]bool, len(files))
	for _, expired := range generations[:excess] {
		for _, index := range expired.indexes {
			file := files[index]
			// 同じpathを重複して渡されても削除は一度だけ行い、結果を共有します。
			if absent, removed := removedPaths[file.Path]; removed {
				confirmedAbsent[index] = absent
				continue
			}
			removedPaths[file.Path] = removeFileAndRecordResult(&result, confirmedAbsent, index, file) == nil
		}
	}
	result.Remaining = collectRemainingFiles(files, confirmedAbsent)
	return result
}

// PurgeByDirectorySizeFromFileNameIfNeededは、全ファイルの合計容量がmaxBytesを超えた
// 場合だけ、ファイル名の日付・数値indexが古い順に削除します。
// deletionExcludedPathsに指定されたファイルは、容量計算には含めますが、
// 削除候補から除外します。これらのファイル名は解析しません。
// それ以外でファイル名を解析できなかったものも、容量には含めますが削除しません。
func PurgeByDirectorySizeFromFileNameIfNeeded(
	files []FileInfo,
	maxBytes uint64,
	deletionExcludedPaths map[string]struct{},
) PurgeResult {
	totalBytes := totalFileBytes(files)
	if totalBytes <= maxBytes {
		return purgeNotRun(files)
	}
	result := PurgeResult{Ran: true, Remaining: append([]FileInfo(nil), files...)}
	confirmedAbsent := make([]bool, len(files))
	indexes := validFileNameIndexes(files, deletionExcludedPaths, &result)
	if !sortValidatedFileIndexesByName(files, indexes, &result) {
		return result
	}
	// 削除禁止・名前を解析できないファイルもtotalBytesには含まれます。
	// 削除エラーはcallbackでRemovalResultsへ記録します。
	PurgeFilesToSizeLimit(indexes, totalBytes, maxBytes, func(index *int) (uint64, error) {
		file := files[*index]
		if err := removeFileAndRecordResult(&result, confirmedAbsent, *index, file); err != nil {
			return 0, err
		}
		return file.Size, nil
	})
	result.Remaining = collectRemainingFiles(files, confirmedAbsent)
	return result
}

// validFileNameIndexesは、除外一覧にない、名前を解析できたファイルの入力位置を返します。
// 元のfilesは変更しません。容量による削除では、除外ファイルも含むfilesで容量を計算し、
// この関数が返す位置だけを削除候補として使います。
func validFileNameIndexes(
	files []FileInfo,
	excludedPaths map[string]struct{},
	result *PurgeResult,
) []int {
	indexes := make([]int, 0, len(files))
	for index, file := range files {
		if pathidentity.Contains(excludedPaths, file.Path) {
			continue
		}
		if _, _, err := parseFileNameDateAndIndex(file.Path); err != nil {
			recordPurgeFileNameError(result, file, err)
			continue
		}
		indexes = append(indexes, index)
	}
	return indexes
}

func sortValidatedFileIndexesByName(files []FileInfo, indexes []int, result *PurgeResult) bool {
	// validFileNameIndexesで検証済みの名前だけを渡します。
	// 並べ替えに失敗した場合は、順序を確定できていない候補を削除しません。
	if err := SortFilesByNameDateAndIndexAsc(indexes, func(index int) string { return files[index].Path }); err != nil {
		result.RemovalResults = append(result.RemovalResults, FileRemovalResult{
			Err: &FileRemovalError{Step: FileRemovalStepSortNames, Err: err},
		})
		return false
	}
	return true
}

func recordPurgeFileNameError(result *PurgeResult, file FileInfo, err error) {
	result.RemovalResults = append(result.RemovalResults, FileRemovalResult{
		File: file, Err: &FileRemovalError{Step: FileRemovalStepInspectName, Path: file.Path, Err: err},
	})
}
