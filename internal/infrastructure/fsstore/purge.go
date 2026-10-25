package fsstore

import (
	"errors"
	"fmt"
	"math"
	"os"
	"time"
)

// FileRemovalResultは、一つの観測済みfileに対する削除処理の結果です。
// Errがnilなら削除済み、または既に不存在と確認できています。
// Errには名前・ファイル種別の検査や削除の失敗だけを記録します。
// 削除後の親ディレクトリ同期・Closeのエラーは含めず、呼び出し側で別に扱います。
type FileRemovalResult struct {
	File FileInfo
	Err  error
}

// PurgeResultは、一回のpurge判定と削除処理の結果です。
//
// Ranは条件を満たして削除処理を開始したことを表し、削除成功件数や全処理成功を
// 表しません。Remainingは呼出後も存在する可能性がある入力fileを入力順で保持します。
// RemovalResultsは名前検査に失敗したfileと、実際に削除を試みたfileの結果を処理順で保持します。
// 各RemovalResultのErrがnilなら、fileの不存在を確認できています。
type PurgeResult struct {
	Ran            bool
	Remaining      []FileInfo
	RemovalResults []FileRemovalResult
}

// HasFileRemovalFailuresは、検査または削除に失敗したfileが一件でもあるかを返します。
func HasFileRemovalFailures(results []FileRemovalResult) bool {
	for _, result := range results {
		if result.Err != nil {
			return true
		}
	}
	return false
}

// JoinFileRemovalFailuresは失敗した削除結果のErrへfileの情報を付け、
// 一つのerrorへ集約します。成功結果だけならnilを返します。
func JoinFileRemovalFailures(results []FileRemovalResult) error {
	if len(results) == 0 {
		return nil
	}

	errs := make([]error, 0, len(results))
	for _, result := range results {
		if result.Err == nil {
			continue
		}
		errs = append(errs, fileRemovalFailureError(result))
	}

	return errors.Join(errs...)
}

func fileRemovalFailureError(result FileRemovalResult) error {
	if result.File.Path == "" {
		return result.Err
	}
	return &FileRemovalFailure{File: result.File, Err: result.Err}
}

// ShouldPurgeByAgeTodayは、nowのcalendar dateでage purgeをまだ実行していないかを
// 返します。lastPurgeAtは保持元のtimezoneではなく、nowのtimezoneへ変換して比較します。
func ShouldPurgeByAgeToday(now time.Time, lastPurgeAt time.Time) bool {
	if lastPurgeAt.IsZero() {
		return true
	}

	last := lastPurgeAt.In(now.Location())
	year, month, day := now.Date()
	lastYear, lastMonth, lastDay := last.Date()
	return year != lastYear || month != lastMonth || day != lastDay
}

// ShouldPurgeByDirectorySizeは、観測fileの合計sizeがmaxBytesを超えているかを返します。
// 容量の判定だけを行い、削除や保護対象の除外は行いません。filesには保護中のファイルも含めてください。
// 合計値はuint64をoverflowさせず、表現可能な最大値で飽和させます。
func ShouldPurgeByDirectorySize(
	files []FileInfo,
	maxBytes uint64,
) bool {
	return totalFileBytes(files) > maxBytes
}

// PurgeExpiredFilesは、古い順のcandidatesをexpiredで判定し、期限切れのものをremoveで削除します。
// 最初の期限内候補で終了するため、呼び出し側は期限判定と一致する順に並べ替えてください。
// 判定・削除のエラーは処理順で返し、その候補を残して次へ進みます。
// 候補の取得・保護対象の除外・期限の基準・削除時のロックや同期は呼び出し側が担当します。
// callbackには元のスライスの要素を渡すため、削除済みなどの変更は後続の容量purgeにも使えます。
func PurgeExpiredFiles[T any](
	candidates []T,
	expired func(*T) (bool, error),
	remove func(*T) error,
) []error {
	var errs []error
	for index := range candidates {
		candidate := &candidates[index]
		isExpired, err := expired(candidate)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !isExpired {
			break
		}
		if err := remove(candidate); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// PurgeFilesToSizeLimitは、totalBytesがmaxBytesを超える間、candidatesを渡された順に削除します。
// totalBytesには削除禁止のファイルも含め、candidatesには削除可能な候補だけを渡してください。
// removeは削除済みと扱える場合だけそのサイズとnilを返します。処理済み候補は0とnilで飛ばせます。
// エラーがあれば容量を減らさず次へ進み、削除成功分だけ減算した残容量とエラーを返します。
// 呼び出し側が候補の取得・並べ替え・保護対象の除外・削除時のロックや同期を担当します。
// callbackには元のスライスの要素を渡します。
func PurgeFilesToSizeLimit[T any](
	candidates []T,
	totalBytes, maxBytes uint64,
	remove func(*T) (removedBytes uint64, err error),
) (remainingBytes uint64, errs []error) {
	remainingBytes = totalBytes
	for index := range candidates {
		if remainingBytes <= maxBytes {
			break
		}
		removedBytes, err := remove(&candidates[index])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		remainingBytes = subtractFloorZero(remainingBytes, removedBytes)
	}
	return remainingBytes, errs
}

func removeObservedFile(file FileInfo) FileRemovalResult {
	result := FileRemovalResult{File: file}
	info, err := os.Lstat(file.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result
		}
		result.Err = &FileRemovalError{Step: FileRemovalStepInspect, Path: file.Path, Err: err}
		return result
	}
	// 走査時の候補は通常ファイルだけですが、削除前にも確認します。
	// リンクやディレクトリへ変わっていた場合は削除せず、未削除の結果を返します。
	// 外部操作による、この確認後のパス置換まで排他制御するものではありません。
	if !info.Mode().IsRegular() {
		result.Err = newNotRegularFileError(file.Path, info.Mode(),
			fmt.Sprintf("purge target is not a regular file: %s", file.Path))
		return result
	}

	if err := os.Remove(file.Path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result
		}
		result.Err = &FileRemovalError{Step: FileRemovalStepRemove, Path: file.Path, Err: err}
	}
	return result
}

// removeFileAndRecordResultは一件のfileについて不存在確認または削除を実行し、結果を
// resultへ記録します。呼出後にfileが存在しないことを確認できた場合はnilを返します。
// 失敗したfileはconfirmedAbsentへ記録せず、Remainingに残します。
func removeFileAndRecordResult(
	result *PurgeResult,
	confirmedAbsent []bool,
	index int,
	file FileInfo,
) error {
	removal := removeObservedFile(file)
	result.RemovalResults = append(result.RemovalResults, removal)
	if removal.Err != nil {
		return removal.Err
	}

	confirmedAbsent[index] = true
	return nil
}

func purgeNotRun(files []FileInfo) PurgeResult {
	return PurgeResult{
		Remaining: append([]FileInfo(nil), files...),
	}
}

func collectRemainingFiles(files []FileInfo, confirmedAbsent []bool) []FileInfo {
	remaining := make([]FileInfo, 0, len(files))
	for index, file := range files {
		if confirmedAbsent[index] {
			continue
		}
		remaining = append(remaining, file)
	}
	return remaining
}

func totalFileBytes(files []FileInfo) uint64 {
	var total uint64
	for _, file := range files {
		if math.MaxUint64-total < file.Size {
			return math.MaxUint64
		}
		total += file.Size
	}
	return total
}

func subtractFloorZero(value uint64, subtraction uint64) uint64 {
	if subtraction >= value {
		return 0
	}
	return value - subtraction
}
