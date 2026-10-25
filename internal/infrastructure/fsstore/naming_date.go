package fsstore

import (
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

var fileNameDateAndIndexPattern = regexp.MustCompile(`^.+_([0-9]{8})_([1-9][0-9]*)\.[^./\\]+(?:\.[^./\\]+)?$`)

// ValidateFileNameDateAndIndexは、ソートに使用できる日付・index付きの名前か検証します。
// 保持期限との比較やファイルへのアクセスは行いません。
func ValidateFileNameDateAndIndex(path string) error {
	_, _, err := parseFileNameDateAndIndex(path)
	return err
}

// CompareFileNameDateは、ファイル名の日付がreferenceDateの日付より古ければ-1、
// 同日なら0、新しければ1を返します。時刻・index・ファイルの更新日時は比較しません。
// referenceDateの日付は、そのLocationでの年月日を使います。呼び出し側は
// ファイル名の生成と同じ日付基準で、比較したい期限日などを渡してください。
//
// 例えばreferenceDateが2026年9月20日なら、20260919は-1、20260920は0です。
// 保持期限と同日のファイルを残す判定には、返り値が0未満かどうかを使います。
// SortFilesByNameDateAndIndexAscと同じ命名規則で検証し、解析不能ならエラーを返します。
// ファイルへのアクセスや削除は行いません。
func CompareFileNameDate(path string, referenceDate time.Time) (int, error) {
	date, _, err := parseFileNameDateAndIndex(path)
	if err != nil {
		return 0, err
	}
	year, month, day := referenceDate.Date()
	// UTCへ時刻変換するのではなく、基準地の年月日だけを比較用の値にします。
	referenceDay := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return date.Compare(referenceDay), nil
}

// parseFileNameDateAndIndexは名前の検証・ソート・日付比較に共通する抽出処理です。
// 日付は時刻ではなく年月日を表すため、UTCの0時として返します。
func parseFileNameDateAndIndex(path string) (time.Time, int, error) {
	parts := fileNameDateAndIndexPattern.FindStringSubmatch(filepath.Base(path))
	if parts == nil {
		return time.Time{}, 0, &FileNameDateAndIndexError{Path: path, Part: FileNameDateAndIndexPattern}
	}
	date, err := time.Parse(pkgfsstore.FileDateFormat, parts[1])
	if err != nil {
		return time.Time{}, 0, &FileNameDateAndIndexError{Path: path, Part: FileNameDateAndIndexDate, Err: err}
	}
	index, err := strconv.Atoi(parts[2])
	if err != nil {
		return time.Time{}, 0, &FileNameDateAndIndexError{Path: path, Part: FileNameDateAndIndexIndex, Err: err}
	}
	return date, index, nil
}
