package fsstore

import (
	"fmt"
	"path/filepath"
	"regexp"
	"time"
)

// 日付をファイル名から抽出して time.Time に変換する関数
func ExtractDateFromFileName(filename string) (time.Time, error) {
	// **ファイル名だけを取得**
	filename = filepath.Base(filename)

	// FileLogger の .log と failure archive の .jsonl の両方を扱えるよう、
	// 末尾の拡張子自体には依存せず `_YYYYMMDD_index.ext` を抽出する。
	re := regexp.MustCompile(`_(19|20)(\d{6})_\d+\.[^./\\]+$`)

	matches := re.FindStringSubmatch(filename)
	if len(matches) == 0 {
		return time.Time{}, fmt.Errorf("ファイル名から有効な日付を取得できませんでした: %s", filename)
	}

	// 日付文字列を取得（matches[1] が先頭2桁、matches[2] が残り6桁）
	dateStr := matches[1] + matches[2] // YYYYMMDD

	parsedTime, err := time.Parse("20060102", dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("日付のパースに失敗: %s", dateStr)
	}

	return parsedTime, nil
}
