package fsstore

import (
	"cmp"
	"slices"
	"time"
)

// SortFilesByNameDateAndIndexAscは、filesをファイル名の日付、indexの数値の昇順に並べ替えます。
// 名前は「接頭辞_YYYYMMDD_index.拡張子」で、末尾に一段の圧縮拡張子を付けられます。
// 日付とindexが同じ場合はパスの文字列順、パスも同じ場合は元の順序を維持します。
// indexはwriterと同じく、先頭に0のない正のintに限ります。
//
// pathOfは各要素からパスを取得します。パスの解析は要素ごとに一度だけ行い、
// 解析できない名前があれば、filesを並べ替えずにそのパスを含むエラーを返します。
// 更新日時は使いません。ファイルの存在・種別や処理対象の拡張子の確認は呼び出し側が行います。
func SortFilesByNameDateAndIndexAsc[T any](files []T, pathOf func(T) string) error {
	type namedFile struct {
		file  T
		path  string
		date  time.Time
		index int
	}
	ordered := make([]namedFile, len(files))
	for i, file := range files {
		path := pathOf(file)
		date, index, err := parseFileNameDateAndIndex(path)
		if err != nil {
			return err
		}
		ordered[i] = namedFile{file: file, path: path, date: date, index: index}
	}

	slices.SortStableFunc(ordered, func(left, right namedFile) int {
		if order := left.date.Compare(right.date); order != 0 {
			return order
		}
		if order := cmp.Compare(left.index, right.index); order != 0 {
			return order
		}
		return cmp.Compare(left.path, right.path)
	})
	for i, file := range ordered {
		files[i] = file.file
	}
	return nil
}
