package lifecycle

import (
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// resolveLifecycleCompressedPathsは、元ログごとに対応する圧縮パスを決め、
// 元ログの正規化パスをキー、圧縮パスを値とする対応表を返します。
// 元ログとその圧縮ファイルは同じキーへまとめます。元ログ自体の存在は前提にしません。
//
// - 既存の圧縮ファイルがある：設定変更前の形式も含め、そのパスを使います。
// - 既存ファイルがなく、圧縮が有効：作成予定の圧縮パスを生成します。
// - 既存ファイルがなく、圧縮も無効：空文字を設定します。
// 失敗時の再試行登録に使います。
//
// filesには、対象ログ系列の命名規則に一致する未圧縮・圧縮済みのパス一覧を渡します。
// 同じ元ログに対応する（圧縮ファイル等）複数のパスを含む場合があります。
// sourceGroupsには、その一覧を選んだ走査結果を元ログごとにまとめて渡します。
// 元ログごとの分類はinlineと共通のCompressionSourceGroupsを参照し、
// 既存の圧縮ファイルは、その元ログのRegularFilesから観測順に選びます。
// 検査エラーのある元ログは、呼び出し元が圧縮・再利用を中止します。
// 圧縮ファイルの内容検証は、後続処理でパスを保護してから行います。
// この関数は追加のディレクトリ走査・ファイル作成・圧縮を行いません。
func (m *FileSeriesManager) resolveLifecycleCompressedPaths(
	files []string,
	sourceGroups fsstore.CompressionSourceGroups,
) (map[string]string, error) {
	fileName, err := m.fileNamePattern()
	if err != nil {
		return nil, err
	}
	compressedPathsBySource := make(map[string]string)
	for _, path := range files {
		source, valid := fsstore.CompressionSourcePath(path, fileName.Extension)
		if !valid {
			return nil, fmt.Errorf("resolve appendlog archive source for %q", path)
		}
		key := pathidentity.ComparisonKey(source)
		if _, resolved := compressedPathsBySource[key]; resolved {
			continue
		}
		sourceGroup, _ := sourceGroups.Source(source)
		observedPaths := make([]string, len(sourceGroup.RegularFiles))
		for index, file := range sourceGroup.RegularFiles {
			observedPaths[index] = file.Path
		}
		var compressedPath string
		if m.Config.Lifecycle().EnableFileCompress() {
			compressedPath, _, err = fsstore.ResolveCompressedFilePath(source, observedPaths, m.store.GetCompressFormat())
			if err != nil {
				return nil, err
			}
		} else {
			compressedPath, _ = fsstore.FindExistingCompressedFilePath(source, observedPaths)
		}
		compressedPathsBySource[key] = compressedPath
	}
	return compressedPathsBySource, nil
}
