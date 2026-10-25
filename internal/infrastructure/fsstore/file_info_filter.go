package fsstore

import "github.com/go-jappe/heterolog/pkg/path/pathidentity"

// ExcludeFileInfosByPathsは、excludedPathsに一致するパスのファイル情報を一覧から除きます。
// パスはpathidentity.Containsの規則で照合し、残す要素の情報と入力順を保持します。
// どちらかの一覧が空ならfilesをそのまま返し、それ以外は新しいスライスを返します。
// ファイルの検査・削除や、DirectoryGuardの状態取得は行いません。
// ディレクトリ全体の容量を集計する場合は、除外前の一覧を使ってください。
func ExcludeFileInfosByPaths(files []FileInfo, excludedPaths map[string]struct{}) []FileInfo {
	if len(files) == 0 || len(excludedPaths) == 0 {
		return files
	}
	remaining := make([]FileInfo, 0, len(files))
	for _, file := range files {
		if pathidentity.Contains(excludedPaths, file.Path) {
			continue
		}
		remaining = append(remaining, file)
	}
	return remaining
}
