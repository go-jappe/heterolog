package pathidentity

// AppendUniquePathsはpathsとadditionalを順にまとめ、空のパスと重複を除いた新しい一覧を返します。
// paths内の重複も除き、最初に現れたパスの表記と順序を保持します。入力のスライスは変更しません。
// 空文字以外は整理済みの絶対パスを渡してください。この関数では整理・絶対化を行いません。
// 比較にはComparisonKeyと同じfilesystemのcase規則を使用します。
func AppendUniquePaths(paths []string, additional ...string) []string {
	result := make([]string, 0, len(paths)+len(additional))
	seen := make(map[string]struct{}, len(paths)+len(additional))
	appendPath := func(path string) {
		if path == "" {
			return
		}
		key := platformKey(path)
		if _, duplicate := seen[key]; duplicate {
			return
		}
		seen[key] = struct{}{}
		result = append(result, path)
	}
	for _, path := range paths {
		appendPath(path)
	}
	for _, path := range additional {
		appendPath(path)
	}
	return result
}
