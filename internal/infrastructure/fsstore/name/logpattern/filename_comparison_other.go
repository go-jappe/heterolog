//go:build !windows

package logpattern

// FileNameComparisonKeyは、ディレクトリを参照しないOSの既定規則で、ファイル名や拡張子の
// 比較に使う文字列を返します。Windows以外では大小文字を区別するため、入力をそのまま返します。
// 保存先ディレクトリのファイル名を照合する場合は、FileNamePattern.EffectiveNameCaseや
// pathidentity.DirectoryNameCaseの規則を使ってください。
// 比較専用であり、実際に開くパスやobject storage keyには使いません。
func FileNameComparisonKey(name string) string {
	return name
}
