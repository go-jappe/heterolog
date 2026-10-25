//go:build windows

package logpattern

import "strings"

// FileNameComparisonKeyは、ディレクトリを参照しないOSの既定規則で、ファイル名や拡張子の
// 比較に使う文字列を返します。Windowsでは大小文字を区別しないため小文字に揃えます。
// 保存先ディレクトリのファイル名を照合する場合は、FileNamePattern.EffectiveNameCaseや
// pathidentity.DirectoryNameCaseの規則を使ってください。
// 比較専用であり、実際に開くパスやobject storage keyには使いません。
func FileNameComparisonKey(name string) string {
	return strings.ToLower(name)
}
