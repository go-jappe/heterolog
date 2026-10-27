package target

import "strings"

// IsValidTargetは、nameを、対応するすべてのOSでapplicationのstorage namespaceとして
// 使えるかどうかを返します。nameはASCIIの英字か数字で始まり、英字、数字、ドット、
// ハイフン、アンダースコアだけを含みます。末尾のドットとWindowsのdevice名は、
// 移植可能なdirectory名ではありません。
func IsValidTarget(name string) bool {
	if name == "" || name[len(name)-1] == '.' {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		alphanumeric := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alphanumeric && (i == 0 || c != '.' && c != '-' && c != '_') {
			return false
		}
	}
	base, _, _ := strings.Cut(strings.ToUpper(name), ".")
	switch base {
	case "CON", "PRN", "AUX", "NUL":
		return false
	}
	if len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '1' && base[3] <= '9' {
		return false
	}
	return true
}
