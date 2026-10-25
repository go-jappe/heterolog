package target

import "strings"

// IsValidTarget reports whether name can be used as an application's storage
// namespace on all supported operating systems. Names begin with an ASCII
// letter or digit and contain only letters, digits, dots, hyphens, or underscores.
// Trailing dots and Windows device names are not portable directory names.
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
