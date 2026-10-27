package failurestore

import "strings"

// ParseDurableType normalizes one string to a DurableType. Known values, in any
// letter case and with leading and trailing white space removed, are mapped to
// the canonical constants, and unknown and empty values are kept after only
// trimming and lowercasing them. A later validator decides whether the value
// is supported.
func ParseDurableType(value string) DurableType {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case string(DurableTypeNone):
		return DurableTypeNone
	case strings.ToLower(string(DurableTypeFileArchive)):
		return DurableTypeFileArchive
	case strings.ToLower(string(DurableTypeSpool)):
		return DurableTypeSpool
	default:
		return DurableType(normalized)
	}
}
