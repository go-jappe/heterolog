package failurestore

import "strings"

// ParseDurableTypeは一つの文字列をDurableTypeへ正規化します。
// 既知値の表記揺れはcanonicalな定数へ揃え、未知値と空値はtrimと
// 小文字化だけを行って保持します。対応可否は後段のvalidatorが判断します。
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
