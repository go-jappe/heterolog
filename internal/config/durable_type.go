package config

import "github.com/go-jappe/heterolog/pkg/logger/durable"

func containsDurableType(values []durable.DurableType, target durable.DurableType) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// durableTypesSelectNoneは、保存先なしを表すnoneが単独で選択されているかを返します。
// 未知値やnoneとの混在は後段のvalidationへ渡し、direct degrade設定は読みません。
func durableTypesSelectNone(values []durable.DurableType) bool {
	return len(values) == 1 && values[0] == durable.DurableTypeNone
}
