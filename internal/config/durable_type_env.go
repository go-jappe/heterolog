package config

import (
	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// loadDurableTypesは読み込み済みの退避先一覧を変換し、空の一覧ならdefaultを使います。
// 非空の未知値・重複は正規化したDurableTypeとして保持し、受け取り側の
// failure store validatorが起動時の設定エラーとして報告します。noneは明示的に
// file archiveとspoolのどちらも選択しない値として保持します。
// 通常/Deliveryの対応可否はここで選別せず、それぞれのvalidatorに委ねます。
// logger failure storeとcompressed log file upload failure storeの双方が同じ
// normalization規則を使用します。
func loadDurableTypes(
	values []string,
	defaultTypes []failurestore.DurableType,
) []failurestore.DurableType {
	if len(values) == 0 {
		return defaultTypes
	}

	types := make([]failurestore.DurableType, 0, len(values))
	for _, value := range values {
		types = append(types, failurestore.ParseDurableType(value))
	}
	return types
}
