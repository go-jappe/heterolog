package config

import (
	"github.com/go-jappe/heterolog/internal/config/failure"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// loadSpillTypesは読み込み済みの直接退避先一覧を変換し、空の一覧ならdefaultを使います。
// 非空の未知値・重複は正規化したSpillTypeとして保持し、受け取り側の
// direct degrade validatorが起動時の設定エラーとして報告します。noneは明示的に
// 直接退避先を選択しない値として保持し、有効化フラグとの整合性をvalidatorへ委ねます。
func loadSpillTypes(
	values []string,
	defaultTypes []pkgdurable.SpillType,
) []pkgdurable.SpillType {
	if len(values) == 0 {
		return defaultTypes
	}
	return failure.ParseSpillTypes(values)
}
