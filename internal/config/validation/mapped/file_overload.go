package mappedvalidation

import (
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// validateMappedLogFileOverloadConfigは、通常ログと内部ログのfile書込み時の空き容量不足への方針を確認します。
// 未使用のfile出力設定は呼び出し側で除外します。Deliveryの書込みは対象外です。
func validateMappedLogFileOverloadConfig(sourceLabel string, cfg pkgfsstore.FileArchiveOverloadPolicyConfig) error {
	if err := pkgfsstore.ValidateFileOverloadConfig(cfg.OverloadPolicy, cfg.SpillDestination); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionOverload, Err: err}
	}
	return nil
}
