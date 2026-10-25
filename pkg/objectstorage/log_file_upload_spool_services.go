package objectstorage

import (
	"slices"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// CompressedLogFileUploadSpoolServicesは、この送信経路が圧縮ログのspool退避を選択している
// 場合に、送信先サービスを設定順で返します。重複したサービスは一件にまとめます。
func (cfg UploadTargetConfig) CompressedLogFileUploadSpoolServices() []UploadTarget {
	if !cfg.Enabled || !slices.Contains(cfg.CompressedLogFileUploadFailureStore.DurableType, failurestore.DurableTypeSpool) {
		return nil
	}
	var services []UploadTarget
	for _, service := range cfg.Targets {
		if !slices.Contains(services, service) {
			services = append(services, service)
		}
	}
	return services
}

// CompressedLogFileUploadSpoolServicesは、使用するroleがspool退避を選択した
// 送信backendを設定順に返します。同じbackendを複数roleが使う場合も一件にまとめます。
func (cfg LogFileUploadConfig) CompressedLogFileUploadSpoolServices(separateError, separateDebug bool) []UploadTarget {
	if !cfg.Enabled {
		return nil
	}
	targets := []UploadTargetConfig{cfg.Default}
	if separateError {
		targets = append(targets, cfg.Error)
	}
	if separateDebug {
		targets = append(targets, cfg.Debug)
	}
	var services []UploadTarget
	for _, target := range targets {
		for _, service := range target.CompressedLogFileUploadSpoolServices() {
			if !slices.Contains(services, service) {
				services = append(services, service)
			}
		}
	}
	return services
}
