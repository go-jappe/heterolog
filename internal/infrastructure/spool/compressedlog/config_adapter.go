package compressedlog

import "github.com/go-jappe/heterolog/pkg/objectstorage"

// ConfigAdapterは共有spool設定のproviderへ送信先のserviceを対応付けます。
// 値の補完や検証は行わず、具体的な設定構造体には依存しません。
type ConfigAdapter struct {
	objectstorage.CompressedLogFileUploadSpoolConfigProvider
	service objectstorage.UploadTarget
}

var _ ConfigProvider = ConfigAdapter{}

func NewConfigAdapter(cfg objectstorage.CompressedLogFileUploadSpoolConfigProvider, service objectstorage.UploadTarget) ConfigAdapter {
	return ConfigAdapter{CompressedLogFileUploadSpoolConfigProvider: cfg, service: service}
}

// NewConfigAdaptersは、構築側で共有spool設定をbackendごとのConfigProviderへ変換します。
// servicesの順序と重複を保ち、設定値の補完・検証は行いません。
func NewConfigAdapters(cfg objectstorage.CompressedLogFileUploadSpoolConfigProvider, services []objectstorage.UploadTarget) []ConfigProvider {
	configs := make([]ConfigProvider, len(services))
	for index, service := range services {
		configs[index] = NewConfigAdapter(cfg, service)
	}
	return configs
}

func (a ConfigAdapter) Service() objectstorage.UploadTarget { return a.service }
