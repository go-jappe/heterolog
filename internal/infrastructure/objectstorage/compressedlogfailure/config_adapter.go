package compressedlogfailure

import "github.com/go-jappe/heterolog/pkg/objectstorage"

// ConfigAdapterは公開設定をConfigProviderへ変換します。
// 設定をコピーして保持し、値の補完や検証は行いません。
type ConfigAdapter = objectstorage.CompressedLogFileUploadFailureStoreConfigAdapter

var _ ConfigProvider = ConfigAdapter{}

func NewConfigAdapter(cfg objectstorage.CompressedLogFileUploadFailureStoreConfig) ConfigAdapter {
	return objectstorage.NewCompressedLogFileUploadFailureStoreConfigAdapter(cfg)
}

// FileArchiveConfigAdapterは、archiveの保存先・周期・保持設定を値として保持します。
type FileArchiveConfigAdapter = objectstorage.CompressedLogFileUploadFailureArchiveConfigAdapter

var _ FileArchiveConfigProvider = FileArchiveConfigAdapter{}

func NewFileArchiveConfigAdapter(cfg objectstorage.CompressedLogFileUploadFailureArchiveConfig) FileArchiveConfigAdapter {
	return objectstorage.NewCompressedLogFileUploadFailureArchiveConfigAdapter(cfg)
}
