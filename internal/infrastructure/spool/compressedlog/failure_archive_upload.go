package compressedlog

import (
	"fmt"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// FailureArchiveUploadは、送信失敗のfailure archiveの.jsonl系列の送信設定を、圧縮ログの
// spoolのrootを作る設定にします。lifecycleを無効にしたarchiveは送信しないため、無効にします。
func FailureArchiveUpload(cfg durable.FileArchiveConfig) objectstorage.LogFileUploadConfig {
	upload := cfg.ObjectStorageUploadConfig
	return objectstorage.LogFileUploadConfig{
		Enabled:                      upload.Enabled && cfg.LifecycleMode != pkgfsstore.FileLifecycleModeDisable,
		Default:                      upload,
		CompressedLogFileUploadSpool: upload.CompressedLogFileUploadSpool,
	}
}

// UploadConfigsは、送信設定が送信の失敗を圧縮ログのspoolへ保存する場合に、そのspoolの
// rootを作る設定を、object storageのserviceごとに返します。spoolを使わなければnilを返します。
func UploadConfigs(upload objectstorage.LogFileUploadConfig) ([]ConfigProvider, error) {
	if !upload.UsesCompressedLogFileUploadSpool(true, true) {
		return nil, nil
	}
	if upload.CompressedLogFileUploadSpool == nil {
		return nil, fmt.Errorf("%w: spool config provider is nil", ErrInvalidCompressedLogFileUploadSpoolConfig)
	}
	return NewConfigAdapters(upload.CompressedLogFileUploadSpool, upload.CompressedLogFileUploadSpoolServices(true, true)), nil
}
