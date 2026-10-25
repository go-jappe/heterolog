package compressedlogfailure

import "github.com/go-jappe/heterolog/pkg/objectstorage"

// ConfigProviderは、upload失敗時に使用する保存方式と各保存先の設定を取得する契約です。
// 選択されていない保存先の設定は参照しません。
type ConfigProvider = objectstorage.CompressedLogFileUploadFailureStoreConfigProvider

// FileArchiveConfigProviderは、再送せず保管する圧縮ログの保存先と保持設定です。
type FileArchiveConfigProvider = objectstorage.CompressedLogFileUploadFailureArchiveConfigProvider
