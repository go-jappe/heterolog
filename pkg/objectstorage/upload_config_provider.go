package objectstorage

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// LogFileUploadConfigProviderは、通常・error・debugのログ系列で使う送信設定を提供します。
// 利用側は具体的な設定構造体を受け取らず、各ログ系列のproviderから必要な値を取得します。
type LogFileUploadConfigProvider interface {
	Enabled() bool
	Default() UploadTargetConfigProvider
	Error() UploadTargetConfigProvider
	Debug() UploadTargetConfigProvider
	CompressedLogFileUploadSpool() CompressedLogFileUploadSpoolConfigProvider
	UsesCompressedLogFileUploadSpool(separateError, separateDebug bool) bool
	CompressedLogFileUploadSpoolServices(separateError, separateDebug bool) []UploadTarget
}

// UploadTargetConfigProviderは、一つのログ系列の送信先と、送信失敗時の退避設定を提供します。
type UploadTargetConfigProvider interface {
	Enabled() bool
	Targets() []UploadTarget
	S3DestinationConfigs() []S3ConfigProvider
	CompressedLogFileUploadFailureStore() CompressedLogFileUploadFailureStoreConfigProvider
	CompressedLogFileUploadSpool() CompressedLogFileUploadSpoolConfigProvider
	Has(UploadTarget) bool
	CompressedLogFileUploadSpoolServices() []UploadTarget
}

// S3ConfigProviderは、一つのS3宛先の接続・認証・保存先設定を提供します。
type S3ConfigProvider interface {
	Region() string
	Endpoint() string
	AccessKeyID() string
	SecretAccessKey() string
	SessionToken() string
	Bucket() string
	Prefix() string
	ForcePathStyle() bool
	Timeout() time.Duration
}

// CompressedLogFileUploadFailureStoreConfigProviderは、送信に失敗した圧縮ログの退避方式と保存先を提供します。
type CompressedLogFileUploadFailureStoreConfigProvider interface {
	DurableType() []failurestore.DurableType
	FileArchiveConfig() CompressedLogFileUploadFailureArchiveConfigProvider
}

// CompressedLogFileUploadFailureArchiveConfigProviderは、再送せず保管する圧縮ログの保存先と保持設定を提供します。
type CompressedLogFileUploadFailureArchiveConfigProvider interface {
	RootPath() string
	LoggerContract() string
	LifecycleInterval() time.Duration
	RetryablePurge() CompressedLogFileUploadFailureArchivePurgeConfig
	PermanentPurge() CompressedLogFileUploadFailureArchivePurgeConfig
}
