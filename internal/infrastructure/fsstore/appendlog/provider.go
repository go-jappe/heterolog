package appendlog

import (
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"time"
)

type ConfigProvider interface {
	FileStoreConfigProvider
	Purge() FilePurgeConfigProvider
	// ErrorPurge/DebugPurgeは、通常logからの継承とdirectory共有を解決済みの設定を返します。
	// 個別設定が未指定でもnilではなく通常logの保持設定を返し、利用側では継承を判断しません。
	ErrorPurge() FilePurgeConfigProvider
	DebugPurge() FilePurgeConfigProvider
}

type FileStoreConfigProvider interface {
	FileWriteStoreConfigProvider
	Lifecycle() FileLifecycleConfigProvider
}

// CompressionCodecProviderは、組み込み以外の圧縮形式をprogrammaticに注入する
// optional契約です。既存のConfigProvider実装へ不要なmethodを強制しないため、
// FileStoreがtype assertionで参照します。
type CompressionCodecProvider interface {
	CompressionCodec() fsstore.CompressionCodec
}

type FileWriteStoreConfigProvider interface {
	Layout() FsstoreLayoutConfigProvider
	Rotation() FileRotationConfigProvider
	Duration() DurationConfigProvider
	Retry() RetryConfigProvider
}

type FsstoreLayoutConfigProvider interface {
	Target() string      // ログ保存先の名前空間
	LogPath() string     // ex.) /var/log/{target}/  {/var/log部分}
	LogDir() string      // ex.) /var/log/{target}/{LogDir}
	ErrorLogDir() string // ex.) /var/log/{target}/{ErrorLogDir}
	DebugLogDir() string // 設定の共有・分離flagから構築時に確定したdebug保存先です。
	IsSeparateDebugLogDirEnabled() bool
	IsSeparateErrorLogsEnabled() bool
	SeparateErrorLogsPrefix() string
	IsSeparateDebugLogsEnabled() bool
	SeparateDebugLogsPrefix() string
	FileNameFormat() string
	FileDateFormat() string
}

type FileLifecycleConfigProvider interface {
	FileRotationConfigProvider
	EnableFileCompress() bool
	CompressFormat() fsstore.CompressFormat
}

type FileRotationConfigProvider interface {
	EnableRotate() bool
	FileRotationType() fsstore.FileRotationType
	FileRotationDayOfWeek() time.Weekday
	MaxFileSize() uint64
}

type FilePurgeConfigProvider interface {
	UsageCacheTTL() time.Duration
	MaxFileBackups() int
	MaxFileAge() int
	MaxDirSize() uint64
}

type DurationConfigProvider interface {
	CompressWarnThresholdDuration() time.Duration
	CompressErrorThresholdDuration() time.Duration
	WriteFileWarnThresholdDuration() time.Duration
	WriteFileErrorThresholdDuration() time.Duration
	WriteFileLatencyEnabled() bool
}

type RetryConfigProvider interface {
	MaxWriteRetries() int
	FileOperationRetryWaitTime() time.Duration
}
