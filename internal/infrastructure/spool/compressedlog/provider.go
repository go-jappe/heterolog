package compressedlog

import (
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// ConfigProviderは、圧縮ログspoolの保存先・再送周期・保持設定を取得する契約です。
// RootRuntime構築時に一度だけ内部adapterへ値を固定し、runner・purgeで共有します。
// 各uploaderのroute登録ではこの設定を渡しません。
type ConfigProvider interface {
	objectstorage.CompressedLogFileUploadSpoolConfigProvider
	Service() objectstorage.UploadTarget
}

// rootRuntimeConfigProviderは、共有する周期処理の設定だけを参照する契約です。
// RunnerConfigProviderのIntervalは再送周期で、PurgeIntervalはpurge周期です。
// 保存先の特定は別途行うため、pathやlogger contractは含めません。
type rootRuntimeConfigProvider interface {
	RunnerConfigProvider
	objectstoragespool.PurgeConfigProvider
	CircuitBreakerOpenCooldown() time.Duration
	// MaxRetryAfterは、upload先がRetry-Afterで指定した宛先別の再送待機の上限です。
	MaxRetryAfter() time.Duration
}
