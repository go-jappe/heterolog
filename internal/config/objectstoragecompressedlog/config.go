// Package objectstoragecompressedlogは、圧縮ログファイルを
// S3などのobject storageへアップロードできなかった場合の保存設定を定義します。
//
// 対象はsync・async・Deliveryの通常ログの.log.gzや.log.zstと、backend送信失敗
// レコードを保存したarchiveの.jsonl.gzや.jsonl.zstなどです。
// 再送せず保管するfileArchiveと、再送用に保存するspoolを単独または両方選択でき、
// 保存場所・保持期間・再送周期などを設定します。
package objectstoragecompressedlog

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

const bytesPerGigabyte uint64 = 1024 * 1024 * 1024

const (
	DefaultRunnerInterval = 5 * time.Second
	// DefaultMaxRetryAfterは、upload先がRetry-Afterで指定した再送待機に適用する上限の既定値です。
	DefaultMaxRetryAfter              = time.Hour
	DefaultLifecycleInterval          = 60 * time.Second
	DefaultCircuitBreakerOpenCooldown = 5 * time.Minute
	// DefaultEnableDonePurgeは、再送でuploadできたspoolのdoneを保持期間・容量で削除するかの
	// 既定値です。doneはupload済みのため、既定で削除します。
	DefaultEnableDonePurge = true
	// DefaultEnableRetainedFailurePurgeは、uploadできずに保持しているfailure data
	// （archiveのretryable・permanent、spoolのdead・quarantine）を保持期間・容量で
	// 削除するかの既定値です。確認前に暗黙に削除しないよう、公開APIの
	// DefaultSpoolConfigと同じく既定では削除せず、明示した場合だけ有効にします。
	DefaultEnableRetainedFailurePurge            = false
	DefaultCompressedLogFileUploadFailureDirName = "compressed_log_file_upload_failures"

	DefaultStandardDoneMaxFileAgeDays       = 30
	DefaultStandardDoneMaxDirSizeGB         = 20
	DefaultStandardDeadMaxFileAgeDays       = 365
	DefaultStandardDeadMaxDirSizeGB         = 100
	DefaultStandardQuarantineMaxFileAgeDays = 365
	DefaultStandardQuarantineMaxDirSizeGB   = 100

	DefaultDeliveryDoneMaxFileAgeDays       = 90
	DefaultDeliveryDoneMaxDirSizeGB         = 100
	DefaultDeliveryDeadMaxFileAgeDays       = 730
	DefaultDeliveryDeadMaxDirSizeGB         = 500
	DefaultDeliveryQuarantineMaxFileAgeDays = 730
	DefaultDeliveryQuarantineMaxDirSizeGB   = 500
)

// Configはsync/async loggerで共用する標準failure storeと、より長く保持する
// Delivery failure storeの設定を分離して保持します。
type Config struct {
	Standard FailureStoreConfig
	Delivery FailureStoreConfig
}

// FailureStoreConfigは、失敗compressed log fileを保存するdurable typeと各保存先設定です。
// DurableTypeはfileArchiveとspoolを独立または同時に選択できます。
type FailureStoreConfig struct {
	DurableType        []failurestore.DurableType
	DurableStoreConfig DurableStoreConfig
}

type DurableStoreConfig struct {
	FileArchiveConfig FileArchiveConfig
	SpoolConfig       SpoolConfig
}

// FileArchiveConfigは再送処理を持たず、compressed log fileをbyte-for-byteで保存する
// failure archiveの配置と保持方針です。
type FileArchiveConfig struct {
	DirName           string
	LifecycleInterval time.Duration
	Retryable         PurgeConfig
	Permanent         PurgeConfig
}

// SpoolConfigは一つのcompressed log file spool runnerと、その送信抑制・terminal保持方針です。
type SpoolConfig struct {
	Runner         RunnerConfig
	CircuitBreaker CircuitBreakerConfig
	Lifecycle      LifecycleConfig
}

type RunnerConfig struct {
	Interval time.Duration
	// MaxRetryAfterは、upload先がRetry-Afterで指定した宛先別の再送待機の上限です。
	// より長い指定はこの時間に短縮し、警告を通知します。0はこのfieldを持たない既存設定との
	// 互換のためDefaultMaxRetryAfterとして扱い、無制限にはしません。負値は設定errorです。
	MaxRetryAfter time.Duration
}

// CircuitBreakerConfigはcompressed log fileの再送先が継続的に失敗した場合の
// open期間を保持します。発火条件は通常のlog backendと同じ共通仕様を使用します。
type CircuitBreakerConfig struct {
	OpenCooldown time.Duration
}

// LifecycleConfigは送信処理を終えたterminal stateだけのpurge設定です。
// 未送信compressed log fileを保持するactiveと、処理中のprocessingにはpurge設定を設けません。
type LifecycleConfig struct {
	// Intervalはterminal directoryのpurge判定間隔です。compressed log file再送の
	// Runner.Intervalとは独立しています。
	Interval   time.Duration
	Done       PurgeConfig
	Dead       PurgeConfig
	Quarantine PurgeConfig
}

type PurgeConfig struct {
	Enabled bool
	// MaxFileAgeDaysはcompressed log fileを保持する最大日数です。
	MaxFileAgeDays int
	// MaxDirSizeはstate directory全体の最大byte数です。
	MaxDirSize uint64
}

// DefaultStandardCompressedLogFileUploadFailureStoreConfigはsync/async loggerが共用するfailure storeの
// 既定値を返します。圧縮logは元recordより復元が難しいため、再送用spoolと単純保存用
// file archiveの両方へ引き渡します。
func DefaultStandardCompressedLogFileUploadFailureStoreConfig() FailureStoreConfig {
	return defaultCompressedLogFileUploadFailureStoreConfig(
		DefaultStandardDoneMaxFileAgeDays,
		DefaultStandardDoneMaxDirSizeGB,
		DefaultStandardDeadMaxFileAgeDays,
		DefaultStandardDeadMaxDirSizeGB,
		DefaultStandardQuarantineMaxFileAgeDays,
		DefaultStandardQuarantineMaxDirSizeGB,
	)
}

// DefaultDeliveryCompressedLogFileUploadFailureStoreConfigはDelivery logger用failure storeの既定値を
// 返します。各保存先の保持期間・容量は標準契約より大きくします。
func DefaultDeliveryCompressedLogFileUploadFailureStoreConfig() FailureStoreConfig {
	return defaultCompressedLogFileUploadFailureStoreConfig(
		DefaultDeliveryDoneMaxFileAgeDays,
		DefaultDeliveryDoneMaxDirSizeGB,
		DefaultDeliveryDeadMaxFileAgeDays,
		DefaultDeliveryDeadMaxDirSizeGB,
		DefaultDeliveryQuarantineMaxFileAgeDays,
		DefaultDeliveryQuarantineMaxDirSizeGB,
	)
}

func defaultCompressedLogFileUploadFailureStoreConfig(
	doneMaxFileAgeDays int,
	doneMaxDirSizeGB uint64,
	deadMaxFileAgeDays int,
	deadMaxDirSizeGB uint64,
	quarantineMaxFileAgeDays int,
	quarantineMaxDirSizeGB uint64,
) FailureStoreConfig {
	longTermRetryable := PurgeConfig{
		Enabled:        DefaultEnableRetainedFailurePurge,
		MaxFileAgeDays: deadMaxFileAgeDays,
		MaxDirSize:     deadMaxDirSizeGB * bytesPerGigabyte,
	}
	longTermPermanent := PurgeConfig{
		Enabled:        DefaultEnableRetainedFailurePurge,
		MaxFileAgeDays: quarantineMaxFileAgeDays,
		MaxDirSize:     quarantineMaxDirSizeGB * bytesPerGigabyte,
	}
	return FailureStoreConfig{
		DurableType: []failurestore.DurableType{
			failurestore.DurableTypeFileArchive,
			failurestore.DurableTypeSpool,
		},
		DurableStoreConfig: DurableStoreConfig{
			FileArchiveConfig: FileArchiveConfig{
				DirName:           DefaultCompressedLogFileUploadFailureDirName,
				LifecycleInterval: DefaultLifecycleInterval,
				Retryable:         longTermRetryable,
				Permanent:         longTermPermanent,
			},
			SpoolConfig: SpoolConfig{
				Runner: RunnerConfig{
					Interval:      DefaultRunnerInterval,
					MaxRetryAfter: DefaultMaxRetryAfter,
				},
				CircuitBreaker: CircuitBreakerConfig{
					OpenCooldown: DefaultCircuitBreakerOpenCooldown,
				},
				Lifecycle: LifecycleConfig{
					Interval: DefaultLifecycleInterval,
					Done: PurgeConfig{
						Enabled:        DefaultEnableDonePurge,
						MaxFileAgeDays: doneMaxFileAgeDays,
						MaxDirSize:     doneMaxDirSizeGB * bytesPerGigabyte,
					},
					Dead:       longTermRetryable,
					Quarantine: longTermPermanent,
				},
			},
		},
	}
}
