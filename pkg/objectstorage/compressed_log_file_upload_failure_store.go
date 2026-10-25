package objectstorage

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// CompressedLogFileUploadFailureStoreConfigは、通常ログの.log.<codec>と、backend送信失敗
// レコードを保存したarchiveの.jsonl.<codec>について、
// object storageへの直接uploadを完了できなかった場合の保存方式を保持します。
type CompressedLogFileUploadFailureStoreConfig struct {
	DurableType        []failurestore.DurableType
	DurableStoreConfig CompressedLogFileUploadFailureDurableStoreConfig
}

// CompressedLogFileUploadFailureDurableStoreConfigは、routeごとのfile archive設定です。
// spoolの設定はCompressedLogFileUploadSpoolから取得し、保存先が同じ送信経路でruntimeを共有します。
type CompressedLogFileUploadFailureDurableStoreConfig struct {
	FileArchiveConfig CompressedLogFileUploadFailureArchiveConfig
}

// CompressedLogFileUploadFailureArchiveConfigはcompressed log fileのbyte列と元file名を変更せず、
// 分類directory直下の一fileとして保存するfile archive設定です。
// RootPathは対象applicationのcompressed log file upload failure archiveまで解決済みのrootです。
// LoggerContractはlogger、async_logger、delivery_loggerの所有領域を分離します。
// object storage serviceと宛先の領域は、送信先のmetadataから構築します。
type CompressedLogFileUploadFailureArchiveConfig struct {
	RootPath          string
	LoggerContract    string
	LifecycleInterval time.Duration
	Retryable         CompressedLogFileUploadFailureArchivePurgeConfig
	Permanent         CompressedLogFileUploadFailureArchivePurgeConfig
}

// CompressedLogFileUploadFailureArchivePurgeConfigは一つの失敗分類に対する保持設定です。
// MaxFileAgeDaysはファイル名の日付で判定し、期限日と同日のファイルは保持します。
// MaxDirSizeの単位はbyteで、超過時はファイル名の日付・数値indexの古い順に削除します。
type CompressedLogFileUploadFailureArchivePurgeConfig struct {
	Enabled        bool
	MaxFileAgeDays int
	MaxDirSize     uint64
}

// CompressedLogFileUploadSpoolConfigは、圧縮ログの再送用spoolについて構築側で指定する設定値です。
// 保存先、再送周期、送信先circuit breaker、終了済みstateの保持方針を持ちます。
// 利用側へはNewCompressedLogFileUploadSpoolConfigAdapterでproviderへ変換して渡します。
// object storage serviceごとのrootは送信先のmetadataと組み合わせて決定します。
type CompressedLogFileUploadSpoolConfig struct {
	SpoolPath      string
	Target         string
	LoggerContract string

	RunnerInterval             time.Duration
	LifecycleInterval          time.Duration
	CircuitBreakerOpenCooldown time.Duration
	// MaxRetryAfterは、upload先がRetry-Afterで指定した宛先別の再送待機の上限です。
	// 0は既定の1時間として扱い、無制限にはしません。負値は設定errorです。
	MaxRetryAfter time.Duration

	Done       CompressedLogFileUploadSpoolPurgeConfig
	Dead       CompressedLogFileUploadSpoolPurgeConfig
	Quarantine CompressedLogFileUploadSpoolPurgeConfig
}

// CompressedLogFileUploadSpoolPurgeConfigは送信処理がterminalになった圧縮log fileだけに適用します。
// activeとprocessingは未送信dataを含むため、削除設定を公開しません。
type CompressedLogFileUploadSpoolPurgeConfig struct {
	Enabled        bool
	MaxFileAgeDays int
	MaxDirSize     uint64
}
