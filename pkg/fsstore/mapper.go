package fsstore

import (
	"github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"time"
)

type OverloadPolicy string   // disk space空き容量　限界時のポリシー
type SpillDestination string // 1次退避先がoverload時の2次退避先

type Config struct {
	Layout    FsstoreConfig
	Lifecycle FileLifecycleConfig
	Overload  FileArchiveOverloadPolicyConfig
	Duration  DurationConfig
	Retry     RetryConfig
	// CompressionCodecはprogrammaticに追加する独自圧縮形式です。nilなら
	// Lifecycle.CompressFormatに対応する組み込みcodecを使用します。
	CompressionCodec CompressionCodec
}

// ArchiveCheckpointConfigは、inline・runner共通のログファイル送信完了記録の
// 保存先と、file role確定前の論理source identityを保持します。
// SourcePrefixにはfactoryがdefault/error/debugまたはretryable/permanentと、
// 実file patternのfingerprintを付与して最終source IDを作ります。
type ArchiveCheckpointConfig struct {
	// Directoryはcheckpoint専用の保存先です。親のリンクは構築時に実体へ解決しますが、
	// このディレクトリ自体がシンボリックリンク・junctionの場合は拒否します。
	Directory    string
	SourcePrefix string
	// AutoRecoverMissingCheckpointは、宛先登録済みのログ系列でcheckpointが欠落した場合に
	// 空のcursorから復旧します。宛先追加前の残存ログも含めて再uploadされ得ます。
	// falseは欠落時に起動を失敗させます。初回作成は許可します。
	// 破損JSON・権限エラー・宛先登録manifestの異常は復旧しません。
	AutoRecoverMissingCheckpoint bool
}

// FileLifecycleModeは、rotation後の圧縮、object storage upload、purgeを実行する主体です。
// 空値の既定動作は利用側のLogger契約で異なります。通常FileLoggerは設定を省略しても
// 使い始められるようinlineとして扱い、Delivery FileLoggerはinline処理を持たないため
// lifecycleを起動しません。動作を明示する場合は以下の定数を指定します。
type FileLifecycleMode string

const (
	// FileLifecycleModeDisableはrotation後の圧縮、object storage upload、purgeを
	// 起動しません。writerによる保存とrotationは継続します。
	FileLifecycleModeDisable FileLifecycleMode = "disable"
	// FileLifecycleModeInlineはwriterの書き込み・rotation経路でlifecycle処理を実行します。
	FileLifecycleModeInline FileLifecycleMode = "inline"
	// FileLifecycleModeRunnerは専用のappendlog lifecycle runnerで周期実行します。
	FileLifecycleModeRunner FileLifecycleMode = "runner"
)

// LogFileLifecycleRunnerConfig は通常 file logger の lifecycle runner 用設定です。
// logger の書き込み設定とは分け、compress/archive/purge に必要な設定だけを保持します。
type LogFileLifecycleRunnerConfig struct {
	FsstoreConfig             Config
	ObjectStorageUploadConfig objectstorage.LogFileUploadConfig
	ArchiveCheckpointConfig   ArchiveCheckpointConfig
	// RunnerConfigは周期loopとoptional Supervisorの実行方針です。
	RunnerConfig AppendLogRunnerConfig
	// HandlerConfigは周期実行の結果を委譲する保存先selectorです。
	// handler selectorはbackend名だけを指定し、internal-log専用保存先の有無は
	// 同じLoggerContractKindのdestination設定が決定します。各levelのselectorは独立して保持し、
	// purge失敗はWarnへ格下げせずErrorとして報告します。
	HandlerConfig handler.HandlerConfig
}

type DeliveryFileLifecycleStoreConfig struct {
	Layout    FsstoreConfig
	Lifecycle DeliveryFileLifecycleConfig
	Duration  DurationConfig
	Retry     RetryConfig
	// CompressionCodecの契約はConfig.CompressionCodecと同じです。
	CompressionCodec CompressionCodec
}

type DeliveryFileWriteConfig struct {
	Layout   FsstoreConfig
	Rotation FileRotationConfig
	Duration DurationConfig
	Retry    RetryConfig
}

// DeliveryFileLifecycleRunnerConfig は delivery log file の lifecycle runner 用設定です。
// logger の書き込み設定とは分け、compress/archive/purge に必要な設定だけを保持します。
type DeliveryFileLifecycleRunnerConfig struct {
	FsstoreConfig             DeliveryFileLifecycleStoreConfig
	ObjectStorageUploadConfig objectstorage.LogFileUploadConfig
	ArchiveCheckpointConfig   ArchiveCheckpointConfig
	// RunnerConfigの意味はLogFileLifecycleRunnerConfigと同じです。
	RunnerConfig AppendLogRunnerConfig
	// HandlerConfigのlevel別契約はLogFileLifecycleRunnerConfigと同じです。
	HandlerConfig handler.HandlerConfig
}

// FsstoreConfigはfile loggerの保存先と命名規則です。error/debugを両方分離する場合、
// defaultの保存先とは別のdirectoryをerror/debugだけで共有する設定は許可しません。
type FsstoreConfig struct {
	Target      string // ログ保存先の名前空間
	LogPath     string // ex.) /var/log/{target}/
	LogDir      string // 通常ログのサブディレクトリ名。使用時は空文字・空白だけを許可しません。
	ErrorLogDir string
	// DebugLogDirはIsSeparateDebugLogDirEnabledがtrueの場合の保存先です。
	// 有効時は空文字を許可しません。無効時はこの値を無視してLogDirを共有します。
	DebugLogDir string
	// IsSeparateDebugLogDirEnabledはdebugの保存directoryを個別指定するかを選択します。
	// debugを別fileへ出力するIsSeparateDebugLogsEnabledとは独立した設定です。
	IsSeparateDebugLogDirEnabled bool
	IsSeparateErrorLogsEnabled   bool
	SeparateErrorLogsPrefix      string
	IsSeparateDebugLogsEnabled   bool
	SeparateDebugLogsPrefix      string
	FileNameFormat               string
	FileDateFormat               string
}

type FileLifecycleConfig struct {
	EnableRotate      bool
	RotationType      FileRotationType
	RotationDayOfWeek time.Weekday
	// UsageCacheTTLは、最後にfilesystemから実測した使用量の概算値を
	// 再観測なしで使用できる時間です。TTL経過後の次のinline cleanupで
	// 再観測しますが、上限超過時はTTL経過を待ちません。
	UsageCacheTTL      time.Duration
	MaxFileBackups     int
	MaxFileAge         int
	MaxFileSize        uint64
	MaxDirSize         uint64
	EnableFileCompress bool
	CompressFormat     CompressFormat
	// ErrorPurgeとDebugPurgeは分離した各log系列の保持設定です。nilなら通常logの
	// MaxFileBackups/MaxFileAge/MaxDirSizeを継承します。指定時は3項目とも必要で、
	// 0を未指定として補完しません。同じdirectoryを通常logと共有する場合に限り、
	// MaxDirSizeは通常logの値を使用します。
	ErrorPurge *FilePurgeConfig
	DebugPurge *FilePurgeConfig
}

// FilePurgeConfigは一つのlog系列の保持上限です。世代数・日数は系列に適用し、
// 容量上限は保存先directory全体に適用します。容量超過時は、世代数・日数の
// 上限に達していないfileも削除され得ます。
type FilePurgeConfig struct {
	MaxFileBackups int    // 保持世代数。未圧縮・圧縮形式を問わず、ファイル名の日付・番号で決まる世代単位で数えます。
	MaxFileAge     int    // 保持日数
	MaxDirSize     uint64 // directory全体のbyte数
}

type FileRotationConfig struct {
	EnableRotate      bool
	RotationType      FileRotationType
	RotationDayOfWeek time.Weekday
	MaxFileSize       uint64
}

type DeliveryFileLifecycleConfig struct {
	EnableRotate       bool
	RotationType       FileRotationType
	RotationDayOfWeek  time.Weekday
	UsageCacheTTL      time.Duration
	MaxFileBackups     int
	MaxFileAge         int
	MaxFileSize        uint64
	MaxDirSize         uint64
	EnableFileCompress bool
	CompressFormat     CompressFormat
	// ErrorPurge/DebugPurgeの継承・directory共有の契約はFileLifecycleConfigと同じです。
	ErrorPurge *FilePurgeConfig
	DebugPurge *FilePurgeConfig
}

// オーバーロード時の処理判定の設定(判定基準はディスクスペースのみ)
type FileArchiveOverloadPolicyConfig struct {
	OverloadPolicy        OverloadPolicy   // drop or degrade
	SpillDestination      SpillDestination // degrade時の退避先。通常file loggerではstdoutだけに対応します。
	MinAvailableDiskSpace uint64           // 空き容量がこの値を下回ると通常のfile書込みを行わない。0なら判定しない。
}

type DurationConfig struct {
	CompressWarnThresholdDuration   time.Duration
	CompressErrorThresholdDuration  time.Duration
	WriteFileWarnThresholdDuration  time.Duration
	WriteFileErrorThresholdDuration time.Duration
	WriteFileLatencyEnabled         bool
}

type RetryConfig struct {
	MaxWriteAttempts           int
	FileOperationRetryWaitTime time.Duration
}
