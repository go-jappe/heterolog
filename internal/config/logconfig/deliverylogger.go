package logconfig

import (
	"time"

	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger"
)

const (
	DefaultDeliverySlackLogLevel = logger.LogLevelError

	// Delivery internal-log fallbackはDelivery本体の同期・一件送信契約から独立して
	// enqueue/batch送信するため、通常loggerとは別のdefaultを所有します。
	DefaultDeliveryInternalLogOpenSearchFlushInterval              = 5 * time.Second
	DefaultDeliveryInternalLogOpenSearchMaxBulkDocument            = 500
	DefaultDeliveryInternalLogOpenSearchBufferFlushBytes           = DefaultOpenSearchBufferFlushBytes
	DefaultDeliveryInternalLogOpenSearchMaxBulkRequestBytes        = DefaultOpenSearchMaxBulkRequestBytes
	DefaultDeliveryInternalLogLokiFlushInterval                    = 5 * time.Second
	DefaultDeliveryInternalLogLokiMaxEntries                       = 500
	DefaultDeliveryInternalLogLokiBufferFlushBytes                 = DefaultLokiBufferFlushBytes
	DefaultDeliveryInternalLogLokiFallbackQueueMaxEntries          = DefaultLokiFallbackQueueMaxEntries
	DefaultDeliveryInternalLogCloudWatchLogsFlushInterval          = 5 * time.Second
	DefaultDeliveryInternalLogCloudWatchLogsBufferFlushMaxEvents   = DefaultCloudWatchLogsBufferFlushMaxEvents
	DefaultDeliveryInternalLogCloudWatchLogsBufferFlushBytes       = DefaultCloudWatchLogsBufferFlushBytes
	DefaultDeliveryInternalLogCloudWatchLogsFallbackQueueMaxEvents = DefaultCloudWatchLogsFallbackQueueMaxEvents
	DefaultDeliveryInternalLogSlackMaxLogsPerNotification          = 10
	DefaultDeliveryInternalLogSlackMaxRestThreshold                = 360 * time.Minute

	// この項目はログファイル設定 送達確認ログファイルの保存先がfileの場合に使用します
	DefaultMaxDeliveryLogFileSizeMB    = 25                                           // MB単位のデフォルトログサイズ .envで指定が無い場合に当値を適用する
	DefaultMaxDeliveryLogFileSize      = DefaultMaxDeliveryLogFileSizeMB * BytesPerMB // バイト単位のデフォルトログサイズ
	DefaultMaxDeliveryLogFileBackups   = 60
	DefaultMaxDeliveryLogFileAge       = 60
	DefaultMaxDeliveryLogFileDirSizeMB = 2048
	DefaultMaxDeliveryLogFileDirSize   = DefaultMaxDeliveryLogFileDirSizeMB * BytesPerMB

	// 非同期ロガーの保存先がfileの場合　非同期ログの保存先ディレクトリ
	DeliveryLogDirCaseFile      = "/delivery_logs"
	DeliveryLogDirCaseErrorFile = "/delivery_logs_error"

	// error logの出力ファイル名を分る設定
	DeliveryDefaultIsSeparateErrorLogsEnabled = true
	DeliverySeparateErrorLogsPrefix           = "error_" // エラーログファイルに付与するファイル名の接頭辞 .envでの設定不可
	// debug logの出力ファイル名を分る設定
	DeliveryDefaultIsSeparateDebugLogsEnabled = true
	DeliverySeparateDebugLogsPrefix           = "debug_" // デバッグログファイルに付与するファイル名の接頭辞 .envでの設定不可
	// ログのローテーション設定
	DefaultEnableDeliveryLogFileRotate   = true
	DeliveryLogRotationDayOfWeek         = time.Sunday // LogRotationWeeklyの設定時に使用。time.Weekday型で任意の曜日を指定
	DefaultEnableDeliveryLogFileCompress = true
	DefaultDeliveryLogCompressFormat     = fsstore.CompressGzip
	DefaultDeliveryLogFileLifecycleMode  = fsstore.FileLifecycleModeRunner

	DefaultWriteDeliveryFileLoggerLatencyEnabled = true // FileLoggerのファイル操作周りのレイテンシーを記録するか

	// DefaultDeliveryLoggerTimoutは、HETEROLOG_DELIVERY_LOG_TIMEOUT_SECONDを省略した場合のDelivery loggerの
	// 呼び出し上限です。既定のtimeout budget policy（must_fit）では、選択したbackend clientの
	// 再試行予算より長い必要があります。既定のclient設定ではLoki・CloudWatch Logsの予算8秒が
	// 最大（Delivery OpenSearchは1件送信の5秒、Slackは2秒）のため、公開設定例（.env.example）と
	// 同じ12秒にします。
	DefaultDeliveryLoggerTimout = 12 * time.Second
)

// DefaultDeliveryInternalLogSlackIntervalsは初回の即時通知後にDelivery internal-log
// Slack fallbackが順に適用する待機時間です。
var DefaultDeliveryInternalLogSlackIntervals = []time.Duration{
	time.Minute,
	5 * time.Minute,
	30 * time.Minute,
	time.Hour,
	2 * time.Hour,
	4 * time.Hour,
}

// DeliveryInternalLogBatchConfigはDelivery loggerのinternal-log fallbackだけが使う
// enqueue/batch設定です。Delivery本体およびsync/async loggerの送信設定には影響しません。
type DeliveryInternalLogBatchConfig struct {
	OpenSearch     DeliveryInternalLogOpenSearchBatchConfig
	Loki           DeliveryInternalLogLokiBatchConfig
	CloudWatchLogs DeliveryInternalLogCloudWatchLogsBatchConfig
	Slack          DeliveryInternalLogSlackBatchConfig
}

type DeliveryInternalLogOpenSearchBatchConfig struct {
	FlushInterval       time.Duration
	BufferFlushBytes    int
	MaxBulkRequestBytes int
	// MaxBulkDocumentは一回のbatch件数とenqueue queueの上限を兼ねます。
	MaxBulkDocument int
}

type DeliveryInternalLogLokiBatchConfig struct {
	FlushInterval           time.Duration
	MaxEntries              int
	BufferFlushBytes        int
	FallbackQueueMaxEntries int
}

type DeliveryInternalLogCloudWatchLogsBatchConfig struct {
	FlushInterval          time.Duration
	BufferFlushMaxEvents   int
	BufferFlushBytes       int
	FallbackQueueMaxEvents int
}

type DeliveryInternalLogSlackBatchConfig struct {
	Intervals []time.Duration
	// MaxLogsPerNotificationは一回の通知件数とenqueue queueの上限を兼ねます。
	MaxLogsPerNotification int
	MaxRestThreshold       time.Duration
}

type DeliveryLoggerConfig struct {
	LogOutputDestinations logger.LogOutputDestinations
	// HandlerTargetDestinations は、通常出力先ではないものの、有効なDelivery logger
	// またはfailure archiveのinternal log委譲先として設定が必要な出力先です。
	HandlerTargetDestinations    logger.LogOutputDestinations
	InternalLogDestinationConfig InternalLogDestinationConfig
	InternalLogBatchConfig       DeliveryInternalLogBatchConfig
	Timeout                      time.Duration
	TimeoutBudgetPolicy          timeoutbudget.TimeoutBudgetPolicy
	ClockConfig
	DeliveryStdoutConfig
	DeliveryFileConfig
	DeliverySlackConfig
	DeliveryOpenSearchConfig
	DeliveryLokiConfig
	DeliveryCloudWatchLogsConfig
	DeliveryClickHouseConfig
}
