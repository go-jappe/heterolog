package logconfig

import (
	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger"
	"time"
)

// 非同期ベストエフォート型ログ固有設定
const (
	DefaultAsyncFileLogLevel = logger.LogLevelInfo
	// この項目はログファイル設定 非同期ログファイルの保存先がfileの場合に使用します
	DefaultMaxAsyncLogFileSizeMB = 25                                   // MB単位のデフォルトログサイズ .envで指定が無い場合に当値を適用する
	DefaultMaxAsycLogFileSize    = DefaultMaxLogFileSizeMB * BytesPerMB // バイト単位のデフォルトログサイズ

	// 非同期ロガーの保存先がfileの場合　非同期ログの保存先ディレクトリ
	AsyncLogDirCaseFile      = "/async_logs"
	AsyncLogDirCaseErrorFile = "/async_logs_error"

	// error logの出力ファイル名を分る設定
	AsyncDefaultIsSeparateErrorLogsEnabled = true
	AsyncSeparateErrorLogsPrefix           = "error_" // エラーログファイルに付与するファイル名の接頭辞 .envでの設定不可
	// debug logの出力ファイル名を分る設定
	AsyncDefaultIsSeparateDebugLogsEnabled = true
	AsyncSeparateDebugLogsPrefix           = "debug_" // デバッグログファイルに付与するファイル名の接頭辞 .envでの設定不可
	// ログのローテーション設定
	DefaultEnableAsyncLogFileRotate = true
	AsyncLogRotationDayOfWeek       = time.Sunday // LogRotationWeeklyの設定時に使用。time.Weekday型で任意の曜日を指定

	// ログ保持最大世代数
	DefaultMaxAsyncLogFileBackups = 60
	// ログファイル最大保持日数
	DefaultMaxAsyncLogFileAge = 60
	// ログディレクトリ 合計ファイル最大サイズ(MB) 閾値を超えると古い順に削除
	DefaultMaxAsyncLogFileDirSizeMB = 2048
	DefaultMaxAsyncLogFileDirSize   = DefaultMaxLogFileDirSizeMB * BytesPerMB
	// ログローテート時に古いファイルを圧縮
	DefaultEnableAsyncLogFileCompress = true
	// env無指定時のデフォルトのファイル圧縮形式
	DefaultAsyncLogCompressFormat             = fsstore.CompressGzip
	DefaultAsyncLogFileLifecycleMode          = fsstore.FileLifecycleModeInline
	DefaultWriteAsyncFileLoggerLatencyEnabled = true // FileLoggerのファイル操作周りのレイテンシーを記録するか

	// ログサンプリング　ファイル
	DefaultIsAsyncLogFileSamplingEnabled     = false
	DefaultEnableAsyncFileLoggingByRequestID = true
	DefaultAsyncFileLoggingByRequestIDRate   = 10
	// request idでサンプリングしない場合はすべてレベル別のレーティングでサンプリングを行う
	DefaultAsyncLogFileSamplingDebugRate = 1 // 1/(指定値)の割合　100なら100分の1 1000なら1000分の1を抽出
	DefaultAsyncLogFileSamplingInfoRate  = 1000
	DefaultAsyncLogFileSamplingWarnRate  = 1
	DefaultAsyncLogFileSamplingErrorRate = 1

	// AsyncBestEffortMultiLoggerTimoutは、HETEROLOG_ASYNC_LOG_TIMEOUT_SECONDを省略した場合の非同期loggerの
	// 呼び出し上限です。ベストエフォート型のため長くしすぎないことが望ましい一方、既定の
	// timeout budget policy（must_fit）では選択したbackend clientの再試行予算より長い必要が
	// あります。既定のclient設定ではOpenSearchのbulk送信の予算17秒が最大のため、
	// 公開設定例（.env.example）と同じ18秒にします。
	AsyncBestEffortMultiLoggerTimout      = 18 * time.Second
	AsyncBestEffortMultiLoggerMaxParallel = 64
)

// HETEROLOG_ASYNC_LOG_TYPEが未設定・空の場合の出力先は、通常loggerと共通のLogDefaultOutputsです。
// 非同期loggerだけSlackなどを暗黙に追加すると認証情報なしで起動できなくなるため、
// 専用の既定値は持ちません。

// デフォルトの非同期ログファイルのローテーション頻度
var DefaultAsyncLogFileRotation = fsstore.FileRotationDaily

type AsyncBestEffortMultiLoggerConfig struct {
	LogOutputDestinations logger.LogOutputDestinations
	// HandlerTargetDestinations は、通常出力先ではないものの、有効なasync logger
	// またはfailure storeのinternal log委譲先として設定が必要な出力先です。
	HandlerTargetDestinations    logger.LogOutputDestinations
	InternalLogDestinationConfig InternalLogDestinationConfig
	Timeout                      time.Duration
	TimeoutBudgetPolicy          timeoutbudget.TimeoutBudgetPolicy
	// MaxParallelはchild Logger処理の同時実行数です。上限到達時は破棄せず、実行中の
	// childが完了して空きができるまで受付側を待機させます。
	MaxParallel int
	ClockConfig
	StdoutConfig
	FileConfig
	SlackConfig
	OpenSearchConfig
	LokiConfig
	CloudWatchLogsConfig
	ClickHouseConfig
}
