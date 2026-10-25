package logconfig

import (
	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/internalerror"
	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"time"
)

const (
	// デフォルトロギングレベル
	// envファイルにLOG_LEVELの設定値がなければこの値を使用する
	// .envファイルには 'DEBUG' 'INFO' 'WARN' 'ERROR' のいずれかの値を設定してください
	DefaultFileLogLevel           = logger.LogLevelWarn
	DefaultStdoutLogLevel         = logger.LogLevelInfo
	DefaultSlackLogLevel          = logger.LogLevelError
	DefaultOpenSearchLogLevel     = logger.LogLevelDebug
	DefaultLokiLogLevel           = logger.LogLevelDebug
	DefaultCloudWatchLogsLogLevel = logger.LogLevelDebug
	DefaultClickHouseLogLevel     = logger.LogLevelDebug

	// この項目はログファイル設定 ログファイルの保存先がfileの場合に使用します
	DefaultMaxLogFileSizeMB = 50                                   // MB単位のデフォルトログサイズ .envで指定が無い場合に当値を適用する
	BytesPerMB              = 1024 * 1024                          // 1MBのバイト数
	DefaultMaxLogFileSize   = DefaultMaxLogFileSizeMB * BytesPerMB // バイト単位のデフォルトログサイズ
	// 通常・asyncのfile書込みは、空き容量不足時に既定で破棄します。
	// degradeを選ぶ場合の退避先はstdoutです。
	DefaultLogFileOverloadPolicy       = durable.OverloadPolicyDrop
	DefaultLogFileSecondaryDestination = durable.SpillDestinationStdout

	// ログの出力先 定数
	LogOutputStdout         = logger.LogOutputStdout
	LogOutputFile           = logger.LogOutputFile
	LogOutputSlack          = logger.LogOutputSlack
	LogOutputOpenSearch     = logger.LogOutputOpenSearch
	LogOutputLoki           = logger.LogOutputLoki
	LogOutputCloudWatchLogs = logger.LogOutputCloudWatchLogs
	LogOutputClickHouse     = logger.LogOutputClickHouse

	// fileの場合　ログの保存先ディレクトリ
	LogDirCaseFile        = "/logs"
	LogDirCaseErrorFile   = "/logs_error"
	LogLineDateFormat     = "2006-01-02 15:04:05"           // ログ本文中の時間に使用するフォーマット
	LogLineDateFormatZone = "2006-01-02 15:04:05 -7:00 MST" // ログ本文中の時間に使用するフォーマット

	// error logの出力ファイル名を分る設定
	DefaultIsSeparateErrorLogsEnabled = true
	SeparateErrorLogsPrefix           = "error_" // エラーログファイルに付与するファイル名の接頭辞 .envでの設定不可
	// debug logの出力ファイル名を分る設定
	DefaultIsSeparateDebugLogsEnabled = true
	SeparateDebugLogsPrefix           = "debug_" // デバッグログファイルに付与するファイル名の接頭辞 .envでの設定不可
	// debugの保存directoryは、個別指定を有効にしなければ通常logと共有します。
	DefaultIsSeparateDebugLogDirEnabled = false

	// ログのローテーション設定
	DefaultEnableLogFileRotate = true
	LogRotationDayOfWeek       = time.Sunday // LogRotationWeeklyの設定時に使用。time.Weekday型で任意の曜日を指定

	// ログ保持最大世代数
	DefaultMaxLogFileBackups = 60
	// ログファイル最大保持日数
	DefaultMaxLogFileAge = 60
	// ログディレクトリ 合計ファイル最大サイズ(MB) 閾値を超えると古い順に削除
	DefaultMaxLogFileDirSizeMB = 2048
	DefaultMaxLogFileDirSize   = DefaultMaxLogFileDirSizeMB * BytesPerMB
	// ログローテート時に古いファイルを圧縮
	DefaultEnableLogFileCompress = true

	// env無指定時のデフォルトのファイル圧縮形式
	DefaultLogCompressFormat    = fsstore.CompressGzip
	DefaultLogFileLifecycleMode = fsstore.FileLifecycleModeInline

	DefaultWriteFileLoggerLatencyEnabled = true // FileLoggerのファイル操作周りのレイテンシーを記録するか
	// DefaultMultiLoggerTimoutは、HETEROLOG_LOG_TIMEOUT_SECONDを省略した場合の通常loggerの呼び出し上限です。
	// 既定のtimeout budget policy（must_fit）では、選択したbackend clientの再試行予算より長い
	// 必要があります。既定のclient設定ではOpenSearchのbulk送信の予算17秒が最大のため、
	// 公開設定例（.env.example）と同じ18秒にします。
	DefaultMultiLoggerTimout = 18 * time.Second

	// slack通知
	// 通知タイミングごとに記載する最大ログ数
	DefaultMaxLogsPerSlackNotification = 10
	// 通知に必要なログレベル
	DefaultRequireLogLevelForSend = logger.LogLevelError
	// 通知が途切れた後に遅延段階を先頭へ戻すまでの無通知時間。
	// 正の値が必須です。
	DefaultMaxRestThreshold = 360 * time.Minute
	// エラー時のスタックトレースの深さ 可能範囲：1 〜 (指定値)
	DefaultErrorTraceDepth = 3
	// DEBUGモードがtrueの場合のスタックトレースの深さ
	DefaultErrorTraceDepthDebugMode = 10

	// opensearch
	// BulkResponse 内の retryable item を送信する総試行回数（初回を含む）。
	MaxOpenSearchLoggerRetryAttempts = 3
	// 現行の OpenSearch flush manager は bulk item retry 間で待機しないため、この値は予約値です。
	OpenSearchRetryWaitTime = 1000 * time.Millisecond
	// opensearch flush送信可否
	DefaultNoBulkFlushModeEnabled = false
	// OpenSearchバルク送信間隔
	DefaultOpenSearchFlushInterval    = 5 * time.Second
	DefaultOpenSearchMaxBulkDocuments = 500
	// DefaultOpenSearchBufferFlushBytesはbuffer内document bodyの概算合計が到達した時点で、
	// 時間・件数条件より先にflushを開始するsoft thresholdです。
	DefaultOpenSearchBufferFlushBytes = 5 * 1024 * 1024
	// DefaultOpenSearchMaxBulkRequestBytesは一回のBulk NDJSON requestを分割するapplication側の
	// 上限です。OpenSearch、proxy、load balancerの拒否上限より余裕を持たせて設定します。
	DefaultOpenSearchMaxBulkRequestBytes = 15 * 1024 * 1024
	// opensearchレイテンシ警告閾値時間
	PostLogWarnThresholdDuration  = 200 * time.Millisecond
	PostLogErrorThresholdDuration = 1000 * time.Millisecond
	DefaultIndexLatencyEnabled    = true
	// opensearch otel準拠
	DefaultOTelFormatEnabledForOpenSearch = true

	// loki
	DefaultLokiMaxEntries = 500
	// DefaultLokiBufferFlushBytesはformatter済みlineの概算合計でflushを早める
	// application側のsoft thresholdです。Push requestの物理分割上限とは独立しています。
	DefaultLokiBufferFlushBytes         = 1 * 1024 * 1024
	DefaultLokiFallbackQueueMaxEntries  = 1000
	DefaultLokiMaxLineBytes             = 256 * 1024
	DefaultLokiMaxPushRequestBytes      = 4 * 1024 * 1024
	DefaultLokiFlushInterval            = 5 * time.Second
	DefaultLokiNoBulkFlushModeEnabled   = false
	DefaultLokiPushLatencyEnabled       = true
	DefaultIsLogLokiSamplingEnabled     = true
	DefaultEnableLokiLoggingByRequestID = true
	DefaultLokiLoggingByRequestIDRate   = 10
	DefaultLogLokiSamplingDebugRate     = 10
	DefaultLogLokiSamplingInfoRate      = 1000
	DefaultLogLokiSamplingWarnRate      = 10
	DefaultLogLokiSamplingErrorRate     = 1

	// cloudwatch logs
	DefaultCloudWatchLogsBufferFlushMaxEvents = 500
	// DefaultCloudWatchLogsBufferFlushBytesはmessage byte数とevent overheadの概算合計で
	// flushを早めるsoft thresholdです。PutLogEventsの物理request分割上限とは独立しています。
	DefaultCloudWatchLogsBufferFlushBytes         = 512 * 1024
	DefaultCloudWatchLogsFallbackQueueMaxEvents   = 1000
	DefaultCloudWatchLogsFlushInterval            = 5 * time.Second
	DefaultCloudWatchLogsNoBulkFlushModeEnabled   = false
	DefaultCloudWatchLogsPutLatencyEnabled        = true
	DefaultIsLogCloudWatchLogsSamplingEnabled     = true
	DefaultEnableCloudWatchLogsLoggingByRequestID = true
	DefaultCloudWatchLogsLoggingByRequestIDRate   = 10
	DefaultLogCloudWatchLogsSamplingDebugRate     = 10
	DefaultLogCloudWatchLogsSamplingInfoRate      = 1000
	DefaultLogCloudWatchLogsSamplingWarnRate      = 10
	DefaultLogCloudWatchLogsSamplingErrorRate     = 1

	// clickhouse
	DefaultClickHouseMaxEntries = 10000
	// DefaultClickHouseBufferFlushBytesはJSONEachRowの概算合計でflushを早めるsoft
	// thresholdです。INSERT requestの物理分割上限とは独立しています。
	DefaultClickHouseBufferFlushBytes = 5 * 1024 * 1024
	// DefaultClickHouseFallbackQueueMaxEntriesはfallback送信中も保持できるentry件数の
	// hard上限です。通常のflush閾値とは独立しています。
	DefaultClickHouseFallbackQueueMaxEntries = 10000
	// DefaultClickHouseMaxInsertRequestBytesはClickHouse固有の固定上限ではなく、proxyや
	// server設定に余裕を持たせるapplication側の物理INSERT request上限です。
	DefaultClickHouseMaxInsertRequestBytes    = 16 * 1024 * 1024
	DefaultClickHouseFlushInterval            = 5 * time.Second
	DefaultClickHouseInsertLatencyEnabled     = true
	DefaultIsLogClickHouseSamplingEnabled     = true
	DefaultEnableClickHouseLoggingByRequestID = true
	DefaultClickHouseLoggingByRequestIDRate   = 10
	DefaultLogClickHouseSamplingDebugRate     = 10
	DefaultLogClickHouseSamplingInfoRate      = 1000
	DefaultLogClickHouseSamplingWarnRate      = 10
	DefaultLogClickHouseSamplingErrorRate     = 1

	// stdout
	// stdout出力ログをotel準拠のjsonにする
	DefaultOTelFormatEnabledForStdout = true

	// ログに記述する時刻のタイムゾーン設定
	// 未設定時は公開APIのDefaultConfigと同じくUTCで記録します。ローカル時刻は
	// HETEROLOG_ENABLE_LOCAL_TIME_LOG=trueを明示した場合だけ使い、HETEROLOG_TIMEZONE_LOGで地域を選びます。
	DefaultIsLocalTimeEnabled = false
	DefaultTimeZone           = "UTC"

	// ログサンプリング　ファイル
	DefaultIsLogFileSamplingEnabled     = true
	DefaultEnableFileLoggingByRequestID = true
	DefaultFileLoggingByRequestIDRate   = 10
	// request idでサンプリングしない場合はすべてレベル別のレーティングでサンプリングを行う
	DefaultLogFileSamplingDebugRate = 1 // 1/(指定値)の割合　100なら100分の1 1000なら1000分の1を抽出
	DefaultLogFileSamplingInfoRate  = 1000
	DefaultLogFileSamplingWarnRate  = 1
	DefaultLogFileSamplingErrorRate = 1

	// ログサンプリング　slack
	DefaultIsLogSlackSamplingEnabled     = true
	DefaultEnableSlackLoggingByRequestID = true
	DefaultSlackLoggingByRequestIDRate   = 10
	DefaultLogSlackSamplingDebugRate     = 10 // 1ならば　1/1の割合。slackはエラー通知のみなので1でもいいのでは？
	DefaultLogSlackSamplingInfoRate      = 1000
	DefaultLogSlackSamplingWarnRate      = 10
	DefaultLogSlackSamplingErrorRate     = 1

	// ログサンプリング　標準出力・標準エラー出力
	DefaultIsLogStdoutSamplingEnabled     = true
	DefaultEnableStdoutLoggingByRequestID = true
	DefaultStdoutLoggingByRequestIDRate   = 10
	DefaultLogStdoutSamplingDebugRate     = 1
	DefaultLogStdoutSamplingInfoRate      = 1
	DefaultLogStdoutSamplingWarnRate      = 1
	DefaultLogStdoutSamplingErrorRate     = 1

	// ログサンプリング　opensearch
	DefaultIsLogOpenSearchSamplingEnabled     = true
	DefaultEnableOpenSearchLoggingByRequestID = true
	DefaultOpenSearchLoggingByRequestIDRate   = 10
	DefaultLogOpenSearchSamplingDebugRate     = 10 // 1ならば　1/1の割合。slackはエラー通知のみなので1でもいいのでは？
	DefaultLogOpenSearchSamplingInfoRate      = 1000
	DefaultLogOpenSearchSamplingWarnRate      = 10
	DefaultLogOpenSearchSamplingErrorRate     = 1
)

// デフォルトのログ出力先設定
// envファイルのHETEROLOG_LOG_TYPE・HETEROLOG_ASYNC_LOG_TYPE・HETEROLOG_DELIVERY_LOG_TYPEが未設定・空の場合は、
// 通常・非同期・Deliveryのいずれもこの値を使用する。
// 外部サービスや認証情報を必要としない標準出力だけを選択し、fileやSlackなどは
// 利用者が出力先として明示した場合だけ有効にする。
// 設定可能な値はconstで設定した定数
var LogDefaultOutputs = []string{
	string(LogOutputStdout),
}

// デフォルトのログファイルのローテーション頻度
var DefaultLogFileRotation = fsstore.FileRotationDaily

// slack
// 初回通知はloggerの状態により即時送信し、以降の連続通知にこの待機時間を順に適用します。
var DefaultSlackIntervals = []time.Duration{time.Minute, 5 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour}

type LoggerConfig struct {
	LogOutputDestinations logger.LogOutputDestinations
	// HandlerTargetDestinations は、通常出力先ではないものの、有効なloggerまたは
	// failure storeのinternal log委譲先として設定を読み込む必要がある出力先です。
	HandlerTargetDestinations logger.LogOutputDestinations
	// InternalLogDestinationConfigは、internal log handlerが通常backendと異なる
	// 保存先を使う場合のidentity上書き値です。
	InternalLogDestinationConfig InternalLogDestinationConfig
	Timeout                      time.Duration
	TimeoutBudgetPolicy          timeoutbudget.TimeoutBudgetPolicy
	ClockConfig
	StdoutConfig
	FileConfig
	SlackConfig
	OpenSearchConfig
	LokiConfig
	CloudWatchLogsConfig
	ClickHouseConfig
}

type ClockConfig struct {
	IsLocalTimeEnabled bool
	TimeZone           string
}

type StdoutConfig struct {
	LogLevel                   logger.LogLevel
	LogLineDateFormat          string
	ErrorTraceDepth            int // 将来的に出力先ごとに値を変える可能性あるが現在は同じ
	ErrorTraceDepthDebugMode   int // 将来的に出力先ごとに値を変える可能性あるが現在は同じ
	SamplingConfig             SamplingConfig
	HandlerConfig              internalerror.RoutingConfig
	OpenTelemetryFormatEnabled bool
}

type DeliveryStdoutConfig struct {
	LogLineDateFormat          string
	ErrorTraceDepth            int // 将来的に出力先ごとに値を変える可能性あるが現在は同じ
	ErrorTraceDepthDebugMode   int // 将来的に出力先ごとに値を変える可能性あるが現在は同じ
	HandlerConfig              internalerror.RoutingConfig
	OpenTelemetryFormatEnabled bool
}

type FileConfig struct {
	LogLevel                logger.LogLevel
	FileSystemStoreConfig   FileSystemStoreConfig
	LogBodyConfig           LogBodyConfig
	SamplingConfig          SamplingConfig
	WriteFileLatencyEnabled bool
	HandlerConfig           internalerror.RoutingConfig
}

type DeliveryFileConfig struct {
	FileSystemStoreConfig   DeliveryFileSystemStoreConfig
	LogBodyConfig           LogBodyConfig
	WriteFileLatencyEnabled bool
	HandlerConfig           internalerror.RoutingConfig
}

type SlackConfig struct {
	LogLevel      logger.LogLevel // RequiredLogLevelForSend
	LogBodyConfig LogBodyConfig
	// TimeoutはSlack HTTP requestの上限です。正の値が必須です。
	Timeout time.Duration
	// Intervalsは初回の即時通知後に適用する待機時間です。
	// 全要素に正の値が必須で、即時通知はloggerの実行状態として管理します。
	Intervals              []time.Duration
	MaxLogsPerNotification int
	// 正の値が必須です。
	MaxRestThreshold time.Duration
	Token            string
	Channel          string
	ErrorChannel     string
	WarnChannel      string
	InfoChannel      string
	DebugChannel     string
	MaxMessageLength int
	SamplingConfig   SamplingConfig
	HandlerConfig    internalerror.RoutingConfig
}

// DeliverySlackConfigはDelivery本体で同期・一件送信します。Slack HTTP応答や通知側の
// 処理がDelivery全体の待ち時間を延ばし得るため、低遅延が必要な出力先には非推奨です。
// Delivery internal-log Slack fallbackはこの型の送信方式ではなく専用batch設定を使います。
type DeliverySlackConfig struct {
	LogLevel         logger.LogLevel // delivery保証loggerはslackのみ例外的にleveling可能とする　現実的には通知利用であるはずゆえ
	LogBodyConfig    LogBodyConfig
	Timeout          time.Duration
	Token            string
	Channel          string
	ErrorChannel     string
	WarnChannel      string
	InfoChannel      string
	DebugChannel     string
	MaxMessageLength int
	HandlerConfig    internalerror.RoutingConfig
}

type OpenSearchConfig struct {
	OpenSearchLoggerBaseConfig
	OpenSearchFlushManagerConfig
	OpenSearchRetryAbleClientConfig
}

type DeliveryOpenSearchConfig struct {
	DeliveryOpenSearchLoggerBaseConfig
	DeliveryOpenSearchFlushManagerConfig
	OpenSearchRetryAbleClientConfig
}

type LokiConfig struct {
	LokiLoggerBaseConfig
	LokiFlushManagerConfig
	LokiRetryAbleClientConfig
}

type DeliveryLokiConfig struct {
	DeliveryLokiLoggerBaseConfig
	DeliveryLokiFlushManagerConfig
	LokiRetryAbleClientConfig
}

type CloudWatchLogsConfig struct {
	CloudWatchLogsLoggerBaseConfig
	CloudWatchLogsFlushManagerConfig
	CloudWatchLogsRetryAbleClientConfig
}

type DeliveryCloudWatchLogsConfig struct {
	DeliveryCloudWatchLogsLoggerBaseConfig
	DeliveryCloudWatchLogsFlushManagerConfig
	CloudWatchLogsRetryAbleClientConfig
}

type ClickHouseConfig struct {
	ClickHouseLoggerBaseConfig
	ClickHouseFlushManagerConfig
	ClickHouseRetryAbleClientConfig
}

type DeliveryClickHouseConfig struct {
	DeliveryClickHouseLoggerBaseConfig
	DeliveryClickHouseFlushManagerConfig
	ClickHouseRetryAbleClientConfig
}

// 通信エラーのみ自動で再試行する wrapper client の設定です。
// RetryClientMode で wrapper 構成を単一値として選びます。
// 固定ID createの409はwrapper構成から独立してOpenSearch backendの結果分類で成功扱いにし、
// RetryClientModeはretry、retry policy、circuit breakerの構成だけを変更します。
type OpenSearchRetryAbleClientConfig struct {
	RetryClientMode logger.RetryClientMode

	// MaxAttempts は「最大試行回数」です（初回含む）。
	// 例: 1 = リトライなし、3 = 最大3回（初回 + 2リトライ）
	MaxAttempts int

	// BaseDelay はリトライごとに加算されるバックオフの基準遅延です。
	BaseDelay time.Duration

	// MaxDelay はバックオフの上限です。
	MaxDelay time.Duration
}

type LokiRetryAbleClientConfig struct {
	RetryClientMode logger.RetryClientMode

	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type CloudWatchLogsRetryAbleClientConfig struct {
	RetryClientMode logger.RetryClientMode

	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type ClickHouseRetryAbleClientConfig struct {
	RetryClientMode logger.RetryClientMode

	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

type OpenSearchLoggerBaseConfig struct {
	ClockConfig                ClockConfig
	LogLevel                   logger.LogLevel // RequiredLogLevelForSend
	LogBodyConfig              LogBodyConfig
	FlushInterval              time.Duration //　バルク送信間隔
	MaxBulkDocument            int           // バルク送信閾値件数（間隔内でも超えたら送信）
	BufferFlushBytes           int           // buffer内document body概算値による早期flush閾値
	MaxBulkRequestBytes        int           // gzip圧縮前のBulk NDJSON request分割上限
	NoBulkFlushModeEnabled     bool
	SamplingConfig             SamplingConfig
	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	IndexLatencyEnabled        bool
	MaxRetryAttempts           int           // BulkResponse 内の retryable item を送信する総試行回数（初回を含む）
	RetryWaitTime              time.Duration // retryableなBulk itemを次に送信するまでの待機時間
	HandlerConfig              internalerror.RoutingConfig
}

type DeliveryOpenSearchLoggerBaseConfig struct {
	ClockConfig   ClockConfig
	LogBodyConfig LogBodyConfig
	HandlerConfig internalerror.RoutingConfig
}

type LokiLoggerBaseConfig struct {
	ClockConfig                ClockConfig
	LogLevel                   logger.LogLevel
	LogBodyConfig              LogBodyConfig
	FlushInterval              time.Duration
	MaxEntries                 int
	BufferFlushBytes           int // formatter済みline byte概算合計による早期flush閾値
	FallbackQueueMaxEntries    int // fallbackが保持できるentry件数のhard上限
	MaxLineBytes               int // format後の1行をUTF-8 byte数で制限する
	MaxPushRequestBytes        int // HTTP圧縮前のPush request bodyをbyte数で制限する
	NoBulkFlushModeEnabled     bool
	SamplingConfig             SamplingConfig
	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	PushLatencyEnabled         bool
	HandlerConfig              internalerror.RoutingConfig
}

// DeliveryLokiLoggerBaseConfig は1件ずつ送信するDelivery Lokiのログ生成設定です。
// HTTP再試行はLokiRetryAbleClientConfigを使うclient wrapperが所有するため、ここには保持しません。
type DeliveryLokiLoggerBaseConfig struct {
	ClockConfig         ClockConfig
	LogBodyConfig       LogBodyConfig
	MaxLineBytes        int // format後の1行をUTF-8 byte数で制限する
	MaxPushRequestBytes int // HTTP圧縮前のPush request bodyをbyte数で制限する
	HandlerConfig       internalerror.RoutingConfig
}

type CloudWatchLogsLoggerBaseConfig struct {
	ClockConfig                ClockConfig
	LogLevel                   logger.LogLevel
	LogBodyConfig              LogBodyConfig
	FlushInterval              time.Duration
	BufferFlushMaxEvents       int // buffer flushを開始するevent件数のsoft threshold
	BufferFlushBytes           int // PutLogEvents計算式によるbuffer byte数のsoft threshold
	FallbackQueueMaxEvents     int // fallbackが保持できるevent件数のhard上限
	NoBulkFlushModeEnabled     bool
	SamplingConfig             SamplingConfig
	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	PutLatencyEnabled          bool
	HandlerConfig              internalerror.RoutingConfig
}

type DeliveryCloudWatchLogsLoggerBaseConfig struct {
	ClockConfig   ClockConfig
	LogBodyConfig LogBodyConfig
	HandlerConfig internalerror.RoutingConfig
}

type ClickHouseLoggerBaseConfig struct {
	ClockConfig                ClockConfig
	LogLevel                   logger.LogLevel
	LogBodyConfig              LogBodyConfig
	FlushInterval              time.Duration
	MaxEntries                 int
	BufferFlushBytes           int // JSONEachRow byte概算合計による早期flush閾値
	FallbackQueueMaxEntries    int // fallbackが保持できるentry件数のhard上限
	MaxInsertRequestBytes      int // 一回の物理INSERT requestを分割する上限
	SamplingConfig             SamplingConfig
	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	InsertLatencyEnabled       bool
	HandlerConfig              internalerror.RoutingConfig
}

type DeliveryClickHouseLoggerBaseConfig struct {
	ClockConfig                ClockConfig
	LogBodyConfig              LogBodyConfig
	FlushInterval              time.Duration
	MaxEntries                 int
	BufferFlushBytes           int // JSONEachRow byte概算合計による早期flush閾値
	FallbackQueueMaxEntries    int // internal-log fallbackが保持できるentry件数のhard上限
	MaxInsertRequestBytes      int // 一回の物理INSERT requestを分割する上限
	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	InsertLatencyEnabled       bool
	HandlerConfig              internalerror.RoutingConfig
}

type OpenSearchFlushManagerConfig struct {
	Index                      string
	OpenTelemetryFormatEnabled bool
	FailureHandlerConfig       failure.FailureHandlerConfig // OpenSearchの登録失敗データの退避設定
}

// DeliveryOpenSearchFlushManagerConfig は1件ずつ行うIndexと、送信失敗後に
// 同期保存してDeliveryReportへ結果を含めるfileArchive設定を保持します。
// bulk item再送設定は通常OpenSearchだけが所有します。
type DeliveryOpenSearchFlushManagerConfig struct {
	Index                      string
	OpenTelemetryFormatEnabled bool
	FailureHandlerConfig       failure.DeliveryFailureHandlerConfig // OpenSearchの登録失敗データの退避設定
	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	IndexLatencyEnabled        bool
}

type LokiFlushManagerConfig struct {
	FailureHandlerConfig failure.FailureHandlerConfig
}

// DeliveryLokiFlushManagerConfig は1件ずつ行うPushと、送信失敗後に
// 同期保存してDeliveryReportへ結果を含めるfileArchive設定を保持します。
// HTTP再試行はclient wrapperが所有し、retry queue用spoolはDelivery Lokiでは扱いません。
type DeliveryLokiFlushManagerConfig struct {
	FailureHandlerConfig       failure.DeliveryFailureHandlerConfig
	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	PushLatencyEnabled         bool
}

type CloudWatchLogsFlushManagerConfig struct {
	LogGroupName  string
	LogStreamName string
	// LogStreamRoutingGranularityは基準stream名をevent発生日時で分割するapplication側の
	// routing policyです。CloudWatch Logs APIへ直接渡す設定ではありません。
	LogStreamRoutingGranularity streamrouting.Granularity
	FailureHandlerConfig        failure.FailureHandlerConfig
}

// DeliveryCloudWatchLogsFlushManagerConfig は1件ずつ行うPutLogEventsと、送信失敗後に
// 同期保存してDeliveryReportへ結果を含めるfileArchive設定を保持します。
// retry queue用spoolはDelivery CloudWatch Logsでは扱いません。
type DeliveryCloudWatchLogsFlushManagerConfig struct {
	LogGroupName  string
	LogStreamName string
	// LogStreamRoutingGranularityの契約は通常CloudWatchLogsFlushManagerConfigと同じです。
	LogStreamRoutingGranularity streamrouting.Granularity
	PostWarnThresholdDuration   time.Duration
	PostErrorThresholdDuration  time.Duration
	PutLatencyEnabled           bool
	FailureHandlerConfig        failure.DeliveryFailureHandlerConfig
}

type ClickHouseFlushManagerConfig struct {
	FailureHandlerConfig failure.FailureHandlerConfig
}

// DeliveryClickHouseFlushManagerConfig は、enqueue後のbatch flushに失敗したrecordを
// 非同期保存するfileArchive設定を保持します。最初のDeliveryReportはenqueue結果だけを返し、
// retry用spoolはDelivery loggerでは扱いません。
type DeliveryClickHouseFlushManagerConfig struct {
	FailureHandlerConfig failure.DeliveryFailureHandlerConfig
}

// FileSystemStoreConfigではdefaultを含むlog directoryの共有だけを許可します。
// error/debugを両方分離する場合、LogDirとは異なる同一directoryを両者に指定できません。
type FileSystemStoreConfig struct {
	MaxFileSize                  uint64
	LogDir                       string // 通常ログのサブディレクトリ名。既定値・継承適用後、使用時は空文字不可。
	ErrorLogDir                  string // ex.)/var/log/{appname}/{dirCaseErrorFile}/
	DebugLogDir                  string // directory分離がfalseなら無視し、trueなら空でない値が必要です。
	IsSeparateDebugLogDirEnabled bool   // DEBUGファイル分離とは独立したdirectory選択です。
	IsSeparateErrorLogsEnabled   bool
	SeparateErrorLogsPrefix      string
	IsSeparateDebugLogsEnabled   bool
	SeparateDebugLogsPrefix      string
	EnableRotate                 bool
	MaxFileBackups               int
	MaxFileAge                   int
	MaxDirSize                   uint64
	// ErrorPurgeとDebugPurgeはenv読込時に項目単位で共通設定を継承します。
	// internal logのdirectory上書き後に実際の容量制限を決めるため、指定容量も保持します。
	ErrorPurge                         *fsstore.FilePurgeConfig
	DebugPurge                         *fsstore.FilePurgeConfig
	EnableFileCompress                 bool
	CompressFormat                     fsstore.CompressFormat
	RotationType                       fsstore.FileRotationType
	RotationDayOfWeek                  time.Weekday
	SamplingConfig                     SamplingConfig
	WriteLogFileWarnThresholdDuration  time.Duration
	WriteLogFileErrorThresholdDuration time.Duration
	CompressWarnThresholdDuration      time.Duration
	CompressErrorThresholdDuration     time.Duration
	WriteFileLatencyEnabled            bool
	MaxWriteAttempts                   int
	FileOperationRetryWaitTime         time.Duration
	OverloadConfig                     FileOverLoadPolicyConfig
	ObjectStorageUploadConfig          objectstorage.LogFileUploadConfig
	LifecycleMode                      fsstore.FileLifecycleMode
	// LifecycleHandlerConfigはrunner modeの周期実行結果に対するinternal log routingです。
	// level別のroutingは独立して保持し、purge失敗はWarnへ格下げせずErrorで記録します。
	LifecycleHandlerConfig internalerror.RoutingConfig
}

// DeliveryFileSystemStoreConfigのdirectory共有規則はFileSystemStoreConfigと同じです。
type DeliveryFileSystemStoreConfig struct {
	MaxFileSize                  uint64
	MaxFileBackups               int
	MaxFileAge                   int
	MaxDirSize                   uint64
	ErrorPurge                   *fsstore.FilePurgeConfig
	DebugPurge                   *fsstore.FilePurgeConfig
	LogDir                       string // 通常ログのサブディレクトリ名。既定値・継承適用後、使用時は空文字不可。
	ErrorLogDir                  string // ex.)/var/log/{appname}/{dirCaseErrorFile}/
	DebugLogDir                  string // directory分離がfalseなら無視し、trueなら空でない値が必要です。
	IsSeparateDebugLogDirEnabled bool   // DEBUGファイル分離とは独立したdirectory選択です。
	IsSeparateErrorLogsEnabled   bool
	SeparateErrorLogsPrefix      string
	IsSeparateDebugLogsEnabled   bool
	SeparateDebugLogsPrefix      string
	EnableRotate                 bool
	RotationType                 fsstore.FileRotationType
	RotationDayOfWeek            time.Weekday
	EnableFileCompress           bool
	CompressFormat               fsstore.CompressFormat

	WriteLogFileWarnThresholdDuration  time.Duration
	WriteLogFileErrorThresholdDuration time.Duration
	CompressWarnThresholdDuration      time.Duration
	CompressErrorThresholdDuration     time.Duration
	MaxWriteAttempts                   int
	FileOperationRetryWaitTime         time.Duration
	ObjectStorageUploadConfig          objectstorage.LogFileUploadConfig
	LifecycleMode                      fsstore.FileLifecycleMode
	// LifecycleHandlerConfigのlevel別契約はFileSystemStoreConfigと同じです。
	LifecycleHandlerConfig internalerror.RoutingConfig
}

// FileOverLoadPolicyConfigは通常・asyncのfile書込み時、空き容量が不足した場合の処理です。
// 判定閾値は共通のMinAvailableDiskSpaceを使い、0なら判定しません。
type FileOverLoadPolicyConfig struct {
	OverloadPolicy   durable.OverloadPolicy   // drop or degrade
	SpillDestination durable.SpillDestination // degrade時の退避先。現在はstdoutだけに対応します。
}

type LogBodyConfig struct {
	LogLineDateFormat        string
	ErrorTraceDepth          int
	ErrorTraceDepthDebugMode int
}

type SamplingConfig struct {
	IsSamplingEnabled        bool
	EnableLoggingByRequestID bool
	LoggingByRequestIDRate   int
	SamplingDebugRate        int
	SamplingInfoRate         int
	SamplingWarnRate         int
	SamplingErrorRate        int
}
