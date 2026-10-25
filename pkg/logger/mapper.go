package logger

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/otel"
)

type LoggerConfig struct {
	// LogOutputDestinations は、設定を保持しているbackendのうち、
	// application logの通常出力先として起動するbackendを表します。
	// fallback handlerだけに使うbackendはconfig pointerを持っていても、ここには含めません。
	// 明示したbackendのconfig pointerは必須です。空の場合だけ、non-nilのconfigを
	// 通常出力として扱う直接構築互換の挙動になります。
	LogOutputDestinations LogOutputDestinations
	Timeout               time.Duration
	// MaxLogEntryBytesは、backendへ分配する前に受理できる一件のapplication logの
	// 上限です。message、error、fieldsをJSONとして数え、正の値を必須とします。
	MaxLogEntryBytes     int
	File                 *FileLoggerConfig
	Stdout               *StdoutLoggerConfig
	Slack                *SlackLoggerConfig
	OpenSearch           *OpenSearchConfig
	OpenSearchClient     *opensearch.Config
	Loki                 *LokiConfig
	LokiClient           *loki.Config
	CloudWatchLogs       *CloudWatchLogsConfig
	CloudWatchLogsClient *cloudwatchlogs.Config
	ClickHouse           *ClickHouseConfig
	ClickHouseClient     *clickhouse.Config
	// InternalLogDestinationsは、internal log handlerだけが使用する保存先設定です。
	// nilのbackendは通常設定を使い、non-nilのbackendだけ保存先identityを置き換えます。
	InternalLogDestinations *InternalLogDestinationConfig
	Resource                *otel.Resource
}

type AsyncBestEffortMultiLoggerConfig struct {
	// LogOutputDestinations は、設定を保持しているbackendのうち、
	// application logの通常出力先として起動するbackendを表します。
	// fallback handlerだけに使うbackendはconfig pointerを持っていても、ここには含めません。
	// 明示したbackendのconfig pointerは必須です。空の場合だけ、non-nilのconfigを
	// 通常出力として扱う直接構築互換の挙動になります。
	LogOutputDestinations LogOutputDestinations
	Timeout               time.Duration
	// MaxLogEntryBytesは、goroutine起動前に受理できる一件のapplication logの
	// 上限です。超過したlogはbest-effort契約に従い、childへ渡さず破棄します。
	MaxLogEntryBytes int
	// MaxParallelはchild Logger処理の同時実行数です。上限到達だけを理由にchildを
	// 破棄せず、実行中のchildが完了するまで受付側へbackpressureをかけます。
	MaxParallel             int
	File                    *FileLoggerConfig
	Stdout                  *StdoutLoggerConfig
	Slack                   *SlackLoggerConfig
	OpenSearch              *OpenSearchConfig
	OpenSearchClient        *opensearch.Config
	Loki                    *LokiConfig
	LokiClient              *loki.Config
	CloudWatchLogs          *CloudWatchLogsConfig
	CloudWatchLogsClient    *cloudwatchlogs.Config
	ClickHouse              *ClickHouseConfig
	ClickHouseClient        *clickhouse.Config
	InternalLogDestinations *InternalLogDestinationConfig
	Resource                *otel.Resource
}

type DeliveryLoggerConfig struct {
	// LogOutputDestinations は、設定を保持しているbackendのうち、
	// application logの通常出力先として起動するbackendを表します。
	// fallback handlerだけに使うbackendはconfig pointerを持っていても、ここには含めません。
	// 明示したbackendのconfig pointerは必須です。空の場合だけ、non-nilのconfigを
	// 通常出力として扱う直接構築互換の挙動になります。
	LogOutputDestinations LogOutputDestinations
	Timeout               time.Duration
	// MaxLogEntryBytesは、backendへ分配する前に受理できる一件のapplication logの
	// 上限です。超過時は全targetをDeliveryRejectedとして返します。
	MaxLogEntryBytes        int
	File                    *DeliveryFileLoggerConfig
	Stdout                  *DeliveryStdoutLoggerConfig
	Slack                   *DeliverySlackLoggerConfig
	OpenSearch              *DeliveryOpenSearchConfig
	OpenSearchClient        *opensearch.Config
	Loki                    *DeliveryLokiConfig
	LokiClient              *loki.Config
	CloudWatchLogs          *DeliveryCloudWatchLogsConfig
	CloudWatchLogsClient    *cloudwatchlogs.Config
	ClickHouse              *DeliveryClickHouseConfig
	ClickHouseClient        *clickhouse.Config
	InternalLogDestinations *DeliveryInternalLogDestinationConfig
	// InternalLogBatchConfigはDelivery loggerのinternal-log fallbackだけが使う
	// enqueue/batch設定です。Delivery本体およびsync/async loggerには適用しません。
	InternalLogBatchConfig DeliveryInternalLogBatchConfig

	Resource *otel.Resource
}

// DeliveryInternalLogBatchConfigはDelivery internal-log fallbackの送信先ごとの
// enqueue/batch設定です。ClickHouseは既存のDelivery ClickHouse設定を使用します。
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
	// MaxRestThresholdはSlackLoggerConfig.MaxRestThresholdと同じ休止時間です。
	MaxRestThreshold time.Duration
}

// InternalLogDestinationConfigは、sync/async loggerのinternal logだけが使用する
// backend設定です。通常設定と異なる保存先を使うbackendだけを保持します。
// client設定を持つbackendはlogger設定とclient設定を組で指定し、片方だけの指定は
// 設定errorです。
//
// Fileは保持・圧縮・purge policyを持てますが、internal logのterminal sinkとして扱います。
// そのappendlog lifecycleのHandlerConfigは実行せず、二次的なinternal log routingを
// 発生させません。
type InternalLogDestinationConfig struct {
	File                 *FileLoggerConfig
	Slack                *SlackLoggerConfig
	OpenSearch           *OpenSearchConfig
	OpenSearchClient     *opensearch.Config
	Loki                 *LokiConfig
	LokiClient           *loki.Config
	CloudWatchLogs       *CloudWatchLogsConfig
	CloudWatchLogsClient *cloudwatchlogs.Config
	ClickHouse           *ClickHouseConfig
	ClickHouseClient     *clickhouse.Config
}

// ToLoggerConfigはinternal log保存先をhandler factoryが扱う共通設定へ変換します。
func (c InternalLogDestinationConfig) ToLoggerConfig() LoggerConfig {
	return LoggerConfig{
		File:                 c.File,
		Slack:                c.Slack,
		OpenSearch:           c.OpenSearch,
		OpenSearchClient:     c.OpenSearchClient,
		Loki:                 c.Loki,
		LokiClient:           c.LokiClient,
		CloudWatchLogs:       c.CloudWatchLogs,
		CloudWatchLogsClient: c.CloudWatchLogsClient,
		ClickHouse:           c.ClickHouse,
		ClickHouseClient:     c.ClickHouseClient,
	}
}

// DeliveryInternalLogDestinationConfigはDelivery loggerのinternal logだけが使用する
// backend設定です。Fileはsync/asyncと同じterminal sink契約に従い、client設定を持つ
// backendもsync/asyncと同じくlogger設定とclient設定を組で指定します。
type DeliveryInternalLogDestinationConfig struct {
	File                 *DeliveryFileLoggerConfig
	Slack                *DeliverySlackLoggerConfig
	OpenSearch           *DeliveryOpenSearchConfig
	OpenSearchClient     *opensearch.Config
	Loki                 *DeliveryLokiConfig
	LokiClient           *loki.Config
	CloudWatchLogs       *DeliveryCloudWatchLogsConfig
	CloudWatchLogsClient *cloudwatchlogs.Config
	ClickHouse           *DeliveryClickHouseConfig
	ClickHouseClient     *clickhouse.Config
}

// ToLoggerConfigはDeliveryのinternal log保存先をhandler factory用設定へ変換します。
func (c DeliveryInternalLogDestinationConfig) ToLoggerConfig() DeliveryLoggerConfig {
	return DeliveryLoggerConfig{
		File:                 c.File,
		Slack:                c.Slack,
		OpenSearch:           c.OpenSearch,
		OpenSearchClient:     c.OpenSearchClient,
		Loki:                 c.Loki,
		LokiClient:           c.LokiClient,
		CloudWatchLogs:       c.CloudWatchLogs,
		CloudWatchLogsClient: c.CloudWatchLogsClient,
		ClickHouse:           c.ClickHouse,
		ClickHouseClient:     c.ClickHouseClient,
	}
}

func (a AsyncBestEffortMultiLoggerConfig) ToLoggerConfig() LoggerConfig {
	return LoggerConfig{
		LogOutputDestinations:   a.LogOutputDestinations,
		MaxLogEntryBytes:        a.MaxLogEntryBytes,
		File:                    a.File,
		Stdout:                  a.Stdout,
		Slack:                   a.Slack,
		OpenSearch:              a.OpenSearch,
		OpenSearchClient:        a.OpenSearchClient,
		Loki:                    a.Loki,
		LokiClient:              a.LokiClient,
		CloudWatchLogs:          a.CloudWatchLogs,
		CloudWatchLogsClient:    a.CloudWatchLogsClient,
		ClickHouse:              a.ClickHouse,
		ClickHouseClient:        a.ClickHouseClient,
		InternalLogDestinations: a.InternalLogDestinations,
		Resource:                a.Resource,
	}
}

type LogBodyConfig struct {
	LogLineDateFormat        string
	ErrorTraceDepth          int
	ErrorTraceDepthDebugMode int
}

type RuntimeConfig struct {
	AppName  string
	IsDebug  bool
	Target   string
	LogLevel LogLevel
}

type DeliveryRuntimeConfig struct {
	AppName string
	IsDebug bool
	Target  string
}

type TimeConfig struct {
	IsLocalTimeEnabled bool
	TimeZone           string
}

type SamplingConfig struct {
	IsSamplingEnabled        bool
	EnableLoggingByRequestID bool
	// LoggingByRequestIDRate must be in [1, 2^32-1] when request-ID sampling
	// is enabled, and must also fit the platform's int type.
	LoggingByRequestIDRate int
	SamplingDebugRate      int
	SamplingInfoRate       int
	SamplingWarnRate       int
	SamplingErrorRate      int
}

type FileLogBodyConfig struct {
	LogLineDateFormat        string
	ErrorTraceDepth          int
	ErrorTraceDepthDebugMode int
}

type FileLoggerConfig struct {
	FsstoreConfig             fsstore.Config
	ObjectStorageUploadConfig objectstorage.LogFileUploadConfig
	ArchiveCheckpointConfig   fsstore.ArchiveCheckpointConfig
	// LifecycleModeの空値はinlineとして扱い、FileLoggerの書込み経路で保守処理を実行します。
	LifecycleMode fsstore.FileLifecycleMode
	// LifecycleRunnerSettingsはrunner modeの周期loopとoptional Supervisor設定です。
	LifecycleRunnerSettings fsstore.AppendLogRunnerConfig
	TimeConfig              TimeConfig
	RuntimeConfig           RuntimeConfig
	LogBodyConfig           LogBodyConfig
	SamplingConfig          SamplingConfig
	HandlerConfig           handler.HandlerConfig
	// LifecycleHandlerConfigはrunner modeの周期実行結果を委譲する保存先selectorです。
	// levelごとのselectorを独立して保持し、発生したinternal logのseverityに応じて委譲します。
	LifecycleHandlerConfig handler.HandlerConfig
}

type DeliveryFileLoggerConfig struct {
	FsstoreConfig fsstore.DeliveryFileWriteConfig
	// LifecycleModeの空値は保守処理を起動しません。Deliveryはinline処理を持たないため、
	// 保守処理が必要な場合はrunnerを明示します。
	LifecycleMode           fsstore.FileLifecycleMode
	LifecycleRunnerSettings DeliveryFileLifecycleRunnerSettings
	TimeConfig              TimeConfig
	RuntimeConfig           DeliveryRuntimeConfig
	LogBodyConfig           LogBodyConfig
	HandlerConfig           handler.HandlerConfig
}

// DeliveryFileLifecycleRunnerSettingsは、delivery fileの書込み設定から導出できない
// runner mode固有のretention、圧縮、upload、internal log routingだけを保持します。
type DeliveryFileLifecycleRunnerSettings struct {
	UsageCacheTTL  time.Duration
	MaxFileBackups int
	MaxFileAge     int
	MaxDirSize     uint64
	// nilなら通常logの保持設定を継承します。directory共有時の容量上限は
	// fsstore.FileLifecycleConfigと同じ規則で解決します。
	ErrorPurge         *fsstore.FilePurgeConfig
	DebugPurge         *fsstore.FilePurgeConfig
	EnableFileCompress bool
	CompressFormat     fsstore.CompressFormat
	// CompressionCodecはprogrammaticに追加する独自圧縮形式です。nilなら
	// CompressFormatに対応する組み込みcodecを使用します。
	CompressionCodec               fsstore.CompressionCodec
	CompressWarnThresholdDuration  time.Duration
	CompressErrorThresholdDuration time.Duration
	ObjectStorageUploadConfig      objectstorage.LogFileUploadConfig
	ArchiveCheckpointConfig        fsstore.ArchiveCheckpointConfig
	// RunnerConfigはrunner modeの周期loopとoptional Supervisor設定です。
	RunnerConfig fsstore.AppendLogRunnerConfig
	// LifecycleHandlerConfigのlevel別契約はFileLoggerConfigと同じです。
	LifecycleHandlerConfig handler.HandlerConfig
}

// LifecycleRunnerConfigは正規のfile logger設定から専用runner用設定を導出します。
// path、rotation、duration、retryを複製して保持しないため、呼出側の設定変更と乖離しません。
func (c *FileLoggerConfig) LifecycleRunnerConfig() *fsstore.LogFileLifecycleRunnerConfig {
	if c == nil || c.LifecycleMode != fsstore.FileLifecycleModeRunner {
		return nil
	}
	return &fsstore.LogFileLifecycleRunnerConfig{
		FsstoreConfig:             c.FsstoreConfig,
		ObjectStorageUploadConfig: c.ObjectStorageUploadConfig,
		ArchiveCheckpointConfig:   c.ArchiveCheckpointConfig,
		RunnerConfig:              c.LifecycleRunnerSettings,
		HandlerConfig:             c.LifecycleHandlerConfig,
	}
}

// LifecycleRunnerConfigはdelivery fileの書込み設定とrunner固有設定を結合します。
// layout、rotation、write duration、retryはFsstoreConfigを唯一の正として使用します。
func (c *DeliveryFileLoggerConfig) LifecycleRunnerConfig() *fsstore.DeliveryFileLifecycleRunnerConfig {
	if c == nil || c.LifecycleMode != fsstore.FileLifecycleModeRunner {
		return nil
	}
	runner := c.LifecycleRunnerSettings
	return &fsstore.DeliveryFileLifecycleRunnerConfig{
		FsstoreConfig: fsstore.DeliveryFileLifecycleStoreConfig{
			Layout: c.FsstoreConfig.Layout,
			Lifecycle: fsstore.DeliveryFileLifecycleConfig{
				EnableRotate:       c.FsstoreConfig.Rotation.EnableRotate,
				RotationType:       c.FsstoreConfig.Rotation.RotationType,
				RotationDayOfWeek:  c.FsstoreConfig.Rotation.RotationDayOfWeek,
				UsageCacheTTL:      runner.UsageCacheTTL,
				MaxFileBackups:     runner.MaxFileBackups,
				MaxFileAge:         runner.MaxFileAge,
				MaxFileSize:        c.FsstoreConfig.Rotation.MaxFileSize,
				MaxDirSize:         runner.MaxDirSize,
				ErrorPurge:         runner.ErrorPurge,
				DebugPurge:         runner.DebugPurge,
				EnableFileCompress: runner.EnableFileCompress,
				CompressFormat:     runner.CompressFormat,
			},
			Duration: fsstore.DurationConfig{
				WriteFileWarnThresholdDuration:  c.FsstoreConfig.Duration.WriteFileWarnThresholdDuration,
				WriteFileErrorThresholdDuration: c.FsstoreConfig.Duration.WriteFileErrorThresholdDuration,
				WriteFileLatencyEnabled:         c.FsstoreConfig.Duration.WriteFileLatencyEnabled,
				CompressWarnThresholdDuration:   runner.CompressWarnThresholdDuration,
				CompressErrorThresholdDuration:  runner.CompressErrorThresholdDuration,
			},
			Retry:            c.FsstoreConfig.Retry,
			CompressionCodec: runner.CompressionCodec,
		},
		ObjectStorageUploadConfig: runner.ObjectStorageUploadConfig,
		ArchiveCheckpointConfig:   runner.ArchiveCheckpointConfig,
		RunnerConfig:              runner.RunnerConfig,
		HandlerConfig:             runner.LifecycleHandlerConfig,
	}
}

type StdoutLoggerConfig struct {
	TimeConfig             TimeConfig
	RuntimeConfig          RuntimeConfig
	LogBodyConfig          LogBodyConfig
	SamplingConfig         SamplingConfig
	HandlerConfig          handler.HandlerConfig
	UseOpenTelemetryFormat bool
}

type DeliveryStdoutLoggerConfig struct {
	TimeConfig             TimeConfig
	RuntimeConfig          DeliveryRuntimeConfig
	LogBodyConfig          LogBodyConfig
	HandlerConfig          handler.HandlerConfig
	UseOpenTelemetryFormat bool
}

type SlackLoggerConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// TimeoutはSlack HTTP clientの上限です。正の値が必須です。
	Timeout time.Duration
	// HTTPClientはSlack API呼び出しに使うHTTP clientです。nilの場合はnet/httpの
	// 既定transportを使います。指定した場合はlogger構築時に設定をcopyして
	// Transport、Jar、CheckRedirectを引き継ぎ、元のclientは変更しません。
	// TimeoutにはこのconfigのTimeoutを使い、元のclientのTimeoutより優先します。
	// 通常出力とinternal-log fallbackのSlack送信に同じ設定を使います。
	HTTPClient             *http.Client
	Token                  string
	Channel                string
	ErrorChannel           string
	WarnChannel            string
	InfoChannel            string
	DebugChannel           string
	MaxLogsPerNotification int
	// MaxRestThresholdは、ログの到着が途切れたときに通知間隔をリセットする
	// 休止時間です。正の値が必須です。この時間を超えて届いたログは、待機中の
	// ログとともに即時送信を予約します。送信中の通知がある場合はその完了を待ちます。
	MaxRestThreshold time.Duration
	// Intervalsは初回の即時通知後、Slackへの連続buffer送信を予約する
	// 待機時間です。各要素に正の値が必須です。
	Intervals                       []time.Duration
	MaxMessageLength                int
	SamplingConfig                  SamplingConfig
	HandlerConfig                   handler.HandlerConfig
	SlackPostWarnThresholdDuration  time.Duration // slack送信がこの秒数よりも遅延したら警告
	SlackPostErrorThresholdDuration time.Duration // slack送信がこの秒数よりも遅延したらエラー
}

// DeliverySlackLoggerConfigはDelivery本体の同期・一件送信用です。Slack HTTP応答が
// Delivery全体の待ち時間を延ばし得るため、低遅延が必要な出力先には非推奨です。
// internal-log fallbackのenqueue/batch設定はDeliveryInternalLogBatchConfigが所有します。
type DeliverySlackLoggerConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// TimeoutはSlack HTTP clientの上限です。正の値が必須です。
	Timeout time.Duration
	// HTTPClientはSlack API呼び出しに使うHTTP clientです。nilの場合はnet/httpの
	// 既定transportを使います。指定した場合はlogger構築時に設定をcopyして
	// Transport、Jar、CheckRedirectを引き継ぎ、元のclientは変更しません。
	// TimeoutにはこのconfigのTimeoutを使い、元のclientのTimeoutより優先します。
	// Delivery本体とinternal-log fallbackのSlack送信に同じ設定を使います。
	HTTPClient                      *http.Client
	Token                           string
	Channel                         string
	ErrorChannel                    string
	WarnChannel                     string
	InfoChannel                     string
	DebugChannel                    string
	MaxMessageLength                int
	HandlerConfig                   handler.HandlerConfig
	SlackPostWarnThresholdDuration  time.Duration // slack送信がこの秒数よりも遅延したら警告
	SlackPostErrorThresholdDuration time.Duration // slack送信がこの秒数よりも遅延したらエラー
}

type OpenSearchConfig struct {
	OpenSearchLoggerBaseConfig
	OpenSearchFlushManagerConfig
	*OpenSearchRetryableClientCaseTransportErrorConfig
}

type DeliveryOpenSearchConfig struct {
	DeliveryOpenSearchLoggerBaseConfig
	DeliveryOpenSearchFlushManagerConfig
	*OpenSearchRetryableClientCaseTransportErrorConfig
}

type LokiConfig struct {
	LokiLoggerBaseConfig
	LokiFlushManagerConfig
	*LokiRetryableClientCaseTransportErrorConfig
}

type DeliveryLokiConfig struct {
	DeliveryLokiLoggerBaseConfig
	DeliveryLokiFlushManagerConfig
	*LokiRetryableClientCaseTransportErrorConfig
}

type CloudWatchLogsConfig struct {
	CloudWatchLogsLoggerBaseConfig
	CloudWatchLogsFlushManagerConfig
	*CloudWatchLogsRetryableClientCaseTransportErrorConfig
}

type DeliveryCloudWatchLogsConfig struct {
	DeliveryCloudWatchLogsLoggerBaseConfig
	DeliveryCloudWatchLogsFlushManagerConfig
	*CloudWatchLogsRetryableClientCaseTransportErrorConfig
}

type ClickHouseConfig struct {
	ClickHouseLoggerBaseConfig
	ClickHouseFlushManagerConfig
	*ClickHouseRetryableClientCaseTransportErrorConfig
}

type DeliveryClickHouseConfig struct {
	DeliveryClickHouseLoggerBaseConfig
	DeliveryClickHouseFlushManagerConfig
	*ClickHouseRetryableClientCaseTransportErrorConfig
}

type RetryClientMode string

const (
	RetryClientModeNone                   RetryClientMode = "none"
	RetryClientModeRetry                  RetryClientMode = "retry"
	RetryClientModeRetryPolicy            RetryClientMode = "retry_policy"
	RetryClientModeCircuitBreaker         RetryClientMode = "circuit_breaker"
	RetryClientModeRetryWithBreaker       RetryClientMode = "retry_with_breaker"
	RetryClientModeRetryPolicyWithBreaker RetryClientMode = "retry_policy_with_breaker"
)

func ParseRetryClientMode(value string, defaultMode RetryClientMode) RetryClientMode {
	mode := RetryClientMode(strings.ToLower(strings.TrimSpace(value)))
	if mode == "" {
		return defaultMode
	}
	// 未知の値をdefaultへ置き換えると設定誤りを検出できないため、正規化した値を
	// そのまま返します。有効性はValidateRetryClientConfigで検証します。
	return mode
}

// RetryClientSelection は RetryClientMode を、実際に使う client 構成へ解決した結果です。
type RetryClientSelection struct {
	// Mode は env/config で選ばれた retry client mode です。
	Mode RetryClientMode

	// UseRetryPolicyClient は retry policyを適用するRetryPolicyClientを使う場合にtrueです。
	UseRetryPolicyClient bool

	// UseRetryingClient は RetryingClient を使う場合に true です。
	// RetryPolicyClientとは排他です。
	UseRetryingClient bool

	// UseCircuitBreakerClient は専用 CircuitBreakerClient を使う場合に true です。
	// retry併用時はRetryingClientまたはRetryPolicyClientの子としてraw clientを内包します。
	// CircuitBreakerClientは常にopen中の接続を抑制し、cooldown後の1件だけをprobeとして通します。
	UseCircuitBreakerClient bool
}

// UsesWrapper は、選択されたmodeでclient wrapperを1つ以上使用する場合にtrueです。
func (s RetryClientSelection) UsesWrapper() bool {
	return s.UseRetryPolicyClient || s.UseRetryingClient || s.UseCircuitBreakerClient
}

func resolveRetryClientSelection(mode RetryClientMode) RetryClientSelection {
	switch mode {
	case RetryClientModeRetryPolicyWithBreaker:
		return RetryClientSelection{
			Mode:                    mode,
			UseRetryPolicyClient:    true,
			UseCircuitBreakerClient: true,
		}
	case RetryClientModeRetryWithBreaker:
		return RetryClientSelection{
			Mode:                    mode,
			UseRetryingClient:       true,
			UseCircuitBreakerClient: true,
		}
	case RetryClientModeCircuitBreaker:
		return RetryClientSelection{
			Mode:                    mode,
			UseCircuitBreakerClient: true,
		}
	case RetryClientModeRetryPolicy:
		return RetryClientSelection{Mode: mode, UseRetryPolicyClient: true}
	case RetryClientModeRetry:
		return RetryClientSelection{Mode: mode, UseRetryingClient: true}
	case RetryClientModeNone:
		return RetryClientSelection{Mode: mode}
	default:
		// unknown modeはnoneへ補正せず、chain構築時のvalidationへ渡す。
		return RetryClientSelection{Mode: mode}
	}
}

type OpenSearchRetryableClientCaseTransportErrorConfig struct {
	// RetryClientMode は client wrapper 構成を単一値で選びます。
	// none / retry / retry_policy / circuit_breaker / retry_with_breaker / retry_policy_with_breaker を指定できます。
	RetryClientMode RetryClientMode

	MaxAttempts int
	// BaseDelay はリトライごとに加算されるバックオフの基準遅延です。
	BaseDelay time.Duration
	// MaxDelay はバックオフの上限です。
	MaxDelay time.Duration
}

func (c OpenSearchRetryableClientCaseTransportErrorConfig) RetryClientSelection() RetryClientSelection {
	return resolveRetryClientSelection(c.RetryClientMode)
}

type LokiRetryableClientCaseTransportErrorConfig struct {
	// RetryClientMode は client wrapper 構成を単一値で選びます。
	// none / retry / retry_policy / circuit_breaker / retry_with_breaker / retry_policy_with_breaker を指定できます。
	RetryClientMode RetryClientMode

	MaxAttempts int
	// BaseDelay はリトライごとに加算されるバックオフの基準遅延です。
	BaseDelay time.Duration
	// MaxDelay はバックオフの上限です。
	MaxDelay time.Duration
}

func (c LokiRetryableClientCaseTransportErrorConfig) RetryClientSelection() RetryClientSelection {
	return resolveRetryClientSelection(c.RetryClientMode)
}

type CloudWatchLogsRetryableClientCaseTransportErrorConfig struct {
	// RetryClientMode は client wrapper 構成を単一値で選びます。
	// none / retry / retry_policy / circuit_breaker / retry_with_breaker / retry_policy_with_breaker を指定できます。
	RetryClientMode RetryClientMode

	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (c CloudWatchLogsRetryableClientCaseTransportErrorConfig) RetryClientSelection() RetryClientSelection {
	return resolveRetryClientSelection(c.RetryClientMode)
}

type ClickHouseRetryableClientCaseTransportErrorConfig struct {
	// RetryClientMode は client wrapper 構成を単一値で選びます。
	// none / retry / retry_policy / circuit_breaker / retry_with_breaker / retry_policy_with_breaker を指定できます。
	RetryClientMode RetryClientMode

	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (c ClickHouseRetryableClientCaseTransportErrorConfig) RetryClientSelection() RetryClientSelection {
	return resolveRetryClientSelection(c.RetryClientMode)
}

type OpenSearchLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// FlushIntervalはbufferを時間経過でflushする間隔です。正の値が必須です。
	FlushInterval          time.Duration
	MaxBulkDocument        int // バルク送信最大数
	BufferFlushBytes       int // buffer内document body概算値による早期flush閾値
	MaxBulkRequestBytes    int // gzip圧縮前のBulk NDJSON request分割上限
	NoBulkFlushModeEnabled bool
	SamplingConfig         SamplingConfig
	HandlerConfig          handler.HandlerConfig
}

type DeliveryOpenSearchLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig DeliveryRuntimeConfig
	LogBodyConfig LogBodyConfig
	HandlerConfig handler.HandlerConfig
}

type LokiLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// FlushIntervalの契約はOpenSearchLoggerBaseConfig.FlushIntervalと同じです。
	FlushInterval           time.Duration
	MaxEntries              int
	BufferFlushBytes        int // formatter済みline byte概算合計による早期flush閾値
	FallbackQueueMaxEntries int // fallbackが保持できるentry件数のhard上限
	MaxLineBytes            int // format後の1行をUTF-8 byte数で制限する
	MaxPushRequestBytes     int // HTTP圧縮前のPush request bodyをbyte数で制限する
	NoBulkFlushModeEnabled  bool
	SamplingConfig          SamplingConfig
	HandlerConfig           handler.HandlerConfig
}

type DeliveryLokiLoggerBaseConfig struct {
	TimeConfig          TimeConfig
	RuntimeConfig       DeliveryRuntimeConfig
	LogBodyConfig       LogBodyConfig
	MaxLineBytes        int // format後の1行をUTF-8 byte数で制限する
	MaxPushRequestBytes int // HTTP圧縮前のPush request bodyをbyte数で制限する
	HandlerConfig       handler.HandlerConfig
}

type CloudWatchLogsLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// FlushIntervalの契約はOpenSearchLoggerBaseConfig.FlushIntervalと同じです。
	FlushInterval          time.Duration
	BufferFlushMaxEvents   int // buffer flushを開始するevent件数のsoft threshold
	BufferFlushBytes       int // PutLogEvents計算式によるbuffer byte数のsoft threshold
	FallbackQueueMaxEvents int // fallbackが保持できるevent件数のhard上限
	NoBulkFlushModeEnabled bool
	SamplingConfig         SamplingConfig
	HandlerConfig          handler.HandlerConfig
}

type DeliveryCloudWatchLogsLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig DeliveryRuntimeConfig
	LogBodyConfig LogBodyConfig
	HandlerConfig handler.HandlerConfig
}

type ClickHouseLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// FlushIntervalの契約はOpenSearchLoggerBaseConfig.FlushIntervalと同じです。
	FlushInterval           time.Duration
	MaxEntries              int
	BufferFlushBytes        int
	FallbackQueueMaxEntries int
	MaxInsertRequestBytes   int
	SamplingConfig          SamplingConfig
	HandlerConfig           handler.HandlerConfig
}

type DeliveryClickHouseLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig DeliveryRuntimeConfig
	LogBodyConfig LogBodyConfig
	// FlushIntervalの契約はOpenSearchLoggerBaseConfig.FlushIntervalと同じです。
	FlushInterval           time.Duration
	MaxEntries              int
	BufferFlushBytes        int
	FallbackQueueMaxEntries int
	MaxInsertRequestBytes   int
	HandlerConfig           handler.HandlerConfig
}

type OpenSearchFlushManagerConfig struct {
	Index                  string
	IsDebug                bool
	UseOpenTelemetryFormat bool
	FailureStoreConfig     durable.FailureStoreConfig
	Resource               Resource

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	IndexLatencyEnabled        bool
	MaxRetryAttempts           int
	RetryWaitTime              time.Duration
}

// DeliveryOpenSearchFlushManagerConfig は、1件送信時の遅延監視・送信metadataと、
// applicationの送達結果とは独立して非同期実行するfileArchive設定を保持します。
// bulk item再送は通常OpenSearchだけが所有し、HTTP再試行はclient wrapperが所有します。
type DeliveryOpenSearchFlushManagerConfig struct {
	Index                  string
	IsDebug                bool
	UseOpenTelemetryFormat bool
	FailureStoreConfig     durable.DeliveryFailureStoreConfig
	Resource               Resource

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	IndexLatencyEnabled        bool
}

type LokiFlushManagerConfig struct {
	FailureStoreConfig durable.FailureStoreConfig
	Resource           Resource
	// StaticLabelsはappName、loglevel、envに加えて全streamへ付与するlabelです。
	StaticLabels map[string]string

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	PushLatencyEnabled         bool
}

// DeliveryLokiFlushManagerConfig は、1件送信時の遅延監視・送信metadataと、
// applicationの送達結果とは独立して非同期実行するfileArchive設定を保持します。
// HTTP再試行はDeliveryLokiConfigのLokiRetryableClientCaseTransportErrorConfigが所有します。
type DeliveryLokiFlushManagerConfig struct {
	FailureStoreConfig durable.DeliveryFailureStoreConfig
	Resource           Resource
	StaticLabels       map[string]string

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	PushLatencyEnabled         bool
}

type CloudWatchLogsFlushManagerConfig struct {
	FailureStoreConfig durable.FailureStoreConfig
	Resource           Resource
	LogGroupName       string
	LogStreamName      string

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	PutLatencyEnabled          bool
}

// DeliveryCloudWatchLogsFlushManagerConfig は、1件送信時の遅延監視・送信先と、
// applicationの送達結果とは独立して非同期実行するfileArchive設定を保持します。
// HTTP再試行はDeliveryCloudWatchLogsConfigのCloudWatchLogsRetryableClientCaseTransportErrorConfigが所有します。
type DeliveryCloudWatchLogsFlushManagerConfig struct {
	FailureStoreConfig durable.DeliveryFailureStoreConfig
	Resource           Resource
	LogGroupName       string
	LogStreamName      string

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	PutLatencyEnabled          bool
}

type ClickHouseFlushManagerConfig struct {
	FailureStoreConfig durable.FailureStoreConfig
	Resource           Resource

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	InsertLatencyEnabled       bool
}

// DeliveryClickHouseFlushManagerConfig は、applicationの送達結果とは独立して
// 非同期実行するfileArchive設定を保持します。
// retry用spoolはDelivery loggerでは扱いません。
type DeliveryClickHouseFlushManagerConfig struct {
	FailureStoreConfig durable.DeliveryFailureStoreConfig
	Resource           Resource

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	InsertLatencyEnabled       bool
}

// 実行した環境を保持
type Resource struct {
	ServiceName    string
	ServiceVersion string
	HostName       string
	OSType         string
	Environment    string
	RuntimeType    string
	Cloud          *CloudResourceInfo
}

type CloudResourceInfo struct {
	RuntimeInstanceID string
	Provider          string
	Region            string
}
