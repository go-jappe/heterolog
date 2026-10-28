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
	// LogOutputDestinations lists the backends, among those whose settings are
	// held, that are started as regular outputs of application logs. A backend
	// used only by fallback handlers is not listed here, even if its config
	// pointer is set. The config pointer of each listed backend is required. Only
	// an empty list selects the behavior compatible with direct construction,
	// which treats each non-nil config as a regular output.
	LogOutputDestinations LogOutputDestinations
	Timeout               time.Duration
	// MaxLogEntryBytes is the maximum size of a single application log entry that
	// can be accepted before it is distributed to the backends. The message, error
	// and fields are counted as JSON, and the value must be positive.
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
	// InternalLogDestinations holds destination settings used only by internal
	// log handlers. A nil backend uses the regular settings, and only a non-nil
	// backend replaces the destination identity.
	InternalLogDestinations *InternalLogDestinationConfig
	Resource                *otel.Resource
}

type AsyncBestEffortMultiLoggerConfig struct {
	// LogOutputDestinations lists the backends, among those whose settings are
	// held, that are started as regular outputs of application logs. A backend
	// used only by fallback handlers is not listed here, even if its config
	// pointer is set. The config pointer of each listed backend is required. Only
	// an empty list selects the behavior compatible with direct construction,
	// which treats each non-nil config as a regular output.
	LogOutputDestinations LogOutputDestinations
	Timeout               time.Duration
	// MaxLogEntryBytes is the maximum size of a single application log entry that
	// can be accepted before a goroutine is started. Following the best-effort
	// contract, an entry over the limit is dropped without being passed to the
	// children.
	MaxLogEntryBytes int
	// MaxParallel is the number of child Logger operations that can run at the
	// same time. Reaching the limit alone does not drop a child; instead,
	// backpressure is applied to the accepting side until a running child
	// completes.
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
	// LogOutputDestinations lists the backends, among those whose settings are
	// held, that are started as regular outputs of application logs. A backend
	// used only by fallback handlers is not listed here, even if its config
	// pointer is set. The config pointer of each listed backend is required. Only
	// an empty list selects the behavior compatible with direct construction,
	// which treats each non-nil config as a regular output.
	LogOutputDestinations LogOutputDestinations
	Timeout               time.Duration
	// MaxLogEntryBytes is the maximum size of a single application log entry that
	// can be accepted before it is distributed to the backends. When an entry
	// exceeds it, every target is returned as DeliveryRejected.
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
	// InternalLogBatchConfig holds the enqueue and batch settings used only by the
	// internal-log fallback of the Delivery logger. They do not apply to the
	// Delivery logger's own sending or to the sync and async loggers.
	InternalLogBatchConfig DeliveryInternalLogBatchConfig

	Resource *otel.Resource
}

// DeliveryInternalLogBatchConfig holds the enqueue and batch settings for each
// destination of the Delivery internal-log fallback. ClickHouse uses the
// existing Delivery ClickHouse settings.
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
	// MaxBulkDocument limits both the number of documents in one batch and the
	// enqueue queue.
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
	// MaxLogsPerNotification limits both the number of logs in one notification
	// and the enqueue queue.
	MaxLogsPerNotification int
	// MaxRestThreshold is the same idle time as SlackLoggerConfig.MaxRestThreshold.
	MaxRestThreshold time.Duration
}

// InternalLogDestinationConfig holds backend settings used only by the internal
// logs of the sync and async loggers. It holds only the backends that use a
// destination different from the regular settings. A backend that has client
// settings takes its logger settings and client settings as a pair, and setting
// only one of them is a configuration error.
//
// File can have retention, compression and purge policies, but it is treated as
// a terminal sink for internal logs. The HandlerConfig of its appendlog
// lifecycle is not run, so it causes no secondary internal log routing.
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

// ToLoggerConfig converts the internal log destinations into the common
// configuration that the handler factory handles.
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

// DeliveryInternalLogDestinationConfig holds backend settings used only by the
// internal logs of the Delivery logger. File follows the same terminal sink
// contract as for sync and async, and a backend that has client settings also
// takes its logger settings and client settings as a pair, as for sync and
// async.
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

// ToLoggerConfig converts the Delivery internal log destinations into the
// configuration for the handler factory.
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
	// Live, when set, makes the output use the values of Live, which can be
	// changed while the output runs, instead of LogLevel and the SamplingConfig
	// of the same backend configuration. New and NewAsync of heterolog create one
	// for each output that does not set Live. Outputs that share one Live change
	// together; the admin API of heterolog does not accept loggers whose outputs
	// share one.
	Live *LiveSettings
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
	// LoggingByRequestIDRate is checked when both IsSamplingEnabled and
	// EnableLoggingByRequestID are true. It must then be at least 1 and at most
	// 2^32-1; where int is 32 bits wide, the type itself limits it to 2^31-1.
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
	// LifecycleMode is treated as inline when empty, which runs maintenance in
	// the write path of the FileLogger.
	LifecycleMode fsstore.FileLifecycleMode
	// LifecycleRunnerSettings holds the periodic loop and the optional
	// Supervisor settings of runner mode.
	LifecycleRunnerSettings fsstore.AppendLogRunnerConfig
	TimeConfig              TimeConfig
	RuntimeConfig           RuntimeConfig
	LogBodyConfig           LogBodyConfig
	SamplingConfig          SamplingConfig
	HandlerConfig           handler.HandlerConfig
	// LifecycleHandlerConfig holds the destination selectors to which the results
	// of the periodic runs in runner mode are delegated. It holds the selectors
	// of each level independently and delegates according to the severity of the
	// internal log that occurs.
	LifecycleHandlerConfig handler.HandlerConfig
}

type DeliveryFileLoggerConfig struct {
	FsstoreConfig fsstore.DeliveryFileWriteConfig
	// LifecycleMode starts no maintenance when empty. Delivery has no inline
	// processing, so specify runner explicitly when maintenance is needed.
	LifecycleMode           fsstore.FileLifecycleMode
	LifecycleRunnerSettings DeliveryFileLifecycleRunnerSettings
	TimeConfig              TimeConfig
	RuntimeConfig           DeliveryRuntimeConfig
	LogBodyConfig           LogBodyConfig
	HandlerConfig           handler.HandlerConfig
}

// DeliveryFileLifecycleRunnerSettings holds only the periodic loop, retention,
// compression, upload and internal log routing settings that are specific to
// runner mode and cannot be derived from the write settings of the delivery
// file.
type DeliveryFileLifecycleRunnerSettings struct {
	UsageCacheTTL  time.Duration
	MaxFileBackups int
	MaxFileAge     int
	MaxDirSize     uint64
	// ErrorPurge and DebugPurge inherit the retention settings of regular logs
	// when nil. When a directory is shared, the size limit is resolved by the
	// same rules as in fsstore.FileLifecycleConfig.
	ErrorPurge         *fsstore.FilePurgeConfig
	DebugPurge         *fsstore.FilePurgeConfig
	EnableFileCompress bool
	CompressFormat     fsstore.CompressFormat
	// CompressionCodec is a custom compression format added programmatically.
	// When it is nil, the built-in codec for CompressFormat is used.
	CompressionCodec               fsstore.CompressionCodec
	CompressWarnThresholdDuration  time.Duration
	CompressErrorThresholdDuration time.Duration
	ObjectStorageUploadConfig      objectstorage.LogFileUploadConfig
	ArchiveCheckpointConfig        fsstore.ArchiveCheckpointConfig
	// RunnerConfig holds the periodic loop and the optional Supervisor settings
	// of runner mode.
	RunnerConfig fsstore.AppendLogRunnerConfig
	// LifecycleHandlerConfig has the same per-level contract as in
	// FileLoggerConfig.
	LifecycleHandlerConfig handler.HandlerConfig
}

// LifecycleRunnerConfig derives the settings for the dedicated runner from the
// canonical file logger configuration. It keeps no copies of the path, rotation,
// duration and retry settings, so it does not drift from changes the caller
// makes to the configuration.
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

// LifecycleRunnerConfig combines the write settings of the delivery file with
// the runner-specific settings. FsstoreConfig is used as the single source of
// truth for the layout, rotation, write duration and retry.
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
	// Timeout is the time limit of the Slack HTTP client. It must be positive.
	Timeout time.Duration
	// HTTPClient is the HTTP client used to call the Slack API. When it is nil,
	// the default transport of net/http is used. When it is set, its settings
	// are copied when the logger is built, carrying over Transport and Jar, and
	// the original client is not modified. The Timeout of this config is used
	// as the timeout and takes precedence over the Timeout of the original
	// client. Slack API calls do not follow redirects, whatever the
	// CheckRedirect of the original client, and treat a 3xx response as a
	// failure. The same settings are used for the Slack posts of the regular
	// output and of the internal-log fallback.
	HTTPClient             *http.Client
	Token                  string
	Channel                string
	ErrorChannel           string
	WarnChannel            string
	InfoChannel            string
	DebugChannel           string
	MaxLogsPerNotification int
	// MaxRestThreshold is the idle time that resets the notification intervals
	// when logs stop arriving. It must be positive. A log that arrives after
	// more than this time schedules an immediate send together with the waiting
	// logs. If a notification is being sent, the send waits for it to complete.
	MaxRestThreshold time.Duration
	// Intervals are the waits used to schedule the successive buffered sends to
	// Slack after the first immediate notification. Each element must be
	// positive.
	Intervals                       []time.Duration
	MaxMessageLength                int
	SamplingConfig                  SamplingConfig
	HandlerConfig                   handler.HandlerConfig
	SlackPostWarnThresholdDuration  time.Duration // warning when a Slack post takes longer than this
	SlackPostErrorThresholdDuration time.Duration // error when a Slack post takes longer than this
}

// DeliverySlackLoggerConfig configures the Delivery logger's own synchronous
// sending of one entry at a time. A Slack HTTP response can make the whole
// Delivery wait longer, so it is not recommended for outputs that need low
// latency. The enqueue and batch settings of the internal-log fallback are owned
// by DeliveryInternalLogBatchConfig.
type DeliverySlackLoggerConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// Timeout is the time limit of the Slack HTTP client. It must be positive.
	Timeout time.Duration
	// HTTPClient is the HTTP client used to call the Slack API. When it is nil,
	// the default transport of net/http is used. When it is set, its settings
	// are copied when the logger is built, carrying over Transport and Jar, and
	// the original client is not modified. The Timeout of this config is used
	// as the timeout and takes precedence over the Timeout of the original
	// client. Slack API calls do not follow redirects, whatever the
	// CheckRedirect of the original client, and treat a 3xx response as a
	// failure. The same settings are used for the Slack posts of the Delivery
	// logger's own sending and of the internal-log fallback.
	HTTPClient                      *http.Client
	Token                           string
	Channel                         string
	ErrorChannel                    string
	WarnChannel                     string
	InfoChannel                     string
	DebugChannel                    string
	MaxMessageLength                int
	HandlerConfig                   handler.HandlerConfig
	SlackPostWarnThresholdDuration  time.Duration // warning when a Slack post takes longer than this
	SlackPostErrorThresholdDuration time.Duration // error when a Slack post takes longer than this
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

// RetryClientSelection is the result of resolving a RetryClientMode into the
// client composition that is actually used.
type RetryClientSelection struct {
	// Mode is the retry client mode selected in env or config.
	Mode RetryClientMode

	// UseRetryPolicyClient is true when the RetryPolicyClient, which applies a
	// retry policy, is used.
	UseRetryPolicyClient bool

	// UseRetryingClient is true when RetryingClient is used. It is mutually
	// exclusive with RetryPolicyClient.
	UseRetryingClient bool

	// UseCircuitBreakerClient is true when the dedicated CircuitBreakerClient is
	// used. When retries are also used, the CircuitBreakerClient wraps the raw
	// client as the child of RetryingClient or RetryPolicyClient. The
	// CircuitBreakerClient always suppresses connections while it is open, and
	// lets only one request through as a probe after the cooldown.
	UseCircuitBreakerClient bool
}

// UsesWrapper reports whether the selected mode uses one or more client
// wrappers.
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
	// RetryClientMode selects the client wrapper composition with a single
	// value. It accepts none / retry / retry_policy / circuit_breaker /
	// retry_with_breaker / retry_policy_with_breaker.
	RetryClientMode RetryClientMode

	// MaxRetries is how many times a failed request is retried after the first
	// request. 0 sends once without retrying. In the modes that retry, a
	// negative value is reported as a ConfigFieldError with the kind
	// ErrNegativeSetting.
	MaxRetries int
	// BaseDelay is the base backoff delay, which is added for each retry.
	BaseDelay time.Duration
	// MaxDelay is the upper limit of the backoff.
	MaxDelay time.Duration
}

func (c OpenSearchRetryableClientCaseTransportErrorConfig) RetryClientSelection() RetryClientSelection {
	return resolveRetryClientSelection(c.RetryClientMode)
}

type LokiRetryableClientCaseTransportErrorConfig struct {
	// RetryClientMode selects the client wrapper composition with a single
	// value. It accepts none / retry / retry_policy / circuit_breaker /
	// retry_with_breaker / retry_policy_with_breaker.
	RetryClientMode RetryClientMode

	// MaxRetries is how many times a failed request is retried after the first
	// request. 0 sends once without retrying. In the modes that retry, a
	// negative value is reported as a ConfigFieldError with the kind
	// ErrNegativeSetting.
	MaxRetries int
	// BaseDelay is the base backoff delay, which is added for each retry.
	BaseDelay time.Duration
	// MaxDelay is the upper limit of the backoff.
	MaxDelay time.Duration
}

func (c LokiRetryableClientCaseTransportErrorConfig) RetryClientSelection() RetryClientSelection {
	return resolveRetryClientSelection(c.RetryClientMode)
}

type CloudWatchLogsRetryableClientCaseTransportErrorConfig struct {
	// RetryClientMode selects the client wrapper composition with a single
	// value. It accepts none / retry / retry_policy / circuit_breaker /
	// retry_with_breaker / retry_policy_with_breaker.
	RetryClientMode RetryClientMode

	// MaxRetries is how many times a failed request is retried after the first
	// request. 0 sends once without retrying. In the modes that retry, a
	// negative value is reported as a ConfigFieldError with the kind
	// ErrNegativeSetting.
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func (c CloudWatchLogsRetryableClientCaseTransportErrorConfig) RetryClientSelection() RetryClientSelection {
	return resolveRetryClientSelection(c.RetryClientMode)
}

type ClickHouseRetryableClientCaseTransportErrorConfig struct {
	// RetryClientMode selects the client wrapper composition with a single
	// value. It accepts none / retry / retry_policy / circuit_breaker /
	// retry_with_breaker / retry_policy_with_breaker.
	RetryClientMode RetryClientMode

	// MaxRetries is how many times a failed request is retried after the first
	// request. 0 sends once without retrying. In the modes that retry, a
	// negative value is reported as a ConfigFieldError with the kind
	// ErrNegativeSetting.
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func (c ClickHouseRetryableClientCaseTransportErrorConfig) RetryClientSelection() RetryClientSelection {
	return resolveRetryClientSelection(c.RetryClientMode)
}

type OpenSearchLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// FlushInterval is the interval at which the buffer is flushed as time
	// passes. It must be positive.
	FlushInterval          time.Duration
	MaxBulkDocument        int // maximum number of documents per bulk send
	BufferFlushBytes       int // early-flush threshold on the estimated size of document bodies in the buffer
	MaxBulkRequestBytes    int // size limit for splitting Bulk NDJSON requests, before gzip compression
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
	// FlushInterval has the same contract as
	// OpenSearchLoggerBaseConfig.FlushInterval.
	FlushInterval           time.Duration
	MaxEntries              int
	BufferFlushBytes        int // early-flush threshold on the estimated total bytes of formatted lines
	FallbackQueueMaxEntries int // hard limit on the number of entries the fallback can hold
	MaxLineBytes            int // limit on each line after formatting, in UTF-8 bytes
	MaxPushRequestBytes     int // limit on the Push request body before HTTP compression, in bytes
	NoBulkFlushModeEnabled  bool
	SamplingConfig          SamplingConfig
	HandlerConfig           handler.HandlerConfig
}

type DeliveryLokiLoggerBaseConfig struct {
	TimeConfig          TimeConfig
	RuntimeConfig       DeliveryRuntimeConfig
	LogBodyConfig       LogBodyConfig
	MaxLineBytes        int // limit on each line after formatting, in UTF-8 bytes
	MaxPushRequestBytes int // limit on the Push request body before HTTP compression, in bytes
	HandlerConfig       handler.HandlerConfig
}

type CloudWatchLogsLoggerBaseConfig struct {
	TimeConfig    TimeConfig
	RuntimeConfig RuntimeConfig
	LogBodyConfig LogBodyConfig
	// FlushInterval has the same contract as
	// OpenSearchLoggerBaseConfig.FlushInterval.
	FlushInterval          time.Duration
	BufferFlushMaxEvents   int // soft threshold on the number of events that starts a buffer flush
	BufferFlushBytes       int // soft threshold on buffer bytes computed with the PutLogEvents formula
	FallbackQueueMaxEvents int // hard limit on the number of events the fallback can hold
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
	// FlushInterval has the same contract as
	// OpenSearchLoggerBaseConfig.FlushInterval.
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
	// FlushInterval has the same contract as
	// OpenSearchLoggerBaseConfig.FlushInterval.
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
	// MaxItemRetries is how many times the items that a bulk response marks as
	// retryable are sent again after the first bulk request. 0 sends them once
	// without retrying, and a negative value is rejected when the logger is
	// built.
	MaxItemRetries int
	// RetryWaitTime is the wait before the retryable items are sent again. A
	// negative value is rejected when the logger is built.
	RetryWaitTime time.Duration
}

// DeliveryOpenSearchFlushManagerConfig holds the latency monitoring and send
// metadata of single-entry sends, and the settings of the fileArchive, which
// saves an entry whose send failed. The Delivery logger waits for the save and
// includes its result in the DeliveryReport. Resending bulk items is owned only
// by the regular OpenSearch logger, and HTTP retries are owned by the client
// wrapper.
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
	// StaticLabels are the labels added to every stream in addition to appName,
	// loglevel and env. A label with one of those three names is ignored.
	StaticLabels map[string]string

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	PushLatencyEnabled         bool
}

// DeliveryLokiFlushManagerConfig holds the latency monitoring and send metadata
// of single-entry sends, and the settings of the fileArchive, which saves an
// entry whose send failed. The Delivery logger waits for the save and includes
// its result in the DeliveryReport. HTTP retries are owned by the
// LokiRetryableClientCaseTransportErrorConfig of DeliveryLokiConfig.
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

// DeliveryCloudWatchLogsFlushManagerConfig holds the latency monitoring and
// destination of single-entry sends, and the settings of the fileArchive, which
// saves an entry whose send failed. The Delivery logger waits for the save and
// includes its result in the DeliveryReport. HTTP retries are owned by the
// CloudWatchLogsRetryableClientCaseTransportErrorConfig of
// DeliveryCloudWatchLogsConfig.
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

// DeliveryClickHouseFlushManagerConfig holds the settings of the fileArchive,
// which runs asynchronously and independently of the delivery results for the
// application. The Delivery logger does not handle the spool for retries.
type DeliveryClickHouseFlushManagerConfig struct {
	FailureStoreConfig durable.DeliveryFailureStoreConfig
	Resource           Resource

	PostWarnThresholdDuration  time.Duration
	PostErrorThresholdDuration time.Duration
	InsertLatencyEnabled       bool
}

// Resource holds the environment in which the application runs.
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
