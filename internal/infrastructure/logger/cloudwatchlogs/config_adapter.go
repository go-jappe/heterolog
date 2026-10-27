package cloudwatchlogs

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// flushManagerRuntimeConfigAdapter は通常 logger と delivery logger が共有する送信設定です。
type flushManagerRuntimeConfigAdapter struct {
	logGroupName                string
	logStreamName               string
	logStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting
	postWarnThresholdDuration   time.Duration
	postErrorThresholdDuration  time.Duration
	putLatencyEnabled           bool
}

// flushManagerConfigAdapter は通常 logger の durable 設定を含む adapter です。
type flushManagerConfigAdapter struct {
	flushManagerRuntimeConfigAdapter
	failureStoreConfig loggerdurable.FailureStoreConfigProvider
}

// deliveryFlushManagerConfigAdapter はDelivery専用のfile archive設定を含むadapterです。
type deliveryFlushManagerConfigAdapter struct {
	flushManagerRuntimeConfigAdapter
	failureStoreConfig loggerdurable.DeliveryFailureStoreConfigProvider
}

// retryConfigAdapterはapplicationのCloudWatch Logs retry設定を、logbackendのclient
// chain構築契約へ適合させます。
type retryConfigAdapter struct {
	cfg pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig
}

// NewRetryConfigProviderはclient chain構築用のretry設定を返します。
// 設定自体がnil、またはmodeが明示的なnoneの場合だけnilを返します。
// 不明なmode（空文字を含む）は保持し、chain構築時の検証で拒否します。
func NewRetryConfigProvider(
	cfg *pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig,
) wrapper.RetryConfigProvider {
	if cfg == nil || cfg.RetryClientMode == pkglogger.RetryClientModeNone {
		return nil
	}
	return retryConfigAdapter{cfg: *cfg}
}

// MaxAttemptsは、wrapperが数える初回を含む試行回数です。設定は再試行の回数なので、
// 初回の1回を足します。負の再試行回数は、adapterを作る前の設定検証で拒否しています。
func (a retryConfigAdapter) MaxAttempts() int {
	return a.cfg.MaxRetries + 1
}

func (a retryConfigAdapter) BaseDelay() time.Duration {
	return a.cfg.BaseDelay
}

func (a retryConfigAdapter) MaxDelay() time.Duration {
	return a.cfg.MaxDelay
}

func (a retryConfigAdapter) RetryClientSelection() pkglogger.RetryClientSelection {
	return a.cfg.RetryClientSelection()
}

// NewFlushManagerConfigAdapterは送信設定とapplication側のstream routing granularityから
// flush manager用adapterを作ります。
func NewFlushManagerConfigAdapter(
	cfg pkglogger.CloudWatchLogsFlushManagerConfig,
	routingGranularity pkgcloudwatchlogs.StreamRouting,
) flushManagerConfigAdapter {
	return flushManagerConfigAdapter{
		flushManagerRuntimeConfigAdapter: newFlushManagerRuntimeConfigAdapter(
			cfg.LogGroupName,
			cfg.LogStreamName,
			routingGranularity,
			cfg.PostWarnThresholdDuration,
			cfg.PostErrorThresholdDuration,
			cfg.PutLatencyEnabled,
		),
		failureStoreConfig: loggerdurable.NewFailureStoreConfigAdapter(cfg.FailureStoreConfig),
	}
}

// NewDeliveryFlushManagerConfigAdapterはDelivery専用送信設定とapplication側のstream routing
// granularityからflush manager用adapterを作ります。
func NewDeliveryFlushManagerConfigAdapter(
	cfg pkglogger.DeliveryCloudWatchLogsFlushManagerConfig,
	routingGranularity pkgcloudwatchlogs.StreamRouting,
) deliveryFlushManagerConfigAdapter {
	return deliveryFlushManagerConfigAdapter{
		flushManagerRuntimeConfigAdapter: newFlushManagerRuntimeConfigAdapter(
			cfg.LogGroupName,
			cfg.LogStreamName,
			routingGranularity,
			cfg.PostWarnThresholdDuration,
			cfg.PostErrorThresholdDuration,
			cfg.PutLatencyEnabled,
		),
		failureStoreConfig: loggerdurable.NewDeliveryFailureStoreConfigAdapter(cfg.FailureStoreConfig),
	}
}

func newFlushManagerRuntimeConfigAdapter(
	logGroupName string,
	logStreamName string,
	logStreamRoutingGranularity pkgcloudwatchlogs.StreamRouting,
	postWarnThresholdDuration time.Duration,
	postErrorThresholdDuration time.Duration,
	putLatencyEnabled bool,
) flushManagerRuntimeConfigAdapter {
	return flushManagerRuntimeConfigAdapter{
		logGroupName:                logGroupName,
		logStreamName:               logStreamName,
		logStreamRoutingGranularity: logStreamRoutingGranularity.Normalize(),
		postWarnThresholdDuration:   postWarnThresholdDuration,
		postErrorThresholdDuration:  postErrorThresholdDuration,
		putLatencyEnabled:           putLatencyEnabled,
	}
}

// NewCloudWatchLogsLoggerAdapterはlogger設定とapplication側のstream routing granularityを、
// client設定、flush manager設定、retry設定へcomposition root向けに分解します。
func NewCloudWatchLogsLoggerAdapter(
	cfg pkglogger.LoggerConfig,
	routingGranularity pkgcloudwatchlogs.StreamRouting,
) (
	CloudWatchLogsConfigProvider,
	*pkgcloudwatchlogs.Config,
	CloudWatchLogsFlushManagerConfigProvider,
	resource.ResourceProvider,
	wrapper.RetryConfigProvider,
) {
	loggerConfig := NewCloudWatchLogsLoggerConfigAdapter(cfg.CloudWatchLogs.CloudWatchLogsLoggerBaseConfig)
	clientConfig := cfg.CloudWatchLogsClient
	fmConfig := NewFlushManagerConfigAdapter(
		cfg.CloudWatchLogs.CloudWatchLogsFlushManagerConfig,
		routingGranularity,
	)
	resourceProvider := resource.NewResourceAdapter(cfg.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.Resource)
	retryCfg := NewRetryConfigProvider(
		cfg.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig,
	)

	return loggerConfig, clientConfig, fmConfig, resourceProvider, retryCfg
}

// FailureStoreConfig は failure handler 構築に使う durable 設定を返します。
func (a flushManagerConfigAdapter) FailureStoreConfig() loggerdurable.FailureStoreConfigProvider {
	return a.failureStoreConfig
}

// DeliveryFailureStoreConfig はDelivery専用のfailure store設定を返します。
func (a deliveryFlushManagerConfigAdapter) DeliveryFailureStoreConfig() loggerdurable.DeliveryFailureStoreConfigProvider {
	return a.failureStoreConfig
}

// LogGroupName は CloudWatch Logs の送信先 log group 名を返します。
func (a flushManagerRuntimeConfigAdapter) LogGroupName() string {
	return a.logGroupName
}

// LogStreamName は CloudWatch Logs の送信先 log stream 名を返します。
func (a flushManagerRuntimeConfigAdapter) LogStreamName() string {
	return a.logStreamName
}

// LogStreamRoutingGranularityは基準stream名へevent発生日時のsuffixを付けるrouting粒度を返します。
func (a flushManagerRuntimeConfigAdapter) LogStreamRoutingGranularity() pkgcloudwatchlogs.StreamRouting {
	return a.logStreamRoutingGranularity
}

// PostWarnThresholdDuration は PutLogEvents 遅延 warning の閾値を返します。
func (a flushManagerRuntimeConfigAdapter) PostWarnThresholdDuration() time.Duration {
	return a.postWarnThresholdDuration
}

// PostErrorThresholdDuration は PutLogEvents 遅延 error の閾値を返します。
func (a flushManagerRuntimeConfigAdapter) PostErrorThresholdDuration() time.Duration {
	return a.postErrorThresholdDuration
}

// PutLatencyEnabled は PutLogEvents latency 監視が有効かを返します。
func (a flushManagerRuntimeConfigAdapter) PutLatencyEnabled() bool {
	return a.putLatencyEnabled
}

type cloudWatchLogsLoggerConfigAdapter struct {
	cfg            pkglogger.CloudWatchLogsLoggerBaseConfig
	timeConfig     loggertime.TimeConfigProvider
	runtimeConfig  RuntimeConfigProvider
	logBodyConfig  LogBodyConfigProvider
	samplingConfig sampling.SamplingConfigProvider
	handlerConfig  handler.ConfigProvider
}

// NewCloudWatchLogsLoggerConfigAdapter は通常 CloudWatch Logs logger 用の設定 adapter を作ります。
func NewCloudWatchLogsLoggerConfigAdapter(cfg pkglogger.CloudWatchLogsLoggerBaseConfig) *cloudWatchLogsLoggerConfigAdapter {
	return &cloudWatchLogsLoggerConfigAdapter{
		cfg:            cfg,
		timeConfig:     loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		runtimeConfig:  NewRuntimeConfigAdapter(cfg.RuntimeConfig),
		logBodyConfig:  NewLogBodyConfigAdapter(cfg.LogBodyConfig),
		samplingConfig: sampling.NewConfigProvider(cfg.SamplingConfig, cfg.RuntimeConfig.Live),
		handlerConfig:  handler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

// NewDeliveryCloudWatchLogsLoggerConfigAdapterはDelivery本体の同期・一件送信用設定を
// providerへ変換します。internal-log fallbackのbatch設定には使用しません。
func NewDeliveryCloudWatchLogsLoggerConfigAdapter(cfg pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig) *cloudWatchLogsLoggerConfigAdapter {
	return NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		TimeConfig: cfg.TimeConfig,
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  cfg.RuntimeConfig.AppName,
			IsDebug:  cfg.RuntimeConfig.IsDebug,
			Target:   cfg.RuntimeConfig.Target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig:          cfg.LogBodyConfig,
		BufferFlushMaxEvents:   1,
		BufferFlushBytes:       1,
		FallbackQueueMaxEvents: 1,
		NoBulkFlushModeEnabled: true,
		HandlerConfig:          cfg.HandlerConfig,
	})
}

// NewDeliveryCloudWatchLogsFallbackLoggerConfigAdapterはDelivery internal-log fallback
// 専用のenqueue/batch設定を通常logger providerへ変換します。
func NewDeliveryCloudWatchLogsFallbackLoggerConfigAdapter(
	cfg pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig,
	batchCfg pkglogger.DeliveryInternalLogCloudWatchLogsBatchConfig,
) *cloudWatchLogsLoggerConfigAdapter {
	return NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		TimeConfig: cfg.TimeConfig,
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  cfg.RuntimeConfig.AppName,
			IsDebug:  cfg.RuntimeConfig.IsDebug,
			Target:   cfg.RuntimeConfig.Target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig:          cfg.LogBodyConfig,
		FlushInterval:          batchCfg.FlushInterval,
		BufferFlushMaxEvents:   batchCfg.BufferFlushMaxEvents,
		BufferFlushBytes:       batchCfg.BufferFlushBytes,
		FallbackQueueMaxEvents: batchCfg.FallbackQueueMaxEvents,
		NoBulkFlushModeEnabled: false,
	})
}

// Timeはlog eventとlog stream切替に使用するtimezone設定を返します。
func (a *cloudWatchLogsLoggerConfigAdapter) Time() loggertime.TimeConfigProvider {
	return a.timeConfig
}

// Runtime は runtime 設定 provider を返します。
func (a *cloudWatchLogsLoggerConfigAdapter) Runtime() RuntimeConfigProvider {
	return a.runtimeConfig
}

// LogBody は log 本文設定 provider を返します。
func (a *cloudWatchLogsLoggerConfigAdapter) LogBody() LogBodyConfigProvider {
	return a.logBodyConfig
}

// FlushInterval は bulk flush の間隔を返します。
func (a *cloudWatchLogsLoggerConfigAdapter) FlushInterval() time.Duration {
	return a.cfg.FlushInterval
}

// BufferFlushMaxEventsはbuffer flushを開始するLogEntry件数を返します。切り離したbufferは
// FlushManager以下のCloudWatch Logs request plannerがPutLogEvents単位へ分割します。
func (a *cloudWatchLogsLoggerConfigAdapter) BufferFlushMaxEvents() int {
	return a.cfg.BufferFlushMaxEvents
}

// BufferFlushBytesはAWSのPutLogEvents size公式で概算したbufferの
// 早期flush条件を返します。物理requestの分割上限ではありません。
func (a *cloudWatchLogsLoggerConfigAdapter) BufferFlushBytes() int {
	return a.cfg.BufferFlushBytes
}

// FallbackQueueMaxEventsはfallbackが保持するhard queue件数を返します。
func (a *cloudWatchLogsLoggerConfigAdapter) FallbackQueueMaxEvents() int {
	return a.cfg.FallbackQueueMaxEvents
}

// NoBulkFlushModeEnabledはbufferへ蓄積せず、log呼び出しごとに1件を送信する設定を返します。
func (a *cloudWatchLogsLoggerConfigAdapter) NoBulkFlushModeEnabled() bool {
	return a.cfg.NoBulkFlushModeEnabled
}

// Sampling は sampling 設定 provider を返します。
func (a *cloudWatchLogsLoggerConfigAdapter) Sampling() sampling.SamplingConfigProvider {
	return a.samplingConfig
}

// Handler は logger handler 設定 provider を返します。
func (a *cloudWatchLogsLoggerConfigAdapter) Handler() handler.ConfigProvider {
	return a.handlerConfig
}

// RuntimeConfigAdapter は pkg/logger の runtime 設定をこの package の interface に合わせます。
type RuntimeConfigAdapter struct {
	appName  string
	isDebug  bool
	target   string
	logLevel pkglogger.LogLevel
	// liveを設定した場合は、logLevelの代わりにその現在のlevelを使います。
	live *pkglogger.LiveSettings
}

// NewRuntimeConfigAdapter は runtime 設定 adapter を作ります。
func NewRuntimeConfigAdapter(cfg pkglogger.RuntimeConfig) RuntimeConfigAdapter {
	return RuntimeConfigAdapter{
		appName:  cfg.AppName,
		isDebug:  cfg.IsDebug,
		target:   cfg.Target,
		logLevel: cfg.LogLevel,
		live:     cfg.Live,
	}
}

// AppName は application 名を返します。
func (a RuntimeConfigAdapter) AppName() string {
	return a.appName
}

// IsDebug は debug mode かを返します。
func (a RuntimeConfigAdapter) IsDebug() bool {
	return a.isDebug
}

// Target は logger target 名を返します。
func (a RuntimeConfigAdapter) Target() string {
	return a.target
}

// LogLevel は出力対象の最小 level を返します。
func (a RuntimeConfigAdapter) LogLevel() pkglogger.LogLevel {
	if a.live != nil {
		return a.live.Level()
	}
	return a.logLevel
}

// LogBodyConfigAdapter は pkg/logger の本文設定をこの package の interface に合わせます。
type LogBodyConfigAdapter struct {
	logLineDateFormat        string
	errorTraceDepth          int
	errorTraceDepthDebugMode int
}

// NewLogBodyConfigAdapter は log body 設定 adapter を作ります。
func NewLogBodyConfigAdapter(cfg pkglogger.LogBodyConfig) LogBodyConfigAdapter {
	return LogBodyConfigAdapter{
		logLineDateFormat:        cfg.LogLineDateFormat,
		errorTraceDepth:          cfg.ErrorTraceDepth,
		errorTraceDepthDebugMode: cfg.ErrorTraceDepthDebugMode,
	}
}

// LogLineDateFormat は formatter が使う時刻 format を返します。
func (a LogBodyConfigAdapter) LogLineDateFormat() string {
	return a.logLineDateFormat
}

// ErrorTraceDepth は通常 mode の stack trace depth を返します。
func (a LogBodyConfigAdapter) ErrorTraceDepth() int {
	return a.errorTraceDepth
}

// ErrorTraceDepthDebugMode は debug mode の stack trace depth を返します。
func (a LogBodyConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.errorTraceDepthDebugMode
}

// stackTraceConfigAdapter は stack trace 収集に必要な設定だけを切り出します。
type stackTraceConfigAdapter struct {
	isDebug                  bool
	errorTraceDepth          int
	errorTraceDepthDebugMode int
}

// NewStackTraceConfigAdapter は LogEntry に caller 情報を付けるための設定 adapter を作ります。
func NewStackTraceConfigAdapter(cfg CloudWatchLogsConfigProvider) logging.StackTraceConfigProvider {
	return &stackTraceConfigAdapter{
		isDebug:                  cfg.Runtime().IsDebug(),
		errorTraceDepth:          cfg.LogBody().ErrorTraceDepth(),
		errorTraceDepthDebugMode: cfg.LogBody().ErrorTraceDepthDebugMode(),
	}
}

// IsDebug は stack trace depth 切り替え用の debug mode を返します。
func (a *stackTraceConfigAdapter) IsDebug() bool {
	return a.isDebug
}

// ErrorTraceDepth は通常 mode の stack trace depth を返します。
func (a *stackTraceConfigAdapter) ErrorTraceDepth() int {
	return a.errorTraceDepth
}

// ErrorTraceDepthDebugMode は debug mode の stack trace depth を返します。
func (a *stackTraceConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.errorTraceDepthDebugMode
}
