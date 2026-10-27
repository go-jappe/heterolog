package clickhouse

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/wrapper"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type flushManagerRuntimeConfigAdapter struct {
	postWarnThresholdDuration  time.Duration
	postErrorThresholdDuration time.Duration
	insertLatencyEnabled       bool
}

type flushManagerConfigAdapter struct {
	flushManagerRuntimeConfigAdapter
	failureStoreConfig loggerdurable.FailureStoreConfigProvider
}

type deliveryFlushManagerConfigAdapter struct {
	flushManagerRuntimeConfigAdapter
	failureStoreConfig loggerdurable.DeliveryFailureStoreConfigProvider
}

// retryConfigAdapterはapplicationのClickHouse retry設定を、logbackendのclient chain構築契約へ
// 適合させます。
type retryConfigAdapter struct {
	cfg pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig
}

// NewRetryConfigProviderはclient chain構築用のretry設定を返します。
// 設定自体がnil、またはmodeが明示的なnoneの場合だけnilを返します。
// 不明なmode（空文字を含む）は保持し、chain構築時の検証で拒否します。
func NewRetryConfigProvider(
	cfg *pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig,
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

func NewFlushManagerConfigAdapter(cfg pkglogger.ClickHouseFlushManagerConfig) flushManagerConfigAdapter {
	return flushManagerConfigAdapter{
		flushManagerRuntimeConfigAdapter: newFlushManagerRuntimeConfigAdapter(
			cfg.PostWarnThresholdDuration,
			cfg.PostErrorThresholdDuration,
			cfg.InsertLatencyEnabled,
		),
		failureStoreConfig: loggerdurable.NewFailureStoreConfigAdapter(cfg.FailureStoreConfig),
	}
}

func NewDeliveryFlushManagerConfigAdapter(cfg pkglogger.DeliveryClickHouseFlushManagerConfig) deliveryFlushManagerConfigAdapter {
	return deliveryFlushManagerConfigAdapter{
		flushManagerRuntimeConfigAdapter: newFlushManagerRuntimeConfigAdapter(
			cfg.PostWarnThresholdDuration,
			cfg.PostErrorThresholdDuration,
			cfg.InsertLatencyEnabled,
		),
		failureStoreConfig: loggerdurable.NewDeliveryFailureStoreConfigAdapter(cfg.FailureStoreConfig),
	}
}

func newFlushManagerRuntimeConfigAdapter(
	postWarnThresholdDuration time.Duration,
	postErrorThresholdDuration time.Duration,
	insertLatencyEnabled bool,
) flushManagerRuntimeConfigAdapter {
	return flushManagerRuntimeConfigAdapter{
		postWarnThresholdDuration:  postWarnThresholdDuration,
		postErrorThresholdDuration: postErrorThresholdDuration,
		insertLatencyEnabled:       insertLatencyEnabled,
	}
}

func (a flushManagerConfigAdapter) FailureStoreConfig() loggerdurable.FailureStoreConfigProvider {
	return a.failureStoreConfig
}

func (a deliveryFlushManagerConfigAdapter) DeliveryFailureStoreConfig() loggerdurable.DeliveryFailureStoreConfigProvider {
	return a.failureStoreConfig
}

func (a flushManagerRuntimeConfigAdapter) PostWarnThresholdDuration() time.Duration {
	return a.postWarnThresholdDuration
}

func (a flushManagerRuntimeConfigAdapter) PostErrorThresholdDuration() time.Duration {
	return a.postErrorThresholdDuration
}

func (a flushManagerRuntimeConfigAdapter) InsertLatencyEnabled() bool {
	return a.insertLatencyEnabled
}

type clickHouseLoggerConfigAdapter struct {
	cfg            pkglogger.ClickHouseLoggerBaseConfig
	timeConfig     loggertime.TimeConfigProvider
	runtimeConfig  RuntimeConfigProvider
	logBodyConfig  LogBodyConfigProvider
	samplingConfig sampling.SamplingConfigProvider
	handlerConfig  handler.ConfigProvider
}

func NewClickHouseLoggerConfigAdapter(cfg pkglogger.ClickHouseLoggerBaseConfig) *clickHouseLoggerConfigAdapter {
	return &clickHouseLoggerConfigAdapter{
		cfg:            cfg,
		timeConfig:     loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		runtimeConfig:  NewRuntimeConfigAdapter(cfg.RuntimeConfig),
		logBodyConfig:  NewLogBodyConfigAdapter(cfg.LogBodyConfig),
		samplingConfig: sampling.NewConfigProvider(cfg.SamplingConfig, cfg.RuntimeConfig.Live),
		handlerConfig:  handler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

func NewDeliveryClickHouseLoggerConfigAdapter(cfg pkglogger.DeliveryClickHouseLoggerBaseConfig) *clickHouseLoggerConfigAdapter {
	return NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
		TimeConfig: cfg.TimeConfig,
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  cfg.RuntimeConfig.AppName,
			IsDebug:  cfg.RuntimeConfig.IsDebug,
			Target:   cfg.RuntimeConfig.Target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig:           cfg.LogBodyConfig,
		FlushInterval:           cfg.FlushInterval,
		MaxEntries:              cfg.MaxEntries,
		BufferFlushBytes:        cfg.BufferFlushBytes,
		FallbackQueueMaxEntries: cfg.FallbackQueueMaxEntries,
		MaxInsertRequestBytes:   cfg.MaxInsertRequestBytes,
		HandlerConfig:           cfg.HandlerConfig,
	})
}

func (a *clickHouseLoggerConfigAdapter) Time() loggertime.TimeConfigProvider {
	return a.timeConfig
}

func (a *clickHouseLoggerConfigAdapter) Runtime() RuntimeConfigProvider {
	return a.runtimeConfig
}

func (a *clickHouseLoggerConfigAdapter) LogBody() LogBodyConfigProvider {
	return a.logBodyConfig
}

func (a *clickHouseLoggerConfigAdapter) FlushInterval() time.Duration {
	return a.cfg.FlushInterval
}

func (a *clickHouseLoggerConfigAdapter) MaxEntries() int {
	return a.cfg.MaxEntries
}

func (a *clickHouseLoggerConfigAdapter) BufferFlushBytes() int {
	return a.cfg.BufferFlushBytes
}

func (a *clickHouseLoggerConfigAdapter) FallbackQueueMaxEntries() int {
	return a.cfg.FallbackQueueMaxEntries
}

func (a *clickHouseLoggerConfigAdapter) MaxInsertRequestBytes() int {
	return a.cfg.MaxInsertRequestBytes
}

func (a *clickHouseLoggerConfigAdapter) Sampling() sampling.SamplingConfigProvider {
	return a.samplingConfig
}

func (a *clickHouseLoggerConfigAdapter) Handler() handler.ConfigProvider {
	return a.handlerConfig
}

type RuntimeConfigAdapter struct {
	appName  string
	isDebug  bool
	target   string
	logLevel pkglogger.LogLevel
	// liveを設定した場合は、logLevelの代わりにその現在のlevelを使います。
	live *pkglogger.LiveSettings
}

func NewRuntimeConfigAdapter(cfg pkglogger.RuntimeConfig) RuntimeConfigAdapter {
	return RuntimeConfigAdapter{
		appName:  cfg.AppName,
		isDebug:  cfg.IsDebug,
		target:   cfg.Target,
		logLevel: cfg.LogLevel,
		live:     cfg.Live,
	}
}

func (a RuntimeConfigAdapter) AppName() string {
	return a.appName
}

func (a RuntimeConfigAdapter) IsDebug() bool {
	return a.isDebug
}

func (a RuntimeConfigAdapter) Target() string {
	return a.target
}

func (a RuntimeConfigAdapter) LogLevel() pkglogger.LogLevel {
	if a.live != nil {
		return a.live.Level()
	}
	return a.logLevel
}

type LogBodyConfigAdapter struct {
	logLineDateFormat        string
	errorTraceDepth          int
	errorTraceDepthDebugMode int
}

func NewLogBodyConfigAdapter(cfg pkglogger.LogBodyConfig) LogBodyConfigAdapter {
	return LogBodyConfigAdapter{
		logLineDateFormat:        cfg.LogLineDateFormat,
		errorTraceDepth:          cfg.ErrorTraceDepth,
		errorTraceDepthDebugMode: cfg.ErrorTraceDepthDebugMode,
	}
}

func (a LogBodyConfigAdapter) LogLineDateFormat() string {
	return a.logLineDateFormat
}

func (a LogBodyConfigAdapter) ErrorTraceDepth() int {
	return a.errorTraceDepth
}

func (a LogBodyConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.errorTraceDepthDebugMode
}

type stackTraceConfigAdapter struct {
	isDebug                  bool
	errorTraceDepth          int
	errorTraceDepthDebugMode int
}

func NewStackTraceConfigAdapter(cfg ClickHouseConfigProvider) logging.StackTraceConfigProvider {
	return &stackTraceConfigAdapter{
		isDebug:                  cfg.Runtime().IsDebug(),
		errorTraceDepth:          cfg.LogBody().ErrorTraceDepth(),
		errorTraceDepthDebugMode: cfg.LogBody().ErrorTraceDepthDebugMode(),
	}
}

func (a *stackTraceConfigAdapter) IsDebug() bool {
	return a.isDebug
}

func (a *stackTraceConfigAdapter) ErrorTraceDepth() int {
	return a.errorTraceDepth
}

func (a *stackTraceConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.errorTraceDepthDebugMode
}

func NewClickHouseLoggerAdapter(cfg pkglogger.LoggerConfig) (
	ClickHouseConfigProvider,
	*pkgclickhouse.Config,
	ClickHouseFlushManagerConfigProvider,
	resource.ResourceProvider,
	wrapper.RetryConfigProvider,
) {
	loggerConfig := NewClickHouseLoggerConfigAdapter(cfg.ClickHouse.ClickHouseLoggerBaseConfig)
	clientConfig := cfg.ClickHouseClient
	fmConfig := NewFlushManagerConfigAdapter(cfg.ClickHouse.ClickHouseFlushManagerConfig)
	resourceProvider := resource.NewResourceAdapter(cfg.ClickHouse.ClickHouseFlushManagerConfig.Resource)
	retryCfg := NewRetryConfigProvider(
		cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig,
	)

	return loggerConfig, clientConfig, fmConfig, resourceProvider, retryCfg
}
