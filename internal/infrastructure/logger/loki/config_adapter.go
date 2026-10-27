package loki

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/wrapper"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

type flushManagerRuntimeConfigAdapter struct {
	postWarnThresholdDuration  time.Duration
	postErrorThresholdDuration time.Duration
	pushLatencyEnabled         bool
	staticLabels               map[string]string
}

type flushManagerConfigAdapter struct {
	flushManagerRuntimeConfigAdapter
	failureStoreConfig loggerdurable.FailureStoreConfigProvider
}

type deliveryFlushManagerConfigAdapter struct {
	flushManagerRuntimeConfigAdapter
	failureStoreConfig loggerdurable.DeliveryFailureStoreConfigProvider
}

// retryConfigAdapterはlogger設定DTOをLoki wrapper chainの実行時契約へ適合させます。
// retry、retry policy、Circuit Breakerの選択はRetryClientModeから一度だけ解決します。
type retryConfigAdapter struct {
	cfg pkglogger.LokiRetryableClientCaseTransportErrorConfig
}

// NewRetryConfigProviderはclient chain構築用のretry設定を返します。
// 設定自体がnil、またはmodeが明示的なnoneの場合だけnilを返します。
// 不明なmode（空文字を含む）は保持し、chain構築時の検証で拒否します。
func NewRetryConfigProvider(
	cfg *pkglogger.LokiRetryableClientCaseTransportErrorConfig,
) wrapper.RetryConfigProvider {
	if cfg == nil || cfg.RetryClientMode == pkglogger.RetryClientModeNone {
		return nil
	}
	return retryConfigAdapter{cfg: *cfg}
}

// MaxAttemptsは、wrapperが数える初回を含む試行回数です。設定は再試行の回数なので、
// 初回の1回を足します。負の再試行回数は、adapterを作る前の設定検証で拒否しています。
func (a retryConfigAdapter) MaxAttempts() int { return a.cfg.MaxRetries + 1 }

func (a retryConfigAdapter) BaseDelay() time.Duration { return a.cfg.BaseDelay }
func (a retryConfigAdapter) MaxDelay() time.Duration  { return a.cfg.MaxDelay }
func (a retryConfigAdapter) RetryClientSelection() pkglogger.RetryClientSelection {
	return a.cfg.RetryClientSelection()
}

func NewFlushManagerConfigAdapter(cfg pkglogger.LokiFlushManagerConfig) flushManagerConfigAdapter {
	return flushManagerConfigAdapter{
		flushManagerRuntimeConfigAdapter: newFlushManagerRuntimeConfigAdapter(
			cfg.PostWarnThresholdDuration,
			cfg.PostErrorThresholdDuration,
			cfg.PushLatencyEnabled,
			cfg.StaticLabels,
		),
		failureStoreConfig: loggerdurable.NewFailureStoreConfigAdapter(cfg.FailureStoreConfig),
	}
}

func NewDeliveryFlushManagerConfigAdapter(cfg pkglogger.DeliveryLokiFlushManagerConfig) deliveryFlushManagerConfigAdapter {
	return deliveryFlushManagerConfigAdapter{
		flushManagerRuntimeConfigAdapter: newFlushManagerRuntimeConfigAdapter(
			cfg.PostWarnThresholdDuration,
			cfg.PostErrorThresholdDuration,
			cfg.PushLatencyEnabled,
			cfg.StaticLabels,
		),
		failureStoreConfig: loggerdurable.NewDeliveryFailureStoreConfigAdapter(cfg.FailureStoreConfig),
	}
}

func newFlushManagerRuntimeConfigAdapter(
	postWarnThresholdDuration time.Duration,
	postErrorThresholdDuration time.Duration,
	pushLatencyEnabled bool,
	staticLabels map[string]string,
) flushManagerRuntimeConfigAdapter {
	return flushManagerRuntimeConfigAdapter{
		postWarnThresholdDuration:  postWarnThresholdDuration,
		postErrorThresholdDuration: postErrorThresholdDuration,
		pushLatencyEnabled:         pushLatencyEnabled,
		staticLabels:               cloneStringMap(staticLabels),
	}
}

func NewLokiLoggerAdapter(cfg pkglogger.LoggerConfig) (
	LokiConfigProvider,
	*pkgloki.Config,
	LokiFlushManagerConfigProvider,
	resource.ResourceProvider,
	wrapper.RetryConfigProvider,
) {
	lokiLoggerConfig := NewLokiLoggerConfigAdapter(cfg.Loki.LokiLoggerBaseConfig)
	lokiClientConfig := cfg.LokiClient
	lokiFmConfig := NewFlushManagerConfigAdapter(cfg.Loki.LokiFlushManagerConfig)
	resourceProvider := resource.NewResourceAdapter(cfg.Loki.LokiFlushManagerConfig.Resource)
	retryCfg := NewRetryConfigProvider(
		cfg.Loki.LokiRetryableClientCaseTransportErrorConfig,
	)

	return lokiLoggerConfig, lokiClientConfig, lokiFmConfig, resourceProvider, retryCfg
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

func (a flushManagerRuntimeConfigAdapter) PushLatencyEnabled() bool {
	return a.pushLatencyEnabled
}

func (a flushManagerRuntimeConfigAdapter) StaticLabels() map[string]string {
	return cloneStringMap(a.staticLabels)
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

type lokiLoggerConfigAdapter struct {
	cfg            pkglogger.LokiLoggerBaseConfig
	timeConfig     loggertime.TimeConfigProvider
	runtimeConfig  RuntimeConfigProvider
	logBodyConfig  LogBodyConfigProvider
	samplingConfig sampling.SamplingConfigProvider
	handlerConfig  handler.ConfigProvider
}

func NewLokiLoggerConfigAdapter(cfg pkglogger.LokiLoggerBaseConfig) *lokiLoggerConfigAdapter {
	return &lokiLoggerConfigAdapter{
		cfg:            cfg,
		timeConfig:     loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		runtimeConfig:  NewRuntimeConfigAdapter(cfg.RuntimeConfig),
		logBodyConfig:  NewLogBodyConfigAdapter(cfg.LogBodyConfig),
		samplingConfig: sampling.NewConfigProvider(cfg.SamplingConfig, cfg.RuntimeConfig.Live),
		handlerConfig:  handler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

func NewDeliveryLokiLoggerConfigAdapter(cfg pkglogger.DeliveryLokiLoggerBaseConfig) *lokiLoggerConfigAdapter {
	return NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		TimeConfig: cfg.TimeConfig,
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  cfg.RuntimeConfig.AppName,
			IsDebug:  cfg.RuntimeConfig.IsDebug,
			Target:   cfg.RuntimeConfig.Target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig:           cfg.LogBodyConfig,
		MaxEntries:              1,
		BufferFlushBytes:        1,
		FallbackQueueMaxEntries: 1,
		MaxLineBytes:            cfg.MaxLineBytes,
		MaxPushRequestBytes:     cfg.MaxPushRequestBytes,
		NoBulkFlushModeEnabled:  true,
		HandlerConfig:           cfg.HandlerConfig,
	})
}

// NewDeliveryLokiFallbackLoggerConfigAdapterはDelivery internal-log fallback専用の
// enqueue/batch設定を通常logger providerへ変換します。
func NewDeliveryLokiFallbackLoggerConfigAdapter(
	cfg pkglogger.DeliveryLokiLoggerBaseConfig,
	batchCfg pkglogger.DeliveryInternalLogLokiBatchConfig,
) *lokiLoggerConfigAdapter {
	return NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		TimeConfig: cfg.TimeConfig,
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  cfg.RuntimeConfig.AppName,
			IsDebug:  cfg.RuntimeConfig.IsDebug,
			Target:   cfg.RuntimeConfig.Target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig:           cfg.LogBodyConfig,
		FlushInterval:           batchCfg.FlushInterval,
		MaxEntries:              batchCfg.MaxEntries,
		BufferFlushBytes:        batchCfg.BufferFlushBytes,
		FallbackQueueMaxEntries: batchCfg.FallbackQueueMaxEntries,
		MaxLineBytes:            cfg.MaxLineBytes,
		MaxPushRequestBytes:     cfg.MaxPushRequestBytes,
		NoBulkFlushModeEnabled:  false,
	})
}

func (a *lokiLoggerConfigAdapter) Time() loggertime.TimeConfigProvider {
	return a.timeConfig
}

func (a *lokiLoggerConfigAdapter) Runtime() RuntimeConfigProvider {
	return a.runtimeConfig
}

func (a *lokiLoggerConfigAdapter) LogBody() LogBodyConfigProvider {
	return a.logBodyConfig
}

func (a *lokiLoggerConfigAdapter) FlushInterval() time.Duration {
	return a.cfg.FlushInterval
}

func (a *lokiLoggerConfigAdapter) MaxEntries() int {
	return a.cfg.MaxEntries
}

func (a *lokiLoggerConfigAdapter) BufferFlushBytes() int {
	return a.cfg.BufferFlushBytes
}

func (a *lokiLoggerConfigAdapter) FallbackQueueMaxEntries() int {
	return a.cfg.FallbackQueueMaxEntries
}

func (a *lokiLoggerConfigAdapter) MaxLineBytes() int {
	return a.cfg.MaxLineBytes
}

func (a *lokiLoggerConfigAdapter) MaxPushRequestBytes() int {
	return a.cfg.MaxPushRequestBytes
}

func (a *lokiLoggerConfigAdapter) NoBulkFlushModeEnabled() bool {
	return a.cfg.NoBulkFlushModeEnabled
}

func (a *lokiLoggerConfigAdapter) Sampling() sampling.SamplingConfigProvider {
	return a.samplingConfig
}

func (a *lokiLoggerConfigAdapter) Handler() handler.ConfigProvider {
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

func NewStackTraceConfigAdapter(cfg LokiConfigProvider) logging.StackTraceConfigProvider {
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
