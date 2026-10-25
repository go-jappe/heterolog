package opensearch

import (
	"time"

	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	opensearchwrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/pkg/logger"
)

type flushManagerRuntimeConfigAdapter struct {
	index                      string
	isDebug                    bool
	useOpenTelemetryFormat     bool
	postWarnThresholdDuration  time.Duration
	postErrorThresholdDuration time.Duration
	indexLatencyEnabled        bool
}

type bulkFlushManagerConfigAdapter struct {
	maxRetryAttempts int
	retryWaitTime    time.Duration
}

type flushManagerConfigAdapter struct {
	flushManagerRuntimeConfigAdapter
	bulkFlushManagerConfigAdapter
	failureStoreConfig loggerdurable.FailureStoreConfigProvider
}

type deliveryFlushManagerConfigAdapter struct {
	flushManagerRuntimeConfigAdapter
	failureStoreConfig loggerdurable.DeliveryFailureStoreConfigProvider
}

// retryClientChainConfigAdapterは公開logger設定をOpenSearch client chainが必要とする
// selectionとbackoff設定へ適合させます。chainの組み立て自体はwrapperが担当します。
type retryClientChainConfigAdapter struct {
	cfg logger.OpenSearchRetryableClientCaseTransportErrorConfig
}

// NewRetryConfigProviderはclient chain構築用のretry設定を返します。
// 設定自体がnil、またはmodeが明示的なnoneの場合だけnilを返します。
// 不明なmode（空文字を含む）は保持し、chain構築時の検証で拒否します。
func NewRetryConfigProvider(
	cfg *logger.OpenSearchRetryableClientCaseTransportErrorConfig,
) opensearchwrapper.RetryConfigProvider {
	if cfg == nil || cfg.RetryClientMode == logger.RetryClientModeNone {
		return nil
	}
	return retryClientChainConfigAdapter{cfg: *cfg}
}

func (a retryClientChainConfigAdapter) MaxAttempts() int {
	return a.cfg.MaxAttempts
}

func (a retryClientChainConfigAdapter) BaseDelay() time.Duration {
	return a.cfg.BaseDelay
}

func (a retryClientChainConfigAdapter) MaxDelay() time.Duration {
	return a.cfg.MaxDelay
}

func (a retryClientChainConfigAdapter) RetryClientSelection() logger.RetryClientSelection {
	return a.cfg.RetryClientSelection()
}

func NewFlushManagerConfigAdapter(cfg logger.OpenSearchFlushManagerConfig) flushManagerConfigAdapter {
	return flushManagerConfigAdapter{
		flushManagerRuntimeConfigAdapter: newFlushManagerRuntimeConfigAdapter(
			cfg.Index,
			cfg.IsDebug,
			cfg.UseOpenTelemetryFormat,
			cfg.PostWarnThresholdDuration,
			cfg.PostErrorThresholdDuration,
			cfg.IndexLatencyEnabled,
		),
		bulkFlushManagerConfigAdapter: bulkFlushManagerConfigAdapter{
			maxRetryAttempts: cfg.MaxRetryAttempts,
			retryWaitTime:    cfg.RetryWaitTime,
		},
		failureStoreConfig: loggerdurable.NewFailureStoreConfigAdapter(cfg.FailureStoreConfig),
	}
}

func NewDeliveryFlushManagerConfigAdapter(cfg logger.DeliveryOpenSearchFlushManagerConfig) deliveryFlushManagerConfigAdapter {
	return deliveryFlushManagerConfigAdapter{
		flushManagerRuntimeConfigAdapter: newFlushManagerRuntimeConfigAdapter(
			cfg.Index,
			cfg.IsDebug,
			cfg.UseOpenTelemetryFormat,
			cfg.PostWarnThresholdDuration,
			cfg.PostErrorThresholdDuration,
			cfg.IndexLatencyEnabled,
		),
		failureStoreConfig: loggerdurable.NewDeliveryFailureStoreConfigAdapter(cfg.FailureStoreConfig),
	}
}

func newFlushManagerRuntimeConfigAdapter(
	index string,
	isDebug bool,
	useOpenTelemetryFormat bool,
	postWarnThresholdDuration time.Duration,
	postErrorThresholdDuration time.Duration,
	indexLatencyEnabled bool,
) flushManagerRuntimeConfigAdapter {
	return flushManagerRuntimeConfigAdapter{
		index:                      index,
		isDebug:                    isDebug,
		useOpenTelemetryFormat:     useOpenTelemetryFormat,
		postWarnThresholdDuration:  postWarnThresholdDuration,
		postErrorThresholdDuration: postErrorThresholdDuration,
		indexLatencyEnabled:        indexLatencyEnabled,
	}
}

func (f flushManagerRuntimeConfigAdapter) Index() string {
	return f.index
}
func (f flushManagerRuntimeConfigAdapter) IsDebug() bool {
	return f.isDebug
}

func (f flushManagerRuntimeConfigAdapter) UseOpenTelemetryFormat() bool {
	return f.useOpenTelemetryFormat
}
func (f flushManagerConfigAdapter) FailureStoreConfig() loggerdurable.FailureStoreConfigProvider {
	return f.failureStoreConfig
}

func (f deliveryFlushManagerConfigAdapter) DeliveryFailureStoreConfig() loggerdurable.DeliveryFailureStoreConfigProvider {
	return f.failureStoreConfig
}

func (f flushManagerRuntimeConfigAdapter) PostWarnThresholdDuration() time.Duration {
	return f.postWarnThresholdDuration
}

func (f flushManagerRuntimeConfigAdapter) PostErrorThresholdDuration() time.Duration {
	return f.postErrorThresholdDuration
}

func (f flushManagerRuntimeConfigAdapter) IndexLatencyEnabled() bool {
	return f.indexLatencyEnabled
}

func (f bulkFlushManagerConfigAdapter) MaxRetryAttempts() int {
	return f.maxRetryAttempts
}
func (f bulkFlushManagerConfigAdapter) RetryWaitTime() time.Duration {
	return f.retryWaitTime
}

type openSearchLoggerConfigAdapter struct {
	cfg            logger.OpenSearchLoggerBaseConfig
	timeConfig     loggertime.TimeConfigProvider
	runtimeConfig  RuntimeConfigProvider
	logBodyConfig  LogBodyConfigProvider
	samplingConfig sampling.SamplingConfigProvider
	handlerConfig  handler.ConfigProvider
}

func NewOpenSearchLoggerAdapter(cfg logger.LoggerConfig) (
	OpenSearchLoggerConfigProvider,
	infraopensearch.OpenSearchClientConfigProvider,
	OpenSearchFlushManagerConfigProvider,
	OpenSearchFlushManagerResourceProvider,
	opensearchwrapper.RetryConfigProvider,
) {
	osLoggerConfig := NewOpenSearchLoggerConfigAdapter(cfg.OpenSearch.OpenSearchLoggerBaseConfig)
	osClientConfig := infraopensearch.NewOpenSearchClientConfigAdapter(cfg.OpenSearchClient)
	osFmConfig := NewFlushManagerConfigAdapter(cfg.OpenSearch.OpenSearchFlushManagerConfig)
	resourceProvider := resource.NewResourceAdapter(cfg.OpenSearch.OpenSearchFlushManagerConfig.Resource)
	retryCfg := NewRetryConfigProvider(
		cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig,
	)

	return osLoggerConfig, osClientConfig, osFmConfig, resourceProvider, retryCfg
}

func NewOpenSearchLoggerConfigAdapter(
	cfg logger.OpenSearchLoggerBaseConfig,
) *openSearchLoggerConfigAdapter {
	return &openSearchLoggerConfigAdapter{
		cfg:            cfg,
		timeConfig:     loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		runtimeConfig:  NewRuntimeConfigAdapter(cfg.RuntimeConfig),
		logBodyConfig:  NewLogBodyConfigAdapter(cfg.LogBodyConfig),
		samplingConfig: sampling.NewSamplingConfigAdapter(cfg.SamplingConfig),
		handlerConfig:  handler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

// NewDeliveryOpenSearchLoggerConfigAdapterはDelivery本体の同期・一件送信用設定を
// providerへ変換します。internal-log fallbackのbatch設定には使用しません。
func NewDeliveryOpenSearchLoggerConfigAdapter(
	cfg logger.DeliveryOpenSearchLoggerBaseConfig,
) *openSearchLoggerConfigAdapter {
	return NewOpenSearchLoggerConfigAdapter(logger.OpenSearchLoggerBaseConfig{
		TimeConfig: cfg.TimeConfig,
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  cfg.RuntimeConfig.AppName,
			IsDebug:  cfg.RuntimeConfig.IsDebug,
			Target:   cfg.RuntimeConfig.Target,
			LogLevel: logger.LogLevelDebug,
		},
		LogBodyConfig:          cfg.LogBodyConfig,
		MaxBulkDocument:        1,
		NoBulkFlushModeEnabled: true,
		HandlerConfig:          cfg.HandlerConfig,
	})
}

// NewDeliveryOpenSearchFallbackLoggerConfigAdapterはDelivery internal-log fallback
// 専用のenqueue/batch設定を通常logger providerへ変換します。
func NewDeliveryOpenSearchFallbackLoggerConfigAdapter(
	cfg logger.DeliveryOpenSearchLoggerBaseConfig,
	batchCfg logger.DeliveryInternalLogOpenSearchBatchConfig,
) *openSearchLoggerConfigAdapter {
	return NewOpenSearchLoggerConfigAdapter(logger.OpenSearchLoggerBaseConfig{
		TimeConfig: cfg.TimeConfig,
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  cfg.RuntimeConfig.AppName,
			IsDebug:  cfg.RuntimeConfig.IsDebug,
			Target:   cfg.RuntimeConfig.Target,
			LogLevel: logger.LogLevelDebug,
		},
		LogBodyConfig:          cfg.LogBodyConfig,
		FlushInterval:          batchCfg.FlushInterval,
		MaxBulkDocument:        batchCfg.MaxBulkDocument,
		BufferFlushBytes:       batchCfg.BufferFlushBytes,
		MaxBulkRequestBytes:    batchCfg.MaxBulkRequestBytes,
		NoBulkFlushModeEnabled: false,
	})
}

func (a *openSearchLoggerConfigAdapter) MaxBulkDocument() int {
	return a.cfg.MaxBulkDocument
}

func (a *openSearchLoggerConfigAdapter) BufferFlushBytes() int {
	return a.cfg.BufferFlushBytes
}

func (a *openSearchLoggerConfigAdapter) MaxBulkRequestBytes() int {
	return a.cfg.MaxBulkRequestBytes
}

func (a *openSearchLoggerConfigAdapter) NoBulkFlushModeEnabled() bool {
	return a.cfg.NoBulkFlushModeEnabled
}

func (a *openSearchLoggerConfigAdapter) Time() loggertime.TimeConfigProvider {
	return a.timeConfig
}
func (a *openSearchLoggerConfigAdapter) Runtime() RuntimeConfigProvider {
	return a.runtimeConfig
}
func (a *openSearchLoggerConfigAdapter) LogBody() LogBodyConfigProvider {
	return a.logBodyConfig
}
func (a *openSearchLoggerConfigAdapter) Sampling() sampling.SamplingConfigProvider {
	return a.samplingConfig
}

type RuntimeConfigAdapter struct {
	appName  string
	isDebug  bool
	target   string
	logLevel logger.LogLevel
}

func NewRuntimeConfigAdapter(cfg logger.RuntimeConfig) RuntimeConfigAdapter {
	return RuntimeConfigAdapter{
		appName:  cfg.AppName,
		isDebug:  cfg.IsDebug,
		target:   cfg.Target,
		logLevel: cfg.LogLevel,
	}
}

func (a RuntimeConfigAdapter) IsDebug() bool {
	return a.isDebug
}

func (a RuntimeConfigAdapter) AppName() string {
	return a.appName
}

func (a RuntimeConfigAdapter) Target() string {
	return a.target
}

func (a RuntimeConfigAdapter) LogLevel() logger.LogLevel {
	return a.logLevel
}

type LogBodyConfigAdapter struct {
	logLineDateFormat        string
	errorTraceDepth          int
	errorTraceDepthDebugMode int
}

func NewLogBodyConfigAdapter(cfg logger.LogBodyConfig) LogBodyConfigAdapter {
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

func (a openSearchLoggerConfigAdapter) FlushInterval() time.Duration {
	return a.cfg.FlushInterval
}

func (a openSearchLoggerConfigAdapter) Handler() handler.ConfigProvider {
	return a.handlerConfig
}

type stackTraceConfigAdapter struct {
	IsDebugConfig                  bool
	ErrorTraceDepthDebugModeConfig int
	ErrorTraceDepthConfig          int
}

func NewStackTraceConfigAdapter(cfg OpenSearchLoggerConfigProvider) logging.StackTraceConfigProvider {
	return &stackTraceConfigAdapter{
		IsDebugConfig:                  cfg.Runtime().IsDebug(),
		ErrorTraceDepthDebugModeConfig: cfg.LogBody().ErrorTraceDepthDebugMode(),
		ErrorTraceDepthConfig:          cfg.LogBody().ErrorTraceDepth(),
	}
}

func (a *stackTraceConfigAdapter) IsDebug() bool {
	return a.IsDebugConfig
}
func (a *stackTraceConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.ErrorTraceDepthDebugModeConfig
}
func (a *stackTraceConfigAdapter) ErrorTraceDepth() int {
	return a.ErrorTraceDepthConfig
}
