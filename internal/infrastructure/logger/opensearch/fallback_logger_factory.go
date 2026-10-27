package opensearch

import (
	"fmt"
	"time"

	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	opensearchwrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// newFallbackLoggerはfallback送信用のclient、FlushManager、bufferを構築し、
// batch workerを開始した利用可能なFallbackLoggerを返します。
func newFallbackLogger(
	cfg OpenSearchLoggerConfigProvider,
	clientCfg infraopensearch.OpenSearchClientConfigProvider,
	retryCfg opensearchwrapper.RetryConfigProvider,
	fmCfg OpenSearchFlushManagerRuntimeConfigProvider,
	resource resource.ResourceProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	opts ...OpenSearchLoggerOption,
) (*FallbackLogger, error) {
	if cfg == nil {
		return nil, fmt.Errorf("opensearch fallback logger config is nil")
	}
	if err := logger.ValidateBufferedFlushInterval(cfg.FlushInterval()); err != nil {
		return nil, fmt.Errorf("opensearch fallback logger config is invalid: %w", err)
	}
	if err := logger.ValidateOpenSearchMaxBulkDocument(cfg.MaxBulkDocument()); err != nil {
		return nil, fmt.Errorf("opensearch fallback logger config is invalid: %w", err)
	}
	if err := validateBufferFlushBytes(cfg.BufferFlushBytes()); err != nil {
		return nil, fmt.Errorf("opensearch fallback logger config is invalid: %w", err)
	}
	if err := infraopensearch.ValidateMaxBulkRequestBytes(cfg.MaxBulkRequestBytes()); err != nil {
		return nil, fmt.Errorf("opensearch fallback logger config is invalid: %w", err)
	}
	if err := sampling.ValidateSamplingConfigProviderFor(logger.LogOutputOpenSearch, cfg.Sampling()); err != nil {
		return nil, fmt.Errorf("opensearch fallback logger sampling config: %w", err)
	}
	if fmCfg == nil {
		return nil, fmt.Errorf("opensearch fallback flush manager config is nil")
	}

	configuredFormatter, err := NewConfiguredTimestampFormatter(cfg)
	if err != nil {
		return nil, err
	}
	loggerBase := &LoggerBase{
		Clock:              clock.NewUTCClock(),
		TimestampFormatter: configuredFormatter,
		config:             cfg,
		Tracer:             tracer,
		Meter:              meter,
	}

	converter := NewLogEntryConverter(
		fmCfg.UseOpenTelemetryFormat(),
		resource,
		loggerBase.TimestampFormatter,
	)
	logger := &OpenSearchLogger{
		LoggerBase:       loggerBase,
		entries:          make([]*logging.LogEntry, 0, cfg.MaxBulkDocument()),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		entryConverter:   converter,
		Tracer:           tracer,
		Meter:            meter,
	}
	logger.lastFlushTime = logger.timeProvider()

	for _, opt := range opts {
		opt(logger)
	}

	osClient, err := infraopensearch.NewClient(clientCfg, retryCfg, tracer, meter)
	if err != nil {
		return nil, fmt.Errorf("OpenSearchクライアント作成に失敗しました: %w", err)
	}

	// fallbackはitem retryやfailure handlerを使わないため、通常batch用constructorの
	// bulk retry config検証を通さず、専用のno-fallback managerとして初期化します。
	flushManager := newOpenSearchFlushManager(
		loggerBase,
		osClient,
		fmCfg,
		converter,
		nil,
		nil,
		resource,
		tracer,
		meter,
	)
	logger.flushManager = flushManager

	fallback := &FallbackLogger{OpenSearchLogger: logger}
	fallback.startFallbackWorker()
	return fallback, nil
}

func NewNamedErrorFallbackHandler(
	adapter OpenSearchLoggerConfigProvider,
	clientCfg infraopensearch.OpenSearchClientConfigProvider,
	fmCfg OpenSearchFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg opensearchwrapper.RetryConfigProvider,
	opts ...OpenSearchLoggerOption,
) logging.NamedLoggerErrorHandler {
	controller := newFallbackHandlerController(
		adapter,
		clientCfg,
		retryCfg,
		fmCfg,
		resourceProvider,
		opts...,
	)
	return logging.NamedLoggerErrorHandler{
		Name:    common.HandlerNameOpenSearchLogger,
		Handler: controller.HandleError,
		Closer:  controller,
	}
}

func NewNamedWarnFallbackHandler(
	adapter OpenSearchLoggerConfigProvider,
	clientCfg infraopensearch.OpenSearchClientConfigProvider,
	fmCfg OpenSearchFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg opensearchwrapper.RetryConfigProvider,
	opts ...OpenSearchLoggerOption,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		adapter,
		clientCfg,
		retryCfg,
		fmCfg,
		resourceProvider,
		opts...,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameOpenSearchLogger,
		Handler: controller.Handle,
		Level:   logger.LogLevelWarn,
		Closer:  controller,
	}
}

func NewNamedInfoFallbackHandler(
	adapter OpenSearchLoggerConfigProvider,
	clientCfg infraopensearch.OpenSearchClientConfigProvider,
	fmCfg OpenSearchFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg opensearchwrapper.RetryConfigProvider,
	opts ...OpenSearchLoggerOption,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		adapter,
		clientCfg,
		retryCfg,
		fmCfg,
		resourceProvider,
		opts...,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameOpenSearchLogger,
		Handler: controller.Handle,
		Level:   logger.LogLevelInfo,
		Closer:  controller,
	}
}

func NewNamedDebugFallbackHandler(
	adapter OpenSearchLoggerConfigProvider,
	clientCfg infraopensearch.OpenSearchClientConfigProvider,
	fmCfg OpenSearchFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg opensearchwrapper.RetryConfigProvider,
	opts ...OpenSearchLoggerOption,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		adapter,
		clientCfg,
		retryCfg,
		fmCfg,
		resourceProvider,
		opts...,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameOpenSearchLogger,
		Handler: controller.Handle,
		Level:   logger.LogLevelDebug,
		Closer:  controller,
	}
}
