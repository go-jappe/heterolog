package loki

import (
	"context"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// newFallbackLoggerはfallback送信用のclient、FlushManager、bufferを構築し、
// batch workerを開始した利用可能なFallbackLoggerを返します。
func newFallbackLogger(
	ctx context.Context,
	cfg LokiConfigProvider,
	clientCfg *pkgloki.Config,
	fmCfg LokiFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	opts ...LokiLoggerOption,
) (*FallbackLogger, error) {
	if err := validateLokiBufferedConfig(cfg); err != nil {
		return nil, err
	}
	if err := validateLokiFallbackQueueMaxEntries(cfg); err != nil {
		return nil, err
	}
	if err := pkglogger.ValidateLokiPushLimits(
		cfg.MaxLineBytes(),
		cfg.MaxPushRequestBytes(),
	); err != nil {
		return nil, fmt.Errorf("loki fallback logger push limits are invalid: %w", err)
	}
	if err := pkglogger.ValidateBufferedFlushInterval(cfg.FlushInterval()); err != nil {
		return nil, fmt.Errorf("loki fallback logger config is invalid: %w", err)
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputLoki, cfg.Sampling()); err != nil {
		return nil, fmt.Errorf("loki fallback logger sampling config: %w", err)
	}
	if clientCfg == nil {
		return nil, fmt.Errorf("loki client config is nil")
	}

	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)

	configuredFormatter, err := NewConfiguredTimestampFormatter(cfg)
	if err != nil {
		return nil, err
	}
	loggerBase := &LoggerBase{
		Clock:        clock.NewUTCClock(),
		LogFormatter: formatter.NewPlainTextFormatter(configuredFormatter),
		config:       cfg,
		Tracer:       tracer,
		Meter:        meter,
	}

	logger := &LokiLogger{
		LoggerBase:       loggerBase,
		entries:          make([]*logging.LogEntry, 0, cfg.FallbackQueueMaxEntries()),
		entryBytes:       make([]int, 0, cfg.FallbackQueueMaxEntries()),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		Tracer:           tracer,
		Meter:            meter,
	}
	logger.lastFlushTime = logger.timeProvider()

	for _, opt := range opts {
		opt(logger)
	}

	client, err := infraloki.NewClient(*clientCfg, retryCfg, tracer, meter)
	if err != nil {
		return nil, fmt.Errorf("lokiクライアント作成に失敗しました: %w", err)
	}

	logger.flushManager = NewLokiFlushManager(
		loggerBase,
		client,
		fmCfg,
		nil,
		resourceProvider,
		tracer,
		meter,
	)

	fallback := &FallbackLogger{LokiLogger: logger}
	fallback.startFallbackWorker()
	return fallback, nil
}

func NewNamedErrorFallbackHandler(
	cfg LokiConfigProvider,
	clientCfg *pkgloki.Config,
	fmCfg LokiFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...LokiLoggerOption,
) logging.NamedLoggerErrorHandler {
	controller := newFallbackHandlerController(
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
		opts...,
	)
	return logging.NamedLoggerErrorHandler{
		Name:    common.HandlerNameLokiLogger,
		Handler: controller.HandleError,
		Closer:  controller,
	}
}

func NewNamedWarnFallbackHandler(
	cfg LokiConfigProvider,
	clientCfg *pkgloki.Config,
	fmCfg LokiFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...LokiLoggerOption,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
		opts...,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameLokiLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelWarn,
		Closer:  controller,
	}
}

func NewNamedInfoFallbackHandler(
	cfg LokiConfigProvider,
	clientCfg *pkgloki.Config,
	fmCfg LokiFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...LokiLoggerOption,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
		opts...,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameLokiLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelInfo,
		Closer:  controller,
	}
}

func NewNamedDebugFallbackHandler(
	cfg LokiConfigProvider,
	clientCfg *pkgloki.Config,
	fmCfg LokiFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...LokiLoggerOption,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
		opts...,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameLokiLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelDebug,
		Closer:  controller,
	}
}
