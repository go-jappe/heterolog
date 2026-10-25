package cloudwatchlogs

import (
	"context"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// newFallbackLoggerはfallback送信用のclient、FlushManager、bufferを構築し、
// batch workerを開始した利用可能なFallbackLoggerを返します。
func newFallbackLogger(
	ctx context.Context,
	cfg CloudWatchLogsConfigProvider,
	clientCfg *pkgcloudwatchlogs.Config,
	fmCfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
	opts ...CloudWatchLogsLoggerOption,
) (*FallbackLogger, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cloudwatch logs logger config is nil")
	}
	if err := pkglogger.ValidateBufferedFlushInterval(cfg.FlushInterval()); err != nil {
		return nil, fmt.Errorf("cloudwatch logs fallback logger config is invalid: %w", err)
	}
	if err := validateCloudWatchLogsBufferConfig(cfg); err != nil {
		return nil, fmt.Errorf("cloudwatch logs fallback logger config is invalid: %w", err)
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputCloudWatchLogs, cfg.Sampling()); err != nil {
		return nil, fmt.Errorf("cloudwatch logs fallback logger sampling config: %w", err)
	}
	if clientCfg == nil {
		return nil, fmt.Errorf("cloudwatch logs client config is nil")
	}
	if fmCfg == nil {
		return nil, fmt.Errorf("cloudwatch logs flush manager config is nil")
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

	logger := &CloudWatchLogsLogger{
		LoggerBase:       loggerBase,
		entries:          make([]*logging.LogEntry, 0, cfg.FallbackQueueMaxEvents()),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		Tracer:           tracer,
		Meter:            meter,
	}
	logger.lastFlushTime = logger.timeProvider()

	for _, opt := range opts {
		opt(logger)
	}

	client, err := infracloudwatchlogs.NewClient(
		*clientCfg,
		retryCfg,
		tracer,
		meter,
	)
	if err != nil {
		return nil, fmt.Errorf("CloudWatch Logsクライアント作成に失敗しました: %w", err)
	}

	streamNameResolver, err := infracloudwatchlogs.NewLogStreamNameResolver(
		fmCfg.LogStreamName(),
		fmCfg.LogStreamRoutingGranularity(),
	)
	if err != nil {
		return nil, fmt.Errorf("CloudWatch Logs stream resolverの作成に失敗しました: %w", err)
	}

	logger.flushManager = NewCloudWatchLogsFlushManager(
		loggerBase,
		client,
		streamNameResolver,
		fmCfg,
		nil,
		resourceProvider,
		tracer,
		meter,
	)

	fallback := &FallbackLogger{CloudWatchLogsLogger: logger}
	fallback.startFallbackWorker()
	return fallback, nil
}

// NewNamedErrorFallbackHandler は error 用の名前付き CloudWatch Logs fallback handler を作ります。
func NewNamedErrorFallbackHandler(
	cfg CloudWatchLogsConfigProvider,
	clientCfg *pkgcloudwatchlogs.Config,
	fmCfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...CloudWatchLogsLoggerOption,
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
		Name:    common.HandlerNameCloudWatchLogsLogger,
		Handler: controller.HandleError,
		Closer:  controller,
	}
}

// NewNamedWarnFallbackHandler は warn 用の名前付き CloudWatch Logs fallback handler を作ります。
func NewNamedWarnFallbackHandler(
	cfg CloudWatchLogsConfigProvider,
	clientCfg *pkgcloudwatchlogs.Config,
	fmCfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...CloudWatchLogsLoggerOption,
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
		Name:    common.HandlerNameCloudWatchLogsLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelWarn,
		Closer:  controller,
	}
}

// NewNamedInfoFallbackHandler は info 用の名前付き CloudWatch Logs fallback handler を作ります。
func NewNamedInfoFallbackHandler(
	cfg CloudWatchLogsConfigProvider,
	clientCfg *pkgcloudwatchlogs.Config,
	fmCfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...CloudWatchLogsLoggerOption,
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
		Name:    common.HandlerNameCloudWatchLogsLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelInfo,
		Closer:  controller,
	}
}

// NewNamedDebugFallbackHandler は debug 用の名前付き CloudWatch Logs fallback handler を作ります。
func NewNamedDebugFallbackHandler(
	cfg CloudWatchLogsConfigProvider,
	clientCfg *pkgcloudwatchlogs.Config,
	fmCfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...CloudWatchLogsLoggerOption,
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
		Name:    common.HandlerNameCloudWatchLogsLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelDebug,
		Closer:  controller,
	}
}
