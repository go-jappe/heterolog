package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// newFallbackLoggerはfallback送信用のclient、FlushManager、bufferを構築し、
// batch workerを開始した利用可能なFallbackLoggerを返します。
func newFallbackLogger(
	ctx context.Context,
	cfg ClickHouseConfigProvider,
	clientCfg *pkgclickhouse.Config,
	fmCfg ClickHouseFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*FallbackLogger, error) {
	if err := validateClickHouseBatchConfig(cfg); err != nil {
		return nil, err
	}
	if err := pkglogger.ValidateBufferedFlushInterval(cfg.FlushInterval()); err != nil {
		return nil, fmt.Errorf("clickhouse fallback logger config is invalid: %w", err)
	}
	if err := sampling.ValidateSamplingConfigProviderFor(pkglogger.LogOutputClickHouse, cfg.Sampling()); err != nil {
		return nil, fmt.Errorf("clickhouse fallback logger sampling config: %w", err)
	}
	if clientCfg == nil {
		return nil, fmt.Errorf("clickhouse client config is nil")
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

	logger := &ClickHouseLogger{
		LoggerBase:         loggerBase,
		entries:            make([]*logging.LogEntry, 0, cfg.FallbackQueueMaxEntries()),
		entryBytes:         make([]int, 0, cfg.FallbackQueueMaxEntries()),
		entryRequiresFlush: make([]bool, 0, cfg.FallbackQueueMaxEntries()),
		timeProvider:       time.Now,
		flushIntervalGen:   time.NewTicker,
		resourceProvider:   resourceProvider,
		Tracer:             tracer,
		Meter:              meter,
	}

	client, err := infraclickhouse.NewClient(*clientCfg, retryCfg, tracer, meter)
	if err != nil {
		return nil, fmt.Errorf("ClickHouseクライアント作成に失敗しました: %w", err)
	}

	logger.flushManager = NewClickHouseFlushManager(
		loggerBase,
		client,
		fmCfg,
		nil,
		resourceProvider,
		tracer,
		meter,
	)

	logger.lastFlushTime = logger.timeProvider()
	fallback := &FallbackLogger{ClickHouseLogger: logger}
	fallback.startFallbackWorker()
	return fallback, nil
}

func NewNamedErrorFallbackHandler(
	cfg ClickHouseConfigProvider,
	clientCfg *pkgclickhouse.Config,
	fmCfg ClickHouseFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
) logging.NamedLoggerErrorHandler {
	controller := newFallbackHandlerController(
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
	)
	return logging.NamedLoggerErrorHandler{
		Name:    common.HandlerNameClickHouseLogger,
		Handler: controller.HandleError,
		Closer:  controller,
	}
}

func NewNamedWarnFallbackHandler(
	cfg ClickHouseConfigProvider,
	clientCfg *pkgclickhouse.Config,
	fmCfg ClickHouseFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameClickHouseLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelWarn,
		Closer:  controller,
	}
}

func NewNamedInfoFallbackHandler(
	cfg ClickHouseConfigProvider,
	clientCfg *pkgclickhouse.Config,
	fmCfg ClickHouseFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameClickHouseLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelInfo,
		Closer:  controller,
	}
}

func NewNamedDebugFallbackHandler(
	cfg ClickHouseConfigProvider,
	clientCfg *pkgclickhouse.Config,
	fmCfg ClickHouseFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
) logging.NamedLoggerHandler {
	controller := newFallbackHandlerController(
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
	)
	return logging.NamedLoggerHandler{
		Name:    common.HandlerNameClickHouseLogger,
		Handler: controller.Handle,
		Level:   pkglogger.LogLevelDebug,
		Closer:  controller,
	}
}
