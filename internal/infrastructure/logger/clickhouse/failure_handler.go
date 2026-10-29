package clickhouse

import (
	"context"

	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// BuildFailureHandlerはClickHouseのfailure store設定とinternal log routingを、
// sync/async loggerが使用する共通failure handler factoryへ渡します。
func BuildFailureHandler(
	ctx context.Context,
	timeSource clock.Clock,
	fmCfg ClickHouseFlushManagerConfigProvider,
	memoryMonitor memorypressure.Monitor,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	internalLogHandlerFactory flushfailure.InternalLogHandlerFactory,
	spoolHandlerRegistrar flushfailure.SpoolHandlerRegistrar,
	spoolRuntimes compressedlog.RootRuntimes,
) (*flushfailure.CompositeFailureHandler, error) {
	var failureStoreConfig loggerdurable.FailureStoreConfigProvider
	if fmCfg != nil {
		failureStoreConfig = fmCfg.FailureStoreConfig()
	}

	return flushfailure.Build(
		ctx,
		timeSource,
		failureStoreConfig,
		memoryMonitor,
		tracer,
		meter,
		flushfailure.FactoryOptions{
			Backend:                    string(pkglogger.LogOutputClickHouse),
			InternalLogHandlerFactory:  internalLogHandlerFactory,
			SpoolHandlerRegistrar:      spoolHandlerRegistrar,
			CompressedLogSpoolRuntimes: spoolRuntimes,
		},
	)
}

// BuildDeliveryFailureHandlerはClickHouseのDelivery failure store設定とinternal log routingを、
// 確定した処理結果をreportできる共通factoryへ渡します。
func BuildDeliveryFailureHandler(
	ctx context.Context,
	timeSource clock.Clock,
	fmCfg DeliveryClickHouseFlushManagerConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	internalLogHandlerFactory flushfailure.InternalLogHandlerFactory,
	spoolRuntimes compressedlog.RootRuntimes,
) (*flushfailure.ReportingCompositeFailureHandler, error) {
	var failureStoreConfig loggerdurable.DeliveryFailureStoreConfigProvider
	if fmCfg != nil {
		failureStoreConfig = fmCfg.DeliveryFailureStoreConfig()
	}

	return flushfailure.BuildDelivery(
		ctx,
		timeSource,
		failureStoreConfig,
		tracer,
		meter,
		flushfailure.FactoryOptions{InternalLogHandlerFactory: internalLogHandlerFactory, CompressedLogSpoolRuntimes: spoolRuntimes},
	)
}
