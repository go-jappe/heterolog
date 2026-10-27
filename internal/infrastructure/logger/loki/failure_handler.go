package loki

import (
	"context"

	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func BuildFailureHandler(
	ctx context.Context,
	timeSource clock.Clock,
	fmCfg LokiFlushManagerConfigProvider,
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
			InternalLogHandlerFactory:  internalLogHandlerFactory,
			SpoolHandlerRegistrar:      spoolHandlerRegistrar,
			CompressedLogSpoolRuntimes: spoolRuntimes,
		},
	)
}

// BuildDeliveryFailureHandlerはLokiのDelivery failure store設定とinternal log routingを、
// 確定した処理結果をreportできる共通factoryへ渡します。
func BuildDeliveryFailureHandler(
	ctx context.Context,
	timeSource clock.Clock,
	fmCfg DeliveryLokiFlushManagerConfigProvider,
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
