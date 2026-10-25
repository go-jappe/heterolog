package cloudwatchlogs

import (
	"context"
	"errors"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/memory/pressure"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
)

// BuildFailureHandler はCloudWatch Logsのfailure store設定とinternal log routingを
// backend共通factoryへ渡します。
func BuildFailureHandler(
	ctx context.Context,
	timeSource clock.Clock,
	fmCfg CloudWatchLogsFlushManagerConfigProvider,
	memoryMonitor pressure.MemoryPressureMonitor,
	tracer observer.Tracer,
	meter observer.Meter,
	internalLogHandlerFactory flushfailure.InternalLogHandlerFactory,
	spoolHandlerRegistrar flushfailure.SpoolHandlerRegistrar,
	spoolRuntimes compressedlog.RootRuntimes,
) (*flushfailure.CompositeFailureHandler, error) {
	if fmCfg == nil {
		// 未構築の空Compositeには、cleanup用の内部capacityだけを用意します。
		handler, err := flushfailure.NewCompositeFailureHandler(nil, 1)
		if err != nil {
			return nil, err
		}
		return handler, errors.New("cloudwatch logs flush manager config is nil")
	}
	return flushfailure.Build(
		ctx,
		timeSource,
		fmCfg.FailureStoreConfig(),
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

// BuildDeliveryFailureHandlerはCloudWatch LogsのDelivery failure store設定とinternal log
// routingを、確定した処理結果をreportできる共通factoryへ渡します。
func BuildDeliveryFailureHandler(
	ctx context.Context,
	timeSource clock.Clock,
	fmCfg DeliveryCloudWatchLogsFlushManagerConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
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
