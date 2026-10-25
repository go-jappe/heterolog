package config

import (
	"strings"

	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/internalerror"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// このfileは、有効なlogger設定のinternal log routingを走査し、通常出力先とは別に
// 設定を読み込む必要があるhandler targetを検出します。

// routingUsesTargetは、OnError、OnWarn、OnInfo、OnDebugのいずれかで、
// targetがinternal logの委譲先backendとして指定されているかを返します。
// envでは大文字小文字や前後空白を許容しているため、handler選択側と同じ正規化を行います。
func routingUsesTarget(cfg internalerror.RoutingConfig, target pkglogger.LogOutputDestination) bool {
	for _, names := range [][]string{
		cfg.OnErrorConfig,
		cfg.OnWarnConfig,
		cfg.OnInfoConfig,
		cfg.OnDebugConfig,
	} {
		for _, name := range names {
			if strings.EqualFold(strings.TrimSpace(name), target.String()) {
				return true
			}
		}
	}
	return false
}

// failureStoreUsesInternalHandlerTargetは、DurableTypeで選択された
// fileArchiveまたはspool自身のinternal logがtargetへ委譲される設定かを返します。
// 選択されていないdurable実装の`.env`値が残っていてもruntimeでは呼ばれないため、
// そのHandlerConfigは可用性の判定に含めません。
func failureStoreUsesInternalHandlerTarget(
	cfg failure.FailureHandlerConfig,
	target pkglogger.LogOutputDestination,
) bool {
	for _, durableType := range cfg.DurableType {
		switch durableType {
		case pkgloggerdurable.DurableTypeFileArchive:
			if routingUsesTarget(cfg.Fsstore.FileArchiveConfig.HandlerConfig, target) {
				return true
			}
		case pkgloggerdurable.DurableTypeSpool:
			if routingUsesTarget(cfg.Fsstore.SpoolConfig.HandlerConfig, target) {
				return true
			}
		}
	}
	return false
}

// deliveryFailureStoreUsesInternalHandlerTargetは、DeliveryのDurableTypeで
// 選択されたfileArchiveのinternal logがtargetへ委譲される設定かを返します。
func deliveryFailureStoreUsesInternalHandlerTarget(
	cfg failure.DeliveryFailureHandlerConfig,
	target pkglogger.LogOutputDestination,
) bool {
	for _, durableType := range cfg.DurableType {
		if durableType == pkgloggerdurable.DurableTypeFileArchive {
			return routingUsesTarget(cfg.FileArchiveConfig.HandlerConfig, target)
		}
	}
	return false
}

// loggerUsesInternalHandlerTarget は、有効な通常loggerとそのfailure storeの設定だけを調べます。
// 出力先に選ばれていないbackendのenv値はruntimeでsourceにならないため対象外です。
func loggerUsesInternalHandlerTarget(
	cfg logconfig.LoggerConfig,
	target pkglogger.LogOutputDestination,
) bool {
	destinations := cfg.LogOutputDestinations

	if destinations.Has(logconfig.LogOutputFile) {
		if routingUsesTarget(cfg.FileConfig.HandlerConfig, target) {
			return true
		}
		if cfg.FileConfig.FileSystemStoreConfig.LifecycleMode == pkgfsstore.FileLifecycleModeRunner &&
			routingUsesTarget(cfg.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig, target) {
			return true
		}
	}
	if destinations.Has(logconfig.LogOutputStdout) &&
		routingUsesTarget(cfg.StdoutConfig.HandlerConfig, target) {
		return true
	}
	if destinations.Has(logconfig.LogOutputSlack) &&
		routingUsesTarget(cfg.SlackConfig.HandlerConfig, target) {
		return true
	}
	if destinations.Has(logconfig.LogOutputOpenSearch) &&
		(routingUsesTarget(cfg.OpenSearchConfig.OpenSearchLoggerBaseConfig.HandlerConfig, target) ||
			failureStoreUsesInternalHandlerTarget(
				cfg.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig,
				target,
			)) {
		return true
	}
	if destinations.Has(logconfig.LogOutputLoki) &&
		(routingUsesTarget(cfg.LokiConfig.LokiLoggerBaseConfig.HandlerConfig, target) ||
			failureStoreUsesInternalHandlerTarget(
				cfg.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig,
				target,
			)) {
		return true
	}
	if destinations.Has(logconfig.LogOutputCloudWatchLogs) &&
		(routingUsesTarget(cfg.CloudWatchLogsConfig.CloudWatchLogsLoggerBaseConfig.HandlerConfig, target) ||
			failureStoreUsesInternalHandlerTarget(
				cfg.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
				target,
			)) {
		return true
	}
	return destinations.Has(logconfig.LogOutputClickHouse) &&
		(routingUsesTarget(cfg.ClickHouseConfig.ClickHouseLoggerBaseConfig.HandlerConfig, target) ||
			failureStoreUsesInternalHandlerTarget(
				cfg.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig,
				target,
			))
}

func asyncLoggerUsesInternalHandlerTarget(
	cfg logconfig.AsyncBestEffortMultiLoggerConfig,
	target pkglogger.LogOutputDestination,
) bool {
	// asyncと通常loggerの各backend設定は同じ型なので、通常側の判定へ必要な値だけを移します。
	return loggerUsesInternalHandlerTarget(logconfig.LoggerConfig{
		LogOutputDestinations: cfg.LogOutputDestinations,
		FileConfig:            cfg.FileConfig,
		StdoutConfig:          cfg.StdoutConfig,
		SlackConfig:           cfg.SlackConfig,
		OpenSearchConfig:      cfg.OpenSearchConfig,
		LokiConfig:            cfg.LokiConfig,
		CloudWatchLogsConfig:  cfg.CloudWatchLogsConfig,
		ClickHouseConfig:      cfg.ClickHouseConfig,
	}, target)
}

// deliveryLoggerUsesInternalHandlerTarget は、通常出力先として選択されたDelivery loggerと、
// そのloggerで有効なfailure archiveだけを調べます。選択されていないsourceのenv値は
// runtimeでinternal logを発生させないため走査しません。
func deliveryLoggerUsesInternalHandlerTarget(
	cfg logconfig.DeliveryLoggerConfig,
	target pkglogger.LogOutputDestination,
) bool {
	destinations := cfg.LogOutputDestinations

	if destinations.Has(logconfig.LogOutputFile) {
		if routingUsesTarget(cfg.DeliveryFileConfig.HandlerConfig, target) {
			return true
		}
		if cfg.DeliveryFileConfig.FileSystemStoreConfig.LifecycleMode == pkgfsstore.FileLifecycleModeRunner &&
			routingUsesTarget(cfg.DeliveryFileConfig.FileSystemStoreConfig.LifecycleHandlerConfig, target) {
			return true
		}
	}
	if destinations.Has(logconfig.LogOutputStdout) &&
		routingUsesTarget(cfg.DeliveryStdoutConfig.HandlerConfig, target) {
		return true
	}
	if destinations.Has(logconfig.LogOutputSlack) &&
		routingUsesTarget(cfg.DeliverySlackConfig.HandlerConfig, target) {
		return true
	}
	if destinations.Has(logconfig.LogOutputOpenSearch) &&
		(routingUsesTarget(cfg.DeliveryOpenSearchConfig.DeliveryOpenSearchLoggerBaseConfig.HandlerConfig, target) ||
			deliveryFailureStoreUsesInternalHandlerTarget(
				cfg.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig,
				target,
			)) {
		return true
	}
	if destinations.Has(logconfig.LogOutputLoki) &&
		(routingUsesTarget(cfg.DeliveryLokiConfig.DeliveryLokiLoggerBaseConfig.HandlerConfig, target) ||
			deliveryFailureStoreUsesInternalHandlerTarget(
				cfg.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.FailureHandlerConfig,
				target,
			)) {
		return true
	}
	if destinations.Has(logconfig.LogOutputCloudWatchLogs) &&
		(routingUsesTarget(
			cfg.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsLoggerBaseConfig.HandlerConfig,
			target,
		) ||
			deliveryFailureStoreUsesInternalHandlerTarget(
				cfg.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
				target,
			)) {
		return true
	}
	return destinations.Has(logconfig.LogOutputClickHouse) &&
		(routingUsesTarget(cfg.DeliveryClickHouseConfig.DeliveryClickHouseLoggerBaseConfig.HandlerConfig, target) ||
			deliveryFailureStoreUsesInternalHandlerTarget(
				cfg.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig,
				target,
			))
}

// collectInternalHandlerTargets は、選択済みsourceがinternal logの委譲先に指定した
// 通常出力先以外のtargetを、定義順かつ重複なしで返します。
//
// handler専用に読み込むtarget自身のroutingは走査しません。dedicated fallbackは
// application loggerのfailure handlingを再帰的に行わないためです。
// 未知のselectorは対応するtarget設定を決定できないためここでは収集しませんが、
// 元のrouting値は保持し、MapLoggerConfigなどのmapping後validationで起動エラーにします。
func collectInternalHandlerTargets(
	destinations pkglogger.LogOutputDestinations,
	usesTarget func(pkglogger.LogOutputDestination) bool,
) pkglogger.LogOutputDestinations {
	allTargets := pkglogger.AllLogOutputDestinations()
	targets := make(pkglogger.LogOutputDestinations, 0, len(allTargets))
	for _, target := range allTargets {
		if destinations.Has(target) || !usesTarget(target) {
			continue
		}
		targets = append(targets, target)
	}
	return targets
}

// requiresLoggerTargetConfig は、targetが通常出力先またはinternal logの委譲先として
// 必要かを返します。config loaderと公開DTO mapperで同じ可用性判定を使います。
func requiresLoggerTargetConfig(
	destinations pkglogger.LogOutputDestinations,
	handlerTargets pkglogger.LogOutputDestinations,
	target pkglogger.LogOutputDestination,
) bool {
	return destinations.Has(target) || handlerTargets.Has(target)
}
