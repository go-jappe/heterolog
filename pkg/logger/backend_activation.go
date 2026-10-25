package logger

import (
	"strings"

	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

// LoggerConfigRequiresBaseBackendは、targetのbase backendが構築・検証対象かを返します。
// base backendとはLoggerConfig直下のFile、OpenSearchなどの設定から構築するbackendで、
// InternalLogDestinationsに指定されたinternal log専用backendとは区別します。
//
// targetを通常出力先として明示した場合、destination指定が空でtargetのbase設定が
// non-nilの場合、または専用backendを持たない有効な内部handlerがtargetを参照する場合に
// trueを返します。
func LoggerConfigRequiresBaseBackend(
	cfg LoggerConfig,
	target LogOutputDestination,
) bool {
	handlerRequiresBase := handlerRoutingSourcesUseTarget(
		loggerActiveHandlerRoutingSources(cfg),
		target,
	) && !loggerConfigHasInternalLogOverride(cfg, target)

	return configRequiresBaseBackend(
		cfg.LogOutputDestinations,
		target,
		loggerConfigHasBaseBackend(cfg, target),
		handlerRequiresBase,
	)
}

// DeliveryLoggerConfigRequiresBaseBackendは、Delivery loggerについてtargetのbase
// backendが構築・検証対象かを返します。base backendはDeliveryLoggerConfig直下の
// 設定から構築するbackendで、InternalLogDestinationsの専用backendとは区別します。
// 判定条件はLoggerConfigRequiresBaseBackendと同じです。
func DeliveryLoggerConfigRequiresBaseBackend(
	cfg DeliveryLoggerConfig,
	target LogOutputDestination,
) bool {
	handlerRequiresBase := handlerRoutingSourcesUseTarget(
		deliveryLoggerActiveHandlerRoutingSources(cfg),
		target,
	) && !deliveryLoggerConfigHasInternalLogOverride(cfg, target)

	return configRequiresBaseBackend(
		cfg.LogOutputDestinations,
		target,
		deliveryLoggerConfigHasBaseBackend(cfg, target),
		handlerRequiresBase,
	)
}

// configRequiresBaseBackendは、Logger contract固有のconfig表現から受け取った情報を使い、
// sync/asyncとDeliveryに共通するbase backendの必要性を判定します。
// handlerRequiresBaseは、internal log専用backendによる置換まで解決済みの値です。
func configRequiresBaseBackend(
	destinations LogOutputDestinations,
	target LogOutputDestination,
	baseConfigured bool,
	handlerRequiresBase bool,
) bool {
	if len(destinations) > 0 {
		return destinations.Has(target) || handlerRequiresBase
	}
	return baseConfigured || handlerRequiresBase
}

// loggerConfigHasInternalLogOverrideは、targetについてbase設定を置き換えるinternal log
// 専用backendが明示されているかを返します。
func loggerConfigHasInternalLogOverride(
	cfg LoggerConfig,
	target LogOutputDestination,
) bool {
	if cfg.InternalLogDestinations == nil {
		return false
	}
	return loggerConfigHasBaseBackend(
		cfg.InternalLogDestinations.ToLoggerConfig(),
		target,
	)
}

// deliveryLoggerConfigHasInternalLogOverrideはDelivery loggerについて、targetのbase設定を
// 置き換えるinternal log専用backendが明示されているかを返します。
func deliveryLoggerConfigHasInternalLogOverride(
	cfg DeliveryLoggerConfig,
	target LogOutputDestination,
) bool {
	if cfg.InternalLogDestinations == nil {
		return false
	}
	return deliveryLoggerConfigHasBaseBackend(
		cfg.InternalLogDestinations.ToLoggerConfig(),
		target,
	)
}

func loggerConfigHasBaseBackend(
	cfg LoggerConfig,
	target LogOutputDestination,
) bool {
	switch target {
	case LogOutputFile:
		return cfg.File != nil
	case LogOutputStdout:
		return cfg.Stdout != nil
	case LogOutputSlack:
		return cfg.Slack != nil
	case LogOutputOpenSearch:
		return cfg.OpenSearch != nil
	case LogOutputLoki:
		return cfg.Loki != nil
	case LogOutputCloudWatchLogs:
		return cfg.CloudWatchLogs != nil
	case LogOutputClickHouse:
		return cfg.ClickHouse != nil
	default:
		return false
	}
}

func deliveryLoggerConfigHasBaseBackend(
	cfg DeliveryLoggerConfig,
	target LogOutputDestination,
) bool {
	switch target {
	case LogOutputFile:
		return cfg.File != nil
	case LogOutputStdout:
		return cfg.Stdout != nil
	case LogOutputSlack:
		return cfg.Slack != nil
	case LogOutputOpenSearch:
		return cfg.OpenSearch != nil
	case LogOutputLoki:
		return cfg.Loki != nil
	case LogOutputCloudWatchLogs:
		return cfg.CloudWatchLogs != nil
	case LogOutputClickHouse:
		return cfg.ClickHouse != nil
	default:
		return false
	}
}

func configLogOutputEnabled(
	destinations LogOutputDestinations,
	target LogOutputDestination,
) bool {
	return len(destinations) == 0 || destinations.Has(target)
}

func handlerRoutingUsesBaseTarget(
	cfg loggerhandler.HandlerConfig,
	target LogOutputDestination,
) bool {
	want := normalizeHandlerSelector(target.String())
	for _, selectors := range [][]string{
		cfg.OnErrorConfig,
		cfg.OnWarnConfig,
		cfg.OnInfoConfig,
		cfg.OnDebugConfig,
	} {
		for _, selector := range selectors {
			if normalizeHandlerSelector(selector) == want {
				return true
			}
		}
	}
	return false
}

func normalizeHandlerSelector(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
