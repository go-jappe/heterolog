package logger

import (
	"strings"

	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

// LoggerConfigRequiresBaseBackend reports whether the base backend of target is
// to be constructed and validated. A base backend is a backend constructed from
// the settings directly in LoggerConfig, such as File and OpenSearch, as
// distinct from a backend dedicated to internal logs that is specified in
// InternalLogDestinations.
//
// It returns true when target is explicitly listed as a regular output, when
// no destinations are specified and the base settings of target are non-nil,
// or when an active internal log handler refers to target and target has no
// dedicated backend.
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

// DeliveryLoggerConfigRequiresBaseBackend reports, for a Delivery logger,
// whether the base backend of target is to be constructed and validated. A base
// backend is a backend constructed from the settings directly in
// DeliveryLoggerConfig, as distinct from a dedicated backend in
// InternalLogDestinations. The conditions are the same as those of
// LoggerConfigRequiresBaseBackend.
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
