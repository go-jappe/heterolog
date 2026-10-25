package logger

import (
	"errors"
)

// validateDeliveryInternalLogBatchConfigは、Deliveryの有効なinternal log handlerが
// 実際に解決できるbackendについてだけfallback batch設定を検証します。handlerが参照しても
// backend設定が存在しないtargetは、handler routing validationが欠落を報告します。
func validateDeliveryInternalLogBatchConfig(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	sources := deliveryLoggerActiveHandlerRoutingSources(cfg)
	available := deliveryLoggerHandlerSelectors(cfg)
	usesConfiguredTarget := func(target LogOutputDestination) bool {
		if !handlerRoutingSourcesUseTarget(sources, target) {
			return false
		}
		_, configured := available[normalizeHandlerSelector(target.String())]
		return configured
	}

	var errs []error
	if usesConfiguredTarget(LogOutputOpenSearch) {
		errs = append(errs, validateDeliveryInternalLogOpenSearchBatchConfig(
			sourceLabel,
			cfg.InternalLogBatchConfig.OpenSearch,
		))
	}
	if usesConfiguredTarget(LogOutputLoki) {
		errs = append(errs, validateDeliveryInternalLogLokiBatchConfig(
			sourceLabel,
			cfg.InternalLogBatchConfig.Loki,
		))
	}
	if usesConfiguredTarget(LogOutputCloudWatchLogs) {
		errs = append(errs, validateDeliveryInternalLogCloudWatchLogsBatchConfig(
			sourceLabel,
			cfg.InternalLogBatchConfig.CloudWatchLogs,
		))
	}
	if usesConfiguredTarget(LogOutputSlack) {
		errs = append(errs, validateDeliveryInternalLogSlackBatchConfig(
			sourceLabel,
			cfg.InternalLogBatchConfig.Slack,
		))
	}

	return errors.Join(errs...)
}

func validateDeliveryInternalLogOpenSearchBatchConfig(
	sourceLabel string,
	cfg DeliveryInternalLogOpenSearchBatchConfig,
) error {
	var errs []error
	errs = append(errs, validateBufferedFlushInterval(
		sourceLabel+" internal log fallback",
		LogOutputOpenSearch,
		cfg.FlushInterval,
	))
	if err := ValidateOpenSearchMaxBulkDocument(cfg.MaxBulkDocument); err != nil {
		errs = append(errs, wrapBackendSettingErrors(
			sourceLabel+" internal log fallback", LogOutputOpenSearch, "logger config", err,
		))
	}
	if err := ValidateOpenSearchBulkByteLimits(
		cfg.BufferFlushBytes,
		cfg.MaxBulkRequestBytes,
	); err != nil {
		errs = append(errs, wrapBackendSettingErrors(
			sourceLabel+" internal log fallback", LogOutputOpenSearch, "logger config", err,
		))
	}
	return errors.Join(errs...)
}

func validateDeliveryInternalLogLokiBatchConfig(
	sourceLabel string,
	cfg DeliveryInternalLogLokiBatchConfig,
) error {
	var errs []error
	errs = append(errs, validateBufferedFlushInterval(
		sourceLabel+" internal log fallback",
		LogOutputLoki,
		cfg.FlushInterval,
	))
	if err := ValidateLokiBufferConfig(
		cfg.MaxEntries,
		cfg.BufferFlushBytes,
		cfg.FallbackQueueMaxEntries,
	); err != nil {
		errs = append(errs, wrapBackendSettingErrors(
			sourceLabel+" internal log fallback", LogOutputLoki, "logger config", err,
		))
	}
	return errors.Join(errs...)
}

func validateDeliveryInternalLogCloudWatchLogsBatchConfig(
	sourceLabel string,
	cfg DeliveryInternalLogCloudWatchLogsBatchConfig,
) error {
	var errs []error
	errs = append(errs, validateBufferedFlushInterval(
		sourceLabel+" internal log fallback",
		LogOutputCloudWatchLogs,
		cfg.FlushInterval,
	))
	if err := ValidateCloudWatchLogsBufferConfig(
		cfg.BufferFlushMaxEvents,
		cfg.BufferFlushBytes,
		cfg.FallbackQueueMaxEvents,
	); err != nil {
		errs = append(errs, wrapBackendSettingErrors(
			sourceLabel+" internal log fallback", LogOutputCloudWatchLogs, "logger config", err,
		))
	}
	return errors.Join(errs...)
}

func validateDeliveryInternalLogSlackBatchConfig(
	sourceLabel string,
	cfg DeliveryInternalLogSlackBatchConfig,
) error {
	var errs []error
	if err := ValidateSlackMaxLogsPerNotification(cfg.MaxLogsPerNotification); err != nil {
		errs = append(errs, wrapBackendSettingErrors(
			sourceLabel+" internal log fallback", LogOutputSlack, "logger config", err,
		))
	}
	if err := ValidateSlackIntervals(cfg.Intervals); err != nil {
		errs = append(errs, wrapBackendSettingErrors(
			sourceLabel+" internal log fallback", LogOutputSlack, "logger config", err,
		))
	}
	if err := ValidateSlackMaxRestThreshold(cfg.MaxRestThreshold); err != nil {
		errs = append(errs, wrapBackendSettingErrors(
			sourceLabel+" internal log fallback", LogOutputSlack, "logger config", err,
		))
	}
	return errors.Join(errs...)
}
