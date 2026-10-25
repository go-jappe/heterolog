package logger

import (
	"errors"
	"fmt"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

func validateLoggerCloudWatchLogsBackendConfig(
	sourceLabel string,
	cfg *CloudWatchLogsConfig,
	client *pkgcloudwatchlogs.Config,
) error {
	return errors.Join(
		validateBufferedFlushInterval(sourceLabel, LogOutputCloudWatchLogs, cfg.FlushInterval),
		validateCloudWatchLogsLoggerConfig(
			sourceLabel,
			cfg.BufferFlushMaxEvents,
			cfg.BufferFlushBytes,
			cfg.FallbackQueueMaxEvents,
		),
		validateCloudWatchLogsBackend(
			sourceLabel,
			cfg.LogGroupName,
			cfg.LogStreamName,
			client,
		),
	)
}

func validateDeliveryCloudWatchLogsBackendConfig(
	sourceLabel string,
	cfg *DeliveryCloudWatchLogsConfig,
	client *pkgcloudwatchlogs.Config,
) error {
	return validateCloudWatchLogsBackend(
		sourceLabel,
		cfg.LogGroupName,
		cfg.LogStreamName,
		client,
	)
}

func validateCloudWatchLogsBackend(
	sourceLabel string,
	logGroupName string,
	logStreamName string,
	cfg *pkgcloudwatchlogs.Config,
) error {
	if cfg == nil {
		return missingClientConfigError(sourceLabel, LogOutputCloudWatchLogs, "cloudwatch logs")
	}
	var errs []error
	if err := pkgcloudwatchlogs.ValidateConfig(*cfg); err != nil {
		errs = append(errs, backendRuleError(
			sourceLabel, LogOutputCloudWatchLogs, ConfigFieldClient, nil, ErrInvalidClientConfig,
			"cloudwatch logs client config is invalid", err,
		))
	}
	if err := pkgcloudwatchlogs.ValidateLogStreamName(logStreamName); err != nil {
		errs = append(errs, backendRuleError(
			sourceLabel, LogOutputCloudWatchLogs, ConfigFieldLogStreamName, logStreamName, ErrInvalidSetting,
			"cloudwatch logs log stream config is invalid", err,
		))
	}
	if logGroupName != cfg.LogGroupName {
		errs = append(errs, &ConfigFieldError{
			Source:  sourceLabel,
			Backend: LogOutputCloudWatchLogs,
			Field:   ConfigFieldLogGroupName,
			Detail: fmt.Sprintf(
				"cloudwatch logs log group does not match client config: logger=%q client=%q",
				logGroupName,
				cfg.LogGroupName,
			),
			Value: logGroupName,
			Limit: cfg.LogGroupName,
			Err:   ErrLogGroupMismatch,
		})
	}
	return errors.Join(errs...)
}

// ValidateCloudWatchLogsBufferConfigは、通常bufferのsoft flush閾値とfallback queueの
// hard上限を検証します。PutLogEventsの固定上限に応じた物理request分割はplannerが
// 独立して担当するため、ここでは10,000件や1MiBとの大小を強制しません。
// 不正な項目はErrNonPositiveSettingまたはErrInconsistentSettingを持つConfigFieldErrorです。
func ValidateCloudWatchLogsBufferConfig(
	bufferFlushMaxEvents int,
	bufferFlushBytes int,
	fallbackQueueMaxEvents int,
) error {
	var errs []error
	if bufferFlushMaxEvents <= 0 {
		errs = append(errs, settingError(
			ConfigFieldBufferFlushMaxEvents, ErrNonPositiveSetting, bufferFlushMaxEvents,
			"buffer flush max events must be positive: %d",
			bufferFlushMaxEvents,
		))
	}
	if bufferFlushBytes <= 0 {
		errs = append(errs, settingError(
			ConfigFieldBufferFlushBytes, ErrNonPositiveSetting, bufferFlushBytes,
			"buffer flush bytes must be positive: %d",
			bufferFlushBytes,
		))
	}
	if fallbackQueueMaxEvents <= 0 {
		errs = append(errs, settingError(
			ConfigFieldFallbackQueueMaxEvents, ErrNonPositiveSetting, fallbackQueueMaxEvents,
			"fallback queue max events must be positive: %d",
			fallbackQueueMaxEvents,
		))
	} else if bufferFlushMaxEvents > 0 && fallbackQueueMaxEvents < bufferFlushMaxEvents {
		inconsistent := settingError(
			ConfigFieldFallbackQueueMaxEvents, ErrInconsistentSetting, fallbackQueueMaxEvents,
			"fallback queue max events %d must be greater than or equal to buffer flush max events %d",
			fallbackQueueMaxEvents,
			bufferFlushMaxEvents,
		)
		inconsistent.Limit = bufferFlushMaxEvents
		errs = append(errs, inconsistent)
	}
	return errors.Join(errs...)
}

func validateCloudWatchLogsLoggerConfig(
	sourceLabel string,
	bufferFlushMaxEvents int,
	bufferFlushBytes int,
	fallbackQueueMaxEvents int,
) error {
	return wrapBackendSettingErrors(
		sourceLabel, LogOutputCloudWatchLogs, "logger config",
		ValidateCloudWatchLogsBufferConfig(bufferFlushMaxEvents, bufferFlushBytes, fallbackQueueMaxEvents),
	)
}
