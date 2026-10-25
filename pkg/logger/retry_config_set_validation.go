package logger

import (
	"errors"
	"time"
)

// ValidateLoggerRetryClientConfigsは、sync/async loggerが実際に構築するbase backendと
// internal-log専用保存先のretry設定を検証します。
func ValidateLoggerRetryClientConfigs(sourceLabel string, cfg LoggerConfig) error {
	var errs []error
	errs = append(errs, validateActiveLoggerRetryClientConfigs(sourceLabel, cfg))
	if cfg.InternalLogDestinations != nil {
		errs = append(errs, validateConfiguredLoggerRetryClientConfigs(
			sourceLabel+" internal log destination",
			cfg.InternalLogDestinations.ToLoggerConfig(),
		))
	}
	return errors.Join(errs...)
}

// ValidateDeliveryLoggerRetryClientConfigsは、Delivery loggerが実際に構築する
// base backendとinternal-log専用保存先のretry設定を検証します。
func ValidateDeliveryLoggerRetryClientConfigs(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	var errs []error
	errs = append(errs, validateActiveDeliveryRetryClientConfigs(sourceLabel, cfg))
	if cfg.InternalLogDestinations != nil {
		errs = append(
			errs,
			validateConfiguredDeliveryRetryClientConfigs(
				sourceLabel+" internal log destination",
				cfg.InternalLogDestinations.ToLoggerConfig(),
			),
		)
	}
	return errors.Join(errs...)
}

func validateActiveLoggerRetryClientConfigs(sourceLabel string, cfg LoggerConfig) error {
	var errs []error
	if cfg.OpenSearch != nil &&
		LoggerConfigRequiresBaseBackend(cfg, LogOutputOpenSearch) {
		errs = append(errs, validateOpenSearchRetryClientConfig(
			sourceLabel,
			cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.Loki != nil && LoggerConfigRequiresBaseBackend(cfg, LogOutputLoki) {
		errs = append(errs, validateLokiRetryClientConfig(
			sourceLabel,
			cfg.Loki.LokiRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.CloudWatchLogs != nil &&
		LoggerConfigRequiresBaseBackend(cfg, LogOutputCloudWatchLogs) {
		errs = append(errs, validateCloudWatchLogsRetryClientConfig(
			sourceLabel,
			cfg.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.ClickHouse != nil &&
		LoggerConfigRequiresBaseBackend(cfg, LogOutputClickHouse) {
		errs = append(errs, validateClickHouseRetryClientConfig(
			sourceLabel,
			cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig,
		))
	}
	return errors.Join(errs...)
}

func validateConfiguredLoggerRetryClientConfigs(sourceLabel string, cfg LoggerConfig) error {
	var errs []error
	if cfg.OpenSearch != nil {
		errs = append(errs, validateOpenSearchRetryClientConfig(
			sourceLabel,
			cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.Loki != nil {
		errs = append(errs, validateLokiRetryClientConfig(
			sourceLabel,
			cfg.Loki.LokiRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.CloudWatchLogs != nil {
		errs = append(errs, validateCloudWatchLogsRetryClientConfig(
			sourceLabel,
			cfg.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.ClickHouse != nil {
		errs = append(errs, validateClickHouseRetryClientConfig(
			sourceLabel,
			cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig,
		))
	}
	return errors.Join(errs...)
}

func validateActiveDeliveryRetryClientConfigs(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	var errs []error
	if cfg.OpenSearch != nil &&
		DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputOpenSearch) {
		errs = append(errs, validateOpenSearchRetryClientConfig(
			sourceLabel,
			cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.Loki != nil &&
		DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputLoki) {
		errs = append(errs, validateLokiRetryClientConfig(
			sourceLabel,
			cfg.Loki.LokiRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.CloudWatchLogs != nil &&
		DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputCloudWatchLogs) {
		errs = append(errs, validateCloudWatchLogsRetryClientConfig(
			sourceLabel,
			cfg.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.ClickHouse != nil &&
		DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputClickHouse) {
		errs = append(errs, validateClickHouseRetryClientConfig(
			sourceLabel,
			cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig,
		))
	}
	return errors.Join(errs...)
}

func validateConfiguredDeliveryRetryClientConfigs(
	sourceLabel string,
	cfg DeliveryLoggerConfig,
) error {
	var errs []error
	if cfg.OpenSearch != nil {
		errs = append(errs, validateOpenSearchRetryClientConfig(
			sourceLabel,
			cfg.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.Loki != nil {
		errs = append(errs, validateLokiRetryClientConfig(
			sourceLabel,
			cfg.Loki.LokiRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.CloudWatchLogs != nil {
		errs = append(errs, validateCloudWatchLogsRetryClientConfig(
			sourceLabel,
			cfg.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig,
		))
	}
	if cfg.ClickHouse != nil {
		errs = append(errs, validateClickHouseRetryClientConfig(
			sourceLabel,
			cfg.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig,
		))
	}
	return errors.Join(errs...)
}

func validateOpenSearchRetryClientConfig(
	sourceLabel string,
	cfg *OpenSearchRetryableClientCaseTransportErrorConfig,
) error {
	if cfg == nil {
		return nil
	}
	return validateRetryClientConfigValues(
		sourceLabel,
		LogOutputOpenSearch,
		cfg.RetryClientMode,
		cfg.MaxAttempts,
		cfg.BaseDelay,
		cfg.MaxDelay,
	)
}

func validateLokiRetryClientConfig(
	sourceLabel string,
	cfg *LokiRetryableClientCaseTransportErrorConfig,
) error {
	if cfg == nil {
		return nil
	}
	return validateRetryClientConfigValues(
		sourceLabel,
		LogOutputLoki,
		cfg.RetryClientMode,
		cfg.MaxAttempts,
		cfg.BaseDelay,
		cfg.MaxDelay,
	)
}

func validateCloudWatchLogsRetryClientConfig(
	sourceLabel string,
	cfg *CloudWatchLogsRetryableClientCaseTransportErrorConfig,
) error {
	if cfg == nil {
		return nil
	}
	return validateRetryClientConfigValues(
		sourceLabel,
		LogOutputCloudWatchLogs,
		cfg.RetryClientMode,
		cfg.MaxAttempts,
		cfg.BaseDelay,
		cfg.MaxDelay,
	)
}

func validateClickHouseRetryClientConfig(
	sourceLabel string,
	cfg *ClickHouseRetryableClientCaseTransportErrorConfig,
) error {
	if cfg == nil {
		return nil
	}
	return validateRetryClientConfigValues(
		sourceLabel,
		LogOutputClickHouse,
		cfg.RetryClientMode,
		cfg.MaxAttempts,
		cfg.BaseDelay,
		cfg.MaxDelay,
	)
}

func validateRetryClientConfigValues(
	sourceLabel string,
	backend LogOutputDestination,
	mode RetryClientMode,
	maxAttempts int,
	baseDelay time.Duration,
	maxDelay time.Duration,
) error {
	return wrapBackendSettingErrors(
		sourceLabel, backend, "retry client config",
		ValidateRetryClientConfig(mode, maxAttempts, baseDelay, maxDelay),
	)
}
