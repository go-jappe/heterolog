package logger

import (
	"errors"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

func validateLoggerClickHouseBackendConfig(
	sourceLabel string,
	cfg *ClickHouseConfig,
	client *pkgclickhouse.Config,
) error {
	return errors.Join(
		validateBufferedFlushInterval(sourceLabel, LogOutputClickHouse, cfg.FlushInterval),
		validateClickHouseLoggerConfig(
			sourceLabel,
			cfg.MaxEntries,
			cfg.BufferFlushBytes,
			cfg.FallbackQueueMaxEntries,
			cfg.MaxInsertRequestBytes,
		),
		validateClickHouseClient(sourceLabel, client),
	)
}

func validateDeliveryClickHouseBackendConfig(
	sourceLabel string,
	cfg *DeliveryClickHouseConfig,
	client *pkgclickhouse.Config,
) error {
	return errors.Join(
		validateBufferedFlushInterval(sourceLabel, LogOutputClickHouse, cfg.FlushInterval),
		validateClickHouseLoggerConfig(
			sourceLabel,
			cfg.MaxEntries,
			cfg.BufferFlushBytes,
			cfg.FallbackQueueMaxEntries,
			cfg.MaxInsertRequestBytes,
		),
		validateClickHouseClient(sourceLabel, client),
	)
}

// ValidateClickHouseMaxEntries validates that the threshold on the number of
// entries that starts a buffer flush is positive. A value of 0 or less is
// reported as a ConfigFieldError with the kind ErrNonPositiveSetting.
func ValidateClickHouseMaxEntries(maxEntries int) error {
	if maxEntries <= 0 {
		return settingError(
			ConfigFieldMaxEntries, ErrNonPositiveSetting, maxEntries,
			"max entries must be greater than zero: %d", maxEntries,
		)
	}
	return nil
}

// ValidateClickHouseBatchLimits validates the soft thresholds on the number of
// entries and the bytes in the buffer, the hard limit of the fallback queue,
// and the hard limit at which a single physical JSONEachRow INSERT request is
// split. Keeping them consistent with the effective limits of the ClickHouse
// server or proxies is the user's responsibility. An invalid setting is
// reported as a ConfigFieldError with the kind ErrNonPositiveSetting or
// ErrInconsistentSetting.
func ValidateClickHouseBatchLimits(
	maxEntries int,
	bufferFlushBytes int,
	fallbackQueueMaxEntries int,
	maxInsertRequestBytes int,
) error {
	var errs []error
	if err := ValidateClickHouseMaxEntries(maxEntries); err != nil {
		errs = append(errs, err)
	}
	if bufferFlushBytes <= 0 {
		errs = append(errs, settingError(
			ConfigFieldBufferFlushBytes, ErrNonPositiveSetting, bufferFlushBytes,
			"buffer flush bytes must be positive: %d",
			bufferFlushBytes,
		))
	}
	errs = append(errs, validateFallbackQueueMaxEntries(fallbackQueueMaxEntries, maxEntries))
	if maxInsertRequestBytes <= 0 {
		errs = append(errs, settingError(
			ConfigFieldMaxInsertRequestBytes, ErrNonPositiveSetting, maxInsertRequestBytes,
			"max insert request bytes must be positive: %d",
			maxInsertRequestBytes,
		))
	}
	return errors.Join(errs...)
}

func validateClickHouseLoggerConfig(
	sourceLabel string,
	maxEntries int,
	bufferFlushBytes int,
	fallbackQueueMaxEntries int,
	maxInsertRequestBytes int,
) error {
	return wrapBackendSettingErrors(
		sourceLabel, LogOutputClickHouse, "logger config",
		ValidateClickHouseBatchLimits(maxEntries, bufferFlushBytes, fallbackQueueMaxEntries, maxInsertRequestBytes),
	)
}

func validateClickHouseClient(sourceLabel string, cfg *pkgclickhouse.Config) error {
	if cfg == nil {
		return missingClientConfigError(sourceLabel, LogOutputClickHouse, "clickhouse")
	}
	if err := pkgclickhouse.ValidateConfig(*cfg); err != nil {
		return backendRuleError(
			sourceLabel, LogOutputClickHouse, ConfigFieldClient, nil, ErrInvalidClientConfig,
			"clickhouse client config is invalid", err,
		)
	}
	return nil
}
