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

// ValidateClickHouseMaxEntriesは、buffer flushを開始するentry件数の閾値が
// 正の値であることを検証します。0以下の値はErrNonPositiveSettingを持つ
// ConfigFieldErrorです。
func ValidateClickHouseMaxEntries(maxEntries int) error {
	if maxEntries <= 0 {
		return settingError(
			ConfigFieldMaxEntries, ErrNonPositiveSetting, maxEntries,
			"max entries must be greater than zero: %d", maxEntries,
		)
	}
	return nil
}

// ValidateClickHouseBatchLimitsはbufferの件数・byte soft threshold、fallback queueの
// hard上限、一回の物理JSONEachRow INSERT requestを分割するhard limitを検証します。
// ClickHouse serverやproxyの実効上限との整合は利用者が管理します。
// 不正な項目はErrNonPositiveSettingまたはErrInconsistentSettingを持つConfigFieldErrorです。
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
