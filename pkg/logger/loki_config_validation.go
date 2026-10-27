package logger

import (
	"errors"

	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func validateLoggerLokiBackendConfig(
	sourceLabel string,
	cfg *LokiConfig,
	client *pkgloki.Config,
) error {
	return errors.Join(
		validateBufferedFlushInterval(sourceLabel, LogOutputLoki, cfg.FlushInterval),
		validateLokiLoggerConfig(
			sourceLabel,
			cfg.MaxEntries,
			cfg.BufferFlushBytes,
			cfg.FallbackQueueMaxEntries,
			cfg.MaxLineBytes,
			cfg.MaxPushRequestBytes,
		),
		validateLokiStaticLabels(sourceLabel, cfg.StaticLabels),
		validateLokiClient(sourceLabel, client),
	)
}

func validateDeliveryLokiBackendConfig(
	sourceLabel string,
	cfg *DeliveryLokiConfig,
	client *pkgloki.Config,
) error {
	return errors.Join(
		validateLokiRequestConfig(sourceLabel, cfg.MaxLineBytes, cfg.MaxPushRequestBytes),
		validateLokiStaticLabels(sourceLabel, cfg.StaticLabels),
		validateLokiClient(sourceLabel, client),
	)
}

func validateLokiStaticLabels(sourceLabel string, labels map[string]string) error {
	// 送信時にはloggerがappName・loglevel・envを付与するため、追加labelは空でも有効です。
	if len(labels) == 0 {
		return nil
	}
	if err := pkgloki.ValidateLabels(labels); err != nil {
		return backendRuleError(
			sourceLabel, LogOutputLoki, ConfigFieldStaticLabels, nil, ErrInvalidSetting,
			"loki static labels are invalid", err,
		)
	}
	return nil
}

// ValidateLokiPushLimits validates the byte limits on a single line and on a
// whole Push request that are applied before sending. Keeping them in line
// with the actual limits of Loki or a gateway is the user's responsibility. An
// invalid setting is reported as a ConfigFieldError with the kind
// ErrNonPositiveSetting or ErrInconsistentSetting.
func ValidateLokiPushLimits(maxLineBytes, maxPushRequestBytes int) error {
	var errs []error
	if maxLineBytes <= 0 {
		errs = append(errs, settingError(
			ConfigFieldMaxLineBytes, ErrNonPositiveSetting, maxLineBytes,
			"max line bytes must be positive: %d", maxLineBytes,
		))
	}
	if maxPushRequestBytes <= 0 {
		errs = append(errs, settingError(
			ConfigFieldMaxPushRequestBytes, ErrNonPositiveSetting, maxPushRequestBytes,
			"max push request bytes must be positive: %d",
			maxPushRequestBytes,
		))
	}
	if maxLineBytes > 0 && maxPushRequestBytes > 0 && maxLineBytes >= maxPushRequestBytes {
		inconsistent := settingError(
			ConfigFieldMaxLineBytes, ErrInconsistentSetting, maxLineBytes,
			"max line bytes %d must be less than max push request bytes %d",
			maxLineBytes,
			maxPushRequestBytes,
		)
		inconsistent.Limit = maxPushRequestBytes
		errs = append(errs, inconsistent)
	}
	return errors.Join(errs...)
}

// ValidateLokiMaxEntries validates that the threshold on the number of entries
// that starts a buffer flush is positive. A value of 0 or less is reported as
// a ConfigFieldError with the kind ErrNonPositiveSetting.
func ValidateLokiMaxEntries(maxEntries int) error {
	if maxEntries <= 0 {
		return settingError(
			ConfigFieldMaxEntries, ErrNonPositiveSetting, maxEntries,
			"max entries must be greater than zero: %d", maxEntries,
		)
	}
	return nil
}

// ValidateLokiBufferConfig validates the soft flush thresholds of the regular
// buffer and the hard limit of the fallback queue. MaxPushRequestBytes governs
// the physical splitting of Push requests on its own.
func ValidateLokiBufferConfig(
	maxEntries int,
	bufferFlushBytes int,
	fallbackQueueMaxEntries int,
) error {
	var errs []error
	if err := ValidateLokiMaxEntries(maxEntries); err != nil {
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
	return errors.Join(errs...)
}

// validateFallbackQueueMaxEntriesは、fallback queueのhard上限が正の値で、buffer flushの
// entry件数閾値以上であることを検証します。LokiとClickHouseで同じ規則とmessageを使います。
func validateFallbackQueueMaxEntries(fallbackQueueMaxEntries, maxEntries int) error {
	if fallbackQueueMaxEntries <= 0 {
		return settingError(
			ConfigFieldFallbackQueueMaxEntries, ErrNonPositiveSetting, fallbackQueueMaxEntries,
			"fallback queue max entries must be positive: %d",
			fallbackQueueMaxEntries,
		)
	}
	if maxEntries > 0 && fallbackQueueMaxEntries < maxEntries {
		inconsistent := settingError(
			ConfigFieldFallbackQueueMaxEntries, ErrInconsistentSetting, fallbackQueueMaxEntries,
			"fallback queue max entries %d must be greater than or equal to buffer flush max entries %d",
			fallbackQueueMaxEntries,
			maxEntries,
		)
		inconsistent.Limit = maxEntries
		return inconsistent
	}
	return nil
}

func validateLokiLoggerConfig(
	sourceLabel string,
	maxEntries int,
	bufferFlushBytes int,
	fallbackQueueMaxEntries int,
	maxLineBytes int,
	maxPushRequestBytes int,
) error {
	var errs []error
	if err := ValidateLokiBufferConfig(
		maxEntries,
		bufferFlushBytes,
		fallbackQueueMaxEntries,
	); err != nil {
		errs = append(errs, err)
	}
	if err := ValidateLokiPushLimits(maxLineBytes, maxPushRequestBytes); err != nil {
		errs = append(errs, err)
	}
	return wrapBackendSettingErrors(sourceLabel, LogOutputLoki, "logger config", errors.Join(errs...))
}

func validateLokiRequestConfig(
	sourceLabel string,
	maxLineBytes int,
	maxPushRequestBytes int,
) error {
	return wrapBackendSettingErrors(
		sourceLabel, LogOutputLoki, "logger config", ValidateLokiPushLimits(maxLineBytes, maxPushRequestBytes),
	)
}

func validateLokiClient(sourceLabel string, cfg *pkgloki.Config) error {
	if cfg == nil {
		return missingClientConfigError(sourceLabel, LogOutputLoki, "loki")
	}
	if err := pkgloki.ValidateConfig(*cfg); err != nil {
		return backendRuleError(
			sourceLabel, LogOutputLoki, ConfigFieldClient, nil, ErrInvalidClientConfig,
			"loki client config is invalid", err,
		)
	}
	return nil
}
