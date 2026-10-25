package logger

import (
	"errors"

	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func validateLoggerOpenSearchBackendConfig(
	sourceLabel string,
	cfg *OpenSearchConfig,
	client *pkgopensearch.Config,
) error {
	return errors.Join(
		validateBufferedFlushInterval(sourceLabel, LogOutputOpenSearch, cfg.FlushInterval),
		validateOpenSearchLoggerConfig(
			sourceLabel,
			cfg.MaxBulkDocument,
			cfg.BufferFlushBytes,
			cfg.MaxBulkRequestBytes,
		),
		validateOpenSearchBackend(sourceLabel, cfg.Index, client),
	)
}

func validateDeliveryOpenSearchBackendConfig(
	sourceLabel string,
	cfg *DeliveryOpenSearchConfig,
	client *pkgopensearch.Config,
) error {
	return validateOpenSearchBackend(sourceLabel, cfg.Index, client)
}

func validateOpenSearchBackend(
	sourceLabel string,
	index string,
	client *pkgopensearch.Config,
) error {
	var errs []error
	errs = append(errs, validateOpenSearchClient(sourceLabel, client))
	if err := pkgopensearch.ValidateIndex(index); err != nil {
		errs = append(errs, backendRuleError(
			sourceLabel, LogOutputOpenSearch, ConfigFieldIndex, index, ErrInvalidSetting,
			"opensearch logger config is invalid", err,
		))
	}
	return errors.Join(errs...)
}

func validateOpenSearchClient(sourceLabel string, cfg *pkgopensearch.Config) error {
	if cfg == nil {
		return missingClientConfigError(sourceLabel, LogOutputOpenSearch, "opensearch")
	}
	if err := pkgopensearch.ValidateConfig(*cfg); err != nil {
		return backendRuleError(
			sourceLabel, LogOutputOpenSearch, ConfigFieldClient, nil, ErrInvalidClientConfig,
			"opensearch client config is invalid", err,
		)
	}
	return nil
}

// ValidateOpenSearchBulkByteLimitsは、bufferを早期flushするsoft thresholdと、一回の
// Bulk NDJSON requestを分割するhard limitを検証します。前者は後者より小さくなければ
// なりません。OpenSearchやproxyの実効上限との整合は利用者が管理します。
// 不正な項目はErrNonPositiveSettingまたはErrInconsistentSettingを持つConfigFieldErrorです。
func ValidateOpenSearchBulkByteLimits(bufferFlushBytes, maxBulkRequestBytes int) error {
	var errs []error
	if bufferFlushBytes <= 0 {
		errs = append(errs, settingError(
			ConfigFieldBufferFlushBytes, ErrNonPositiveSetting, bufferFlushBytes,
			"buffer flush bytes must be positive: %d",
			bufferFlushBytes,
		))
	}
	if maxBulkRequestBytes <= 0 {
		errs = append(errs, settingError(
			ConfigFieldMaxBulkRequestBytes, ErrNonPositiveSetting, maxBulkRequestBytes,
			"max bulk request bytes must be positive: %d",
			maxBulkRequestBytes,
		))
	}
	if bufferFlushBytes > 0 && maxBulkRequestBytes > 0 &&
		bufferFlushBytes >= maxBulkRequestBytes {
		inconsistent := settingError(
			ConfigFieldBufferFlushBytes, ErrInconsistentSetting, bufferFlushBytes,
			"buffer flush bytes must be less than max bulk request bytes: buffer=%d max=%d",
			bufferFlushBytes,
			maxBulkRequestBytes,
		)
		inconsistent.Limit = maxBulkRequestBytes
		errs = append(errs, inconsistent)
	}
	return errors.Join(errs...)
}

// ValidateOpenSearchMaxBulkDocumentは、buffer flushを開始するdocument件数の
// 閾値が正の値であることを検証します。0以下の値はErrNonPositiveSettingを持つ
// ConfigFieldErrorです。
func ValidateOpenSearchMaxBulkDocument(maxBulkDocument int) error {
	if maxBulkDocument <= 0 {
		return settingError(
			ConfigFieldMaxBulkDocument, ErrNonPositiveSetting, maxBulkDocument,
			"max bulk document must be greater than zero: %d", maxBulkDocument,
		)
	}
	return nil
}

func validateOpenSearchLoggerConfig(
	sourceLabel string,
	maxBulkDocument int,
	bufferFlushBytes int,
	maxBulkRequestBytes int,
) error {
	var errs []error
	if err := ValidateOpenSearchMaxBulkDocument(maxBulkDocument); err != nil {
		errs = append(errs, err)
	}
	if err := ValidateOpenSearchBulkByteLimits(bufferFlushBytes, maxBulkRequestBytes); err != nil {
		errs = append(errs, err)
	}
	return wrapBackendSettingErrors(sourceLabel, LogOutputOpenSearch, "logger config", errors.Join(errs...))
}
