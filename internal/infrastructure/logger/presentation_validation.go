package logger

import (
	"errors"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// validateLoggerBodyConfigsは、sync/async loggerが使う出力先の本文の表示に関する設定を、
// fileを確保したりworkerを起動したりする前に検証します。backendを使うかどうかは、
// factoryの通常の経路と代替の経路に従って判断します。levelとsamplingは稼働中に変えられる
// 設定なので、それを用意する呼び出し元が検証します。不正な設定はそれぞれ、出所・backend・
// 項目を示す*pkglogger.ConfigFieldErrorです。
func validateLoggerBodyConfigs(label string, cfg pkglogger.LoggerConfig) error {
	var failures []error
	check := func(destination pkglogger.LogOutputDestination, body pkglogger.LogBodyConfig) {
		if pkglogger.LoggerConfigRequiresBaseBackend(cfg, destination) {
			failures = append(failures, pkglogger.ValidateLogBodyConfig(label, destination, body))
		}
	}
	if c := cfg.File; c != nil {
		check(pkglogger.LogOutputFile, c.LogBodyConfig)
	}
	if c := cfg.Stdout; c != nil {
		check(pkglogger.LogOutputStdout, c.LogBodyConfig)
	}
	if c := cfg.Slack; c != nil {
		check(pkglogger.LogOutputSlack, c.LogBodyConfig)
	}
	if c := cfg.OpenSearch; c != nil {
		check(pkglogger.LogOutputOpenSearch, c.LogBodyConfig)
	}
	if c := cfg.Loki; c != nil {
		check(pkglogger.LogOutputLoki, c.LogBodyConfig)
	}
	if c := cfg.CloudWatchLogs; c != nil {
		check(pkglogger.LogOutputCloudWatchLogs, c.LogBodyConfig)
	}
	if c := cfg.ClickHouse; c != nil {
		check(pkglogger.LogOutputClickHouse, c.LogBodyConfig)
	}
	if cfg.InternalLogDestinations != nil {
		failures = append(failures, validateLoggerBodyConfigs(label+" internal log destination", cfg.InternalLogDestinations.ToLoggerConfig()))
	}
	return errors.Join(failures...)
}

// validateDeliveryPresentationは、Delivery loggerの表示に関する設定を検証します。
// Delivery loggerはseverityで絞り込まず、samplingもしないため、本文の表示とSlackの
// levelだけを検証します。
func validateDeliveryPresentation(label string, cfg pkglogger.DeliveryLoggerConfig) error {
	var failures []error
	check := func(destination pkglogger.LogOutputDestination, body pkglogger.LogBodyConfig) {
		if pkglogger.DeliveryLoggerConfigRequiresBaseBackend(cfg, destination) {
			failures = append(failures, pkglogger.ValidateLogBodyConfig(label, destination, body))
		}
	}
	if c := cfg.File; c != nil {
		check(pkglogger.LogOutputFile, c.LogBodyConfig)
	}
	if c := cfg.Stdout; c != nil {
		check(pkglogger.LogOutputStdout, c.LogBodyConfig)
	}
	if c := cfg.Slack; c != nil {
		check(pkglogger.LogOutputSlack, c.LogBodyConfig)
		if pkglogger.DeliveryLoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputSlack) {
			failures = append(failures, pkglogger.ValidateLogLevel(label, pkglogger.LogOutputSlack, c.RuntimeConfig.LogLevel))
		}
	}
	if c := cfg.OpenSearch; c != nil {
		check(pkglogger.LogOutputOpenSearch, c.LogBodyConfig)
	}
	if c := cfg.Loki; c != nil {
		check(pkglogger.LogOutputLoki, c.LogBodyConfig)
	}
	if c := cfg.CloudWatchLogs; c != nil {
		check(pkglogger.LogOutputCloudWatchLogs, c.LogBodyConfig)
	}
	if c := cfg.ClickHouse; c != nil {
		check(pkglogger.LogOutputClickHouse, c.LogBodyConfig)
	}
	if cfg.InternalLogDestinations != nil {
		failures = append(failures, validateDeliveryPresentation(
			label+" internal log destination",
			cfg.InternalLogDestinations.ToLoggerConfig(),
		))
	}
	return errors.Join(failures...)
}
