package loggerruntime

import (
	"errors"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// validateLoggerPresentationは、constructorがfileを確保したりworkerを起動したりする前に、
// 出力の表示に関する設定を検証します。backendを使うかどうかは、factoryの通常の経路と
// 代替の経路に従って判断します。不正な設定はそれぞれ、出所・backend・項目を示す
// *pkglogger.ConfigFieldErrorです。errors.Asかpkglogger.ConfigFieldErrorsで取り出せます。
func validateLoggerPresentation(label string, cfg pkglogger.LoggerConfig) error {
	var failures []error
	check := func(
		destination pkglogger.LogOutputDestination,
		runtime pkglogger.RuntimeConfig,
		body pkglogger.LogBodyConfig,
		sampling pkglogger.SamplingConfig,
	) {
		if !pkglogger.LoggerConfigRequiresBaseBackend(cfg, destination) {
			return
		}
		// 稼働中に変えられる設定があれば、出力先の固定のlevelとsamplingの代わりに使います。
		level := runtime.LogLevel
		if runtime.Live != nil {
			level, sampling = runtime.Live.Level(), runtime.Live.Sampling()
		}
		failures = append(failures,
			pkglogger.ValidateLogLevel(label, destination, level),
			pkglogger.ValidateLogBodyConfig(label, destination, body),
			pkglogger.ValidateSamplingConfig(label, destination, sampling),
		)
	}
	if c := cfg.File; c != nil {
		check(pkglogger.LogOutputFile, c.RuntimeConfig, c.LogBodyConfig, c.SamplingConfig)
	}
	if c := cfg.Stdout; c != nil {
		check(pkglogger.LogOutputStdout, c.RuntimeConfig, c.LogBodyConfig, c.SamplingConfig)
	}
	if c := cfg.Slack; c != nil {
		check(pkglogger.LogOutputSlack, c.RuntimeConfig, c.LogBodyConfig, c.SamplingConfig)
	}
	if c := cfg.OpenSearch; c != nil {
		check(pkglogger.LogOutputOpenSearch, c.RuntimeConfig, c.LogBodyConfig, c.SamplingConfig)
	}
	if c := cfg.Loki; c != nil {
		check(pkglogger.LogOutputLoki, c.RuntimeConfig, c.LogBodyConfig, c.SamplingConfig)
	}
	if c := cfg.CloudWatchLogs; c != nil {
		check(pkglogger.LogOutputCloudWatchLogs, c.RuntimeConfig, c.LogBodyConfig, c.SamplingConfig)
	}
	if c := cfg.ClickHouse; c != nil {
		check(pkglogger.LogOutputClickHouse, c.RuntimeConfig, c.LogBodyConfig, c.SamplingConfig)
	}
	if cfg.InternalLogDestinations != nil {
		failures = append(failures, validateLoggerPresentation(label+" internal log destination", cfg.InternalLogDestinations.ToLoggerConfig()))
	}
	return errors.Join(failures...)
}

func validateDeliveryPresentation(cfg pkglogger.DeliveryLoggerConfig) error {
	return validateDeliveryPresentationWithLabel("delivery logger", cfg)
}

// validateDeliveryPresentationWithLabelは、Delivery loggerの表示に関する設定を検証します。
// Delivery loggerはseverityで絞り込まず、samplingもしないため、本文の表示とSlackの
// levelだけを検証します。
func validateDeliveryPresentationWithLabel(label string, cfg pkglogger.DeliveryLoggerConfig) error {
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
		failures = append(failures, validateDeliveryPresentationWithLabel(
			label+" internal log destination",
			cfg.InternalLogDestinations.ToLoggerConfig(),
		))
	}
	return errors.Join(failures...)
}
