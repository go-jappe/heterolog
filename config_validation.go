package heterolog

import (
	"errors"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Validate presentation settings before constructors acquire files or start
// workers. Backend activation follows the factory's normal and fallback routes.
// Each invalid setting is a *pkglogger.ConfigFieldError that identifies the
// source, backend and field; use errors.As or pkglogger.ConfigFieldErrors.
func validateLoggerPresentation(label string, cfg Config) error {
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
		failures = append(failures,
			pkglogger.ValidateLogLevel(label, destination, runtime.LogLevel),
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

func validateDeliveryPresentation(cfg DeliveryConfig) error {
	return validateDeliveryPresentationWithLabel("delivery logger", cfg)
}

// Delivery loggers neither filter severity nor sample entries, so only the
// body presentation and Slack's level are validated.
func validateDeliveryPresentationWithLabel(label string, cfg DeliveryConfig) error {
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
