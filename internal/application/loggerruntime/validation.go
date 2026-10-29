package loggerruntime

import (
	"errors"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// validateOutputSettingsは、constructorがBuilderを呼ぶ前に、稼働中に変えられる出力先の
// 設定（levelとsampling）を検証します。backendを使うかどうかは、factoryの通常の経路と
// 代替の経路に従って判断します。不正な設定はそれぞれ、出所・backend・項目を示す
// *pkglogger.ConfigFieldErrorです。errors.Asかpkglogger.ConfigFieldErrorsで取り出せます。
func validateOutputSettings(label string, cfg pkglogger.LoggerConfig) error {
	var failures []error
	check := func(destination pkglogger.LogOutputDestination, runtime pkglogger.RuntimeConfig, sampling pkglogger.SamplingConfig) {
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
			pkglogger.ValidateSamplingConfig(label, destination, sampling),
		)
	}
	if c := cfg.File; c != nil {
		check(pkglogger.LogOutputFile, c.RuntimeConfig, c.SamplingConfig)
	}
	if c := cfg.Stdout; c != nil {
		check(pkglogger.LogOutputStdout, c.RuntimeConfig, c.SamplingConfig)
	}
	if c := cfg.Slack; c != nil {
		check(pkglogger.LogOutputSlack, c.RuntimeConfig, c.SamplingConfig)
	}
	if c := cfg.OpenSearch; c != nil {
		check(pkglogger.LogOutputOpenSearch, c.RuntimeConfig, c.SamplingConfig)
	}
	if c := cfg.Loki; c != nil {
		check(pkglogger.LogOutputLoki, c.RuntimeConfig, c.SamplingConfig)
	}
	if c := cfg.CloudWatchLogs; c != nil {
		check(pkglogger.LogOutputCloudWatchLogs, c.RuntimeConfig, c.SamplingConfig)
	}
	if c := cfg.ClickHouse; c != nil {
		check(pkglogger.LogOutputClickHouse, c.RuntimeConfig, c.SamplingConfig)
	}
	if cfg.InternalLogDestinations != nil {
		failures = append(failures, validateOutputSettings(label+" internal log destination", cfg.InternalLogDestinations.ToLoggerConfig()))
	}
	return errors.Join(failures...)
}
