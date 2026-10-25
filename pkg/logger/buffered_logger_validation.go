package logger

import (
	"time"
)

// ValidateBufferedFlushIntervalは、buffered loggerの時間flush間隔を検証します。
// 定期flushが必ず進行するよう、正の値だけを受け付けます。0以下の値は
// ErrNonPositiveSettingを持つConfigFieldErrorです。
func ValidateBufferedFlushInterval(flushInterval time.Duration) error {
	if flushInterval <= 0 {
		return settingError(
			ConfigFieldFlushInterval, ErrNonPositiveSetting, flushInterval,
			"flush interval must be positive: %s", flushInterval,
		)
	}
	return nil
}

func validateBufferedFlushInterval(
	sourceLabel string,
	backend LogOutputDestination,
	flushInterval time.Duration,
) error {
	return wrapBackendSettingErrors(
		sourceLabel, backend, "logger config", ValidateBufferedFlushInterval(flushInterval),
	)
}
