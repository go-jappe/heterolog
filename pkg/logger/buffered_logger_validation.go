package logger

import (
	"time"
)

// ValidateBufferedFlushInterval validates the interval of the time-based
// flushes of a buffered logger. It accepts only positive values, so that
// periodic flushes always make progress. A value of 0 or less is reported as a
// ConfigFieldError with the kind ErrNonPositiveSetting.
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
