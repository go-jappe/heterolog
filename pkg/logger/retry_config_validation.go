package logger

import (
	"time"
)

// IsValid returns true when the mode represents an available retry client
// configuration. In a configuration object, the empty string is a value that
// has not been decided yet, and it is not treated as a valid mode.
func (m RetryClientMode) IsValid() bool {
	switch m {
	case RetryClientModeNone,
		RetryClientModeRetry,
		RetryClientModeRetryPolicy,
		RetryClientModeCircuitBreaker,
		RetryClientModeRetryWithBreaker,
		RetryClientModeRetryPolicyWithBreaker:
		return true
	default:
		return false
	}
}

// ValidateRetryClientConfig validates the settings for building the retry
// client wrappers.
//
// maxRetries is the number of retries after the first request, so 0 sends
// once without retrying. In modes that retry, the number of retries must not
// be negative and the backoff delays must be positive. The none and
// circuit_breaker modes do not use these values, so they accept unset values.
// An unsupported mode is reported as a ConfigFieldError with the kind
// ErrUnsupportedRetryClientMode, a negative number of retries as one with the
// kind ErrNegativeSetting, and a delay of 0 or less as one with the kind
// ErrNonPositiveSetting.
func ValidateRetryClientConfig(
	mode RetryClientMode,
	maxRetries int,
	baseDelay time.Duration,
	maxDelay time.Duration,
) error {
	if !mode.IsValid() {
		return settingError(
			ConfigFieldRetryClientMode, ErrUnsupportedRetryClientMode, mode,
			"unsupported retry client mode: %q", mode,
		)
	}
	selection := resolveRetryClientSelection(mode)
	if !selection.UseRetryingClient && !selection.UseRetryPolicyClient {
		return nil
	}
	if maxRetries < 0 {
		return settingError(
			ConfigFieldRetryMaxRetries, ErrNegativeSetting, maxRetries,
			"retry max retries must not be negative: %d", maxRetries,
		)
	}
	if baseDelay <= 0 {
		return settingError(
			ConfigFieldRetryBaseDelay, ErrNonPositiveSetting, baseDelay,
			"retry base delay must be greater than zero: %s", baseDelay,
		)
	}
	if maxDelay <= 0 {
		return settingError(
			ConfigFieldRetryMaxDelay, ErrNonPositiveSetting, maxDelay,
			"retry max delay must be greater than zero: %s", maxDelay,
		)
	}
	return nil
}
