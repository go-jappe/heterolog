package logger

import (
	"time"
)

// IsValidは、modeが利用可能なretry client構成を表す場合にtrueを返します。
// 空文字は設定オブジェクト上では未確定な値であり、有効なmodeとは扱いません。
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

// ValidateRetryClientConfigはretry client wrapperを組み立てるための設定を検証します。
//
// retryを実行するmodeでは、試行回数とbackoff遅延をすべて正の値にする必要があります。
// noneとcircuit_breakerではこれらの値を使用しないため、未設定値を許容します。
// 未対応のmodeはErrUnsupportedRetryClientMode、0以下の値はErrNonPositiveSettingを持つ
// ConfigFieldErrorです。
func ValidateRetryClientConfig(
	mode RetryClientMode,
	maxAttempts int,
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
	if maxAttempts <= 0 {
		return settingError(
			ConfigFieldRetryMaxAttempts, ErrNonPositiveSetting, maxAttempts,
			"retry max attempts must be greater than zero: %d", maxAttempts,
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
