package logger

import (
	"testing"
	"time"
)

func TestParseRetryClientMode(t *testing.T) {
	const defaultMode = RetryClientModeRetryPolicy

	tests := []struct {
		name  string
		value string
		want  RetryClientMode
	}{
		{
			name: "empty uses default",
			want: defaultMode,
		},
		{
			name:  "whitespace uses default",
			value: " \t ",
			want:  defaultMode,
		},
		{
			name:  "known mode is normalized",
			value: " RETRY_WITH_BREAKER ",
			want:  RetryClientModeRetryWithBreaker,
		},
		{
			name:  "unknown mode is normalized and preserved",
			value: " TYPO_MODE ",
			want:  RetryClientMode("typo_mode"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseRetryClientMode(tt.value, defaultMode); got != tt.want {
				t.Fatalf("ParseRetryClientMode(%q) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestValidateRetryClientConfigAcceptsSupportedModes(t *testing.T) {
	for _, mode := range []RetryClientMode{
		RetryClientModeNone,
		RetryClientModeRetry,
		RetryClientModeRetryPolicy,
		RetryClientModeCircuitBreaker,
		RetryClientModeRetryWithBreaker,
		RetryClientModeRetryPolicyWithBreaker,
	} {
		t.Run(string(mode), func(t *testing.T) {
			if err := ValidateRetryClientConfig(mode, 1, time.Millisecond, time.Millisecond); err != nil {
				t.Fatalf("ValidateRetryClientConfig() error = %v", err)
			}
		})
	}
}

func TestValidateRetryClientConfigRejectsUnknownAndEmptyModes(t *testing.T) {
	for _, mode := range []RetryClientMode{"", "retryable", "typo"} {
		t.Run(string(mode), func(t *testing.T) {
			err := ValidateRetryClientConfig(mode, 1, time.Millisecond, time.Millisecond)
			fieldErr := findFieldError(err, ConfigFieldRetryClientMode, ErrUnsupportedRetryClientMode)
			if fieldErr == nil || fieldErr.Value != mode {
				t.Fatalf("ValidateRetryClientConfig() error = %v, want unsupported mode %q", err, mode)
			}
		})
	}
}

// 再試行の回数は0を有効な値（再試行なし）とし、負の値だけを拒否します。待機時間は正の値が必要です。
func TestValidateRetryClientConfigRejectsNegativeRetriesAndNonPositiveDelays(t *testing.T) {
	retryModes := []RetryClientMode{
		RetryClientModeRetry,
		RetryClientModeRetryPolicy,
		RetryClientModeRetryWithBreaker,
		RetryClientModeRetryPolicyWithBreaker,
	}
	invalidValues := []struct {
		name       string
		maxRetries int
		baseDelay  time.Duration
		maxDelay   time.Duration
		// wantField・wantKind・wantValueは、不正として報告される項目、不正の種類、値です。
		wantField ConfigField
		wantKind  error
		wantValue any
	}{
		{
			name:       "negative retries",
			maxRetries: -1,
			baseDelay:  time.Millisecond,
			maxDelay:   time.Millisecond,
			wantField:  ConfigFieldRetryMaxRetries,
			wantKind:   ErrNegativeSetting,
			wantValue:  -1,
		},
		{
			name:       "zero base delay",
			maxRetries: 0,
			baseDelay:  0,
			maxDelay:   time.Millisecond,
			wantField:  ConfigFieldRetryBaseDelay,
			wantKind:   ErrNonPositiveSetting,
			wantValue:  time.Duration(0),
		},
		{
			name:       "negative base delay",
			maxRetries: 0,
			baseDelay:  -time.Millisecond,
			maxDelay:   time.Millisecond,
			wantField:  ConfigFieldRetryBaseDelay,
			wantKind:   ErrNonPositiveSetting,
			wantValue:  -time.Millisecond,
		},
		{
			name:       "zero max delay",
			maxRetries: 0,
			baseDelay:  time.Millisecond,
			maxDelay:   0,
			wantField:  ConfigFieldRetryMaxDelay,
			wantKind:   ErrNonPositiveSetting,
			wantValue:  time.Duration(0),
		},
		{
			name:       "negative max delay",
			maxRetries: 0,
			baseDelay:  time.Millisecond,
			maxDelay:   -time.Millisecond,
			wantField:  ConfigFieldRetryMaxDelay,
			wantKind:   ErrNonPositiveSetting,
			wantValue:  -time.Millisecond,
		},
	}

	for _, mode := range retryModes {
		t.Run(string(mode)+"/zero retries", func(t *testing.T) {
			if err := ValidateRetryClientConfig(mode, 0, time.Millisecond, time.Millisecond); err != nil {
				t.Fatalf("ValidateRetryClientConfig() error = %v, want nil for no retries", err)
			}
		})
		for _, tt := range invalidValues {
			t.Run(string(mode)+"/"+tt.name, func(t *testing.T) {
				err := ValidateRetryClientConfig(mode, tt.maxRetries, tt.baseDelay, tt.maxDelay)
				fieldErr := findFieldError(err, tt.wantField, tt.wantKind)
				if fieldErr == nil || fieldErr.Value != tt.wantValue {
					t.Fatalf("ValidateRetryClientConfig() error = %v, want %v for %s %v", err, tt.wantKind, tt.wantField, tt.wantValue)
				}
			})
		}
	}
}

func TestValidateRetryClientConfigIgnoresUnusedRetryValues(t *testing.T) {
	for _, mode := range []RetryClientMode{
		RetryClientModeNone,
		RetryClientModeCircuitBreaker,
	} {
		t.Run(string(mode), func(t *testing.T) {
			if err := ValidateRetryClientConfig(mode, -1, -time.Second, -time.Second); err != nil {
				t.Fatalf("ValidateRetryClientConfig() error = %v, want nil", err)
			}
		})
	}
}
