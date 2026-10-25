package env

import (
	"errors"
	"testing"
	"time"
)

func TestGetEnvPositiveInt(t *testing.T) {
	const key = "TEST_POSITIVE_INTEGER"

	tests := []struct {
		name         string
		value        string
		defaultValue int
		want         int
		wantError    error
	}{
		{name: "empty uses default", defaultValue: 7, want: 7},
		{name: "positive value", value: "3", defaultValue: 7, want: 3},
		// 明示値のerrorでは成功扱いにせず、どの理由でも未設定時と同じdefaultをerrorとともに返します。
		{name: "zero is rejected", value: "0", defaultValue: 7, want: 7, wantError: ErrNotPositive},
		{name: "negative value is rejected", value: "-1", defaultValue: 7, want: 7, wantError: ErrNotPositive},
		{name: "non integer is rejected", value: "one", defaultValue: 7, want: 7, wantError: ErrNotInteger},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(key, tt.value)
			got, err := GetEnvPositiveInt(key, tt.defaultValue)
			if got != tt.want {
				t.Fatalf("GetEnvPositiveInt() = %d, want %d", got, tt.want)
			}
			assertVariableError(t, err, key, tt.value, tt.wantError)
		})
	}
}

func TestGetEnvPositiveMilliseconds(t *testing.T) {
	const key = "TEST_POSITIVE_MILLISECONDS"

	tests := []struct {
		name         string
		value        string
		defaultValue time.Duration
		want         time.Duration
		wantError    error
	}{
		{name: "empty uses default", defaultValue: 250 * time.Millisecond, want: 250 * time.Millisecond},
		{name: "positive value", value: "125", defaultValue: time.Second, want: 125 * time.Millisecond},
		// 明示値のerrorでは成功扱いにせず、どの理由でも未設定時と同じdefaultをerrorとともに返します。
		{name: "zero is rejected", value: "0", defaultValue: time.Second, want: time.Second, wantError: ErrNotPositive},
		{name: "negative value is rejected", value: "-1", defaultValue: time.Second, want: time.Second, wantError: ErrNotPositive},
		{name: "non integer is rejected", value: "soon", defaultValue: time.Second, want: time.Second, wantError: ErrNotInteger},
		{name: "overflow is rejected", value: "9223372036855", defaultValue: time.Second, want: time.Second, wantError: ErrOutOfRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(key, tt.value)
			got, err := GetEnvPositiveMilliseconds(key, tt.defaultValue)
			if got != tt.want {
				t.Fatalf("GetEnvPositiveMilliseconds() = %s, want %s", got, tt.want)
			}
			assertVariableError(t, err, key, tt.value, tt.wantError)
		})
	}
}

// assertVariableErrorは、errがkeyの値valueをkindの不正として示すVariableErrorであることを
// 確認します。kindがnilならerrなしを求めます。
func assertVariableError(t *testing.T, err error, key, value string, kind error) {
	t.Helper()
	if kind == nil {
		if err != nil {
			t.Fatalf("error = %v, want nil", err)
		}
		return
	}
	var variableErr *VariableError
	if !errors.As(err, &variableErr) || variableErr.Key != key || variableErr.Value != value || !errors.Is(err, kind) {
		t.Fatalf("error = %v, want VariableError{Key: %q, Value: %q} of kind %v", err, key, value, kind)
	}
}
