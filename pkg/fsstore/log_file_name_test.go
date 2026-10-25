package fsstore

import (
	"errors"
	"testing"
)

func TestValidateLogFileNameValue(t *testing.T) {
	for _, value := range []string{"", "app", "error_", "app[1]", "記録", FileNameFormat, FileDateFormat} {
		if err := ValidateLogFileNameValue("test field", value); err != nil {
			t.Errorf("valid value %q: %v", value, err)
		}
	}
	for _, value := range []string{"app:stream", "error:", ":debug", "/%s%s_%s_%d.log:stream", "20060102_15:04"} {
		err := ValidateLogFileNameValue("test field", value)
		var componentErr *LogFileNameComponentError
		if !errors.Is(err, ErrInvalidLogFileNameComponent) || !errors.As(err, &componentErr) ||
			componentErr.Field != "test field" || componentErr.Value != value {
			t.Errorf("invalid value %q: error = %v", value, err)
		}
	}
}

// 設定検証の結果は利用者へそのまま表示されるため、設定名と値を含む文言を固定します。
func TestLogFileNameComponentErrorMessage(t *testing.T) {
	err := ValidateLogFileNameValue("target", "app:stream")
	if got, want := err.Error(), `target must not contain ':' in log file names: "app:stream"`; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}
