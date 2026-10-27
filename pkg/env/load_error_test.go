package env

import (
	"errors"
	"testing"
)

// NewInvalidEnvironmentValueErrorの文面を指定した場合はその文面を、空の場合はkey・値・原因
// から組み立てた文面を返します。どちらもErrを原因としてたどれます。
func TestInvalidEnvironmentValueErrorMessage(t *testing.T) {
	cause := errors.New("cause")
	custom := NewInvalidEnvironmentValueError("HETEROLOG_KEY", "value", cause, "custom message")
	if got := custom.Error(); got != "custom message" || !errors.Is(custom, cause) {
		t.Errorf("custom Error() = %q, errors.Is(cause) = %v; want the custom message and the cause", got, errors.Is(custom, cause))
	}
	fallback := NewInvalidEnvironmentValueError("HETEROLOG_KEY", "value", cause, "")
	if got, want := fallback.Error(), "invalid HETEROLOG_KEY value: cause"; got != want {
		t.Errorf("default Error() = %q, want %q", got, want)
	}
}
