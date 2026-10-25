package retryable

import (
	"errors"
	"io"
	"testing"
)

func TestRetryAttemptsExhaustedErrorKeepsKindAndLastError(t *testing.T) {
	t.Parallel()

	for _, policy := range []bool{false, true} {
		err := error(&RetryAttemptsExhaustedError{Policy: policy, Attempts: 3, LastErr: io.EOF})
		if !errors.Is(err, ErrRetryAttemptsExhausted) || !errors.Is(err, io.EOF) {
			t.Fatalf("errors.Is(%v) lost the kind or the last error", err)
		}
		var exhausted *RetryAttemptsExhaustedError
		if !errors.As(err, &exhausted) || exhausted.Attempts != 3 || exhausted.Policy != policy {
			t.Fatalf("errors.As(%v) = %+v", err, exhausted)
		}
	}
}

// 既存のlog出力と同じ文言を保ちます。
func TestRetryAttemptsExhaustedErrorFormat(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		policy bool
		want   string
	}{
		{policy: false, want: "all retry attempts failed: EOF"},
		{policy: true, want: "all retry policy attempts failed: EOF"},
	} {
		if got := (&RetryAttemptsExhaustedError{Policy: tt.policy, Attempts: 2, LastErr: io.EOF}).Error(); got != tt.want {
			t.Fatalf("Error() = %q, want %q", got, tt.want)
		}
	}
}
