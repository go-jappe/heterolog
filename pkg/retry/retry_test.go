package retry_test

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/pkg/retry"
)

var (
	errFirstAttempt = errors.New("first attempt failed")
	errLastAttempt  = errors.New("last attempt failed")
)

// failingAttemptsは、最初の試行だけerrFirstAttempt、以降はerrLastAttemptで失敗します。
func failingAttempts(calls *int) func() error {
	return func() error {
		*calls++
		if *calls == 1 {
			return errFirstAttempt
		}
		return errLastAttempt
	}
}

// Retryがすべての試行に失敗した場合、最後の試行のerrorをerrors.Isで辿れます。
func TestRetryKeepsLastAttemptErrorAsCause(t *testing.T) {
	calls := 0
	err := retry.Retry(failingAttempts(&calls), 3, 0)
	if calls != 3 {
		t.Fatalf("attempts = %d, want 3", calls)
	}
	if !errors.Is(err, errLastAttempt) {
		t.Fatalf("Retry() error = %v, want the last attempt error as its cause", err)
	}
	if errors.Is(err, errFirstAttempt) {
		t.Fatalf("Retry() error = %v, want only the last attempt error as its cause", err)
	}
}

// RetryWithResultがすべての試行に失敗した場合も、最後の試行のerrorをerrors.Isで辿れます。
func TestRetryWithResultKeepsLastAttemptErrorAsCause(t *testing.T) {
	calls := 0
	attempt := failingAttempts(&calls)
	got, err := retry.RetryWithResult(func() (int, error) { return 7, attempt() }, 3, 0)
	if calls != 3 {
		t.Fatalf("attempts = %d, want 3", calls)
	}
	if got != 0 {
		t.Fatalf("RetryWithResult() result = %d, want the zero value after every attempt failed", got)
	}
	if !errors.Is(err, errLastAttempt) {
		t.Fatalf("RetryWithResult() error = %v, want the last attempt error as its cause", err)
	}
	if errors.Is(err, errFirstAttempt) {
		t.Fatalf("RetryWithResult() error = %v, want only the last attempt error as its cause", err)
	}
}
