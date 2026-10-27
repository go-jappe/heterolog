package retry_test

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

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
// 再試行2回では、初回と合わせて3回実行します。
func TestRetryKeepsLastAttemptErrorAsCause(t *testing.T) {
	calls := 0
	err := retry.Retry(failingAttempts(&calls), 2, 0)
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
	got, err := retry.RetryWithResult(func() (int, error) { return 7, attempt() }, 2, 0)
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

// 再試行0回は不正な値ではなく、再試行せずに初回だけを実行します。
func TestRetryWithoutRetriesRunsOnce(t *testing.T) {
	calls := 0
	err := retry.Retry(failingAttempts(&calls), 0, time.Hour)
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1", calls)
	}
	if !errors.Is(err, errFirstAttempt) {
		t.Fatalf("Retry() error = %v, want the only attempt error as its cause", err)
	}

	calls = 0
	attempt := failingAttempts(&calls)
	_, err = retry.RetryWithResult(func() (int, error) { return 0, attempt() }, 0, time.Hour)
	if calls != 1 {
		t.Fatalf("RetryWithResult attempts = %d, want 1", calls)
	}
	if !errors.Is(err, errFirstAttempt) {
		t.Fatalf("RetryWithResult() error = %v, want the only attempt error as its cause", err)
	}
}

// 再試行1回では、初回と合わせて2回実行します。成功した時点で再試行をやめます。
func TestRetryStopsAtFirstSuccess(t *testing.T) {
	calls := 0
	got, err := retry.RetryWithResult(func() (string, error) {
		calls++
		if calls == 1 {
			return "", errFirstAttempt
		}
		return "ok", nil
	}, 1, 0)
	if err != nil || got != "ok" {
		t.Fatalf("RetryWithResult() = %q, %v; want ok, nil", got, err)
	}
	if calls != 2 {
		t.Fatalf("attempts = %d, want 2", calls)
	}

	calls = 0
	if err := retry.Retry(func() error { calls++; return nil }, 3, 0); err != nil {
		t.Fatalf("Retry() error = %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("attempts after an immediate success = %d, want 1", calls)
	}
}

// 負の再試行回数は不正な値として、fnを実行せずにErrNegativeRetriesを返します。
func TestRetryRejectsNegativeRetries(t *testing.T) {
	calls := 0
	err := retry.Retry(func() error { calls++; return nil }, -1, 0)
	if !errors.Is(err, retry.ErrNegativeRetries) {
		t.Fatalf("Retry() error = %v, want ErrNegativeRetries", err)
	}
	got, err := retry.RetryWithResult(func() (int, error) { calls++; return 7, nil }, -1, 0)
	if !errors.Is(err, retry.ErrNegativeRetries) {
		t.Fatalf("RetryWithResult() error = %v, want ErrNegativeRetries", err)
	}
	if got != 0 {
		t.Fatalf("RetryWithResult() result = %d, want the zero value", got)
	}
	if calls != 0 {
		t.Fatalf("attempts = %d, want 0 for a negative retry count", calls)
	}
}

// 待機は再試行の前にだけ入り、最後の失敗の後には待ちません。
func TestRetryWaitsOnlyBeforeRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		start := time.Now()
		_ = retry.Retry(failingAttempts(&calls), 2, time.Second)
		if elapsed := time.Since(start); elapsed != 2*time.Second {
			t.Fatalf("elapsed = %s, want 2s for two retries", elapsed)
		}
	})
}
