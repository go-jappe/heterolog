package logging

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/pkg/retry"
)

// retryTestAttemptsは、試行ごとに異なるerrorを返すretry対象を作ります。
// 最後の要素がnilでなければ、すべての試行が失敗します。
func retryTestAttempts(results ...error) (func() error, *int) {
	calls := 0
	return func() error {
		err := results[calls]
		calls++
		return err
	}, &calls
}

func TestRetryWithLogWrapsLastAttemptError(t *testing.T) {
	first := errors.New("first attempt failed")
	last := errors.New("last attempt failed")
	fn, calls := retryTestAttempts(first, last)
	var logged []error

	// 再試行1回では、初回と合わせて2回実行します。
	err := RetryWithLog(
		fn,
		1,
		0,
		func(_ context.Context, err error, _ *LogEntry) { logged = append(logged, err) },
		func(context.Context, string, error) *LogEntry { return &LogEntry{} },
		"retry failed",
		context.Background(),
	)

	if !errors.Is(err, last) {
		t.Fatalf("RetryWithLog() error = %v, want cause %v", err, last)
	}
	if errors.Is(err, first) {
		t.Fatalf("RetryWithLog() error = %v, want only the last attempt as its cause", err)
	}
	if *calls != 2 {
		t.Fatalf("attempts = %d, want 2", *calls)
	}
	if len(logged) != 2 || logged[0] != first || logged[1] != last {
		t.Fatalf("logged attempt errors = %v, want [%v %v]", logged, first, last)
	}
}

func TestRetryWithResultAndLogWrapsLastAttemptError(t *testing.T) {
	first := errors.New("first attempt failed")
	last := errors.New("last attempt failed")
	results := []error{first, last}
	calls := 0

	result, err := RetryWithResultAndLog(
		func() (string, error) {
			err := results[calls]
			calls++
			return "partial", err
		},
		1,
		0,
		nil,
		nil,
		"retry failed",
		context.Background(),
	)

	if !errors.Is(err, last) {
		t.Fatalf("RetryWithResultAndLog() error = %v, want cause %v", err, last)
	}
	if errors.Is(err, first) {
		t.Fatalf("RetryWithResultAndLog() error = %v, want only the last attempt as its cause", err)
	}
	if result != "" {
		t.Fatalf("RetryWithResultAndLog() result = %q, want zero value after all attempts failed", result)
	}
	if calls != 2 {
		t.Fatalf("attempts = %d, want 2", calls)
	}
}

func TestRetryWithLogReturnsNilAfterRecoveredAttempt(t *testing.T) {
	failed := errors.New("temporary failure")
	fn, calls := retryTestAttempts(failed, nil, failed)

	err := RetryWithLog(fn, 2, 0, nil, nil, "retry failed", context.Background())

	if err != nil {
		t.Fatalf("RetryWithLog() error = %v, want nil after the second attempt succeeds", err)
	}
	if *calls != 2 {
		t.Fatalf("attempts = %d, want to stop after the successful second attempt", *calls)
	}
}

// 再試行0回は1回だけ実行し、負の回数はfnを実行せずにerrorを返します。
func TestRetryWithLogCountsRetriesAfterTheFirstAttempt(t *testing.T) {
	failed := errors.New("failed")
	fn, calls := retryTestAttempts(failed, failed)
	if err := RetryWithLog(fn, 0, time.Hour, nil, nil, "retry failed", context.Background()); !errors.Is(err, failed) {
		t.Fatalf("RetryWithLog() error = %v, want the only attempt error", err)
	}
	if *calls != 1 {
		t.Fatalf("attempts = %d, want 1 without retries", *calls)
	}

	fn, calls = retryTestAttempts(nil)
	if err := RetryWithLog(fn, -1, 0, nil, nil, "retry failed", context.Background()); !errors.Is(err, retry.ErrNegativeRetries) {
		t.Fatalf("RetryWithLog() error = %v, want ErrNegativeRetries", err)
	}
	if *calls != 0 {
		t.Fatalf("attempts = %d, want 0 for a negative retry count", *calls)
	}
}

// 待機は再試行の前にだけ入り、最後の失敗の後には待ちません。
func TestRetryWithLogWaitsOnlyBeforeRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failed := errors.New("failed")
		fn, _ := retryTestAttempts(failed, failed, failed)
		start := time.Now()
		_ = RetryWithLog(fn, 2, time.Second, nil, nil, "retry failed", context.Background())
		if elapsed := time.Since(start); elapsed != 2*time.Second {
			t.Fatalf("elapsed = %s, want 2s for two retries", elapsed)
		}
	})
}
