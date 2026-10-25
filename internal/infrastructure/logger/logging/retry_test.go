package logging

import (
	"context"
	"errors"
	"testing"
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

	err := RetryWithLog(
		fn,
		2,
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
		2,
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

	err := RetryWithLog(fn, 3, 0, nil, nil, "retry failed", context.Background())

	if err != nil {
		t.Fatalf("RetryWithLog() error = %v, want nil after the second attempt succeeds", err)
	}
	if *calls != 2 {
		t.Fatalf("attempts = %d, want to stop after the successful second attempt", *calls)
	}
}
