// Package retry runs a function again after it fails.
package retry

import (
	"errors"
	"fmt"
	"time"
)

// ErrNegativeRetries reports that a negative number of retries was given.
var ErrNegativeRetries = errors.New("retry count must not be negative")

// RetryableFunc is a function that Retry runs.
type RetryableFunc func() error

// Retry runs fn and, while it fails, runs it again up to maxRetries more
// times, waiting delay before each retry. With maxRetries 1, fn runs at most
// twice; with 0, it runs once and is not retried. A negative maxRetries returns
// an error wrapping ErrNegativeRetries without running fn. When every run
// fails, errors.Is and errors.As can reach the error of the last run through
// the returned error.
func Retry(fn RetryableFunc, maxRetries int, delay time.Duration) error {
	_, err := RetryWithResult(func() (struct{}, error) { return struct{}{}, fn() }, maxRetries, delay)
	return err
}

// RetryableFuncWithResult is a function that RetryWithResult runs.
type RetryableFuncWithResult[T any] func() (T, error)

// RetryWithResult runs fn by the same rules as Retry and returns the result of
// the successful run. When every run fails, it returns the zero value of T and
// an error through which errors.Is and errors.As can reach the error of the
// last run.
func RetryWithResult[T any](fn RetryableFuncWithResult[T], maxRetries int, delay time.Duration) (T, error) {
	var zero T
	if maxRetries < 0 {
		return zero, fmt.Errorf("%w: %d", ErrNegativeRetries, maxRetries)
	}
	var lastErr error
	for retry := 0; retry <= maxRetries; retry++ {
		// 最後の失敗の後には待たず、再試行の前にだけ待ちます。
		if retry > 0 {
			time.Sleep(delay)
		}
		result, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err
	}
	return zero, fmt.Errorf("all %d attempts failed: %w", maxRetries+1, lastErr)
}
