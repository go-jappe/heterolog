package retry

import (
	"fmt"
	"time"
)

type RetryableFunc func() error

// Retry は指定した関数を最大 maxRetries 回までリトライします。
// すべての試行に失敗した場合のerrorは、最後の試行のerrorをerrors.Is・errors.Asで辿れます。
func Retry(fn RetryableFunc, maxRetries int, delay time.Duration) error {
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		time.Sleep(delay)
	}
	return fmt.Errorf("all retry attempts failed: %w", err)
}

type RetryableFuncWithResult[T any] func() (T, error)

// RetryWithResultはRetryと同じ規則でfnを再試行し、成功した試行の結果を返します。
// すべての試行に失敗した場合のerrorは、最後の試行のerrorをerrors.Is・errors.Asで辿れます。
func RetryWithResult[T any](fn RetryableFuncWithResult[T], maxRetries int, delay time.Duration) (T, error) {
	var lastErr error
	var result T

	for attempt := 1; attempt <= maxRetries; attempt++ {
		result, lastErr = fn()
		if lastErr == nil {
			return result, nil
		}
		time.Sleep(delay)
	}
	var zero T
	return zero, fmt.Errorf("all retry attempts failed: %w", lastErr)
}
