package logging

import (
	"context"
	"fmt"
	"time"
)

type RetryableFuncWithResult[T any] func() (T, error)
type RetryLogFunc func(ctx context.Context, err error, entry *LogEntry)
type WrapError func(err error, op string) error
type EntryFactory func(ctx context.Context, msg string, err error) *LogEntry

// RetryWithResultAndLog はリトライごとにログ出力も行う
func RetryWithResultAndLog[T any](
	fn RetryableFuncWithResult[T],
	maxRetries int,
	delay time.Duration,
	logFunc RetryLogFunc,
	entry EntryFactory,
	errorMessage string,
	ctx context.Context,
) (T, error) {
	var lastErr error
	var result T

	for attempt := 1; attempt <= maxRetries; attempt++ {
		result, lastErr = fn()
		if lastErr == nil {
			return result, nil
		}
		// 失敗をロギング
		if logFunc != nil {
			logFunc(ctx, lastErr, entry(ctx, errorMessage, lastErr))
		}
		time.Sleep(delay)
	}

	var zero T
	// 最後の失敗を原因として保ち、呼び出し元がerrors.Is/Asで判定できるようにします。
	return zero, fmt.Errorf("all retry attempts failed: %w", lastErr)
}

type RetryableFunc func() error

func RetryWithLog(
	fn RetryableFunc,
	maxRetries int,
	delay time.Duration,
	logFunc RetryLogFunc,
	entry EntryFactory,
	errorMessage string,
	ctx context.Context,
) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		// 失敗をロギング
		if logFunc != nil {
			logFunc(ctx, lastErr, entry(ctx, errorMessage, lastErr))
		}
		time.Sleep(delay)
	}

	// 最後の失敗を原因として保ち、呼び出し元がerrors.Is/Asで判定できるようにします。
	return fmt.Errorf("all retry attempts failed: %w", lastErr)
}
