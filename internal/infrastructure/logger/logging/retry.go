package logging

import (
	"context"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/pkg/retry"
)

type RetryableFuncWithResult[T any] func() (T, error)
type RetryLogFunc func(ctx context.Context, err error, entry *LogEntry)
type WrapError func(err error, op string) error
type EntryFactory func(ctx context.Context, msg string, err error) *LogEntry

// RetryWithResultAndLogは、fnを実行し、失敗したら最大maxRetries回まで再試行します。
// 失敗した試行ごとにログを出し、再試行の前にだけdelayだけ待ちます。maxRetriesが0なら
// 再試行せずに1回だけ実行し、負の値ならfnを実行せずにretry.ErrNegativeRetriesを包んだ
// errorを返します。
func RetryWithResultAndLog[T any](
	fn RetryableFuncWithResult[T],
	maxRetries int,
	delay time.Duration,
	logFunc RetryLogFunc,
	entry EntryFactory,
	errorMessage string,
	ctx context.Context,
) (T, error) {
	var zero T
	if maxRetries < 0 {
		return zero, fmt.Errorf("%w: %d", retry.ErrNegativeRetries, maxRetries)
	}
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(delay)
		}
		result, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err
		// 失敗をロギング
		if logFunc != nil {
			logFunc(ctx, lastErr, entry(ctx, errorMessage, lastErr))
		}
	}

	// 最後の失敗を原因として保ち、呼び出し元がerrors.Is/Asで判定できるようにします。
	return zero, fmt.Errorf("all retry attempts failed: %w", lastErr)
}

type RetryableFunc func() error

// RetryWithLogは、RetryWithResultAndLogと同じ規則でfnを実行し、失敗したら最大maxRetries回まで
// 再試行します。
func RetryWithLog(
	fn RetryableFunc,
	maxRetries int,
	delay time.Duration,
	logFunc RetryLogFunc,
	entry EntryFactory,
	errorMessage string,
	ctx context.Context,
) error {
	_, err := RetryWithResultAndLog(
		func() (struct{}, error) { return struct{}{}, fn() },
		maxRetries,
		delay,
		logFunc,
		entry,
		errorMessage,
		ctx,
	)
	return err
}
