package retryable

import (
	"errors"
	"fmt"
)

// ErrRetryAttemptsExhaustedは、retry wrapperが許可された試行をすべて失敗したことを表します。
// 実際に返すerrorはRetryAttemptsExhaustedErrorです。
var ErrRetryAttemptsExhausted = errors.New("all retry attempts failed")

// RetryAttemptsExhaustedErrorは、retry wrapperが許可された試行をすべて失敗したことを表します。
// errors.Is(err, ErrRetryAttemptsExhausted)で判定でき、最後の試行のerrorもerrors.Is・errors.Asで辿れます。
type RetryAttemptsExhaustedError struct {
	// Policyは、retry policyのclientが試行を打ち切ったことを表します。falseは通常のretry clientです。
	Policy bool
	// Attemptsは、実行した試行の回数です。
	Attempts int
	// LastErrは、最後の試行のerrorです。
	LastErr error
}

func (e *RetryAttemptsExhaustedError) Error() string {
	if e.Policy {
		return fmt.Sprintf("all retry policy attempts failed: %v", e.LastErr)
	}
	return fmt.Sprintf("all retry attempts failed: %v", e.LastErr)
}

// Isは、ErrRetryAttemptsExhaustedと一致します。
func (e *RetryAttemptsExhaustedError) Is(target error) bool {
	return target == ErrRetryAttemptsExhausted
}

// Unwrapは最後の試行のerrorを返します。errors.Unwrapで原因を一本の連鎖として辿れます。
func (e *RetryAttemptsExhaustedError) Unwrap() error { return e.LastErr }
