package retryable

import (
	"context"
	"errors"
	"fmt"
)

// InterruptedWaitは、完了した試行と、そのretry待機を止めた原因を保持します。
// 次の試行が始まる前にだけ使ってください。
// どちらの原因も、errors.Is/errors.Asで見つけられます。
func InterruptedWait(lastAttemptErr, stopErr error) error {
	if lastAttemptErr == nil {
		return stopErr
	}
	if stopErr == nil {
		return lastAttemptErr
	}
	return &interruptedWaitError{lastAttemptErr: lastAttemptErr, stopErr: stopErr}
}

type interruptedWaitError struct {
	lastAttemptErr error
	stopErr        error
}

func (e *interruptedWaitError) Error() string {
	return fmt.Sprintf("retry wait interrupted: %v (last attempt: %v)", e.stopErr, e.lastAttemptErr)
}

func (e *interruptedWaitError) Unwrap() []error {
	return []error{e.lastAttemptErr, e.stopErr}
}

// ClassifyDurableRequestOutcomeは、retry待機が中断されたとき、確認済みの拒否や
// 送信前の失敗の証拠を保持します。渡されたclassifierが、CloudWatchのthrottlingや
// ClickHouseの入力errorのようなbackend固有の規則を保ちます。
//
// 呼び出し元が停止しても、完了したHTTPの拒否は無効になりません。一方、送信中に
// 中断されたrequestや、結果が不確かな試行の後で中断された待機は、quarantineのままです。
// HTTPとcontextのerrorを任意にjoinしたerrorは、試行の間でcancelが起きたことを
// 示しません。
func ClassifyDurableRequestOutcome(
	ctx context.Context,
	err error,
	classify func(context.Context, error) RetryDecision,
) RetryDecision {
	classifyCtx := DurableClassificationContext(ctx)
	// 通常のwrapperはたどりますが、複数errorのjoinはたどりません。別の枝が、結果がまだ
	// 不明な送信中のrequestをもう一つ表している可能性があるためです。
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if waitErr, ok := cause.(*interruptedWaitError); ok {
			previous := classify(classifyCtx, waitErr.lastAttemptErr)
			if (previous.Reason == RetryReasonRequestNotSent || (previous.Code >= 400 && previous.Code <= 599)) &&
				!previous.Quarantine && previous.Reason != RetryReasonUnknownOutcome {
				return previous
			}
			break
		}
	}
	decision := classify(classifyCtx, err)
	if decision.Reason == RetryReasonRequestNotSent && !decision.Quarantine {
		return decision
	}
	if ctx != nil && ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return RetryDecision{Quarantine: true, Reason: RetryReasonUnknownOutcome}
	}
	return decision
}
