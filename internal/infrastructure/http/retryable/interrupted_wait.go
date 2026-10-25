package retryable

import (
	"context"
	"errors"
	"fmt"
)

// InterruptedWait preserves a completed attempt and the cause that stopped its
// retry delay. It must only be used before the next attempt has started.
// Both causes remain discoverable through errors.Is/errors.As.
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

// ClassifyDurableRequestOutcome preserves confirmed rejection or pre-dispatch
// failure evidence when a retry wait was interrupted. The supplied classifier retains backend-specific
// rules, such as CloudWatch throttling or ClickHouse input errors.
//
// A stopped caller does not invalidate a completed HTTP refusal. In contrast,
// an interrupted in-flight request, or an interrupted wait after an uncertain
// attempt, remains quarantined. An arbitrary joined HTTP/context error does not
// establish that cancellation occurred between attempts.
func ClassifyDurableRequestOutcome(
	ctx context.Context,
	err error,
	classify func(context.Context, error) RetryDecision,
) RetryDecision {
	classifyCtx := DurableClassificationContext(ctx)
	// Follow ordinary wrappers, but not a multi-error join: another branch may
	// describe an additional in-flight request whose outcome is still unknown.
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
