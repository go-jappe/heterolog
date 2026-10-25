package retryclassifier

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// PreservePriorAttemptOutcome keeps earlier log-dispatch evidence when the
// latest retry failed during credential retrieval. A new, unsent attempt cannot
// prove that an earlier request was never accepted.
func PreservePriorAttemptOutcome(previous, current error) error {
	if previous == nil || !hasCredentialFailure(current) {
		return current
	}
	prior := ClassifyDurableRequestOutcome(context.Background(), previous)
	latest := ClassifyDurableRequestOutcome(context.Background(), current)
	// Immediate replay policy may permit EOF/timeouts, but these errors carry
	// no acceptance evidence. Credential failure on the next attempt must not
	// turn that earlier uncertain dispatch into a confirmed local rejection.
	if prior.Code == 0 && prior.Reason != retryable.RetryReasonRequestNotSent && prior.Reason != retryable.RetryReasonCircuitBreakerOpen {
		prior = retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome}
	}
	if prior.Quarantine || (latest.Reason == retryable.RetryReasonRequestNotSent && prior.Code >= 400 && prior.Code <= 599) {
		return &requestNotSentAfterAttemptError{previous: previous, current: current, decision: prior}
	}
	return current
}

func hasCredentialFailure(err error) bool {
	if requestNotSentFailure(err) != nil {
		return true
	}
	creation := autoCreateFailure(err)
	return creation != nil && requestNotSentFailure(creation.CreateError) != nil
}

type requestNotSentAfterAttemptError struct {
	previous error
	current  error
	decision retryable.RetryDecision
}

func (e *requestNotSentAfterAttemptError) Error() string {
	return fmt.Sprintf("%v (earlier log attempt: %v)", e.current, e.previous)
}

func (e *requestNotSentAfterAttemptError) Unwrap() []error {
	return []error{e.previous, e.current}
}

func priorAttemptFailure(err error) *requestNotSentAfterAttemptError {
	// An arbitrary joined branch may describe an additional uncertain request.
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if _, unknown := cause.(*cw.ResponseDecodeError); unknown {
			return nil
		}
		if prior, ok := cause.(*requestNotSentAfterAttemptError); ok {
			return prior
		}
	}
	return nil
}
