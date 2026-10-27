package retryclassifier

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// PreservePriorAttemptOutcomeは、最新のretryがcredentialの取得中に失敗したとき、
// それ以前のlog送信の証拠を保持します。送信されていない新しい試行では、以前の
// requestが一度も受理されなかったことを証明できません。
func PreservePriorAttemptOutcome(previous, current error) error {
	if previous == nil || !hasCredentialFailure(current) {
		return current
	}
	prior := ClassifyDurableRequestOutcome(context.Background(), previous)
	latest := ClassifyDurableRequestOutcome(context.Background(), current)
	// 即時replayのpolicyはEOF/timeoutを許容することがありますが、これらのerrorは受理の
	// 証拠を持ちません。次の試行でcredentialが失敗しても、それ以前の不確かな送信を、
	// 確認済みのlocalな拒否に変えてはいけません。
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
	// joinされた任意の枝が、結果が不確かな別のrequestを表している可能性があります。
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
