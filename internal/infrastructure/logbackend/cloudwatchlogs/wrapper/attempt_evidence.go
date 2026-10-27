package wrapper

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
)

// 結果が不確かな送信を、retryをまたいで保持します。後のcredentialの失敗は、その送信に
// ついて何も示しません。この証拠がなければ、まったく送信されていないように見えます。
type attemptEvidence struct {
	previous  error
	uncertain bool
}

func (e *attemptEvidence) retain(err error) error {
	result := retryclassifier.PreservePriorAttemptOutcome(e.previous, err)
	latest := retryclassifier.ClassifyDurableRequestOutcome(context.Background(), err)
	if err != nil && !e.uncertain && latest.Reason != retryable.RetryReasonRequestNotSent && latest.Reason != retryable.RetryReasonCircuitBreakerOpen {
		e.previous = err
		e.uncertain = latest.Quarantine || latest.Code == 0
	}
	return result
}
