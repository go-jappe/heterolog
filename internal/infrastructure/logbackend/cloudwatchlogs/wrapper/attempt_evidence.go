package wrapper

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
)

// Keep an uncertain dispatch across retries. A later credentials failure says
// nothing about that dispatch; without this evidence it would look wholly unsent.
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
