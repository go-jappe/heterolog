package failure

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/pkg/logger"
)

// RequestFailureDeliveryStatus describes acceptance evidence, not replay policy.
// A transport timeout may remain retryable while its delivery is unconfirmed.
func RequestFailureDeliveryStatus(dec retryable.RetryDecision) logger.DeliveryStatus {
	if dec.Reason == retryable.RetryReasonUnknownOutcome {
		return logger.DeliveryUnconfirmed
	}
	if dec.Code != 0 {
		return logger.DeliveryFailed
	}
	switch dec.Reason {
	case retryable.RetryReasonCircuitBreakerOpen, retryable.RetryReasonRequestNotSent:
		return logger.DeliveryRejected
	default:
		return logger.DeliveryUnconfirmed
	}
}

// TargetDeliveryResult preserves uncertainty even if another input has a known
// failure. Durable dispositions alone cannot distinguish local rejection from
// an unknown result, and must not determine the public delivery status.
func (r SendResult) TargetDeliveryResult(backend string) logger.DeliveryResult {
	if !r.HasSendFailure() {
		return logger.DeliveryResult{Backend: backend, Status: logger.DeliverySucceeded}
	}
	status := logger.DeliveryRejected
	causes := []error{r.TerminalError}
	for _, records := range [][]FailedRecord{r.RetryableRecords, r.DeadRecords, r.QuarantineRecords} {
		for _, record := range records {
			if record.Cause != nil {
				causes = append(causes, record.Cause)
			} else if record.FailureReason != "" {
				causes = append(causes, errors.New(record.FailureReason))
			}
			switch record.DeliveryStatus {
			case logger.DeliveryRejected:
			case logger.DeliveryFailed:
				if status != logger.DeliveryUnconfirmed {
					status = logger.DeliveryFailed
				}
			default:
				status = logger.DeliveryUnconfirmed
			}
		}
	}
	if !r.HasFailedRecords() {
		// An error without per-record evidence cannot establish backend rejection.
		status = logger.DeliveryUnconfirmed
	}
	causes = append(causes, fmt.Errorf("%s delivery %s: %d unresolved record(s)", backend, status, r.FailedCount()))
	return logger.DeliveryResult{
		Backend: backend,
		Status:  status,
		Err:     errors.Join(causes...),
	}
}
