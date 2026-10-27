package failure

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/pkg/logger"
)

// RequestFailureDeliveryStatusは、replay policyではなく、受理についての証拠を表します。
// transport timeoutは、deliveryが未確認のままでもretryableであり続けることがあります。
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

// TargetDeliveryResultは、別のinputの失敗が判明していても、不確かさを保ちます。
// durable dispositionだけではlocal rejectionと結果不明を区別できず、
// 公開するdelivery statusをdispositionで決めてはいけません。
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
		// record単位の証拠がないerrorだけでは、送信前の拒否（DeliveryRejected）とは確定できません。
		status = logger.DeliveryUnconfirmed
	}
	causes = append(causes, fmt.Errorf("%s delivery %s: %d unresolved record(s)", backend, status, r.FailedCount()))
	return logger.DeliveryResult{
		Backend: backend,
		Status:  status,
		Err:     errors.Join(causes...),
	}
}
