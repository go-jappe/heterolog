package failure_test

import (
	"net/http"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/pkg/logger"
)

// RequestFailureDeliveryStatusは再送方針ではなく、backendが受理したかの根拠を表します。
// requestを送信しなかったことが確かな場合だけ、送信前の拒否としてDeliveryRejectedを返します。
func TestRequestFailureDeliveryStatus(t *testing.T) {
	tests := []struct {
		name     string
		decision retryable.RetryDecision
		want     logger.DeliveryStatus
	}{
		{
			name:     "request not sent",
			decision: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonRequestNotSent},
			want:     logger.DeliveryRejected,
		},
		{
			name:     "circuit breaker open",
			decision: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonCircuitBreakerOpen},
			want:     logger.DeliveryRejected,
		},
		{
			// 分割前のrequestがHTTP 413で拒否された場合は、未送信の分割requestも
			// backendの明示的な拒否として扱います。
			name: "request not sent after an HTTP rejection",
			decision: retryable.RetryDecision{
				Retry:  true,
				Reason: retryable.RetryReasonRequestNotSent,
				Code:   http.StatusRequestEntityTooLarge,
			},
			want: logger.DeliveryFailed,
		},
		{
			name:     "HTTP status",
			decision: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable},
			want:     logger.DeliveryFailed,
		},
		{
			name:     "unknown outcome",
			decision: retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome},
			want:     logger.DeliveryUnconfirmed,
		},
		{
			name:     "transport error without response",
			decision: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonEOF},
			want:     logger.DeliveryUnconfirmed,
		},
		{
			name:     "no reason",
			decision: retryable.RetryDecision{},
			want:     logger.DeliveryUnconfirmed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := loggerfailure.RequestFailureDeliveryStatus(tt.decision); got != tt.want {
				t.Fatalf("RequestFailureDeliveryStatus(%+v) = %q, want %q", tt.decision, got, tt.want)
			}
		})
	}
}
