package failure

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/logger"
)

// FailureCodeは、backend送信失敗を機械的に分類する識別値です。
//
// backendが返す動的なerror codeも保持するためclosed enumにはしませんが、通常の
// stringとは型を分け、外部値を取り込む箇所では明示的な変換を要求します。
type FailureCode string

// FailedRecord is one failed backend operation and its durable payload.
type FailedRecord struct {
	// Cause preserves the original in-process error for delivery reports. Durable
	// records retain FailureCode/FailureReason instead of serializing Go errors.
	Cause error `json:"-"`

	// DeliveryStatus records what is known about backend acceptance, independently
	// of whether the record is retried, archived, or quarantined. An unset status
	// does not establish a confirmed failure.
	DeliveryStatus logger.DeliveryStatus
	FailedAt       time.Time
	Operation      string
	StatusCode     int
	FailureCode    FailureCode
	FailureReason  string
	Payload        map[string]any

	// RetryAfter is the wait, measured from FailedAt, that the backend requested
	// with Retry-After before the next send to the same destination. It runs
	// until Retry-After has elapsed after the response was observed, so it
	// includes the time between FailedAt and that response. Zero means no
	// request; only RetryableRecords carry it. The spool does not replay the
	// record before FailedAt plus RetryAfter.
	RetryAfter time.Duration
}

// RetryAfterSince returns FailedRecord.RetryAfter for a failure that started at
// failedAt and whose response was observed at the current time of
// observedClock. A nil clock uses the current UTC time. It returns zero unless
// the decision is retryable and carries a positive Retry-After.
func RetryAfterSince(failedAt time.Time, observedClock clock.Clock, decision retryable.RetryDecision) time.Duration {
	if !decision.Retry || decision.RetryAfter <= 0 {
		return 0
	}
	observedAt := time.Now().UTC()
	if observedClock != nil {
		observedAt = observedClock.Now()
	}
	if failedAt.IsZero() || !observedAt.After(failedAt) {
		return decision.RetryAfter
	}
	return observedAt.Sub(failedAt) + decision.RetryAfter
}
