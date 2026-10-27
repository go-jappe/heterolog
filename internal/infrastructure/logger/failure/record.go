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

// FailedRecordは、失敗した1件のbackend操作と、そのdurable payloadです。
type FailedRecord struct {
	// Causeは、delivery report用に元のprocess内errorを保持します。durable recordは
	// Goのerrorをserializeせず、代わりにFailureCode/FailureReasonを残します。
	Cause error `json:"-"`

	// DeliveryStatusは、recordがretry・archive・quarantineのどれになるかとは独立に、
	// backendの受理について分かっていることを記録します。未設定のstatusは、
	// 失敗が確定したことを意味しません。
	DeliveryStatus logger.DeliveryStatus
	FailedAt       time.Time
	Operation      string
	StatusCode     int
	FailureCode    FailureCode
	FailureReason  string
	Payload        map[string]any

	// RetryAfterは、同じ宛先へ次に送信するまでbackendがRetry-Afterで要求した
	// 待ち時間で、FailedAtから測ります。responseを観測してからRetry-Afterが
	// 経過するまで続くため、FailedAtからそのresponseまでの時間も含みます。0は
	// 要求がないことを表し、この値を持つのはRetryableRecordsだけです。spoolは、
	// FailedAtにRetryAfterを足した時刻より前にこのrecordをreplayしません。
	RetryAfter time.Duration
}

// RetryAfterSinceは、failedAtに始まり、observedClockの現在時刻にresponseを
// 観測した失敗について、FailedRecord.RetryAfterを返します。clockがnilの場合は
// 現在のUTC時刻を使います。decisionがretryableで、正のRetry-Afterを持つ場合を
// 除き、0を返します。
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
