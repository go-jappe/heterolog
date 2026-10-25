package failure_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/pkg/clock"
)

// Retry-Afterは応答を受け取った時刻から数えるため、FailedAt（送信開始時刻）からの待機時間には
// 応答までの時間を含める。再送しない分類やRetry-Afterのない応答では待機しない。
func TestRetryAfterSinceMeasuresFromFailedAtUntilRetryAfterElapsesAfterResponse(t *testing.T) {
	failedAt := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	observed := clock.NewFixedClock(failedAt.Add(3 * time.Second))
	throttled := retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPThrottle, Code: 429, RetryAfter: 30 * time.Second}

	tests := []struct {
		name     string
		failedAt time.Time
		decision retryable.RetryDecision
		want     time.Duration
	}{
		{name: "includes response latency", failedAt: failedAt, decision: throttled, want: 33 * time.Second},
		// FailedAtがない場合は、保存側が失敗時刻として使う時刻からRetry-Afterだけ待つ。
		{name: "without failed at", decision: throttled, want: 30 * time.Second},
		{name: "without retry-after", failedAt: failedAt, decision: retryable.RetryDecision{Retry: true, Code: 503}},
		{
			name:     "not retryable",
			failedAt: failedAt,
			decision: retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: 400, RetryAfter: 30 * time.Second},
		},
		{
			name:     "quarantine",
			failedAt: failedAt,
			decision: retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome, RetryAfter: 30 * time.Second},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := loggerfailure.RetryAfterSince(tt.failedAt, observed, tt.decision); got != tt.want {
				t.Fatalf("RetryAfterSince = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestRetryAfterSinceUsesCurrentTimeWithoutClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failedAt := time.Now().UTC()
		time.Sleep(time.Second)
		got := loggerfailure.RetryAfterSince(failedAt, nil, retryable.RetryDecision{Retry: true, RetryAfter: time.Minute})
		if want := time.Minute + time.Second; got != want {
			t.Fatalf("RetryAfterSince without clock = %s, want %s", got, want)
		}
	})
}
