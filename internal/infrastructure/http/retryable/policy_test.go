package retryable

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

func TestPolicyMaxAttemptsUsesRetryClass(t *testing.T) {
	tests := []struct {
		name       string
		decision   RetryDecision
		configured int
		want       int
	}{
		{name: "no retry", decision: RetryDecision{}, configured: 3, want: 1},
		{name: "unset disables retry", decision: RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: 503}, configured: 0, want: 1},
		{name: "timeout is capped to two", decision: RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}, configured: 3, want: 2},
		{name: "request timeout is capped to two", decision: RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: http.StatusRequestTimeout}, configured: 4, want: 2},
		{name: "internal server error is capped to two", decision: RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: http.StatusInternalServerError}, configured: 4, want: 2},
		{name: "gateway timeout is capped to two", decision: RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: http.StatusGatewayTimeout}, configured: 4, want: 2},
		{name: "bad gateway uses configured max", decision: RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: http.StatusBadGateway}, configured: 4, want: 4},
		{name: "service unavailable uses configured max", decision: RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable}, configured: 4, want: 4},
		{name: "service transient uses configured max", decision: RetryDecision{Retry: true, Reason: RetryReasonServiceTransient}, configured: 4, want: 4},
		{name: "throttle uses configured max", decision: RetryDecision{Retry: true, Reason: RetryReasonHTTPThrottle, Code: http.StatusTooManyRequests}, configured: 4, want: 4},
		{name: "breaker open does not retry", decision: RetryDecision{Retry: true, Reason: RetryReasonCircuitBreakerOpen}, configured: 4, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PolicyMaxAttempts(tt.decision, tt.configured); got != tt.want {
				t.Fatalf("PolicyMaxAttempts(%+v, %d) = %d, want %d", tt.decision, tt.configured, got, tt.want)
			}
		})
	}
}

func TestPolicyShouldRetryStopsWhenParentContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	decision := RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable}
	if PolicyShouldRetry(ctx, 1, decision, 3) {
		t.Fatal("PolicyShouldRetry() = true, want false for canceled parent context")
	}
}

func TestClassifyRetryTreatsExpiredParentContextAsNoRetry(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	decision := ClassifyRetry(ctx, context.DeadlineExceeded)
	if decision.Retry || decision.Reason != RetryReasonCtxDeadline {
		t.Fatalf("ClassifyRetry() = %+v, want no-retry ctx deadline", decision)
	}
}

func TestPolicyDelayCapsRetryAfterAtConfiguredMaxDelay(t *testing.T) {
	decision := RetryDecision{
		Retry:      true,
		Reason:     RetryReasonHTTPThrottle,
		Code:       http.StatusTooManyRequests,
		RetryAfter: 2 * time.Second,
	}

	delay, ok := PolicyDelay(context.Background(), 1, time.Millisecond, time.Second, decision)
	if !ok {
		t.Fatal("PolicyDelay() ok = false, want true without parent deadline")
	}
	if delay != time.Second {
		t.Fatalf("PolicyDelay() = %s, want max delay %s", delay, time.Second)
	}
}

func TestPolicyDelayKeepsRetryAfterBelowConfiguredMaxDelay(t *testing.T) {
	decision := RetryDecision{
		Retry:      true,
		Reason:     RetryReasonHTTPStatus,
		Code:       http.StatusServiceUnavailable,
		RetryAfter: 500 * time.Millisecond,
	}

	delay, ok := PolicyDelay(context.Background(), 1, time.Millisecond, time.Second, decision)
	if !ok {
		t.Fatal("PolicyDelay() ok = false, want true without parent deadline")
	}
	if delay != decision.RetryAfter {
		t.Fatalf("PolicyDelay() = %s, want Retry-After %s", delay, decision.RetryAfter)
	}
}

func TestPolicyDelayUsesDefaultMaxDelayForRetryAfter(t *testing.T) {
	decision := RetryDecision{
		Retry:      true,
		Reason:     RetryReasonHTTPThrottle,
		Code:       http.StatusTooManyRequests,
		RetryAfter: 8 * time.Second,
	}

	tests := []struct {
		name     string
		maxDelay time.Duration
	}{
		{name: "zero", maxDelay: 0},
		{name: "negative", maxDelay: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay, ok := PolicyDelay(context.Background(), 1, time.Millisecond, tt.maxDelay, decision)
			if !ok {
				t.Fatal("PolicyDelay() ok = false, want true without parent deadline")
			}
			if delay != 5*time.Second {
				t.Fatalf("PolicyDelay() = %s, want default max delay %s", delay, 5*time.Second)
			}
		})
	}
}

func TestPolicyDelayChecksCappedRetryAfterAgainstParentContext(t *testing.T) {
	decision := RetryDecision{
		Retry:      true,
		Reason:     RetryReasonHTTPThrottle,
		Code:       http.StatusTooManyRequests,
		RetryAfter: 2 * time.Second,
	}
	const maxDelay = 100 * time.Millisecond

	tests := []struct {
		name   string
		budget time.Duration
		wantOK bool
	}{
		// 上限前のRetry-Afterは残り時間を超えても、上限を適用した待機時間で判定します。
		{name: "capped delay fits", budget: 500 * time.Millisecond, wantOK: true},
		// 待機を終えた時点で親contextの期限に達するretryは、次のattemptを実行できません。
		{name: "capped delay reaches deadline", budget: maxDelay, wantOK: false},
		{name: "capped delay does not fit", budget: 50 * time.Millisecond, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// bubble内の時計は待機しない限り進まないため、残り時間はbudgetに一致します。
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), tt.budget)
				defer cancel()

				delay, ok := PolicyDelay(ctx, 1, time.Millisecond, maxDelay, decision)
				if ok != tt.wantOK {
					t.Fatalf("PolicyDelay() ok = %t, want %t with %s context budget", ok, tt.wantOK, tt.budget)
				}
				if delay != maxDelay {
					t.Fatalf("PolicyDelay() = %s, want capped delay %s", delay, maxDelay)
				}
			})
		})
	}
}

func TestPolicyNextAttemptTimeoutOnlyForTimeoutDecisions(t *testing.T) {
	if got := PolicyNextAttemptTimeout(RetryDecision{Reason: RetryReasonNetTimeout}); got != DefaultPolicyTimeoutRetryAttemptTimeout {
		t.Fatalf("PolicyNextAttemptTimeout(net timeout) = %s, want %s", got, DefaultPolicyTimeoutRetryAttemptTimeout)
	}
	if got := PolicyNextAttemptTimeout(RetryDecision{Reason: RetryReasonHTTPStatus}); got != 0 {
		t.Fatalf("PolicyNextAttemptTimeout(http status) = %s, want 0", got)
	}
	if got := PolicyNextAttemptTimeout(RetryDecision{Reason: RetryReasonHTTPStatus, Code: http.StatusRequestTimeout}); got != DefaultPolicyTimeoutRetryAttemptTimeout {
		t.Fatalf("PolicyNextAttemptTimeout(HTTP 408) = %s, want %s", got, DefaultPolicyTimeoutRetryAttemptTimeout)
	}
	if got := PolicyNextAttemptTimeout(RetryDecision{Reason: RetryReasonHTTPStatus, Code: http.StatusGatewayTimeout}); got != 0 {
		t.Fatalf("PolicyNextAttemptTimeout(HTTP 504) = %s, want 0", got)
	}
}
