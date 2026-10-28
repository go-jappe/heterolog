package retryable

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCircuitBreakerSuppressesRetryAfterConsecutiveFailuresPersistForDuration(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonNetTimeout}
	decision := RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}

	first := recordFailedRequest(t, circuitBreaker, key, decision)
	if !first.OpenUntil.IsZero() || first.Consecutive != 1 {
		t.Fatalf("first failure report = %+v, want closed breaker with consecutive=1", first)
	}

	now = now.Add(time.Second)
	second := recordFailedRequest(t, circuitBreaker, key, decision)
	if second.OpenUntil.IsZero() {
		t.Fatalf("second failure report = %+v, want circuit breaker open cooldown deadline", second)
	}
	if got := circuitBreaker.BeforeRequest(); got.AllowRequest || !got.OpenUntil.Equal(second.OpenUntil) {
		t.Fatalf("request after open = %+v, want suppressed until %v", got, second.OpenUntil)
	}
}

func TestCircuitBreakerRequestFailureExcludesOperationDurationFromConsecutiveGap(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Minute,
		MaxFailureIntervalForConsecutive: 15 * time.Second,
		MinConsecutiveFailuresToOpen:     3,
		OpenCooldown:                     5 * time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonNetTimeout}
	decision := RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}

	for attempt := 1; attempt <= 3; attempt++ {
		requestDecision := circuitBreaker.BeforeRequest()
		if !requestDecision.AllowRequest || requestDecision.Probe {
			t.Fatalf("attempt %d request decision = %+v, want normal request", attempt, requestDecision)
		}
		// 各PUT自体には、15秒の連続失敗間隔より長い時間がかかります。間隔として数えるのは、
		// 次のPUTまでの5秒の空き時間だけです。
		now = now.Add(time.Minute)
		report := circuitBreaker.RecordRequestFailure(key, decision, requestDecision)
		if attempt < 3 && (!report.OpenUntil.IsZero() || report.Consecutive != attempt) {
			t.Fatalf("attempt %d failure report = %+v", attempt, report)
		}
		if attempt == 3 && report.OpenUntil.IsZero() {
			t.Fatalf("attempt %d failure report = %+v, want open breaker", attempt, report)
		}
		if attempt < 3 {
			now = now.Add(5 * time.Second)
		}
	}
}

func TestCircuitBreakerDoesNotOpenWhenFailuresAreTooFarApart(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 500 * time.Millisecond,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonNetTimeout}
	decision := RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}

	if got := recordFailedRequest(t, circuitBreaker, key, decision); !got.OpenUntil.IsZero() || got.Consecutive != 1 {
		t.Fatalf("first failure report = %+v, want closed breaker with consecutive=1", got)
	}

	now = now.Add(time.Second)
	got := recordFailedRequest(t, circuitBreaker, key, decision)
	if !got.OpenUntil.IsZero() || got.Consecutive != 1 {
		t.Fatalf("spaced failure report = %+v, want reset consecutive failure", got)
	}
}

func TestCircuitBreakerDoesNotOpenWhenConsecutiveFailuresHaveNotPersistedLongEnough(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: time.Second,
		MinConsecutiveFailuresToOpen:     3,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonEOF}
	decision := RetryDecision{Retry: true, Reason: RetryReasonEOF}

	for i := 1; i <= 3; i++ {
		got := recordFailedRequest(t, circuitBreaker, key, decision)
		if !got.OpenUntil.IsZero() || got.Consecutive != i {
			t.Fatalf("failure[%d] report = %+v, want closed breaker before duration threshold", i, got)
		}
		now = now.Add(100 * time.Millisecond)
	}
}

func TestCircuitBreakerKeepsSingleProbeRuleAfterOpenCooldownUntilProbeResult(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonHTTPStatus, Code: 503}
	decision := RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: 503}
	// open前に許可された通常requestの遅い完了は、open後の状態を変更しません。
	lateNormal := circuitBreaker.BeforeRequest()
	opened := openTestCircuitBreaker(t, circuitBreaker, &now, key, decision)

	now = opened.Add(time.Nanosecond)
	probe := circuitBreaker.BeforeRequest()
	if !probe.AllowRequest || !probe.Probe || !probe.OpenUntil.IsZero() {
		t.Fatalf("after open cooldown request decision = %+v, want single probe", probe)
	}

	// cooldown後もprobeの結果を記録するまでは、時間経過や通常requestの結果で
	// closedへ戻さず、他のrequestを通しません。
	circuitBreaker.RecordSuccess(lateNormal)
	if report := circuitBreaker.RecordRequestFailure(key, decision, lateNormal); report != (CircuitBreakerFailureReport{}) {
		t.Fatalf("late normal failure report = %+v, want ignored", report)
	}
	if report := circuitBreaker.RecordRequestFailure(key, decision, probe); report != (CircuitBreakerFailureReport{}) {
		t.Fatalf("probe recorded as normal failure = %+v, want ignored", report)
	}
	for _, elapsed := range []time.Duration{0, time.Minute, time.Hour} {
		now = opened.Add(time.Nanosecond + elapsed)
		if got := circuitBreaker.BeforeRequest(); got.AllowRequest || !got.Probe || !got.OpenUntil.IsZero() {
			t.Fatalf("request %s after cooldown = %+v, want suppressed while probe is in flight", elapsed, got)
		}
	}

	failedProbe := circuitBreaker.RecordProbeFailure(key, decision, probe)
	if !failedProbe.OpenUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("failed probe report = %+v, want new cooldown from %v", failedProbe, now)
	}
	if got := circuitBreaker.BeforeRequest(); got.AllowRequest || !got.OpenUntil.Equal(failedProbe.OpenUntil) {
		t.Fatalf("request after failed probe = %+v, want suppressed until %v", got, failedProbe.OpenUntil)
	}
}

func TestCircuitBreakerRecordSuccessResetsState(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonEOF}
	decision := RetryDecision{Retry: true, Reason: RetryReasonEOF}

	if got := recordFailedRequest(t, circuitBreaker, key, decision); !got.OpenUntil.IsZero() {
		t.Fatalf("first failure report = %+v, want closed breaker", got)
	}
	now = now.Add(time.Second)
	request := circuitBreaker.BeforeRequest()
	if !request.AllowRequest || request.Probe {
		t.Fatalf("request = %+v, want allowed normal request", request)
	}
	circuitBreaker.RecordSuccess(request)
	got := recordFailedRequest(t, circuitBreaker, key, decision)
	if !got.OpenUntil.IsZero() || got.Consecutive != 1 {
		t.Fatalf("after success report = %+v, want fresh closed failure sequence", got)
	}
}

func TestCircuitBreakerBeforeRequestSuppressesOpenCircuitAndAllowsSingleProbeAfterOpenCooldown(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonNetTimeout}
	decision := RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}
	openTestCircuitBreaker(t, circuitBreaker, &now, key, decision)

	suppressed := circuitBreaker.BeforeRequest()
	if suppressed.AllowRequest {
		t.Fatalf("open circuit breaker request decision = %+v, want suppressed", suppressed)
	}

	now = suppressed.OpenUntil.Add(time.Nanosecond)
	probe := circuitBreaker.BeforeRequest()
	if !probe.AllowRequest || !probe.Probe || !probe.OpenUntil.IsZero() {
		t.Fatalf("after open cooldown request decision = %+v, want single probe allowed", probe)
	}

	secondProbe := circuitBreaker.BeforeRequest()
	if secondProbe.AllowRequest || !secondProbe.Probe || !secondProbe.OpenUntil.IsZero() {
		t.Fatalf("second probe request decision = %+v, want suppressed while probe is in flight", secondProbe)
	}

	failedProbe := circuitBreaker.RecordProbeFailure(key, decision, probe)
	if failedProbe.OpenUntil.IsZero() {
		t.Fatalf("failed probe report = %+v, want reopened circuit breaker", failedProbe)
	}
	if got := circuitBreaker.BeforeRequest(); got.AllowRequest {
		t.Fatalf("after failed probe request decision = %+v, want suppressed", got)
	}
}

func TestCircuitBreakerProbeSuccessClosesCircuitAndNextRequestIsNormal(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonHTTPStatus, Code: 503}
	decision := RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: 503}
	openTestCircuitBreaker(t, circuitBreaker, &now, key, decision)

	open := circuitBreaker.BeforeRequest()
	if open.AllowRequest {
		t.Fatalf("open circuit breaker request decision = %+v, want suppressed", open)
	}

	now = open.OpenUntil.Add(time.Nanosecond)
	probe := circuitBreaker.BeforeRequest()
	if !probe.AllowRequest || !probe.Probe || !probe.OpenUntil.IsZero() {
		t.Fatalf("after open cooldown request decision = %+v, want probe allowed", probe)
	}

	circuitBreaker.RecordSuccess(probe)

	normal := circuitBreaker.BeforeRequest()
	if !normal.AllowRequest || normal.Probe || !normal.OpenUntil.IsZero() {
		t.Fatalf("after successful probe request decision = %+v, want normal closed circuit breaker", normal)
	}

	got := circuitBreaker.RecordRequestFailure(key, decision, normal)
	if !got.OpenUntil.IsZero() || got.Consecutive != 1 {
		t.Fatalf("failure after closed circuit breaker = %+v, want fresh closed failure sequence", got)
	}
}

func TestCircuitBreakerReleaseProbeKeepsOpenStateForNextProbe(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonHTTPStatus, Code: 503}
	decision := RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: 503}
	opened := openTestCircuitBreaker(t, circuitBreaker, &now, key, decision)

	now = opened.Add(time.Nanosecond)
	releasedProbe := circuitBreaker.BeforeRequest()
	if !releasedProbe.AllowRequest || !releasedProbe.Probe {
		t.Fatalf("after open cooldown request decision = %+v, want probe", releasedProbe)
	}
	circuitBreaker.ReleaseProbe(releasedProbe)

	// 解放後もclosedへ戻さず、次のrequestを改めて1件だけprobeとして許可します。
	nextProbe := circuitBreaker.BeforeRequest()
	if !nextProbe.AllowRequest || !nextProbe.Probe || !nextProbe.OpenUntil.IsZero() {
		t.Fatalf("request after released probe = %+v, want a new probe", nextProbe)
	}
	if got := circuitBreaker.BeforeRequest(); got.AllowRequest || !got.Probe {
		t.Fatalf("concurrent request after released probe = %+v, want suppressed by the new probe", got)
	}

	// 解放済みprobeの結果を重ねて記録しても、新しいprobeを完了させません。
	circuitBreaker.ReleaseProbe(releasedProbe)
	circuitBreaker.RecordSuccess(releasedProbe)
	if report := circuitBreaker.RecordProbeFailure(key, decision, releasedProbe); report != (CircuitBreakerFailureReport{}) {
		t.Fatalf("released probe failure report = %+v, want ignored", report)
	}
	if got := circuitBreaker.BeforeRequest(); got.AllowRequest || !got.Probe {
		t.Fatalf("request after stale probe results = %+v, want suppressed by the new probe", got)
	}

	// 連続失敗の記録も保持するため、次のprobe失敗はopen時の2回に続く3回目です。
	failedProbe := circuitBreaker.RecordProbeFailure(key, decision, nextProbe)
	if failedProbe.Consecutive != 3 || !failedProbe.OpenUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("probe failure after release = %+v, want consecutive=3 and a new cooldown", failedProbe)
	}
}

func TestCircuitBreakerReleaseProbeIgnoresNormalRequests(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonEOF}
	decision := RetryDecision{Retry: true, Reason: RetryReasonEOF}

	if got := recordFailedRequest(t, circuitBreaker, key, decision); got.Consecutive != 1 {
		t.Fatalf("first failure report = %+v, want consecutive=1", got)
	}
	now = now.Add(time.Second)
	normal := circuitBreaker.BeforeRequest()
	circuitBreaker.ReleaseProbe(normal)
	// 通常requestの解放要求ではgenerationを進めないため、同じrequestの失敗を記録できます。
	if got := circuitBreaker.RecordRequestFailure(key, decision, normal); got.OpenUntil.IsZero() || got.Consecutive != 2 {
		t.Fatalf("failure after ignored release = %+v, want open breaker with consecutive=2", got)
	}
}

func TestCircuitBreakerFailedProbeWithDifferentFailureKeyReopensBackend(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	timeoutKey := CircuitBreakerKey{Reason: RetryReasonNetTimeout}
	timeoutDecision := RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}
	opened := openTestCircuitBreaker(t, circuitBreaker, &now, timeoutKey, timeoutDecision)

	now = opened.Add(time.Nanosecond)
	probe := circuitBreaker.BeforeRequest()
	if !probe.AllowRequest || !probe.Probe || !probe.OpenUntil.IsZero() {
		t.Fatalf("probe request decision = %+v, want probe allowed", probe)
	}

	serviceKey := CircuitBreakerKey{Reason: RetryReasonHTTPStatus, Code: 503}
	serviceDecision := RetryDecision{Retry: true, Reason: RetryReasonHTTPStatus, Code: 503}
	failedProbe := circuitBreaker.RecordProbeFailure(serviceKey, serviceDecision, probe)
	if failedProbe.OpenUntil.IsZero() {
		t.Fatalf("failed probe report = %+v, want reopened circuit breaker", failedProbe)
	}

	if got := circuitBreaker.BeforeRequest(); got.AllowRequest {
		t.Fatalf("request after failed probe = %+v, want suppressed", got)
	}
	now = failedProbe.OpenUntil.Add(time.Nanosecond)
	if got := circuitBreaker.BeforeRequest(); !got.AllowRequest || !got.Probe || !got.OpenUntil.IsZero() {
		t.Fatalf("request after reopened cooldown = %+v, want a new probe", got)
	}
}

func TestProbeReachedBackendRequiresBackendResponse(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancelExpired()
	badRequest := &statusPrecedenceError{code: 400, message: "400 Bad Request"}

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{name: "success", ctx: context.Background(), want: true},
		{name: "success after caller cancellation", ctx: canceled, want: true},
		{name: "explicit non-retryable response", ctx: context.Background(), err: badRequest, want: true},
		{name: "wrapped response", ctx: context.Background(), err: fmt.Errorf("send: %w", badRequest), want: true},
		{name: "response without context", err: badRequest, want: true},
		{name: "response after caller cancellation", ctx: canceled, err: badRequest},
		{name: "response after caller deadline", ctx: expired, err: badRequest},
		{name: "caller cancellation", ctx: canceled, err: context.Canceled},
		{name: "caller deadline", ctx: expired, err: context.DeadlineExceeded},
		{name: "canceled transport", ctx: context.Background(), err: fmt.Errorf("send: %w", context.Canceled)},
		{name: "local error", ctx: context.Background(), err: errors.New("encode request")},
		{name: "missing status", ctx: context.Background(), err: &statusPrecedenceError{message: "no status"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ProbeReachedBackend(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("ProbeReachedBackend() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestClassifyRetryTreatsCircuitBreakerOpenAsNoRetry(t *testing.T) {
	err := NewCircuitBreakerOpenError("loki:Push", time.Unix(1_700_000_000, 0))
	if err.Operation != "loki:Push" {
		t.Fatalf("CircuitBreakerOpenError.Operation = %q, want %q", err.Operation, "loki:Push")
	}

	decision := ClassifyRetry(nil, err)
	if decision.Retry || decision.Reason != RetryReasonCircuitBreakerOpen {
		t.Fatalf("ClassifyRetry(circuit breaker open) = %+v, want no-retry circuit_breaker_open", decision)
	}
}

// recordFailedRequestは、production経路と同じくBeforeRequestで許可された
// 通常requestの失敗をRecordRequestFailureへ記録します。
func recordFailedRequest(
	t *testing.T,
	circuitBreaker *CircuitBreaker,
	key CircuitBreakerKey,
	decision RetryDecision,
) CircuitBreakerFailureReport {
	t.Helper()
	request := circuitBreaker.BeforeRequest()
	if !request.AllowRequest || request.Probe {
		t.Fatalf("request decision = %+v, want allowed normal request", request)
	}
	return circuitBreaker.RecordRequestFailure(key, decision, request)
}

// openTestCircuitBreakerは、試験用の閾値（1秒以上続く2回の連続失敗）でbreakerを
// openし、open期限を返します。nowは2回目の失敗時刻へ進めます。
func openTestCircuitBreaker(
	t *testing.T,
	circuitBreaker *CircuitBreaker,
	now *time.Time,
	key CircuitBreakerKey,
	decision RetryDecision,
) time.Time {
	t.Helper()
	if got := recordFailedRequest(t, circuitBreaker, key, decision); !got.OpenUntil.IsZero() {
		t.Fatalf("first failure report = %+v, want closed breaker", got)
	}
	*now = now.Add(time.Second)
	opened := recordFailedRequest(t, circuitBreaker, key, decision)
	if opened.OpenUntil.IsZero() {
		t.Fatalf("second failure report = %+v, want open circuit breaker", opened)
	}
	return opened.OpenUntil
}

func TestCircuitBreakerReportsOpeningAndClosing(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := NewCircuitBreaker(CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	key := CircuitBreakerKey{Reason: RetryReasonNetTimeout}
	decision := RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}

	if first := recordFailedRequest(t, circuitBreaker, key, decision); first.Opened {
		t.Fatalf("first failure report = %+v, want not opened", first)
	}
	now = now.Add(time.Second)
	second := recordFailedRequest(t, circuitBreaker, key, decision)
	if !second.Opened {
		t.Fatalf("second failure report = %+v, want opened", second)
	}

	// cooldown後の最初のrequestはprobeです。probeの失敗はbreakerを開き直します。
	now = second.OpenUntil.Add(time.Nanosecond)
	probe := circuitBreaker.BeforeRequest()
	if !probe.Probe {
		t.Fatalf("request after cooldown = %+v, want a probe", probe)
	}
	if reopened := circuitBreaker.RecordProbeFailure(key, decision, probe); !reopened.Opened {
		t.Fatalf("probe failure report = %+v, want opened", reopened)
	}

	now = now.Add(2 * time.Minute)
	probe = circuitBreaker.BeforeRequest()
	if !probe.Probe {
		t.Fatalf("request after the second cooldown = %+v, want a probe", probe)
	}
	if closed := circuitBreaker.RecordSuccess(probe); !closed {
		t.Fatal("RecordSuccess(probe) = false, want the breaker closed")
	}
	if closed := circuitBreaker.RecordSuccess(circuitBreaker.BeforeRequest()); closed {
		t.Fatal("RecordSuccess on a closed breaker = true, want false")
	}
}
