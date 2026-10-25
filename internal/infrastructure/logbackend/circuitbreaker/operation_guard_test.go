package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
)

func TestOperationGuardSharesCircuitBreakerStateAcrossBackendOperations(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	breaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock: func() time.Time {
			return now
		},
	})
	backendErr := errors.New("backend unavailable")
	guard := NewOperationGuard(OperationGuardOptions{
		CircuitBreaker: breaker,
		Tracer:         observer.NopTracer{},
		Meter:          observer.NopMeter{},
		EventName:      "backend.retry_decision",
		Classify: func(context.Context, error) retryable.RetryDecision {
			return retryable.RetryDecision{
				Retry:  true,
				Reason: retryable.RetryReasonServiceTransient,
				Code:   503,
			}
		},
	})

	fail := func(context.Context) error { return backendErr }
	if err := guard.Do(context.Background(), "write", fail); !errors.Is(err, backendErr) {
		t.Fatalf("first Do() error = %v, want backend error", err)
	}

	now = now.Add(time.Second)
	if err := guard.Do(context.Background(), "flush", fail); !errors.Is(err, backendErr) {
		t.Fatalf("second Do() error = %v, want backend error", err)
	}

	called := false
	err := guard.Do(context.Background(), "read", func(context.Context) error {
		called = true
		return nil
	})
	var openErr *retryable.CircuitBreakerOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("third Do() error = %T, want CircuitBreakerOpenError", err)
	}
	if called {
		t.Fatal("operation was called while backend circuit breaker was open")
	}
	if openErr.Operation != "read" {
		t.Fatalf("CircuitBreakerOpenError.Operation = %q, want %q", openErr.Operation, "read")
	}

	now = openErr.OpenUntil.Add(time.Nanosecond)
	if err := guard.Do(context.Background(), "probe", func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("probe Do() error = %v, want nil", err)
	}

	called = false
	if err := guard.Do(context.Background(), "write_after_recovery", func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("Do() after recovery error = %v, want nil", err)
	}
	if !called {
		t.Fatal("operation was not called after successful probe closed the circuit breaker")
	}
}

func TestOperationGuardDefaultClassifierProbeRecoversOnlyOnBackendResponse(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancelExpired()

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want probeOutcome
	}{
		{name: "success", ctx: context.Background(), want: probeCloses},
		// 4xxは再試行しませんが、backendが応答できる状態に戻った証拠です。
		{name: "rejected response", ctx: context.Background(), err: &guardTestHTTPStatusError{code: 400}, want: probeCloses},
		{name: "transient response", ctx: context.Background(), err: &guardTestHTTPStatusError{code: 503}, want: probeReopens},
		// retryable.ClassifyRetryは以下をすべてRetry=falseと分類しますが、応答がないため復旧とはみなしません。
		{name: "caller canceled", ctx: canceled, err: fmt.Errorf("send request: %w", context.Canceled), want: probeReleases},
		{name: "caller deadline", ctx: expired, err: fmt.Errorf("send request: %w", context.DeadlineExceeded), want: probeReleases},
		{name: "response after caller canceled", ctx: canceled, err: &guardTestHTTPStatusError{code: 400}, want: probeReleases},
		{name: "local error", ctx: context.Background(), err: errors.New("encode request"), want: probeReleases},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			guard := NewOperationGuard(OperationGuardOptions{
				CircuitBreaker: retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
					MinFailureDurationToOpen:     time.Second,
					MinConsecutiveFailuresToOpen: 1,
					OpenCooldown:                 time.Minute,
					Clock:                        func() time.Time { return now },
				}),
			})
			unavailable := &guardTestHTTPStatusError{code: 503}
			if err := guard.Do(context.Background(), "write", func(context.Context) error {
				now = now.Add(time.Second)
				return unavailable
			}); !errors.Is(err, unavailable) {
				t.Fatalf("opening request error = %v, want backend error", err)
			}
			opened := doSuppressedForTest(t, guard)
			if opened.OpenUntil.IsZero() {
				t.Fatal("failed request did not open circuit breaker")
			}

			now = opened.OpenUntil.Add(time.Nanosecond)
			probeCalled := false
			err := guard.Do(tc.ctx, "probe", func(context.Context) error {
				probeCalled = true
				return tc.err
			})
			if !probeCalled || !errors.Is(err, tc.err) {
				t.Fatalf("probe called=%t error=%v, want called with %v", probeCalled, err, tc.err)
			}

			switch tc.want {
			case probeCloses:
				if gotProbe := nextRequestIsProbeForTest(t, guard); gotProbe {
					t.Fatal("request after a backend response was handled as another probe, want closed breaker")
				}
			case probeReopens:
				if blocked := doSuppressedForTest(t, guard); blocked.OpenUntil.IsZero() {
					t.Fatal("transient probe failure did not start a new cooldown")
				}
			case probeReleases:
				if gotProbe := nextRequestIsProbeForTest(t, guard); !gotProbe {
					t.Fatal("probe without a backend response closed the breaker, want another probe")
				}
			}
		})
	}
}

// nextRequestIsProbeForTestは、次のrequestの実行中に別のrequestが抑止されるかを調べます。
// 抑止されれば次のrequestはprobeであり、許可されればbreakerはclosedです。
// 次のrequest自体は成功させます。
func nextRequestIsProbeForTest(t *testing.T, guard *OperationGuard) bool {
	t.Helper()
	nextCalled := false
	concurrentSuppressed := false
	err := guard.Do(context.Background(), "next", func(context.Context) error {
		nextCalled = true
		concurrentErr := guard.Do(context.Background(), "concurrent", func(context.Context) error { return nil })
		var blocked *retryable.CircuitBreakerOpenError
		if errors.As(concurrentErr, &blocked) {
			if !blocked.OpenUntil.IsZero() {
				t.Errorf("concurrent request was suppressed by a cooldown: %v", blocked)
			}
			concurrentSuppressed = true
			return nil
		}
		if concurrentErr != nil {
			t.Errorf("concurrent request error = %v, want nil or circuit breaker suppression", concurrentErr)
		}
		return nil
	})
	if !nextCalled || err != nil {
		t.Fatalf("next request called=%t error=%v, want allowed", nextCalled, err)
	}
	return concurrentSuppressed
}

func doSuppressedForTest(t *testing.T, guard *OperationGuard) *retryable.CircuitBreakerOpenError {
	t.Helper()
	called := false
	err := guard.Do(context.Background(), "suppressed", func(context.Context) error {
		called = true
		return nil
	})
	var blocked *retryable.CircuitBreakerOpenError
	if called || !errors.As(err, &blocked) {
		t.Fatalf("request was not suppressed: called=%t error=%v", called, err)
	}
	return blocked
}

func TestOperationGuardCountsLongFailedRequestsByIdleGap(t *testing.T) {
	for _, tc := range []struct {
		name     string
		idleGap  time.Duration
		wantOpen bool
	}{
		{name: "short idle gap keeps failures consecutive", idleGap: 5 * time.Second, wantOpen: true},
		{name: "long idle gap resets consecutive failures", idleGap: 16 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0)
			const cooldown = 30 * time.Second
			breaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
				MinFailureDurationToOpen:         time.Minute,
				MaxFailureIntervalForConsecutive: 15 * time.Second,
				MinConsecutiveFailuresToOpen:     3,
				OpenCooldown:                     cooldown,
				Clock:                            func() time.Time { return now },
			})
			backendErr := errors.New("backend timed out")
			guard := NewOperationGuard(OperationGuardOptions{
				CircuitBreaker: breaker,
				Classify: func(context.Context, error) retryable.RetryDecision {
					return retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonNetTimeout}
				},
			})
			calls := 0
			fail := func(context.Context) error {
				calls++
				// 実時間は待たず、一回のbackend処理に60秒かかった状態を再現します。
				now = now.Add(time.Minute)
				return backendErr
			}
			for attempt := 0; attempt < 3; attempt++ {
				if attempt > 0 {
					now = now.Add(tc.idleGap)
				}
				if err := guard.Do(context.Background(), "write", fail); !errors.Is(err, backendErr) {
					t.Fatalf("attempt %d error = %v, want backend error", attempt+1, err)
				}
			}
			if !tc.wantOpen {
				// 最後の失敗から間隔を空けずに続けると、改めて三回で停止します。
				for attempt := 0; attempt < 2; attempt++ {
					if err := guard.Do(context.Background(), "write", fail); !errors.Is(err, backendErr) {
						t.Fatalf("attempt after long idle gap error = %v, want backend error", err)
					}
				}
			}

			wantOpenUntil := now.Add(cooldown)
			callsBeforeSuppression := calls
			err := guard.Do(context.Background(), "write", fail)
			var openErr *retryable.CircuitBreakerOpenError
			if !errors.As(err, &openErr) {
				t.Fatalf("Do() after consecutive long failures error = %v, want CircuitBreakerOpenError", err)
			}
			if calls != callsBeforeSuppression {
				t.Fatalf("backend calls = %d, want %d while breaker is open", calls, callsBeforeSuppression)
			}
			if !openErr.OpenUntil.Equal(wantOpenUntil) {
				t.Fatalf("OpenUntil = %v, want failure completion plus cooldown %v", openErr.OpenUntil, wantOpenUntil)
			}
		})
	}
}
