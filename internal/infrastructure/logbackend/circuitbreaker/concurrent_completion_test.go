package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
)

// probeOutcomeは、probe完了後にbreakerが取るべき状態です。
type probeOutcome int

const (
	// probeClosesはbackendの応答を確認でき、breakerをclosedへ戻す結果です。
	probeCloses probeOutcome = iota
	// probeReopensはbackendの一時障害が続き、新しいcooldownを始める結果です。
	probeReopens
	// probeReleasesは復旧を確認できず、open状態のままprobe枠だけを解放する結果です。
	probeReleases
)

func TestOperationGuardLateResultsDoNotReleaseRecoveryProbe(t *testing.T) {
	transient := errors.New("backend unavailable")
	permanent := errors.New("invalid document")
	rejected := &guardTestHTTPStatusError{code: 400}
	for _, oldResult := range []struct {
		name string
		err  error
	}{{"success", nil}, {"transient", transient}, {"permanent", permanent}} {
		for _, probeResult := range []struct {
			name string
			// interruptは、probe実行中に呼び出し元contextを終了させます。
			interrupt func(cancel context.CancelFunc)
			// timeoutが正なら、probeの呼び出し元contextにその期限を設定します。
			timeout time.Duration
			result  func(context.Context) error
			wantErr error
			want    probeOutcome
		}{
			{name: "success", result: returnGuardResult(nil), want: probeCloses},
			{name: "transient", result: returnGuardResult(transient), wantErr: transient, want: probeReopens},
			// 再試行しない拒否でも、HTTP応答はbackendが応答できる証拠です。
			{name: "rejected_response", result: returnGuardResult(rejected), wantErr: rejected, want: probeCloses},
			// 応答のない再試行対象外errorは、復旧の証拠になりません。
			{name: "permanent_without_response", result: returnGuardResult(permanent), wantErr: permanent, want: probeReleases},
			{
				name:      "canceled",
				interrupt: func(cancel context.CancelFunc) { cancel() },
				result:    returnGuardContextError,
				wantErr:   context.Canceled,
				want:      probeReleases,
			},
			{
				name: "deadline",
				// synctestの仮想時刻を期限より後まで進め、probe実行中に呼び出し元の期限を過ぎさせます。
				interrupt: func(context.CancelFunc) { time.Sleep(2 * time.Minute) },
				timeout:   time.Minute,
				result:    returnGuardContextError,
				wantErr:   context.DeadlineExceeded,
				want:      probeReleases,
			},
		} {
			t.Run(oldResult.name+"/probe_"+probeResult.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					guard, advance := concurrentTestGuard(transient)
					oldStarted, releaseOld, oldDone := startBlockedOperation(t, guard, context.Background(), returnGuardResult(oldResult.err))
					waitGuardSignal(t, oldStarted)
					openTestGuard(t, guard, advance, transient)
					advance(time.Minute)
					var probeCtx context.Context
					var cancelProbeCtx context.CancelFunc
					if probeResult.timeout > 0 {
						probeCtx, cancelProbeCtx = context.WithTimeout(context.Background(), probeResult.timeout)
					} else {
						probeCtx, cancelProbeCtx = context.WithCancel(context.Background())
					}
					defer cancelProbeCtx()
					probeStarted, releaseProbe, probeDone := startBlockedOperation(t, guard, probeCtx, probeResult.result)
					waitGuardSignal(t, probeStarted)

					releaseOld()
					if err := waitGuardResult(t, oldDone); !errors.Is(err, oldResult.err) {
						t.Fatalf("old request error = %v, want %v", err, oldResult.err)
					}
					blocked := assertGuardSuppressed(t, guard)
					if !blocked.OpenUntil.IsZero() {
						t.Fatalf("old completion replaced the pending probe with a new cooldown: %v", blocked)
					}

					if probeResult.interrupt != nil {
						probeResult.interrupt(cancelProbeCtx)
						if probeCtx.Err() == nil {
							t.Fatal("probe caller context is still active")
						}
					}
					releaseProbe()
					if err := waitGuardResult(t, probeDone); !errors.Is(err, probeResult.wantErr) {
						t.Fatalf("probe error = %v, want %v", err, probeResult.wantErr)
					}
					switch probeResult.want {
					case probeCloses:
						assertGuardClosed(t, guard)
					case probeReopens:
						if blocked := assertGuardSuppressed(t, guard); blocked.OpenUntil.IsZero() {
							t.Fatal("failed probe did not start a new cooldown")
						}
						advance(time.Minute)
						assertNextRequestIsProbe(t, guard)
					case probeReleases:
						// cooldownを始め直さず、次のrequestを改めてprobeとして許可します。
						assertNextRequestIsProbe(t, guard)
					}
				})
			})
		}
	}
}

func TestOperationGuardLateResultsDoNotChangeCooldownOrRecoveredState(t *testing.T) {
	transient := errors.New("backend unavailable")
	for _, phase := range []string{"cooldown", "recovered"} {
		for _, success := range []bool{true, false} {
			name := "failure"
			var oldResult error = transient
			if success {
				name, oldResult = "success", nil
			}
			t.Run(phase+"/"+name, func(t *testing.T) {
				guard, advance := concurrentTestGuard(transient)
				started, release, done := startBlockedOperation(t, guard, context.Background(), returnGuardResult(oldResult))
				waitGuardSignal(t, started)
				openUntil := openTestGuard(t, guard, advance, transient)
				if phase == "recovered" {
					advance(time.Minute)
					assertGuardAllows(t, guard)
				}
				release()
				if err := waitGuardResult(t, done); !errors.Is(err, oldResult) {
					t.Fatalf("old request error = %v, want %v", err, oldResult)
				}
				if phase == "recovered" {
					assertGuardAllows(t, guard)
				} else {
					if got := assertGuardSuppressed(t, guard).OpenUntil; !got.Equal(openUntil) {
						t.Fatalf("old completion changed cooldown: got %v, want %v", got, openUntil)
					}
					advance(time.Minute)
					assertGuardAllows(t, guard)
				}
			})
		}
	}
}

func concurrentTestGuard(transient error) (*OperationGuard, func(time.Duration)) {
	var nanos atomic.Int64
	nanos.Store(time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC).UnixNano())
	breaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
		Clock:                        func() time.Time { return time.Unix(0, nanos.Load()) },
		MinConsecutiveFailuresToOpen: 1,
		MinFailureDurationToOpen:     time.Second,
		OpenCooldown:                 time.Minute,
	})
	guard := NewOperationGuard(OperationGuardOptions{
		CircuitBreaker: breaker,
		Classify: func(_ context.Context, err error) retryable.RetryDecision {
			return retryable.RetryDecision{Retry: errors.Is(err, transient), Reason: retryable.RetryReasonServiceTransient, Code: 503}
		},
	})
	return guard, func(duration time.Duration) { nanos.Add(int64(duration)) }
}

func returnGuardResult(result error) func(context.Context) error {
	return func(context.Context) error { return result }
}

// returnGuardContextErrorは、clientが呼び出し元contextの終了を通信errorとして返す状況を再現します。
func returnGuardContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	return nil
}

func startBlockedOperation(
	t *testing.T,
	guard *OperationGuard,
	ctx context.Context,
	result func(context.Context) error,
) (<-chan struct{}, func(), <-chan error) {
	t.Helper()
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	go func() {
		done <- guard.Do(ctx, "blocked operation", func(opCtx context.Context) error {
			close(started)
			<-release
			return result(opCtx)
		})
	}()
	return started, unblock, done
}

func openTestGuard(t *testing.T, guard *OperationGuard, advance func(time.Duration), failure error) time.Time {
	t.Helper()
	err := guard.Do(context.Background(), "failed request", func(context.Context) error {
		advance(time.Second)
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("failure error = %v, want %v", err, failure)
	}
	blocked := assertGuardSuppressed(t, guard)
	if blocked.OpenUntil.IsZero() {
		t.Fatal("failed request did not open circuit breaker")
	}
	return blocked.OpenUntil
}

func assertGuardSuppressed(t *testing.T, guard *OperationGuard) *retryable.CircuitBreakerOpenError {
	t.Helper()
	called := false
	err := guard.Do(context.Background(), "must wait", func(context.Context) error {
		called = true
		return nil
	})
	var blocked *retryable.CircuitBreakerOpenError
	if called || !errors.As(err, &blocked) {
		t.Fatalf("request was not suppressed: called=%t error=%v", called, err)
	}
	return blocked
}

func assertGuardAllows(t *testing.T, guard *OperationGuard) {
	t.Helper()
	called := false
	err := guard.Do(context.Background(), "recovered request", func(context.Context) error {
		called = true
		return nil
	})
	if !called || err != nil {
		t.Fatalf("request was not allowed: called=%t error=%v", called, err)
	}
}

// assertGuardClosedは、実行中のrequestがあっても次のrequestを許可するclosed状態であることを
// 確認します。probe中なら二件目は抑止されるため、復旧済みかどうかを区別できます。
func assertGuardClosed(t *testing.T, guard *OperationGuard) {
	t.Helper()
	started, release, done := startBlockedOperation(t, guard, context.Background(), returnGuardResult(nil))
	waitGuardSignal(t, started)
	assertGuardAllows(t, guard)
	release()
	if err := waitGuardResult(t, done); err != nil {
		t.Fatalf("in-flight request error = %v, want nil", err)
	}
}

// assertNextRequestIsProbeは、次のrequestだけがprobeとして許可され、実行中は他のrequestを
// cooldownなしで抑止することを確認します。probeを成功させた後はclosedへ戻ります。
func assertNextRequestIsProbe(t *testing.T, guard *OperationGuard) {
	t.Helper()
	started, release, done := startBlockedOperation(t, guard, context.Background(), returnGuardResult(nil))
	waitGuardSignal(t, started)
	if blocked := assertGuardSuppressed(t, guard); !blocked.OpenUntil.IsZero() {
		t.Fatalf("request during probe was suppressed by a cooldown: %v", blocked)
	}
	release()
	if err := waitGuardResult(t, done); err != nil {
		t.Fatalf("probe error = %v, want nil", err)
	}
	assertGuardClosed(t, guard)
}

func waitGuardSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not start")
	}
}

func waitGuardResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not complete")
		return nil
	}
}

// guardTestHTTPStatusErrorは、backendが返したHTTP statusを持つerrorです。
type guardTestHTTPStatusError struct {
	code int
}

func (e *guardTestHTTPStatusError) Error() string {
	return fmt.Sprintf("backend returned HTTP %d", e.code)
}

func (e *guardTestHTTPStatusError) StatusCode() int { return e.code }

func (e *guardTestHTTPStatusError) RetryAfter() time.Duration { return 0 }
