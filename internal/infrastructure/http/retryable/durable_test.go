package retryable

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestDurableReplayQuarantinesUnknownOutcomesWithoutEnablingImmediateRetries(t *testing.T) {
	err := errors.New("request outcome unavailable")
	immediate := ClassifyRetry(context.Background(), err)
	if immediate.Retry {
		t.Fatal("an unknown outcome must not enable an unbounded immediate retry")
	}
	later := ForDurableReplay(context.Background(), err, immediate)
	if later.Retry || !later.Quarantine || later.Reason != RetryReasonUnknownOutcome {
		t.Fatalf("unknown outcome was discarded: %+v", later)
	}
	if PolicyShouldRetry(context.Background(), 1, later, 10) {
		t.Fatal("durable replay decision enabled immediate policy retries")
	}
}

func TestDurableReplayPreservesKnownRejectionsEvenAfterCallerStops(t *testing.T) {
	err := errors.New("failure")
	for _, ctx := range []context.Context{context.Background(), canceledContext(), expiredContext()} {
		for _, code := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden} {
			// 明示的な応答の分類は、呼び出し元が停止した後も維持します。
			original := ClassifyRetryByStatus(code, 0)
			if got := ForDurableReplay(ctx, err, original); got != original || got.Retry || got.Quarantine {
				t.Fatalf("known rejection %d changed after ctx err %v: %+v", code, ctx.Err(), got)
			}
		}
		throttled := ClassifyRetryByStatus(http.StatusTooManyRequests, time.Second)
		if got := ForDurableReplay(ctx, err, throttled); got != throttled {
			t.Fatalf("temporary rejection changed after ctx err %v: %+v", ctx.Err(), got)
		}
	}
	if got := ForDurableReplay(context.Background(), nil, RetryDecision{}); got.Retry || got.Quarantine {
		t.Fatal("nil error requested replay")
	}
}

func TestDurableReplayQuarantinesOutcomesInterruptedByStoppedCaller(t *testing.T) {
	unknown := errors.New("request outcome unavailable")
	for _, ctx := range []context.Context{canceledContext(), expiredContext()} {
		for _, tc := range []struct {
			name     string
			err      error
			decision RetryDecision
		}{
			// 即時retry用の分類は呼び出し元の停止を優先し、errorの内容を分類しません。
			{name: "classified with stopped context", err: unknown, decision: ClassifyRetry(ctx, unknown)},
			{name: "canceled in flight", err: context.Canceled, decision: ClassifyRetry(DurableClassificationContext(ctx), context.Canceled)},
			{name: "deadline in flight", err: context.DeadlineExceeded, decision: ClassifyRetry(DurableClassificationContext(ctx), context.DeadlineExceeded)},
		} {
			got := ForDurableReplay(ctx, tc.err, tc.decision)
			if got.Retry || !got.Quarantine || got.Reason != RetryReasonUnknownOutcome {
				t.Fatalf("%s after ctx err %v = %+v, want quarantine", tc.name, ctx.Err(), got)
			}
		}

		// 送信前と確認できた失敗は、呼び出し元の停止後も再送対象のままです。
		notSent := RetryDecision{Retry: true, Reason: RetryReasonRequestNotSent}
		if got := ForDurableReplay(ctx, context.Canceled, notSent); got != notSent {
			t.Fatalf("request-not-sent decision changed after ctx err %v: %+v", ctx.Err(), got)
		}
		open := ClassifyRetry(DurableClassificationContext(ctx), NewCircuitBreakerOpenError("send", time.Unix(1, 0)))
		if got := ForDurableReplay(ctx, NewCircuitBreakerOpenError("send", time.Unix(1, 0)), open); !got.Retry || got.Quarantine {
			t.Fatalf("circuit breaker open after ctx err %v = %+v, want future replay", ctx.Err(), got)
		}
	}

	// 呼び出し元が有効な間のattempt単位の期限切れは、一時的な失敗として扱います。
	attemptTimeout := ClassifyRetry(context.Background(), context.DeadlineExceeded)
	if got := ForDurableReplay(context.Background(), context.DeadlineExceeded, attemptTimeout); !got.Retry || got.Quarantine {
		t.Fatalf("attempt timeout with live caller = %+v, want retry", got)
	}
}

func TestDurableClassificationContextIgnoresCallerStopButKeepsValues(t *testing.T) {
	type key struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "kept"))
	cancel()
	classifyCtx := DurableClassificationContext(parent)
	if classifyCtx.Err() != nil {
		t.Fatalf("classification context err = %v, want nil", classifyCtx.Err())
	}
	if got := classifyCtx.Value(key{}); got != "kept" {
		t.Fatalf("classification context value = %v, want kept", got)
	}
	var missing context.Context
	if DurableClassificationContext(missing) == nil {
		t.Fatal("nil parent produced nil classification context")
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expiredContext() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	cancel()
	return ctx
}
