package wrapper

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/loki"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

const SpanNamePush = "internal.infrastructure.loki.wrapper.push"

// Client はLokiへログを送信する最小インターフェースです。
type Client interface {
	Push(ctx context.Context, streams []loki.Stream) error
}

// retryBackoffConfigProviderは、個別retry wrapperが試行回数と待機時間の計算に
// 必要とする設定だけを表します。wrapper chainの選択はRetryConfigProviderが担当します。
type retryBackoffConfigProvider interface {
	MaxAttempts() int

	// BaseDelay はリトライごとに加算されるバックオフの基準遅延です。
	BaseDelay() time.Duration

	// MaxDelay はバックオフの上限です。
	MaxDelay() time.Duration
}

// RetryingClient は通信エラー系とLokiの一時的なHTTP失敗をretryするLokiクライアントwrapperです。
type RetryingClient struct {
	next   Client
	cfg    retryBackoffConfigProvider
	tracer pkgobserver.Tracer
}

func newRetryingClient(
	next Client,
	cfg retryBackoffConfigProvider,
	tracer pkgobserver.Tracer,
) *RetryingClient {
	return &RetryingClient{
		next:   next,
		cfg:    cfg,
		tracer: observer.EnsureTracer(tracer),
	}
}

func (c *RetryingClient) Push(ctx context.Context, streams []loki.Stream) error {
	return c.doWithRetry(ctx, SpanNamePush, func(opCtx context.Context) error {
		return c.next.Push(opCtx, streams)
	})
}

func (c *RetryingClient) doWithRetry(ctx context.Context, name string, op func(context.Context) error) error {
	var lastErr error
	maxAttempts := c.maxAttempts()
	var nextDelay time.Duration

	attempts := 0
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attempts = attempt
		attemptCtx, span := c.tracer.StartSpan(ctx, name)

		if attempt > 1 {
			if !sleepWithContext(ctx, nextDelay) {
				if err := ctx.Err(); err != nil {
					span.End()
					return retryable.InterruptedWait(lastErr, err)
				}
				span.End()
				return lastErr
			}
		}

		lastErr = op(attemptCtx)
		if lastErr == nil {
			span.End()
			return nil
		}

		dec := retryclassifier.ClassifyRetry(attemptCtx, lastErr)
		if !dec.Retry || dec.Reason == retryable.RetryReasonCircuitBreakerOpen {
			span.End()
			return lastErr
		}

		if attempt == maxAttempts {
			span.End()
			break
		}
		nextDelay = c.retryDelay(attempt, dec)

		span.AddEvent("loki.retry_decision",
			pkgobserver.Attr{Key: "reason", Value: string(dec.Reason)},
			pkgobserver.Attr{Key: "attempt", Value: attempt},
			pkgobserver.Attr{Key: "retry", Value: dec.Retry},
			pkgobserver.Attr{Key: "http_status", Value: dec.Code},
			pkgobserver.Attr{Key: "error", Value: lastErr.Error()},
		)
		span.End()
	}

	return &retryable.RetryAttemptsExhaustedError{Policy: false, Attempts: attempts, LastErr: lastErr}
}

// retryDelayは通常のlinear backoffを使い、429と503でRetry-Afterが得られた場合だけ
// maxDelayを上限としてserverの待機時間を優先します。
func (c *RetryingClient) retryDelay(retryN int, decision retryable.RetryDecision) time.Duration {
	usesRetryAfter := decision.Reason == retryable.RetryReasonHTTPThrottle ||
		(decision.Reason == retryable.RetryReasonHTTPStatus && decision.Code == 503)
	if usesRetryAfter && decision.RetryAfter > 0 {
		delay := decision.RetryAfter
		if maxDelay := c.maxDelay(); delay > maxDelay {
			delay = maxDelay
		}
		return delay
	}
	return c.backoff(retryN)
}

func (c *RetryingClient) backoff(retryN int) time.Duration {
	d := c.baseDelay() * time.Duration(retryN)
	if maxDelay := c.maxDelay(); d > maxDelay {
		d = maxDelay
	}
	return d
}

func (c *RetryingClient) maxAttempts() int {
	return c.cfg.MaxAttempts()
}

func (c *RetryingClient) baseDelay() time.Duration {
	return c.cfg.BaseDelay()
}

func (c *RetryingClient) maxDelay() time.Duration {
	return c.cfg.MaxDelay()
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
