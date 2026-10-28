package wrapper

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/retryclassifier"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

// RetryingClientは、retry可能と分類されたOpenSearch送信をlinear backoffで再試行します。
// context cancellationとcircuit breaker openは再試行しません。
type RetryingClient struct {
	next   opensearch.Client
	cfg    retryBackoffConfigProvider
	tracer pkgobserver.Tracer
	meter  pkgobserver.Meter
}

func newRetryingClient(
	next opensearch.Client,
	cfg retryBackoffConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *RetryingClient {
	return &RetryingClient{
		next:   next,
		cfg:    cfg,
		tracer: observer.EnsureTracer(tracer),
		meter:  observer.EnsureMeter(meter),
	}
}

func (c *RetryingClient) IndexDocument(
	ctx context.Context,
	doc opensearch.DocumentWriteRequest,
) (*opensearch.IndexResponse, error) {
	var out *opensearch.IndexResponse
	err := c.doWithRetry(
		ctx,
		spanNameIndexDocument,
		func(opCtx context.Context) error {
			resp, err := c.next.IndexDocument(opCtx, doc)
			if err != nil {
				return err
			}
			out = resp
			return nil
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}
func (c *RetryingClient) IndexDocuments(
	ctx context.Context,
	docs []opensearch.DocumentWriteRequest,
) (*opensearch.BulkResponse, error) {

	var out *opensearch.BulkResponse
	err := c.doWithRetry(
		ctx,
		spanNameBulk,
		func(opCtx context.Context) error {
			resp, err := c.next.IndexDocuments(opCtx, docs)
			if err != nil {
				return err
			}
			out = resp
			return nil
		})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *RetryingClient) doWithRetry(ctx context.Context, name string, op func(context.Context) error) error {
	var lastErr error
	maxAttempts := c.maxAttempts()

	attempts := 0
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attempts = attempt
		// 2回目以降はバックオフ
		if attempt > 1 {
			d := c.backoff(attempt - 1) // retry回数(1,2,3...)
			if !sleepWithContext(ctx, d) {
				// ctxが終わった（キャンセル/期限切れ）
				// ctx由来のerrorを優先して返す
				if err := ctx.Err(); err != nil {
					return retryable.InterruptedWait(lastErr, err)
				}
				return lastErr
			}
		}

		attemptCtx, span := telemetry.StartAttempt(ctx, c.tracer, name, attempt)
		lastErr = op(attemptCtx)
		if lastErr == nil {
			telemetry.EndSpan(span, lastErr)
			return nil
		}
		dec := retryclassifier.ClassifyRetry(attemptCtx, lastErr)
		// retry不可なら即返す
		if !dec.Retry || dec.Reason == retryable.RetryReasonCircuitBreakerOpen {
			telemetry.EndSpan(span, lastErr)
			return lastErr
		}

		// 最終attemptなら諦める
		if attempt == maxAttempts {
			telemetry.EndSpan(span, lastErr)
			break
		}

		// 次のattemptへ
		// ここで「retry理由」をトレースに載せる
		// 例：span := observer.SpanFromContext(ctx) みたいなのがあるならそこで attrs
		span.AddEvent("heterolog.opensearch.retry_decision",
			pkgobserver.Attr{Key: "reason", Value: string(dec.Reason)},
			pkgobserver.Attr{Key: "attempt", Value: attempt},
			pkgobserver.Attr{Key: "retry", Value: dec.Retry},
			pkgobserver.Attr{Key: "http_status", Value: dec.Code},
			pkgobserver.Attr{Key: "error", Value: lastErr.Error()},
		)
		telemetry.RecordRetry(ctx, c.meter, backendName, string(dec.Reason))
		telemetry.EndSpan(span, lastErr)
	}

	return &retryable.RetryAttemptsExhaustedError{Policy: false, Attempts: attempts, LastErr: lastErr}
}

func (c *RetryingClient) backoff(retryN int) time.Duration {
	// retryN=1 => base
	// retryN=2 => base*2
	// retryN=3 => base*3 ...
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
