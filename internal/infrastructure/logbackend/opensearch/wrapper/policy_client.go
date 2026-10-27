package wrapper

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/retryclassifier"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

// RetryPolicyClientはpolicy付きretryを担当し、circuit breaker状態は保持しません。
// retry_policy_with_breaker modeではCircuitBreakerClientを子として内包します。
// OpenSearchへの書き込みは固定_idを持つ同じrequestを再送し、成否不明時の重複作成を防ぎます。
type RetryPolicyClient struct {
	next   opensearch.Client
	cfg    retryBackoffConfigProvider
	tracer pkgobserver.Tracer
}

func newRetryPolicyClient(
	next opensearch.Client,
	cfg retryBackoffConfigProvider,
	tracer pkgobserver.Tracer,
) *RetryPolicyClient {
	return &RetryPolicyClient{
		next:   next,
		cfg:    cfg,
		tracer: observer.EnsureTracer(tracer),
	}
}

func (c *RetryPolicyClient) IndexDocument(
	ctx context.Context,
	doc opensearch.DocumentWriteRequest,
) (*opensearch.IndexResponse, error) {
	var out *opensearch.IndexResponse
	err := c.doWithPolicyRetry(ctx, spanNameIndexDocument, func(opCtx context.Context) error {
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

func (c *RetryPolicyClient) IndexDocuments(
	ctx context.Context,
	docs []opensearch.DocumentWriteRequest,
) (*opensearch.BulkResponse, error) {
	var out *opensearch.BulkResponse
	err := c.doWithPolicyRetry(ctx, spanNameFlush, func(opCtx context.Context) error {
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

func (c *RetryPolicyClient) doWithPolicyRetry(ctx context.Context, name string, op func(context.Context) error) error {
	var lastErr error
	maxAttempts := c.maxAttempts()
	var nextAttemptTimeout time.Duration

	attempts := 0
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attempts = attempt
		attemptCtx := ctx
		cancel := func() {}
		if nextAttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, nextAttemptTimeout)
		}

		spanCtx, span := c.tracer.StartSpan(attemptCtx, name)
		lastErr = op(spanCtx)
		if lastErr == nil {
			cancel()
			span.End()
			return nil
		}

		decision := retryclassifier.ClassifyRetry(ctx, lastErr)
		cancel()
		if !decision.Retry || decision.Reason == retryable.RetryReasonCircuitBreakerOpen {
			span.End()
			return lastErr
		}
		if ctx != nil && ctx.Err() != nil {
			span.End()
			return lastErr
		}
		if !retryable.PolicyShouldRetry(ctx, attempt, decision, maxAttempts) {
			span.End()
			break
		}

		delay, ok := retryable.PolicyDelay(ctx, attempt, c.baseDelay(), c.maxDelay(), decision)
		span.AddEvent("opensearch.retry_decision",
			pkgobserver.Attr{Key: "reason", Value: string(decision.Reason)},
			pkgobserver.Attr{Key: "retry_class", Value: string(retryable.PolicyClass(decision))},
			pkgobserver.Attr{Key: "attempt", Value: attempt},
			pkgobserver.Attr{Key: "retry", Value: ok},
			pkgobserver.Attr{Key: "http_status", Value: decision.Code},
			pkgobserver.Attr{Key: "error", Value: lastErr.Error()},
			pkgobserver.Attr{Key: "retry_policy_client", Value: true},
		)
		span.End()
		if !ok {
			return lastErr
		}
		if !sleepWithContext(ctx, delay) {
			if err := ctx.Err(); err != nil {
				return retryable.InterruptedWait(lastErr, err)
			}
			return lastErr
		}
		nextAttemptTimeout = retryable.PolicyNextAttemptTimeout(decision)
	}

	return &retryable.RetryAttemptsExhaustedError{Policy: true, Attempts: attempts, LastErr: lastErr}
}

func (c *RetryPolicyClient) maxAttempts() int {
	return c.cfg.MaxAttempts()
}

func (c *RetryPolicyClient) baseDelay() time.Duration {
	return c.cfg.BaseDelay()
}

func (c *RetryPolicyClient) maxDelay() time.Duration {
	return c.cfg.MaxDelay()
}
