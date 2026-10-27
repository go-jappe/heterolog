package wrapper

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/google/uuid"
)

// RetryPolicyClientはpolicy付きretryを担当し、circuit breaker状態は保持しません。
// retry_policy_with_breaker modeではCircuitBreakerClientを子として内包します。
// ClickHouseへの書き込みは同じinsert_deduplication_tokenを再送し、重複INSERTを防ぎます。
type retryPolicyClient struct {
	next   Client
	cfg    retryBackoffConfigProvider
	tracer pkgobserver.Tracer
}

var _ Client = (*retryPolicyClient)(nil)

func newRetryPolicyClient(
	next Client,
	cfg retryBackoffConfigProvider,
	tracer pkgobserver.Tracer,
) *retryPolicyClient {
	return &retryPolicyClient{
		next:   next,
		cfg:    cfg,
		tracer: observer.EnsureTracer(tracer),
	}
}

func (c *retryPolicyClient) InsertLogsWithOptions(
	ctx context.Context,
	records []clickhouse.LogRecord,
	opts clickhouse.InsertOptions,
) (*clickhouse.InsertResponse, error) {
	if opts.InsertDeduplicationToken == "" {
		opts.InsertDeduplicationToken = uuid.NewString()
	}
	var output *clickhouse.InsertResponse
	err := c.doWithPolicyRetry(ctx, spanNameInsertLogs, func(opCtx context.Context) error {
		var err error
		output, err = c.next.InsertLogsWithOptions(opCtx, records, opts)
		return err
	})
	return output, err
}

func (c *retryPolicyClient) doWithPolicyRetry(ctx context.Context, name string, op func(context.Context) error) error {
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
		span.AddEvent("clickhouse.retry_decision",
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

func (c *retryPolicyClient) maxAttempts() int {
	return c.cfg.MaxAttempts()
}

func (c *retryPolicyClient) baseDelay() time.Duration {
	return c.cfg.BaseDelay()
}

func (c *retryPolicyClient) maxDelay() time.Duration {
	return c.cfg.MaxDelay()
}
