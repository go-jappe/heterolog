package wrapper

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// RetryPolicyClientはpolicy付きretryを担当し、circuit breaker状態は保持しません。
// retry_policy_with_breaker modeではCircuitBreakerClientを子として内包します。
// CloudWatch Logsは書き込みの冪等性を保証しないため、重複より欠損回避を優先するat-least-onceとして再送します。
type retryPolicyClient struct {
	next   cloudwatchlogs.Client
	cfg    retryBackoffConfigProvider
	tracer observer.Tracer
}

var _ cloudwatchlogs.Client = (*retryPolicyClient)(nil)

func newRetryPolicyClient(
	next cloudwatchlogs.Client,
	cfg retryBackoffConfigProvider,
	tracer observer.Tracer,
) *retryPolicyClient {
	return &retryPolicyClient{
		next:   next,
		cfg:    cfg,
		tracer: observer.EnsureTracer(tracer),
	}
}

// Sendは、routing済みrequestを変更せず、policyに基づくretryを適用します。
func (c *retryPolicyClient) PutLogEvents(
	ctx context.Context,
	request cloudwatchlogs.PutLogEventsRequest,
) (*cloudwatchlogs.PutLogEventsOutput, error) {
	var output *cloudwatchlogs.PutLogEventsOutput
	err := c.doWithPolicyRetry(ctx, spanNamePutLogEvents, func(opCtx context.Context) error {
		var err error
		output, err = c.next.PutLogEvents(opCtx, request)
		return err
	})
	return output, err
}

func (c *retryPolicyClient) doWithPolicyRetry(ctx context.Context, name string, op func(context.Context) error) error {
	var lastErr error
	var evidence attemptEvidence
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
		lastErr = evidence.retain(op(spanCtx))
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
		span.AddEvent("cloudwatchlogs.retry_decision",
			observer.Attr{Key: "reason", Value: string(decision.Reason)},
			observer.Attr{Key: "retry_class", Value: string(retryable.PolicyClass(decision))},
			observer.Attr{Key: "attempt", Value: attempt},
			observer.Attr{Key: "retry", Value: ok},
			observer.Attr{Key: "http_status", Value: decision.Code},
			observer.Attr{Key: "error", Value: lastErr.Error()},
			observer.Attr{Key: "retry_policy", Value: true},
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
