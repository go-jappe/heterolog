package wrapper

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// RetryPolicyClientはpolicy付きretryを担当し、circuit breaker状態は保持しません。
// retry_policy_with_breaker modeではCircuitBreakerClientを子として内包します。
// CloudWatch Logsは書き込みの冪等性を保証しないため、重複より欠損回避を優先するat-least-onceとして再送します。
type retryPolicyClient struct {
	next   cloudwatchlogs.Client
	cfg    retryBackoffConfigProvider
	tracer pkgobserver.Tracer
	meter  pkgobserver.Meter
}

var _ cloudwatchlogs.Client = (*retryPolicyClient)(nil)

func newRetryPolicyClient(
	next cloudwatchlogs.Client,
	cfg retryBackoffConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *retryPolicyClient {
	return &retryPolicyClient{
		next:   next,
		cfg:    cfg,
		tracer: observer.EnsureTracer(tracer),
		meter:  observer.EnsureMeter(meter),
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

		spanCtx, span := telemetry.StartAttempt(attemptCtx, c.tracer, name, attempt)
		lastErr = evidence.retain(op(spanCtx))
		if lastErr == nil {
			cancel()
			telemetry.EndSpan(span, lastErr)
			return nil
		}

		decision := retryclassifier.ClassifyRetry(ctx, lastErr)
		cancel()
		if !decision.Retry || decision.Reason == retryable.RetryReasonCircuitBreakerOpen {
			telemetry.EndSpan(span, lastErr)
			return lastErr
		}
		if ctx != nil && ctx.Err() != nil {
			telemetry.EndSpan(span, lastErr)
			return lastErr
		}
		if !retryable.PolicyShouldRetry(ctx, attempt, decision, maxAttempts) {
			telemetry.EndSpan(span, lastErr)
			break
		}

		delay, ok := retryable.PolicyDelay(ctx, attempt, c.baseDelay(), c.maxDelay(), decision)
		span.AddEvent("heterolog.cloudwatchlogs.retry_decision",
			pkgobserver.Attr{Key: "reason", Value: string(decision.Reason)},
			pkgobserver.Attr{Key: "retry_class", Value: string(retryable.PolicyClass(decision))},
			pkgobserver.Attr{Key: "attempt", Value: attempt},
			pkgobserver.Attr{Key: "retry", Value: ok},
			pkgobserver.Attr{Key: "http_status", Value: decision.Code},
			pkgobserver.Attr{Key: "error", Value: lastErr.Error()},
			pkgobserver.Attr{Key: "retry_policy", Value: true},
		)
		if ok {
			telemetry.RecordRetry(ctx, c.meter, backendName, string(decision.Reason))
		}
		telemetry.EndSpan(span, lastErr)
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
