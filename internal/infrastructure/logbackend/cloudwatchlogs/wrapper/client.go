package wrapper

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

const spanNamePutLogEvents = "internal.infrastructure.cloudwatchlogs.wrapper.put_log_events"

type retryBackoffConfigProvider interface {
	MaxAttempts() int
	BaseDelay() time.Duration
	MaxDelay() time.Duration
}

// RetryingClientは、確定済みのCloudWatch Logs送信requestを変更せずに再試行します。
type retryingClient struct {
	next   cloudwatchlogs.Client
	cfg    retryBackoffConfigProvider
	tracer pkgobserver.Tracer
}

var _ cloudwatchlogs.Client = (*retryingClient)(nil)

func newRetryingClient(
	next cloudwatchlogs.Client,
	cfg retryBackoffConfigProvider,
	tracer pkgobserver.Tracer,
) *retryingClient {
	return &retryingClient{
		next:   next,
		cfg:    cfg,
		tracer: observer.EnsureTracer(tracer),
	}
}

// Sendは、routing済みrequestを変更せず、transport errorに対するretryを適用します。
func (c *retryingClient) PutLogEvents(
	ctx context.Context,
	request cloudwatchlogs.PutLogEventsRequest,
) (*cloudwatchlogs.PutLogEventsOutput, error) {
	var output *cloudwatchlogs.PutLogEventsOutput
	err := c.doWithRetry(ctx, spanNamePutLogEvents, func(opCtx context.Context) error {
		var err error
		output, err = c.next.PutLogEvents(opCtx, request)
		return err
	})
	return output, err
}

func (c *retryingClient) doWithRetry(ctx context.Context, name string, op func(context.Context) error) error {
	var lastErr error
	var evidence attemptEvidence
	maxAttempts := c.maxAttempts()

	attempts := 0
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attempts = attempt
		attemptCtx, span := c.tracer.StartSpan(ctx, name)

		if attempt > 1 {
			d := c.backoff(attempt - 1)
			if !sleepWithContext(ctx, d) {
				if err := ctx.Err(); err != nil {
					span.End()
					return retryable.InterruptedWait(lastErr, err)
				}
				span.End()
				return lastErr
			}
		}

		lastErr = evidence.retain(op(attemptCtx))
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

		span.AddEvent("cloudwatchlogs.retry_decision",
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

func (c *retryingClient) backoff(retryN int) time.Duration {
	d := c.baseDelay() * time.Duration(retryN)
	if maxDelay := c.maxDelay(); d > maxDelay {
		d = maxDelay
	}
	return d
}

func (c *retryingClient) maxAttempts() int {
	return c.cfg.MaxAttempts()
}

func (c *retryingClient) baseDelay() time.Duration {
	return c.cfg.BaseDelay()
}

func (c *retryingClient) maxDelay() time.Duration {
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
