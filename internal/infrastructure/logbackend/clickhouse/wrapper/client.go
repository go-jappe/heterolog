package wrapper

import (
	"context"
	"net/http"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
)

const spanNameInsertLogs = "internal.infrastructure.clickhouse.wrapper.insert_logs"

type Client interface {
	InsertLogsWithOptions(ctx context.Context, records []clickhouse.LogRecord, opts clickhouse.InsertOptions) (*clickhouse.InsertResponse, error)
}

type retryBackoffConfigProvider interface {
	MaxAttempts() int
	BaseDelay() time.Duration
	MaxDelay() time.Duration
}

type retryingClient struct {
	next   Client
	cfg    retryBackoffConfigProvider
	tracer observer.Tracer
}

var _ Client = (*retryingClient)(nil)

func newRetryingClient(
	next Client,
	cfg retryBackoffConfigProvider,
	tracer observer.Tracer,
) *retryingClient {
	return &retryingClient{
		next:   next,
		cfg:    cfg,
		tracer: observer.EnsureTracer(tracer),
	}
}

func (c *retryingClient) InsertLogsWithOptions(
	ctx context.Context,
	records []clickhouse.LogRecord,
	opts clickhouse.InsertOptions,
) (*clickhouse.InsertResponse, error) {
	var output *clickhouse.InsertResponse
	err := c.doWithRetry(ctx, spanNameInsertLogs, func(opCtx context.Context) error {
		var err error
		output, err = c.next.InsertLogsWithOptions(opCtx, records, opts)
		return err
	})
	return output, err
}

func (c *retryingClient) doWithRetry(ctx context.Context, name string, op func(context.Context) error) error {
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

		span.AddEvent("clickhouse.retry_decision",
			observer.Attr{Key: "reason", Value: string(dec.Reason)},
			observer.Attr{Key: "attempt", Value: attempt},
			observer.Attr{Key: "retry", Value: dec.Retry},
			observer.Attr{Key: "http_status", Value: dec.Code},
			observer.Attr{Key: "error", Value: lastErr.Error()},
		)
		span.End()
	}

	return &retryable.RetryAttemptsExhaustedError{Policy: false, Attempts: attempts, LastErr: lastErr}
}

// retryDelayは通常のlinear backoffを使い、429と503でRetry-Afterが得られた場合だけ
// maxDelayを上限としてserverの待機時間を優先します。
func (c *retryingClient) retryDelay(retryN int, decision retryable.RetryDecision) time.Duration {
	usesRetryAfter := decision.Reason == retryable.RetryReasonHTTPThrottle ||
		(decision.Reason == retryable.RetryReasonHTTPStatus && decision.Code == http.StatusServiceUnavailable)
	if usesRetryAfter && decision.RetryAfter > 0 {
		delay := decision.RetryAfter
		if maxDelay := c.maxDelay(); delay > maxDelay {
			delay = maxDelay
		}
		return delay
	}
	return c.backoff(retryN)
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
