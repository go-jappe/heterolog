package wrapper

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/circuitbreaker"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/loki"
)

// CircuitBreakerClientはraw clientを内包する子wrapperです。
// retry併用時はRetryingClientまたはRetryPolicyClientがこのclientを内包し、各試行をbreakerへ通します。
// 1つのclientインスタンスが1つのOperationGuardとCircuitBreakerを保持し、全操作で状態を共有します。
type CircuitBreakerClient struct {
	// nextにはbreakerが直接保護するraw clientが入ります。
	next           Client
	operationGuard *circuitbreaker.OperationGuard
}

func newCircuitBreakerClient(
	next Client,
	tracer observer.Tracer,
	meter observer.Meter,
) *CircuitBreakerClient {
	return newCircuitBreakerClientWithCircuitBreaker(next, tracer, meter, retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{}))
}

func newCircuitBreakerClientWithCircuitBreaker(
	next Client,
	tracer observer.Tracer,
	meter observer.Meter,
	circuitBreaker *retryable.CircuitBreaker,
) *CircuitBreakerClient {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)
	return &CircuitBreakerClient{
		next: next,
		operationGuard: circuitbreaker.NewOperationGuard(circuitbreaker.OperationGuardOptions{
			CircuitBreaker: circuitBreaker,
			Tracer:         tracer,
			Meter:          meter,
			EventName:      "loki.retry_decision",
			Classify:       retryclassifier.ClassifyRetry,
		}),
	}
}

func (c *CircuitBreakerClient) Push(ctx context.Context, streams []loki.Stream) error {
	return c.operationGuard.Do(ctx, SpanNamePush, func(opCtx context.Context) error {
		return c.next.Push(opCtx, streams)
	})
}
