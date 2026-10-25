package wrapper

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/circuitbreaker"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// CircuitBreakerClientはraw clientを内包する子wrapperです。
// retry併用時はRetryingClientまたはRetryPolicyClientがこのclientを内包し、各試行をbreakerへ通します。
// 1つのclientインスタンスが1つのOperationGuardとCircuitBreakerを保持し、全送信で状態を共有します。
type circuitBreakerClient struct {
	// nextにはbreakerが直接保護するraw clientが入ります。
	next           cloudwatchlogs.Client
	operationGuard *circuitbreaker.OperationGuard
}

var _ cloudwatchlogs.Client = (*circuitBreakerClient)(nil)

func newCircuitBreakerClient(
	next cloudwatchlogs.Client,
	tracer observer.Tracer,
	meter observer.Meter,
) *circuitBreakerClient {
	return newCircuitBreakerClientWithCircuitBreaker(next, tracer, meter, retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{}))
}

func newCircuitBreakerClientWithCircuitBreaker(
	next cloudwatchlogs.Client,
	tracer observer.Tracer,
	meter observer.Meter,
	circuitBreaker *retryable.CircuitBreaker,
) *circuitBreakerClient {
	tracer = observer.EnsureTracer(tracer)
	meter = observer.EnsureMeter(meter)
	return &circuitBreakerClient{
		next: next,
		operationGuard: circuitbreaker.NewOperationGuard(circuitbreaker.OperationGuardOptions{
			CircuitBreaker: circuitBreaker,
			Tracer:         tracer,
			Meter:          meter,
			EventName:      "cloudwatchlogs.retry_decision",
			Classify:       retryclassifier.ClassifyRetry,
		}),
	}
}

// PutLogEventsは、routing済みrequestの一回のbackend attemptをcircuit breakerで保護します。
func (c *circuitBreakerClient) PutLogEvents(
	ctx context.Context,
	request cloudwatchlogs.PutLogEventsRequest,
) (*cloudwatchlogs.PutLogEventsOutput, error) {
	var out *cloudwatchlogs.PutLogEventsOutput
	err := c.operationGuard.Do(ctx, spanNamePutLogEvents, func(opCtx context.Context) error {
		resp, err := c.next.PutLogEvents(opCtx, request)
		if err != nil {
			return err
		}
		if resp == nil {
			// 応答がなければbackendが応答したことを確認できません。成功として扱うと
			// 復旧確認でbreakerを閉じてしまうため、結果不明のerrorとしてprobe枠だけを解放します。
			return &cloudwatchlogs.ResponseDecodeError{Operation: "PutLogEvents", Err: retryclassifier.ErrMissingPutLogEventsOutput}
		}
		out = resp
		return nil
	})
	return out, err
}
