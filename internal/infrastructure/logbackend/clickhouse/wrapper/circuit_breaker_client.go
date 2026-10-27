package wrapper

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/circuitbreaker"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// CircuitBreakerClientはraw clientを内包する子wrapperです。
// retry併用時はRetryingClientまたはRetryPolicyClientがこのclientを内包し、各試行をbreakerへ通します。
// 1つのclientインスタンスが1つのOperationGuardとCircuitBreakerを保持し、全操作で状態を共有します。
type circuitBreakerClient struct {
	// nextにはbreakerが直接保護するraw clientが入ります。
	next           Client
	operationGuard *circuitbreaker.OperationGuard
}

var _ Client = (*circuitBreakerClient)(nil)

func newCircuitBreakerClient(
	next Client,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *circuitBreakerClient {
	return newCircuitBreakerClientWithCircuitBreaker(next, tracer, meter, retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{}))
}

func newCircuitBreakerClientWithCircuitBreaker(
	next Client,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
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
			EventName:      "clickhouse.retry_decision",
			Classify:       retryclassifier.ClassifyRetry,
		}),
	}
}

func (c *circuitBreakerClient) InsertLogsWithOptions(
	ctx context.Context,
	records []clickhouse.LogRecord,
	opts clickhouse.InsertOptions,
) (*clickhouse.InsertResponse, error) {
	var out *clickhouse.InsertResponse
	err := c.operationGuard.Do(ctx, spanNameInsertLogs, func(opCtx context.Context) error {
		resp, err := c.next.InsertLogsWithOptions(opCtx, records, opts)
		if err != nil {
			return err
		}
		if resp == nil {
			// 応答がなければbackendが応答したことを確認できません。成功として扱うと
			// 復旧確認でbreakerを閉じてしまうため、結果不明のerrorとしてprobe枠だけを解放します。
			return retryclassifier.ErrMissingInsertResponse
		}
		out = resp
		return nil
	})
	return out, err
}
