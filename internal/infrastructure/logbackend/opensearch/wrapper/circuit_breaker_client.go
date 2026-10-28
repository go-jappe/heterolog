package wrapper

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/circuitbreaker"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/retryclassifier"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

// CircuitBreakerClientはraw clientを内包する子wrapperです。
// retry併用時はRetryingClientまたはRetryPolicyClientがこのclientを内包し、各試行をbreakerへ通します。
// 1つのclientインスタンスが1つのOperationGuardとCircuitBreakerを保持し、全操作で状態を共有します。
type CircuitBreakerClient struct {
	// nextにはbreakerが直接保護するraw clientが入ります。
	next           opensearch.Client
	operationGuard *circuitbreaker.OperationGuard
}

func newCircuitBreakerClient(
	next opensearch.Client,
	meter pkgobserver.Meter,
) *CircuitBreakerClient {
	return newCircuitBreakerClientWithCircuitBreaker(next, meter, retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{}))
}

func newCircuitBreakerClientWithCircuitBreaker(
	next opensearch.Client,
	meter pkgobserver.Meter,
	circuitBreaker *retryable.CircuitBreaker,
) *CircuitBreakerClient {
	meter = observer.EnsureMeter(meter)
	return &CircuitBreakerClient{
		next: next,
		operationGuard: circuitbreaker.NewOperationGuard(circuitbreaker.OperationGuardOptions{
			CircuitBreaker: circuitBreaker,
			Meter:          meter,
			Backend:        backendName,
			EventName:      "heterolog.opensearch.retry_decision",
			Classify:       retryclassifier.ClassifyRetry,
		}),
	}
}

func (c *CircuitBreakerClient) IndexDocument(
	ctx context.Context,
	doc opensearch.DocumentWriteRequest,
) (*opensearch.IndexResponse, error) {
	var out *opensearch.IndexResponse
	err := c.operationGuard.Do(ctx, spanNameIndexDocument, func(opCtx context.Context) error {
		resp, err := c.next.IndexDocument(opCtx, doc)
		if err != nil {
			return err
		}
		if resp == nil {
			// 応答がなければbackendが応答したことを確認できません。成功として扱うと
			// 復旧確認でbreakerを閉じてしまうため、結果不明のerrorとしてprobe枠だけを解放します。
			return &opensearch.ResponseDecodeError{Operation: "index", Err: retryclassifier.ErrMissingResponse}
		}
		out = resp
		return nil
	})
	return out, err
}

func (c *CircuitBreakerClient) IndexDocuments(
	ctx context.Context,
	docs []opensearch.DocumentWriteRequest,
) (*opensearch.BulkResponse, error) {
	var out *opensearch.BulkResponse
	err := c.operationGuard.Do(ctx, spanNameBulk, func(opCtx context.Context) error {
		resp, err := c.next.IndexDocuments(opCtx, docs)
		if err != nil {
			return err
		}
		if resp == nil {
			// 応答がなければbackendが応答したことを確認できません。成功として扱うと
			// 復旧確認でbreakerを閉じてしまうため、結果不明のerrorとしてprobe枠だけを解放します。
			return &opensearch.ResponseDecodeError{Operation: "bulk", Err: retryclassifier.ErrMissingResponse}
		}
		out = resp
		return nil
	})
	return out, err
}
