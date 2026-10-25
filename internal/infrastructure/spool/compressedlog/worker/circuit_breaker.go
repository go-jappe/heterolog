package worker

import (
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// destinationCircuitBreakerRegistryは、object storageの物理宛先ごとに一つの
// circuit breakerを保持します。RootRuntimeが所有するWorkerに保持し、送信経路を
// 追加・削除しても、同じ宛先の連続失敗回数や送信停止状態を維持します。
type destinationCircuitBreakerRegistry struct {
	mu            sync.Mutex
	byDestination map[objectstorage.DestinationID]*retryable.CircuitBreaker
	options       retryable.CircuitBreakerOptions
}

func newDestinationCircuitBreakerRegistry(
	openCooldown time.Duration,
	clock func() time.Time,
) (*destinationCircuitBreakerRegistry, error) {
	if openCooldown <= 0 {
		return nil, ErrInvalidCircuitBreakerOpenCooldown
	}
	return &destinationCircuitBreakerRegistry{
		byDestination: make(map[objectstorage.DestinationID]*retryable.CircuitBreaker),
		options: retryable.CircuitBreakerOptions{
			// 連続失敗の判定に使う継続時間・間隔・回数は未指定とし、
			// backend送信用のcircuit breakerと同じ既定値を使います。
			OpenCooldown: openCooldown,
			Clock:        clock,
		},
	}, nil
}

// getOrCreateは、指定宛先のcircuit breakerを返します。未登録なら共通の設定で
// 作成・登録し、以後は同じ宛先に同じインスタンスを返します。
func (r *destinationCircuitBreakerRegistry) getOrCreate(
	destinationID objectstorage.DestinationID,
) *retryable.CircuitBreaker {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	breaker := r.byDestination[destinationID]
	if breaker == nil {
		breaker = retryable.NewCircuitBreaker(r.options)
		r.byDestination[destinationID] = breaker
	}
	return breaker
}
