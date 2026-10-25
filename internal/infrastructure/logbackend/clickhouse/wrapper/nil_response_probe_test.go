package wrapper

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/stretchr/testify/require"
)

// 応答もerrorも返さない復旧確認は、backendが応答した証拠にならない。結果不明のerrorとして返し、
// breakerを閉じずにprobe枠だけを解放する。応答を受け取ったprobeだけがbreakerを閉じる。
func TestCircuitBreakerClientKeepsBreakerOpenWhenProbeReturnsNoResponse(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock:                            func() time.Time { return now },
	})
	var response *clickhouse.InsertResponse
	var responseErr error = &net.DNSError{IsTimeout: true}
	client := &recordingClickHouseClient{}
	client.insertLogsWithOptions = func(context.Context, []clickhouse.LogRecord, clickhouse.InsertOptions) (*clickhouse.InsertResponse, error) {
		return response, responseErr
	}
	breaker := newCircuitBreakerClientWithCircuitBreaker(client, observer.NopTracer{}, observer.NopMeter{}, circuitBreaker)
	insert := func() (*clickhouse.InsertResponse, error) {
		return breaker.InsertLogsWithOptions(context.Background(), []clickhouse.LogRecord{{Message: "probe"}}, clickhouse.InsertOptions{})
	}
	for range 2 {
		_, err := insert()
		require.Error(t, err)
		now = now.Add(time.Second)
	}
	_, err := insert()
	var openErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &openErr)

	now = now.Add(time.Minute)
	responseErr = nil
	got, err := insert()
	require.ErrorIs(t, err, retryclassifier.ErrMissingInsertResponse)
	require.Nil(t, got)
	require.Len(t, client.calls, 3)
	// 応答のないprobeはbreakerを閉じません。次のrequestも復旧確認のprobeとして許可されます。
	probe := circuitBreaker.BeforeRequest()
	require.True(t, probe.AllowRequest)
	require.True(t, probe.Probe, "a probe without a response must not close the breaker")
	circuitBreaker.ReleaseProbe(probe)

	response = &clickhouse.InsertResponse{StatusCode: http.StatusOK}
	got, err = insert()
	require.NoError(t, err, "the released probe slot admits the next probe")
	require.Same(t, response, got)
	decision := circuitBreaker.BeforeRequest()
	require.True(t, decision.AllowRequest)
	require.False(t, decision.Probe, "only a probe with a response closes the breaker")
}
