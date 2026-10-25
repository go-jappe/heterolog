package wrapper

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/stretchr/testify/require"
)

// 応答もerrorも返さない復旧確認は、backendが応答した証拠にならない。単件・Bulkのどちらでも
// 結果不明のerrorとして返し、breakerを閉じずにprobe枠だけを解放する。
func TestCircuitBreakerClientKeepsBreakerOpenWhenProbeReturnsNoResponse(t *testing.T) {
	t.Parallel()

	doc := opensearch.DocumentWriteRequest{Index: "logs", ID: "doc-1", Op: opensearch.WriteOpCreate, Doc: map[string]any{"message": "probe"}}
	for _, tt := range []struct {
		name      string
		operation string
		send      func(*CircuitBreakerClient) (any, error)
		respond   func(*recordingOpenSearchClient, func() error)
	}{
		{
			name:      "index",
			operation: "index",
			send: func(c *CircuitBreakerClient) (any, error) {
				return c.IndexDocument(context.Background(), doc)
			},
			respond: func(c *recordingOpenSearchClient, failure func() error) {
				c.indexDocument = func(context.Context, opensearch.DocumentWriteRequest) (*opensearch.IndexResponse, error) {
					return nil, failure()
				}
			},
		},
		{
			name:      "bulk",
			operation: "bulk",
			send: func(c *CircuitBreakerClient) (any, error) {
				return c.IndexDocuments(context.Background(), []opensearch.DocumentWriteRequest{doc})
			},
			respond: func(c *recordingOpenSearchClient, failure func() error) {
				c.indexDocuments = func(context.Context, []opensearch.DocumentWriteRequest) (*opensearch.BulkResponse, error) {
					return nil, failure()
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Unix(1_700_000_000, 0)
			circuitBreaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
				MinFailureDurationToOpen:         time.Second,
				MaxFailureIntervalForConsecutive: 2 * time.Second,
				MinConsecutiveFailuresToOpen:     2,
				OpenCooldown:                     time.Minute,
				Clock:                            func() time.Time { return now },
			})
			var failure error = &net.DNSError{IsTimeout: true}
			client := &recordingOpenSearchClient{}
			tt.respond(client, func() error { return failure })
			breaker := newCircuitBreakerClientWithCircuitBreaker(client, observer.NopTracer{}, observer.NopMeter{}, circuitBreaker)
			for range 2 {
				_, err := tt.send(breaker)
				require.Error(t, err)
				now = now.Add(time.Second)
			}
			_, err := tt.send(breaker)
			var openErr *retryable.CircuitBreakerOpenError
			require.ErrorAs(t, err, &openErr)

			now = now.Add(time.Minute)
			failure = nil
			_, err = tt.send(breaker)
			var decodeErr *opensearch.ResponseDecodeError
			require.ErrorAs(t, err, &decodeErr)
			require.Equal(t, tt.operation, decodeErr.Operation)
			require.ErrorIs(t, err, retryclassifier.ErrMissingResponse)
			decision := circuitBreaker.BeforeRequest()
			require.True(t, decision.AllowRequest)
			require.True(t, decision.Probe, "a probe without a response must not close the breaker")
		})
	}
}
