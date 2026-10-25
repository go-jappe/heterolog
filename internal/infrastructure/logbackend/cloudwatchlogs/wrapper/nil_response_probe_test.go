package wrapper

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/retryclassifier"
	"github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/stretchr/testify/require"
)

// 応答もerrorも返さない復旧確認は、backendが応答した証拠にならない。結果不明のerrorとして返し、
// breakerを閉じずにprobe枠だけを解放する。応答を受け取ったprobeだけがbreakerを閉じる。
func TestCircuitBreakerClientKeepsBreakerOpenWhenProbeReturnsNoOutput(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	circuitBreaker := retryable.NewCircuitBreaker(retryable.CircuitBreakerOptions{
		MinFailureDurationToOpen:         time.Second,
		MaxFailureIntervalForConsecutive: 2 * time.Second,
		MinConsecutiveFailuresToOpen:     2,
		OpenCooldown:                     time.Minute,
		Clock:                            func() time.Time { return now },
	})
	var output *cloudwatchlogs.PutLogEventsOutput
	var outputErr error = &net.DNSError{IsTimeout: true}
	client := &recordingClient{}
	client.send = func(context.Context, cloudwatchlogs.PutLogEventsRequest) (*cloudwatchlogs.PutLogEventsOutput, error) {
		return output, outputErr
	}
	breaker := newCircuitBreakerClientWithCircuitBreaker(client, observer.NopTracer{}, observer.NopMeter{}, circuitBreaker)
	request := testPutLogEventsRequest([]cloudwatchlogs.Event{{Timestamp: now, Message: "probe"}})
	for range 2 {
		_, err := breaker.PutLogEvents(context.Background(), request)
		require.Error(t, err)
		now = now.Add(time.Second)
	}
	_, err := breaker.PutLogEvents(context.Background(), request)
	var openErr *retryable.CircuitBreakerOpenError
	require.ErrorAs(t, err, &openErr)

	now = now.Add(time.Minute)
	outputErr = nil
	got, err := breaker.PutLogEvents(context.Background(), request)
	var decodeErr *cloudwatchlogs.ResponseDecodeError
	require.ErrorAs(t, err, &decodeErr)
	require.ErrorIs(t, err, retryclassifier.ErrMissingPutLogEventsOutput)
	require.Nil(t, got)
	require.Len(t, client.calls, 3)
	// 応答のないprobeはbreakerを閉じません。次のrequestも復旧確認のprobeとして許可されます。
	probe := circuitBreaker.BeforeRequest()
	require.True(t, probe.AllowRequest)
	require.True(t, probe.Probe, "a probe without a response must not close the breaker")
	circuitBreaker.ReleaseProbe(probe)

	output = &cloudwatchlogs.PutLogEventsOutput{}
	got, err = breaker.PutLogEvents(context.Background(), request)
	require.NoError(t, err, "the released probe slot admits the next probe")
	require.Same(t, output, got)
	decision := circuitBreaker.BeforeRequest()
	require.True(t, decision.AllowRequest)
	require.False(t, decision.Probe, "only a probe with a response closes the breaker")
}
