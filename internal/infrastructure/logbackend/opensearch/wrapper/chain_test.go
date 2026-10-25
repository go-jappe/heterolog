package wrapper

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticClientChainConfig struct {
	staticRetryBackoffConfig
	selection pkglogger.RetryClientSelection
}

func (c staticClientChainConfig) RetryClientSelection() pkglogger.RetryClientSelection {
	return c.selection
}

func TestBuildClientChainUsesConfiguredOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      pkglogger.RetryClientMode
		assertion func(*testing.T, *recordingOpenSearchClient, opensearch.Client)
	}{
		{
			name: "none",
			mode: pkglogger.RetryClientModeNone,
			assertion: func(t *testing.T, raw *recordingOpenSearchClient, client opensearch.Client) {
				assert.Same(t, raw, client)
			},
		},
		{
			name: "retry",
			mode: pkglogger.RetryClientModeRetry,
			assertion: func(t *testing.T, raw *recordingOpenSearchClient, client opensearch.Client) {
				retrying := client.(*RetryingClient)
				assert.Same(t, raw, retrying.next)
			},
		},
		{
			name: "retry policy",
			mode: pkglogger.RetryClientModeRetryPolicy,
			assertion: func(t *testing.T, raw *recordingOpenSearchClient, client opensearch.Client) {
				policy := client.(*RetryPolicyClient)
				assert.Same(t, raw, policy.next)
			},
		},
		{
			name: "circuit breaker",
			mode: pkglogger.RetryClientModeCircuitBreaker,
			assertion: func(t *testing.T, raw *recordingOpenSearchClient, client opensearch.Client) {
				breaker := client.(*CircuitBreakerClient)
				assert.Same(t, raw, breaker.next)
			},
		},
		{
			name: "retry with circuit breaker",
			mode: pkglogger.RetryClientModeRetryWithBreaker,
			assertion: func(t *testing.T, raw *recordingOpenSearchClient, client opensearch.Client) {
				retrying := client.(*RetryingClient)
				breaker := retrying.next.(*CircuitBreakerClient)
				assert.Same(t, raw, breaker.next)
			},
		},
		{
			name: "retry policy with circuit breaker",
			mode: pkglogger.RetryClientModeRetryPolicyWithBreaker,
			assertion: func(t *testing.T, raw *recordingOpenSearchClient, client opensearch.Client) {
				policy := client.(*RetryPolicyClient)
				breaker := policy.next.(*CircuitBreakerClient)
				assert.Same(t, raw, breaker.next)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			selection := pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: tt.mode,
			}.RetryClientSelection()
			config := staticClientChainConfig{
				staticRetryBackoffConfig: staticRetryBackoffConfig{
					maxAttempts: 2,
					baseDelay:   time.Nanosecond,
					maxDelay:    time.Nanosecond,
				},
				selection: selection,
			}

			raw := &recordingOpenSearchClient{}
			client, err := BuildClientChain(raw, config, nil, nil)

			require.NoError(t, err)
			require.NotNil(t, client)
			tt.assertion(t, raw, client)
		})
	}
}

func TestBuildClientChainReturnsRawClientWithoutRetryConfig(t *testing.T) {
	t.Parallel()

	raw := &recordingOpenSearchClient{}
	client, err := BuildClientChain(raw, nil, nil, nil)

	require.NoError(t, err)
	assert.Same(t, raw, client)
}

func TestBuildClientChainReturnsNilForNilClient(t *testing.T) {
	t.Parallel()

	client, err := BuildClientChain(nil, nil, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, client)
}

func TestBuildClientChainRejectsInvalidSelectedRetryConfig(t *testing.T) {
	t.Parallel()

	_, err := BuildClientChain(&recordingOpenSearchClient{}, staticClientChainConfig{
		staticRetryBackoffConfig: staticRetryBackoffConfig{maxAttempts: 0, baseDelay: time.Nanosecond, maxDelay: time.Nanosecond},
		selection:                pkglogger.RetryClientSelection{Mode: pkglogger.RetryClientModeRetry, UseRetryingClient: true},
	}, nil, nil)
	var invalid *retryable.InvalidRetryClientConfigError
	require.ErrorAs(t, err, &invalid)
	require.ErrorIs(t, err, retryable.ErrInvalidRetryClientConfig)
	assert.Equal(t, "opensearch", invalid.Backend)
	var fieldErr *pkglogger.ConfigFieldError
	require.ErrorAs(t, invalid.Err, &fieldErr)
	assert.ErrorIs(t, fieldErr, pkglogger.ErrNonPositiveSetting)
	assert.Equal(t, pkglogger.ConfigFieldRetryMaxAttempts, fieldErr.Field)
}
