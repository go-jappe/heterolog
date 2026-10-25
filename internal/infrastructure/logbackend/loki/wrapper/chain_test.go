package wrapper

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chainRetryConfig struct {
	staticRetryConfig
	selection pkglogger.RetryClientSelection
}

func (c chainRetryConfig) RetryClientSelection() pkglogger.RetryClientSelection {
	return c.selection
}

func TestBuildClientChain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		selection pkglogger.RetryClientSelection
		assert    func(*testing.T, Client, Client)
	}{
		{
			name:      "raw client",
			selection: pkglogger.RetryClientSelection{Mode: pkglogger.RetryClientModeNone},
			assert: func(t *testing.T, raw Client, got Client) {
				assert.Same(t, raw, got)
			},
		},
		{
			name: "retry",
			selection: pkglogger.RetryClientSelection{
				Mode:              pkglogger.RetryClientModeRetry,
				UseRetryingClient: true,
			},
			assert: func(t *testing.T, raw Client, got Client) {
				retrying, ok := got.(*RetryingClient)
				require.True(t, ok)
				assert.Same(t, raw, retrying.next)
			},
		},
		{
			name: "retry policy",
			selection: pkglogger.RetryClientSelection{
				Mode:                 pkglogger.RetryClientModeRetryPolicy,
				UseRetryPolicyClient: true,
			},
			assert: func(t *testing.T, raw Client, got Client) {
				policy, ok := got.(*RetryPolicyClient)
				require.True(t, ok)
				assert.Same(t, raw, policy.next)
			},
		},
		{
			name: "circuit breaker",
			selection: pkglogger.RetryClientSelection{
				Mode:                    pkglogger.RetryClientModeCircuitBreaker,
				UseCircuitBreakerClient: true,
			},
			assert: func(t *testing.T, raw Client, got Client) {
				breaker, ok := got.(*CircuitBreakerClient)
				require.True(t, ok)
				assert.Same(t, raw, breaker.next)
			},
		},
		{
			name: "retry outside circuit breaker",
			selection: pkglogger.RetryClientSelection{
				Mode:                    pkglogger.RetryClientModeRetryWithBreaker,
				UseRetryingClient:       true,
				UseCircuitBreakerClient: true,
			},
			assert: func(t *testing.T, raw Client, got Client) {
				retrying, ok := got.(*RetryingClient)
				require.True(t, ok)
				breaker, ok := retrying.next.(*CircuitBreakerClient)
				require.True(t, ok)
				assert.Same(t, raw, breaker.next)
			},
		},
		{
			name: "retry policy outside circuit breaker",
			selection: pkglogger.RetryClientSelection{
				Mode:                    pkglogger.RetryClientModeRetryPolicyWithBreaker,
				UseRetryPolicyClient:    true,
				UseCircuitBreakerClient: true,
			},
			assert: func(t *testing.T, raw Client, got Client) {
				policy, ok := got.(*RetryPolicyClient)
				require.True(t, ok)
				breaker, ok := policy.next.(*CircuitBreakerClient)
				require.True(t, ok)
				assert.Same(t, raw, breaker.next)
			},
		},
	}

	for _, testCase := range tests {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			raw := &recordingLokiClient{}
			cfg := chainRetryConfig{
				staticRetryConfig: staticRetryConfig{maxAttempts: 2, baseDelay: time.Nanosecond, maxDelay: time.Nanosecond},
				selection:         testCase.selection,
			}

			got, err := BuildClientChain(raw, cfg, nil, nil)

			require.NoError(t, err)
			testCase.assert(t, raw, got)
		})
	}
}

func TestBuildClientChainReturnsRawClientWithoutConfig(t *testing.T) {
	t.Parallel()

	raw := &recordingLokiClient{}

	got, err := BuildClientChain(raw, nil, nil, nil)
	require.NoError(t, err)
	assert.Same(t, raw, got)
}

func TestBuildClientChainRejectsInvalidSelectedRetryConfig(t *testing.T) {
	t.Parallel()

	_, err := BuildClientChain(&recordingLokiClient{}, chainRetryConfig{
		staticRetryConfig: staticRetryConfig{maxAttempts: 2, baseDelay: 0, maxDelay: time.Nanosecond},
		selection: pkglogger.RetryClientSelection{
			Mode:              pkglogger.RetryClientModeRetry,
			UseRetryingClient: true,
		},
	}, nil, nil)
	var invalid *retryable.InvalidRetryClientConfigError
	require.ErrorAs(t, err, &invalid)
	require.ErrorIs(t, err, retryable.ErrInvalidRetryClientConfig)
	assert.Equal(t, "loki", invalid.Backend)
	var fieldErr *pkglogger.ConfigFieldError
	require.ErrorAs(t, invalid.Err, &fieldErr)
	assert.ErrorIs(t, fieldErr, pkglogger.ErrNonPositiveSetting)
	assert.Equal(t, pkglogger.ConfigFieldRetryBaseDelay, fieldErr.Field)
}
