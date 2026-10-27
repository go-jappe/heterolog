package wrapper

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticClientChainConfig struct {
	staticRetryConfig
	selection pkglogger.RetryClientSelection
}

func (c staticClientChainConfig) RetryClientSelection() pkglogger.RetryClientSelection {
	return c.selection
}

func TestBuildClientChainBuildsSelectedParentChildRelationship(t *testing.T) {
	tests := []struct {
		mode   pkglogger.RetryClientMode
		assert func(*testing.T, Client, Client)
	}{
		{
			mode: pkglogger.RetryClientModeNone,
			assert: func(t *testing.T, raw Client, wrapped Client) {
				assert.Same(t, raw, wrapped)
			},
		},
		{
			mode: pkglogger.RetryClientModeRetry,
			assert: func(t *testing.T, raw Client, wrapped Client) {
				parent := wrapped.(*retryingClient)
				assert.Same(t, raw, parent.next)
			},
		},
		{
			mode: pkglogger.RetryClientModeRetryPolicy,
			assert: func(t *testing.T, raw Client, wrapped Client) {
				parent := wrapped.(*retryPolicyClient)
				assert.Same(t, raw, parent.next)
			},
		},
		{
			mode: pkglogger.RetryClientModeCircuitBreaker,
			assert: func(t *testing.T, raw Client, wrapped Client) {
				parent := wrapped.(*circuitBreakerClient)
				assert.Same(t, raw, parent.next)
			},
		},
		{
			mode: pkglogger.RetryClientModeRetryWithBreaker,
			assert: func(t *testing.T, raw Client, wrapped Client) {
				parent := wrapped.(*retryingClient)
				child := parent.next.(*circuitBreakerClient)
				assert.Same(t, raw, child.next)
			},
		},
		{
			mode: pkglogger.RetryClientModeRetryPolicyWithBreaker,
			assert: func(t *testing.T, raw Client, wrapped Client) {
				parent := wrapped.(*retryPolicyClient)
				child := parent.next.(*circuitBreakerClient)
				assert.Same(t, raw, child.next)
			},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			raw := &recordingClickHouseClient{}
			cfg := staticClientChainConfig{
				staticRetryConfig: staticRetryConfig{maxAttempts: 2, baseDelay: time.Nanosecond, maxDelay: time.Nanosecond},
				selection:         retrySelectionForTest(tt.mode),
			}

			wrapped, err := BuildClientChain(raw, cfg, nil, nil)
			require.NoError(t, err)
			tt.assert(t, raw, wrapped)
		})
	}
}

func TestBuildClientChainRejectsInvalidSelectedRetryConfig(t *testing.T) {
	_, err := BuildClientChain(&recordingClickHouseClient{}, staticClientChainConfig{
		staticRetryConfig: staticRetryConfig{maxAttempts: -1, baseDelay: time.Nanosecond, maxDelay: time.Nanosecond},
		selection: pkglogger.RetryClientSelection{
			Mode:                 pkglogger.RetryClientModeRetryPolicy,
			UseRetryPolicyClient: true,
		},
	}, nil, nil)
	var invalid *retryable.InvalidRetryClientConfigError
	require.ErrorAs(t, err, &invalid)
	require.ErrorIs(t, err, retryable.ErrInvalidRetryClientConfig)
	assert.Equal(t, "clickhouse", invalid.Backend)
	var fieldErr *pkglogger.ConfigFieldError
	require.ErrorAs(t, invalid.Err, &fieldErr)
	// 初回を含む試行回数が1未満なら、再試行の回数が負として報告します。
	assert.ErrorIs(t, fieldErr, pkglogger.ErrNegativeSetting)
	assert.Equal(t, pkglogger.ConfigFieldRetryMaxRetries, fieldErr.Field)
}

func retrySelectionForTest(mode pkglogger.RetryClientMode) pkglogger.RetryClientSelection {
	switch mode {
	case pkglogger.RetryClientModeRetry:
		return pkglogger.RetryClientSelection{Mode: mode, UseRetryingClient: true}
	case pkglogger.RetryClientModeRetryPolicy:
		return pkglogger.RetryClientSelection{Mode: mode, UseRetryPolicyClient: true}
	case pkglogger.RetryClientModeCircuitBreaker:
		return pkglogger.RetryClientSelection{Mode: mode, UseCircuitBreakerClient: true}
	case pkglogger.RetryClientModeRetryWithBreaker:
		return pkglogger.RetryClientSelection{Mode: mode, UseRetryingClient: true, UseCircuitBreakerClient: true}
	case pkglogger.RetryClientModeRetryPolicyWithBreaker:
		return pkglogger.RetryClientSelection{Mode: mode, UseRetryPolicyClient: true, UseCircuitBreakerClient: true}
	default:
		return pkglogger.RetryClientSelection{Mode: pkglogger.RetryClientModeNone}
	}
}
