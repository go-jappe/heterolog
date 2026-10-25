package wrapper

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticClientChainConfig struct {
	staticRetryConfig
	mode pkglogger.RetryClientMode
}

func (c staticClientChainConfig) RetryClientSelection() pkglogger.RetryClientSelection {
	return retrySelectionForTest(c.mode)
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

func TestBuildClientChainBuildsSelectedParentChildRelationship(t *testing.T) {
	tests := []struct {
		mode   pkglogger.RetryClientMode
		assert func(*testing.T, pkgcloudwatchlogs.Client, pkgcloudwatchlogs.Client)
	}{
		{
			mode: pkglogger.RetryClientModeNone,
			assert: func(t *testing.T, raw, wrapped pkgcloudwatchlogs.Client) {
				assert.Same(t, raw, wrapped)
			},
		},
		{
			mode: pkglogger.RetryClientModeRetry,
			assert: func(t *testing.T, raw, wrapped pkgcloudwatchlogs.Client) {
				assert.Same(t, raw, wrapped.(*retryingClient).next)
			},
		},
		{
			mode: pkglogger.RetryClientModeRetryPolicy,
			assert: func(t *testing.T, raw, wrapped pkgcloudwatchlogs.Client) {
				assert.Same(t, raw, wrapped.(*retryPolicyClient).next)
			},
		},
		{
			mode: pkglogger.RetryClientModeCircuitBreaker,
			assert: func(t *testing.T, raw, wrapped pkgcloudwatchlogs.Client) {
				assert.Same(t, raw, wrapped.(*circuitBreakerClient).next)
			},
		},
		{
			mode: pkglogger.RetryClientModeRetryWithBreaker,
			assert: func(t *testing.T, raw, wrapped pkgcloudwatchlogs.Client) {
				child := wrapped.(*retryingClient).next.(*circuitBreakerClient)
				assert.Same(t, raw, child.next)
			},
		},
		{
			mode: pkglogger.RetryClientModeRetryPolicyWithBreaker,
			assert: func(t *testing.T, raw, wrapped pkgcloudwatchlogs.Client) {
				child := wrapped.(*retryPolicyClient).next.(*circuitBreakerClient)
				assert.Same(t, raw, child.next)
			},
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			raw := &recordingClient{}
			cfg := staticClientChainConfig{
				staticRetryConfig: staticRetryConfig{maxAttempts: 2, baseDelay: time.Nanosecond, maxDelay: time.Nanosecond},
				mode:              tt.mode,
			}
			wrapped, err := BuildClientChain(raw, cfg, nil, nil)
			require.NoError(t, err)
			tt.assert(t, raw, wrapped)
		})
	}
}

func TestBuildClientChainRejectsInvalidSelectedRetryConfig(t *testing.T) {
	_, err := BuildClientChain(&recordingClient{}, staticClientChainConfig{
		staticRetryConfig: staticRetryConfig{maxAttempts: 2, baseDelay: time.Nanosecond, maxDelay: 0},
		mode:              pkglogger.RetryClientModeRetryPolicy,
	}, nil, nil)
	var invalid *retryable.InvalidRetryClientConfigError
	require.ErrorAs(t, err, &invalid)
	require.ErrorIs(t, err, retryable.ErrInvalidRetryClientConfig)
	assert.Equal(t, "cloudwatch logs", invalid.Backend)
	var fieldErr *pkglogger.ConfigFieldError
	require.ErrorAs(t, invalid.Err, &fieldErr)
	assert.ErrorIs(t, fieldErr, pkglogger.ErrNonPositiveSetting)
	assert.Equal(t, pkglogger.ConfigFieldRetryMaxDelay, fieldErr.Field)
}
