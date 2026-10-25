package loki

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/wrapper"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClientReturnsRawClientWhenRetryConfigIsNil(t *testing.T) {
	t.Parallel()

	client, err := NewClient(newTestConfig(), nil, nil, nil)

	require.NoError(t, err)
	assert.IsType(t, &pkgloki.Client{}, client)
}

func TestNewClientSelectsConfiguredWrapper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		mode       pkglogger.RetryClientMode
		wantClient wrapper.Client
	}{
		{
			name:       "retrying client",
			mode:       pkglogger.RetryClientModeRetry,
			wantClient: &wrapper.RetryingClient{},
		},
		{
			name:       "retrying client with circuit breaker child",
			mode:       pkglogger.RetryClientModeRetryWithBreaker,
			wantClient: &wrapper.RetryingClient{},
		},
		{
			name:       "retry policy client",
			mode:       pkglogger.RetryClientModeRetryPolicy,
			wantClient: &wrapper.RetryPolicyClient{},
		},
		{
			name:       "circuit breaker client",
			mode:       pkglogger.RetryClientModeCircuitBreaker,
			wantClient: &wrapper.CircuitBreakerClient{},
		},
		{
			name:       "raw client",
			mode:       pkglogger.RetryClientModeNone,
			wantClient: &pkgloki.Client{},
		},
	}

	for _, tt := range tests {
		testCase := tt
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			retryCfg := factoryRetryConfig{
				selection: pkglogger.LokiRetryableClientCaseTransportErrorConfig{
					RetryClientMode: testCase.mode,
				}.RetryClientSelection(),
				maxAttempts: 2,
				baseDelay:   time.Nanosecond,
				maxDelay:    time.Nanosecond,
			}

			client, err := NewClient(newTestConfig(), retryCfg, nil, nil)

			require.NoError(t, err)
			assert.IsType(t, testCase.wantClient, client)
		})
	}
}

type factoryRetryConfig struct {
	selection   pkglogger.RetryClientSelection
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
}

func (c factoryRetryConfig) RetryClientSelection() pkglogger.RetryClientSelection { return c.selection }
func (c factoryRetryConfig) MaxAttempts() int                                     { return c.maxAttempts }
func (c factoryRetryConfig) BaseDelay() time.Duration                             { return c.baseDelay }
func (c factoryRetryConfig) MaxDelay() time.Duration                              { return c.maxDelay }

func TestNewClientReturnsConfigError(t *testing.T) {
	t.Parallel()

	cfg := newTestConfig()
	cfg.BasicAuth = &pkgloki.BasicAuthConfig{Username: "user", Password: "password"}
	cfg.BearerToken = "token"

	client, err := NewClient(cfg, nil, nil, nil)

	require.ErrorIs(t, err, ErrCreateClient)
	require.ErrorIs(t, err, pkgloki.ErrConflictingAuth)
	assert.Nil(t, client)
}

func newTestConfig() pkgloki.Config {
	return pkgloki.Config{
		Endpoint: "http://localhost:3100",
		Timeout:  time.Second,
	}
}
