package opensearch

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClientCreatesClientWhenRetryConfigIsNil(t *testing.T) {
	t.Parallel()

	client, err := NewClient(newTestClientConfigProvider(), nil, nil, nil)

	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewClientWrapsClientWhenRetryConfigIsPresent(t *testing.T) {
	t.Parallel()

	retryCfg := newTestRetryConfig(
		pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
			RetryClientMode: pkglogger.RetryClientModeRetry,
			MaxAttempts:     2,
			BaseDelay:       time.Nanosecond,
			MaxDelay:        time.Nanosecond,
		},
	)

	client, err := NewClient(newTestClientConfigProvider(), retryCfg, nil, nil)

	require.NoError(t, err)
	assert.NotNil(t, client)
	_, wrapped := client.(*wrapper.RetryingClient)
	assert.True(t, wrapped)
}

func TestNewClientWrapsRetryPolicyClientWhenPolicyEnabled(t *testing.T) {
	t.Parallel()

	retryCfg := newTestRetryConfig(
		pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
			RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
			MaxAttempts:     2,
			BaseDelay:       time.Nanosecond,
			MaxDelay:        time.Nanosecond,
		},
	)

	client, err := NewClient(newTestClientConfigProvider(), retryCfg, nil, nil)

	require.NoError(t, err)
	assert.NotNil(t, client)
	_, wrapped := client.(*wrapper.RetryPolicyClient)
	assert.True(t, wrapped)
}

func TestNewClientWrapsCircuitBreakerClientWhenCircuitBreakerEnabled(t *testing.T) {
	t.Parallel()

	retryCfg := newTestRetryConfig(
		pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
			RetryClientMode: pkglogger.RetryClientModeCircuitBreaker,
		},
	)

	client, err := NewClient(newTestClientConfigProvider(), retryCfg, nil, nil)

	require.NoError(t, err)
	assert.NotNil(t, client)
	_, wrapped := client.(*wrapper.CircuitBreakerClient)
	assert.True(t, wrapped)
}

type testRetryConfig struct {
	cfg pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig
}

func newTestRetryConfig(
	cfg pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig,
) wrapper.RetryConfigProvider {
	return testRetryConfig{cfg: cfg}
}

func (c testRetryConfig) MaxAttempts() int {
	return c.cfg.MaxAttempts
}

func (c testRetryConfig) BaseDelay() time.Duration {
	return c.cfg.BaseDelay
}

func (c testRetryConfig) MaxDelay() time.Duration {
	return c.cfg.MaxDelay
}

func (c testRetryConfig) RetryClientSelection() pkglogger.RetryClientSelection {
	return c.cfg.RetryClientSelection()
}

func TestNewClientReturnsErrorWhenClientConfigIsNil(t *testing.T) {
	t.Parallel()

	client, err := NewClient(nil, nil, nil, nil)

	require.ErrorIs(t, err, ErrNilClientConfig)
	assert.Nil(t, client)
}

func TestToClientConfigCopiesCompressRequest(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		enabled bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled", enabled: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, provider := range []struct {
				name   string
				config OpenSearchClientConfigProvider
			}{
				{
					name: "adapter",
					config: NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
						Endpoint:        "http://localhost:9200",
						Timeout:         time.Second,
						CompressRequest: test.enabled,
						AuthConfig:      pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone},
					}),
				},
				{name: "required interface only", config: minimalClientConfigProvider{compressRequest: test.enabled}},
			} {
				t.Run(provider.name, func(t *testing.T) {
					cfg, err := ToClientConfig(provider.config)
					require.NoError(t, err)
					assert.Equal(t, test.enabled, cfg.CompressRequest)
				})
			}
		})
	}
}

// minimalClientConfigProviderは必須interfaceのメソッドだけを実装します。
// 圧縮設定の反映が具体的なadapter型に依存しないことを確認します。
type minimalClientConfigProvider struct{ compressRequest bool }

var _ OpenSearchClientConfigProvider = minimalClientConfigProvider{}

func (minimalClientConfigProvider) Endpoint() string        { return "http://localhost:9200" }
func (minimalClientConfigProvider) Timeout() time.Duration  { return time.Second }
func (minimalClientConfigProvider) SkipVerifySSL() bool     { return false }
func (p minimalClientConfigProvider) CompressRequest() bool { return p.compressRequest }
func (minimalClientConfigProvider) AuthConfig() (OpenSearchClientAuthConfigProvider, error) {
	return nil, nil
}

func TestToClientConfigPreservesPackageOwnedAuthConfig(t *testing.T) {
	t.Parallel()

	auth := pkgopensearch.AuthConfig{
		Type: pkgopensearch.AuthTypeCertificate,
		Config: pkgopensearch.CertificateAuthConfig{
			CertFile: "client.crt",
			KeyFile:  "client.key",
			CAFile:   "ca.crt",
		},
	}
	cfg, err := ToClientConfig(NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
		Endpoint:   "https://localhost:9200",
		AuthConfig: auth,
	}))

	require.NoError(t, err)
	assert.Equal(t, auth, cfg.AuthConfig)
}

func newTestClientConfigProvider() OpenSearchClientConfigProvider {
	return newTestClientConfigProviderWithEndpoint("http://localhost:9200")
}

func newTestClientConfigProviderWithEndpoint(endpoint string) OpenSearchClientConfigProvider {
	return NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
		Endpoint: endpoint,
		Timeout:  time.Second,
		AuthConfig: pkgopensearch.AuthConfig{
			Type: pkgopensearch.AuthTypeNone,
		},
	})
}
