package opensearch

import (
	"time"

	opensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

type OpenSearchClientConfigAdapter struct {
	cfg *opensearch.Config
}

func NewOpenSearchClientConfigAdapter(config *opensearch.Config) *OpenSearchClientConfigAdapter {
	return &OpenSearchClientConfigAdapter{
		cfg: config,
	}
}

func (a *OpenSearchClientConfigAdapter) Endpoint() string {
	return a.cfg.Endpoint
}
func (a *OpenSearchClientConfigAdapter) Timeout() time.Duration {
	return a.cfg.Timeout
}
func (a *OpenSearchClientConfigAdapter) SkipVerifySSL() bool {
	return a.cfg.SkipVerifySSL
}
func (a *OpenSearchClientConfigAdapter) AuthConfig() (OpenSearchClientAuthConfigProvider, error) {
	auth := a.cfg.AuthConfig
	if auth.Type == opensearch.AuthTypeNone {
		return nil, nil // 認証なし
	}
	return authConfigProvider{config: auth}, nil
}

func (a *OpenSearchClientConfigAdapter) CompressRequest() bool {
	return a.cfg.CompressRequest
}
