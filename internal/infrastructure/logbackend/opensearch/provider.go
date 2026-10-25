package opensearch

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/opensearch"
)

type OpenSearchClientConfigProvider interface {
	Endpoint() string
	Timeout() time.Duration
	SkipVerifySSL() bool
	CompressRequest() bool
	AuthConfig() (OpenSearchClientAuthConfigProvider, error)
}

type OpenSearchClientAuthConfigProvider interface {
	ToAuthConfig() opensearch.AuthConfig
}

// authConfigProviderはpkg/opensearchで定義された認証設定をそのまま保持します。
// infrastructure側で認証方式を再列挙すると、pkg側へ方式を追加した際に対応漏れが
// 発生するため、認証方式と具体設定の検証はpkg/opensearchへ一元化します。
type authConfigProvider struct {
	config opensearch.AuthConfig
}

func (a authConfigProvider) ToAuthConfig() opensearch.AuthConfig {
	return a.config
}

type BasicAuthConfigProvider struct {
	Username string
	Password string
}

func (b BasicAuthConfigProvider) ToAuthConfig() opensearch.AuthConfig {
	return opensearch.AuthConfig{
		Type: opensearch.AuthTypeBasic,
		Config: opensearch.BasicAuthConfig{
			Username: b.Username,
			Password: b.Password,
		},
	}
}

type APIKeyAuthConfigProvider struct {
	ID     string
	Secret string
}

func (a APIKeyAuthConfigProvider) ToAuthConfig() opensearch.AuthConfig {
	return opensearch.AuthConfig{
		Type: opensearch.AuthTypeAPIKey,
		Config: opensearch.APIKeyAuthConfig{
			ID:     a.ID,
			Secret: a.Secret,
		},
	}
}

type AWSIAMAuthConfigProvider struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

func (a AWSIAMAuthConfigProvider) ToAuthConfig() opensearch.AuthConfig {
	return opensearch.AuthConfig{
		Type: opensearch.AuthTypeAWSIAM,
		Config: opensearch.AWSIAMAuthConfig{
			Region:          a.Region,
			AccessKeyID:     a.AccessKeyID,
			SecretAccessKey: a.SecretAccessKey,
			SessionToken:    a.SessionToken,
		},
	}
}
