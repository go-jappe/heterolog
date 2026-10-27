package opensearch

import (
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"time"
)

// opensearchの接続に必要なもののみを定義
const (
	DefaultOpenSearchEndpoint = "http://localhost:9200"
	DefaultOpenSearchIndex    = "app-log-heterolog"
	// DefaultOpenSearchTimeoutは、HETEROLOG_*OPENSEARCH_TIMEOUT_SECONDを省略した場合の1回のHTTP requestの
	// 上限です。通常・非同期のbulk送信ではclientのretry（既定3回、最大1秒の待機）とbulk itemの
	// 再送（3回、1秒の待機）を掛け合わせた予算17秒が、既定のlogger timeout（18秒）に既定の
	// timeout budget policy（must_fit）で収まるよう、公開設定例（.env.example）と同じ1秒にします。
	DefaultOpenSearchTimeout    = 1 * time.Second
	DefaultOpenSearchRetry      = 3
	DefaultOpenSearchSkipVerify = false
	DefaultAuthType             = opensearch.AuthTypeBasic
	DefaultCompressRequest      = true

	// failure archive/spoolのdirectory名はOpenSearch backend固有のdefaultです。
	// 共通failure設定にbackend名を持ち込まないよう、このpackageで所有します。
	DefaultFailureArchiveDir         = "opensearch_failure_archives"
	DefaultAsyncFailureArchiveDir    = "async_opensearch_failure_archives"
	DefaultDeliveryFailureArchiveDir = "delivery_opensearch_failure_archives"
	DefaultFailureSpoolDir           = "opensearch"
	DefaultAsyncFailureSpoolDir      = "opensearch_async"

	// retry client mode の違い:
	// - none: raw client。
	// - retry: RetryingClient -> raw client。
	// - retry_policy: RetryPolicyClient -> raw client。
	// - circuit_breaker: CircuitBreakerClient -> raw client。
	// - retry_with_breaker: RetryingClient -> CircuitBreakerClient -> raw client。
	// - retry_policy_with_breaker: RetryPolicyClient -> CircuitBreakerClient -> raw client。
	// 固定ID createの409はmodeに関係なくbackendの単件・bulk結果分類で成功扱いにします。
	// defaultではbreakerを暗黙に有効化せず、エラー分類に応じた有界retryだけを適用します。
	DefaultRetryClientMode = "retry_policy"
	// DefaultMaxRetriesは、初回の送信に加える再試行の既定の回数です。初回と合わせて最大3回送ります。
	DefaultMaxRetries = 2
	DefaultBaseDelay  = 200 * time.Millisecond
	DefaultMaxDelay   = 1 * time.Second
)

// OpenSearchClientConfig はOpenSearch clientの設定を保持する構造体
type OpenSearchClientConfig struct {
	Endpoint        string               // 例: "https://localhost:9200"
	Timeout         time.Duration        // クライアントタイムアウト
	SkipVerifySSL   bool                 // 証明書検証をスキップするか（開発環境用）
	Auth            OpenSearchAuthConfig //
	CompressRequest bool                 // 圧縮送信可否
}

type OpenSearchAuthConfig struct {
	Type opensearch.AuthType `json:"type" yaml:"type"`

	Basic       *BasicAuthConfig       `json:"basic,omitempty" yaml:"basic,omitempty"`
	APIKey      *APIKeyAuthConfig      `json:"apikey,omitempty" yaml:"apikey,omitempty"`
	AWSIAM      *AWSIAMAuthConfig      `json:"awsiam,omitempty" yaml:"awsiam,omitempty"`
	SAML        *SAMLAuthConfig        `json:"saml,omitempty" yaml:"saml,omitempty"`
	OIDC        *OIDCAuthConfig        `json:"oidc,omitempty" yaml:"oidc,omitempty"`
	Certificate *CertificateAuthConfig `json:"certificate,omitempty" yaml:"certificate,omitempty"`
}

type BasicAuthConfig struct {
	Username string
	Password string
}

type APIKeyAuthConfig struct {
	ID     string
	Secret string
}

type AWSIAMAuthConfig struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type SAMLAuthConfig struct {
	Token string
}

type OIDCAuthConfig struct {
	Token string
}

type CertificateAuthConfig struct {
	CertFile string
	KeyFile  string
	CAFile   string
}
