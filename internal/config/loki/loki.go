package loki

import "time"

const (
	DefaultLokiEndpoint = "http://localhost:3100"
	DefaultUsername     = ""
	DefaultPassword     = ""
	// DefaultTimeoutは、HETEROLOG_*LOKI_TIMEOUT_SECONDを省略した場合の1回のHTTP requestの上限です。
	// 既定のretry（3回、Retry-Afterで最大1秒の待機）を含む予算8秒が、既定のlogger timeoutに
	// 既定のtimeout budget policy（must_fit）で収まるよう、公開設定例（.env.example）と同じ2秒にします。
	DefaultTimeout      = 2 * time.Second
	DefaultLokiTenantID = ""
	DefaultBearerToken  = ""

	DefaultCompressRequest = true

	DefaultLokiFailureArchiveDir         = "loki_failure_archives"
	DefaultAsyncLokiFailureArchiveDir    = "async_loki_failure_archives"
	DefaultDeliveryLokiFailureArchiveDir = "delivery_loki_failure_archives"
	DefaultLokiFailureSpoolDir           = "loki"
	DefaultAsyncLokiFailureSpoolDir      = "loki_async"

	// retry client mode の違い:
	// - none: raw client。
	// - retry: RetryingClient -> raw client。
	// - retry_policy: RetryPolicyClient -> raw client。
	// - circuit_breaker: CircuitBreakerClient -> raw client。
	// - retry_with_breaker: RetryingClient -> CircuitBreakerClient -> raw client。
	// - retry_policy_with_breaker: RetryPolicyClient -> CircuitBreakerClient -> raw client。
	// defaultではbreakerを暗黙に有効化せず、エラー分類に応じた有界retryだけを適用します。
	DefaultRetryClientMode = "retry_policy"
	DefaultMaxAttempts     = 3
	DefaultBaseDelay       = 200 * time.Millisecond
	DefaultMaxDelay        = 1 * time.Second
)

type LokiClientConfig struct {
	Endpoint        string
	Timeout         time.Duration
	TenantID        string
	BasicAuth       *BasicAuthConfig
	BearerToken     string
	Headers         map[string]string
	CompressRequest bool
}

type BasicAuthConfig struct {
	Username string
	Password string
}
