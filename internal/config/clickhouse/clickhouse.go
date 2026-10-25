package clickhouse

import "time"

const (
	DefaultEndpoint = "http://localhost:8123"
	DefaultDatabase = "default"
	DefaultTable    = "app_logs"
	DefaultUsername = "default"
	DefaultPassword = ""
	// DefaultTimeoutは、HETEROLOG_*CLICKHOUSE_TIMEOUT_SECONDを省略した場合の1回のHTTP requestの上限です。
	// 既定のretry（3回、Retry-Afterで最大1秒の待機）を含む予算8秒が、既定のlogger timeoutに
	// 既定のtimeout budget policy（must_fit）で収まるよう、公開設定例（.env.example）と同じ2秒にします。
	DefaultTimeout         = 2 * time.Second
	DefaultCompressRequest = true

	DefaultClickHouseFailureArchiveDir         = "clickhouse_failure_archives"
	DefaultAsyncClickHouseFailureArchiveDir    = "async_clickhouse_failure_archives"
	DefaultDeliveryClickHouseFailureArchiveDir = "delivery_clickhouse_failure_archives"
	DefaultClickHouseFailureSpoolDir           = "clickhouse"
	DefaultAsyncClickHouseFailureSpoolDir      = "clickhouse_async"

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

type ClientConfig struct {
	Endpoint        string
	Database        string
	Table           string
	Timeout         time.Duration
	Username        string
	Password        string
	Headers         map[string]string
	CompressRequest bool
}
