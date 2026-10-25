package wrapper

import (
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// RetryConfigProviderは、ClickHouse client chainの構成とretry待機設定を返します。
// 個別retry wrapperは待機設定だけを使い、chain構築時だけRetryClientSelectionを参照します。
type RetryConfigProvider interface {
	retryBackoffConfigProvider
	RetryClientSelection() pkglogger.RetryClientSelection
}

// BuildClientChainはRetryClientModeに従ってclient wrapperの親子chainを組み立てます。
// retry clientを外側の親、circuit breaker clientをraw clientを包む内側の子にします。
// retryを選択した場合の試行回数と待機時間はこの構築時に検証し、補正しません。
func BuildClientChain(
	client Client,
	retryCfg RetryConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (Client, error) {
	if client == nil || retryCfg == nil {
		return client, nil
	}

	selection := retryCfg.RetryClientSelection()
	if err := pkglogger.ValidateRetryClientConfig(
		selection.Mode,
		retryCfg.MaxAttempts(),
		retryCfg.BaseDelay(),
		retryCfg.MaxDelay(),
	); err != nil {
		return nil, &retryable.InvalidRetryClientConfigError{Backend: "clickhouse", Err: err}
	}
	wrapped := client
	if selection.UseCircuitBreakerClient {
		wrapped = newCircuitBreakerClient(wrapped, tracer, meter)
	}
	if selection.UseRetryPolicyClient {
		wrapped = newRetryPolicyClient(wrapped, retryCfg, tracer)
	} else if selection.UseRetryingClient {
		wrapped = newRetryingClient(wrapped, retryCfg, tracer)
	}
	return wrapped, nil
}
