package wrapper

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
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
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (Client, error) {
	if client == nil || retryCfg == nil {
		return client, nil
	}

	selection := retryCfg.RetryClientSelection()
	// providerのMaxAttemptsは初回を含む試行回数です。公開の検証は再試行の回数を受け取るため、
	// 初回の1回を引いて渡し、試行回数が0以下ならerrorにします。
	if err := pkglogger.ValidateRetryClientConfig(
		selection.Mode,
		retryCfg.MaxAttempts()-1,
		retryCfg.BaseDelay(),
		retryCfg.MaxDelay(),
	); err != nil {
		return nil, &retryable.InvalidRetryClientConfigError{Backend: "clickhouse", Err: err}
	}
	wrapped := client
	if selection.UseCircuitBreakerClient {
		wrapped = newCircuitBreakerClient(wrapped, meter)
	}
	if selection.UseRetryPolicyClient {
		wrapped = newRetryPolicyClient(wrapped, retryCfg, tracer, meter)
	} else if selection.UseRetryingClient {
		wrapped = newRetryingClient(wrapped, retryCfg, tracer, meter)
	}
	return wrapped, nil
}
