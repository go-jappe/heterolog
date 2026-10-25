package wrapper

import (
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// RetryConfigProviderは、Loki client chainの構成選択とretry待機設定を提供します。
// 個別のretry実装はretryBackoffConfigProviderだけに依存します。
type RetryConfigProvider interface {
	retryBackoffConfigProvider
	RetryClientSelection() pkglogger.RetryClientSelection
}

// BuildClientChainは設定済みmodeに従い、raw clientへCircuit Breakerとretryを適用します。
// Circuit Breakerは各backend attemptを観測できるようretry clientの内側へ配置します。
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
		return nil, &retryable.InvalidRetryClientConfigError{Backend: "loki", Err: err}
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
