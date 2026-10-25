package wrapper

import (
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

// retryBackoffConfigProviderは、retryを実行する個々のwrapperが必要とする
// 試行回数と待機時間だけを表します。client chainの選択は含みません。
type retryBackoffConfigProvider interface {
	MaxAttempts() int
	BaseDelay() time.Duration
	MaxDelay() time.Duration
}

// RetryConfigProviderは、OpenSearch client chainの構築に必要なretry設定を表します。
// BuildClientChainはRetryClientSelectionに従ってretry、policy、circuit breakerを選び、
// 個々のretry wrapperは埋め込まれたbackoff設定だけを使用します。
type RetryConfigProvider interface {
	retryBackoffConfigProvider
	RetryClientSelection() pkglogger.RetryClientSelection
}

// BuildClientChainはraw clientへ設定されたcircuit breakerとretry wrapperを組み立てます。
// 外側から順にretry/policy、circuit breaker、raw clientとなります。固定ID createの409は
// client wrapperでは変更せず、単件・bulkともOpenSearch backendの結果分類で成功扱いにします。
// retryを選択した場合の試行回数と待機時間はこの構築時に検証し、補正しません。
func BuildClientChain(
	client opensearch.Client,
	retryCfg RetryConfigProvider,
	tracer observer.Tracer,
	meter observer.Meter,
) (opensearch.Client, error) {
	if client == nil {
		return nil, nil
	}

	var wrapped opensearch.Client = client
	if retryCfg == nil {
		return wrapped, nil
	}

	selection := retryCfg.RetryClientSelection()
	if err := pkglogger.ValidateRetryClientConfig(
		selection.Mode,
		retryCfg.MaxAttempts(),
		retryCfg.BaseDelay(),
		retryCfg.MaxDelay(),
	); err != nil {
		return nil, &retryable.InvalidRetryClientConfigError{Backend: "opensearch", Err: err}
	}
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
