package opensearch

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	opensearchwrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
)

// ValidateFallbackLoggerConfigは、内部ログの通知先として使うfallback loggerを、通知の初回に
// 作るときと同じ手順（newFallbackLogger）で作って直ちに閉じ、その設定から通知先を作れることを
// loggerの構築時に確かめます。通知の初回に作れずに通知を黙って捨てることがないよう、
// top-level loggerの構築がNewNamed*FallbackHandlerと同じ引数で呼び出します。workerは開始して
// すぐに停止し、送信は行いません。
func ValidateFallbackLoggerConfig(
	adapter OpenSearchLoggerConfigProvider,
	clientCfg infraopensearch.OpenSearchClientConfigProvider,
	fmCfg OpenSearchFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg opensearchwrapper.RetryConfigProvider,
	opts ...OpenSearchLoggerOption,
) error {
	fallback, err := newFallbackLogger(
		adapter,
		clientCfg,
		retryCfg,
		fmCfg,
		resourceProvider,
		observer.NopTracer{},
		observer.NopMeter{},
		opts...,
	)
	if err != nil {
		return err
	}
	return fallback.Close(context.Background())
}
