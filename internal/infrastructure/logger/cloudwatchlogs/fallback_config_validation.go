package cloudwatchlogs

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// ValidateFallbackLoggerConfigは、内部ログの通知先として使うfallback loggerを、通知の初回に
// 作るときと同じ手順（newFallbackLogger）で作って直ちに閉じ、その設定から通知先を作れることを
// loggerの構築時に確かめます。通知の初回に作れずに通知を黙って捨てることがないよう、
// top-level loggerの構築がNewNamed*FallbackHandlerと同じ引数で呼び出します。workerは開始して
// すぐに停止し、送信は行いません。
func ValidateFallbackLoggerConfig(
	cfg CloudWatchLogsConfigProvider,
	clientCfg *pkgcloudwatchlogs.Config,
	fmCfg CloudWatchLogsFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...CloudWatchLogsLoggerOption,
) error {
	ctx := context.Background()
	fallback, err := newFallbackLogger(
		ctx,
		cfg,
		clientCfg,
		fmCfg,
		resourceProvider,
		retryCfg,
		observer.NopTracer{},
		observer.NopMeter{},
		opts...,
	)
	if err != nil {
		return err
	}
	return fallback.Close(ctx)
}
