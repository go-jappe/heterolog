package loki

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/wrapper"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

// ValidateFallbackLoggerConfigは、内部ログの通知先として使うfallback loggerを、通知の初回に
// 作るときと同じ手順（newFallbackLogger）で作って直ちに閉じ、その設定から通知先を作れることを
// loggerの構築時に確かめます。通知の初回に作れずに通知を黙って捨てることがないよう、
// top-level loggerの構築がNewNamed*FallbackHandlerと同じ引数で呼び出します。workerは開始して
// すぐに停止し、送信は行いません。
func ValidateFallbackLoggerConfig(
	cfg LokiConfigProvider,
	clientCfg *pkgloki.Config,
	fmCfg LokiFlushManagerRuntimeConfigProvider,
	resourceProvider resource.ResourceProvider,
	retryCfg wrapper.RetryConfigProvider,
	opts ...LokiLoggerOption,
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
