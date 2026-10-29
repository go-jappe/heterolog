package bootstrap

import (
	"context"

	"github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/infrastructure/logrecovery"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// RecoverLogsは、cfgの設定で、停止中のアプリケーションのログファイルを手動復旧します。
// 復旧の進め方はlogrecovery.Recoverのとおりです。logger設定への変換は、選んだloggerの
// 種類の分だけ行います。
func RecoverLogs(ctx context.Context, cfg config.AppConfig, opts logrecovery.Options) (logrecovery.Report, error) {
	return logrecovery.Recover(ctx, logrecovery.Configs{
		Target:   cfg.RuntimeConfig.Target,
		Logger:   func() (pkglogger.LoggerConfig, error) { return config.MapLoggerConfig(cfg) },
		Async:    func() (pkglogger.AsyncBestEffortMultiLoggerConfig, error) { return config.MapAsyncLoggerConfig(cfg) },
		Delivery: func() (pkglogger.DeliveryLoggerConfig, error) { return config.MapDeliveryLoggerConfig(cfg) },
	}, opts)
}
