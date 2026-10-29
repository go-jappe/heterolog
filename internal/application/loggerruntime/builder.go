package loggerruntime

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Builderは、稼働中に変えられる設定を持たせた設定と、ResolveOptionsで解決したoptionから、
// 出力先へ書くloggerを組み立てます。それ以外の設定とoptionの値は、資源を作る前に
// Builderの実装が検証します。application層はinfrastructure層をimportしないため、実装は
// internal/bootstrapが渡します。New・NewAsync・NewDeliveryは、稼働中に変えられる設定の
// 検証と用意を終えてからBuilderを呼び、組み立てたloggerの終了を受け持ちます。
type Builder interface {
	// NewLoggerは、同期loggerを組み立てます。
	NewLogger(ctx context.Context, cfg pkglogger.LoggerConfig, o Options) (logging.CloseableLogger, error)
	// NewAsyncLoggerは、非同期でbest effortに送るloggerを組み立てます。
	NewAsyncLogger(ctx context.Context, cfg pkglogger.AsyncBestEffortMultiLoggerConfig, o Options) (logging.AsyncCloseableLogger, error)
	// NewDeliveryLoggerは、出力先ごとの送信結果を返すloggerを組み立てます。Delivery loggerは
	// 再送のworkerを持たないため、oのSpoolは常にnilで、Monitorも使いません。
	NewDeliveryLogger(ctx context.Context, cfg pkglogger.DeliveryLoggerConfig, o Options) (logging.DeliveryClosableLogger, error)
}
