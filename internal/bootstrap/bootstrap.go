// Package bootstrapは、application層のloggerの処理に、infrastructure層の実装を渡して
// loggerを組み立てます。application層はinfrastructure層をimportしないため、両方を
// importして組み合わせるのは、このpackageとcmdのmainだけです。root packageは、loggerを
// ここで作ります。
package bootstrap

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/logging"
	infralogger "github.com/go-jappe/heterolog/internal/infrastructure/logger"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Newは、明示した設定から同期loggerを作ります。検証・option・終了はloggerruntime.Newの
// とおりです。
func New(ctx context.Context, cfg pkglogger.LoggerConfig, opts ...loggerruntime.Option) (*loggerruntime.Runtime, error) {
	return loggerruntime.New(ctx, infrastructureBuilder{}, cfg, opts...)
}

// NewAsyncは、非同期でbest effortに送るloggerを作ります。検証・option・終了は
// loggerruntime.NewAsyncのとおりです。
func NewAsync(ctx context.Context, cfg pkglogger.AsyncBestEffortMultiLoggerConfig, opts ...loggerruntime.Option) (*loggerruntime.AsyncRuntime, error) {
	return loggerruntime.NewAsync(ctx, infrastructureBuilder{}, cfg, opts...)
}

// NewDeliveryは、出力先ごとの送信結果を返すloggerを作ります。検証・option・終了は
// loggerruntime.NewDeliveryのとおりです。
func NewDelivery(ctx context.Context, cfg pkglogger.DeliveryLoggerConfig, opts ...loggerruntime.Option) (*loggerruntime.DeliveryRuntime, error) {
	return loggerruntime.NewDelivery(ctx, infrastructureBuilder{}, cfg, opts...)
}

// infrastructureBuilderは、loggerruntime.Builderを、infrastructure層のloggerで実装します。
type infrastructureBuilder struct{}

func (infrastructureBuilder) NewLogger(ctx context.Context, cfg pkglogger.LoggerConfig, o loggerruntime.Options) (logging.CloseableLogger, error) {
	return infralogger.NewLogger(ctx, cfg, o.Routing, o.Spool, o.Monitor, o.Tracer, o.Meter)
}

func (infrastructureBuilder) NewAsyncLogger(ctx context.Context, cfg pkglogger.AsyncBestEffortMultiLoggerConfig, o loggerruntime.Options) (logging.AsyncCloseableLogger, error) {
	return infralogger.NewAsyncLogger(ctx, cfg, o.Routing, o.Spool, o.Monitor, o.Tracer, o.Meter)
}

func (infrastructureBuilder) NewDeliveryLogger(ctx context.Context, cfg pkglogger.DeliveryLoggerConfig, o loggerruntime.Options) (logging.DeliveryClosableLogger, error) {
	return infralogger.NewDeliveryLogger(ctx, cfg, o.Routing, o.Tracer, o.Meter)
}
