// Package loggerruntimeは、公開APIのNew・NewAsync・NewDeliveryの処理を受け持ちます。
// 出力先ごとの稼働中に変えられる設定（levelとsampling）を検証して用意してから、
// 呼び出し元が渡したBuilderでloggerを組み立てます。組み立てたloggerの終了処理の調整と、
// 稼働中の設定の読み取り・変更もここで行います。出力先の設定値の検証と既定値は、
// Builderの実装が受け持ちます。
package loggerruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/otel"
)

// Runtimeは、同期loggerと、その背後で動く資源を持ちます。Newで作ります。zero値は
// loggerとして使えません。Runtimeを複製しないでください。終了するにはCloseかShutdownを
// 呼びます。
type Runtime struct {
	pkglogger.Logger
	*lifecycle
	outputs *outputsettings.Registry
	config  pkglogger.LoggerConfig
}

// AsyncRuntimeは、非同期でbest effortに送るloggerを持ちます。Shutdownは、受け付けた処理と
// 最後のbufferの書き出しが終わるまで待ちます。NewAsyncで作ります。
type AsyncRuntime struct {
	pkglogger.AsyncLogger
	*lifecycle
	outputs *outputsettings.Registry
	config  pkglogger.AsyncBestEffortMultiLoggerConfig
}

// DeliveryRuntimeは、出力先ごとの送信結果を返すloggerを持ちます。NewDeliveryで作ります。
type DeliveryRuntime struct {
	pkglogger.DeliveryLogger
	*lifecycle
	config pkglogger.DeliveryLoggerConfig
}

var (
	// ErrNilContextは、New・NewAsync・NewDeliveryと、internal/bootstrapのNewTelemetryが
	// nilのcontextを受け取ったことを表します。
	ErrNilContext = errors.New("logger context must not be nil")
	// ErrNilBuilderは、New・NewAsync・NewDeliveryがnilのBuilderを受け取ったことを表します。
	ErrNilBuilder = errors.New("logger builder must not be nil")
	// ErrDeliverySpoolUnsupportedは、NewDeliveryがWithSpoolを受け取ったことを表します。
	// Delivery loggerは送信の結果を呼び出し元へ返すため、再送のworkerを持ちません。
	ErrDeliverySpoolUnsupported = errors.New("delivery logging does not support WithSpool")
)

// Newは、明示した設定から、builderで同期loggerを作ります。環境変数は読まず、process全体の
// providerも変更しません。bufferを書き出して資源を解放するため、CloseかShutdownを
// 呼ぶ必要があります。
func New(ctx context.Context, builder Builder, cfg pkglogger.LoggerConfig, opts ...Option) (*Runtime, error) {
	if err := validateConstructor(ctx, builder); err != nil {
		return nil, err
	}
	if err := validateOutputSettings("logger", cfg); err != nil {
		return nil, err
	}
	o := ResolveOptions(opts)
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	outputs, err := attachOutputSettings("logger", cfg.LogOutputDestinations,
		&cfg.File, &cfg.Stdout, &cfg.Slack, &cfg.OpenSearch, &cfg.Loki, &cfg.CloudWatchLogs, &cfg.ClickHouse)
	if err != nil {
		return nil, err
	}
	logger, err := builder.NewLogger(ctx, cfg, o)
	if err != nil {
		return nil, err
	}
	return &Runtime{Logger: logger, lifecycle: &lifecycle{closer: logger}, outputs: outputs, config: cfg}, nil
}

// NewAsyncは、builderで、非同期でbest effortに送るloggerを作ります。
func NewAsync(ctx context.Context, builder Builder, cfg pkglogger.AsyncBestEffortMultiLoggerConfig, opts ...Option) (*AsyncRuntime, error) {
	if err := validateConstructor(ctx, builder); err != nil {
		return nil, err
	}
	if err := validateOutputSettings("async logger", cfg.ToLoggerConfig()); err != nil {
		return nil, err
	}
	o := ResolveOptions(opts)
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	outputs, err := attachOutputSettings("async logger", cfg.LogOutputDestinations,
		&cfg.File, &cfg.Stdout, &cfg.Slack, &cfg.OpenSearch, &cfg.Loki, &cfg.CloudWatchLogs, &cfg.ClickHouse)
	if err != nil {
		return nil, err
	}
	logger, err := builder.NewAsyncLogger(ctx, cfg, o)
	if err != nil {
		return nil, err
	}
	return &AsyncRuntime{AsyncLogger: logger, lifecycle: &lifecycle{closer: logger}, outputs: outputs, config: cfg}, nil
}

// NewDeliveryは、builderで、出力先ごとの送信結果を返すloggerを作ります。Delivery loggerには
// 再送のworkerがないため、optsにWithSpoolがあるとErrDeliverySpoolUnsupportedを返します。
func NewDelivery(ctx context.Context, builder Builder, cfg pkglogger.DeliveryLoggerConfig, opts ...Option) (*DeliveryRuntime, error) {
	if err := validateConstructor(ctx, builder); err != nil {
		return nil, err
	}
	o := ResolveOptions(opts)
	if o.Spool != nil {
		return nil, ErrDeliverySpoolUnsupported
	}
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	logger, err := builder.NewDeliveryLogger(ctx, cfg, o)
	if err != nil {
		return nil, err
	}
	return &DeliveryRuntime{DeliveryLogger: logger, lifecycle: &lifecycle{closer: logger}, config: cfg}, nil
}

// validateConstructorは、constructorに共通の引数を検証します。
func validateConstructor(ctx context.Context, builder Builder) error {
	if ctx == nil {
		return ErrNilContext
	}
	if builder == nil {
		return ErrNilBuilder
	}
	return ctx.Err()
}

// Logは、対応しているseverityのlogを記録します。processは終了しません。
func (r *Runtime) Log(ctx context.Context, level pkglogger.LogLevel, message string, fields ...pkglogger.Field) error {
	switch level {
	case pkglogger.LogLevelDebug:
		return r.Debug(ctx, message, fields...)
	case pkglogger.LogLevelInfo:
		return r.Info(ctx, message, fields...)
	case pkglogger.LogLevelWarn:
		return r.Warn(ctx, message, fields...)
	case pkglogger.LogLevelError:
		return r.Error(ctx, message, fields...)
	default:
		return fmt.Errorf("unsupported log level: %v", level)
	}
}
