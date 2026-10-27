// Package loggerruntimeは、公開APIのNew・NewAsync・NewDeliveryの処理を受け持ちます。
// 設定とoptionを検証し、出力先ごとの稼働中に変えられる設定を用意してから、
// infrastructure層のloggerを組み立てます。組み立てたloggerの終了処理の調整と、
// 稼働中の設定の読み取り・変更もここで行います。
package loggerruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-jappe/heterolog/internal/application/outputsettings"
	infralogger "github.com/go-jappe/heterolog/internal/infrastructure/logger"
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
}

// AsyncRuntimeは、非同期でbest effortに送るloggerを持ちます。Shutdownは、受け付けた処理と
// 最後のbufferの書き出しが終わるまで待ちます。NewAsyncで作ります。
type AsyncRuntime struct {
	pkglogger.AsyncLogger
	*lifecycle
	outputs *outputsettings.Registry
}

// DeliveryRuntimeは、出力先ごとの送信結果を返すloggerを持ちます。NewDeliveryで作ります。
type DeliveryRuntime struct {
	pkglogger.DeliveryLogger
	*lifecycle
}

var (
	// ErrNilContextは、New・NewAsync・NewDeliveryがnilのcontextを受け取ったことを表します。
	ErrNilContext = errors.New("logger context must not be nil")
	// ErrDeliverySpoolUnsupportedは、NewDeliveryがWithSpoolを受け取ったことを表します。
	// Delivery loggerは送信の結果を呼び出し元へ返すため、再送のworkerを持ちません。
	ErrDeliverySpoolUnsupported = errors.New("delivery logging does not support WithSpool")
)

const (
	// ConfigFieldTimeoutは、pkg/loggerのConfigFieldErrorで、Config・AsyncConfig・
	// DeliveryConfigのTimeoutを表します。正でないTimeoutは、pkg/loggerの
	// ErrNonPositiveSettingの種類で報告します。
	ConfigFieldTimeout pkglogger.ConfigField = "Timeout"
	// ConfigFieldMaxParallelは、pkg/loggerのConfigFieldErrorで、AsyncConfigの
	// MaxParallelを表します。正でないMaxParallelは、pkg/loggerのErrNonPositiveSettingの
	// 種類で報告します。
	ConfigFieldMaxParallel pkglogger.ConfigField = "MaxParallel"
)

// Newは、明示した設定から同期loggerを作ります。環境変数は読まず、process全体の
// providerも変更しません。bufferを書き出して資源を解放するため、CloseかShutdownを
// 呼ぶ必要があります。
func New(ctx context.Context, cfg pkglogger.LoggerConfig, opts ...Option) (*Runtime, error) {
	if err := validateConstructor(ctx, "logger", cfg.Timeout, cfg.MaxLogEntryBytes); err != nil {
		return nil, err
	}
	if err := validateLoggerPresentation("logger", cfg); err != nil {
		return nil, err
	}
	o, err := ResolveOptions(opts)
	if err != nil {
		return nil, err
	}
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	outputs, err := attachOutputSettings("logger", cfg.LogOutputDestinations,
		&cfg.File, &cfg.Stdout, &cfg.Slack, &cfg.OpenSearch, &cfg.Loki, &cfg.CloudWatchLogs, &cfg.ClickHouse)
	if err != nil {
		return nil, err
	}
	logger, err := infralogger.NewLogger(ctx, cfg, o.Routing, o.Spool, o.Monitor, o.Tracer, o.Meter)
	if err != nil {
		return nil, err
	}
	return &Runtime{Logger: logger, lifecycle: &lifecycle{closer: logger}, outputs: outputs}, nil
}

// NewAsyncは、非同期でbest effortに送るloggerを作ります。MaxParallelと、通常のtimeoutと
// logの大きさの上限は、すべて正である必要があります。
func NewAsync(ctx context.Context, cfg pkglogger.AsyncBestEffortMultiLoggerConfig, opts ...Option) (*AsyncRuntime, error) {
	if err := validateConstructor(ctx, "async logger", cfg.Timeout, cfg.MaxLogEntryBytes); err != nil {
		return nil, err
	}
	if cfg.MaxParallel <= 0 {
		return nil, &pkglogger.ConfigFieldError{
			Source: "async logger",
			Field:  ConfigFieldMaxParallel,
			Detail: fmt.Sprintf("max parallel must be positive: %d", cfg.MaxParallel),
			Value:  cfg.MaxParallel,
			Err:    pkglogger.ErrNonPositiveSetting,
		}
	}
	if err := validateLoggerPresentation("async logger", cfg.ToLoggerConfig()); err != nil {
		return nil, err
	}
	o, err := ResolveOptions(opts)
	if err != nil {
		return nil, err
	}
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	outputs, err := attachOutputSettings("async logger", cfg.LogOutputDestinations,
		&cfg.File, &cfg.Stdout, &cfg.Slack, &cfg.OpenSearch, &cfg.Loki, &cfg.CloudWatchLogs, &cfg.ClickHouse)
	if err != nil {
		return nil, err
	}
	logger, err := infralogger.NewAsyncLogger(ctx, cfg, o.Routing, o.Spool, o.Monitor, o.Tracer, o.Meter)
	if err != nil {
		return nil, err
	}
	return &AsyncRuntime{AsyncLogger: logger, lifecycle: &lifecycle{closer: logger}, outputs: outputs}, nil
}

// NewDeliveryは、出力先ごとの送信結果を返すloggerを作ります。Delivery loggerには再送の
// workerがないため、optsにWithSpoolがあるとErrDeliverySpoolUnsupportedを返します。
func NewDelivery(ctx context.Context, cfg pkglogger.DeliveryLoggerConfig, opts ...Option) (*DeliveryRuntime, error) {
	if err := validateConstructor(ctx, "delivery logger", cfg.Timeout, cfg.MaxLogEntryBytes); err != nil {
		return nil, err
	}
	if err := validateDeliveryPresentation(cfg); err != nil {
		return nil, err
	}
	o, err := ResolveOptions(opts)
	if err != nil {
		return nil, err
	}
	if o.Spool != nil {
		return nil, ErrDeliverySpoolUnsupported
	}
	if cfg.Resource == nil {
		cfg.Resource = &otel.Resource{}
	}
	logger, err := infralogger.NewDeliveryLogger(ctx, cfg, o.Routing, o.Tracer, o.Meter)
	if err != nil {
		return nil, err
	}
	return &DeliveryRuntime{DeliveryLogger: logger, lifecycle: &lifecycle{closer: logger}}, nil
}

// validateConstructorは、constructorに共通の引数を検証します。sourceは設定の出所
// （"logger"・"async logger"・"delivery logger"）です。
func validateConstructor(ctx context.Context, source string, timeout time.Duration, maxBytes int) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if timeout <= 0 {
		return &pkglogger.ConfigFieldError{
			Source: source,
			Field:  ConfigFieldTimeout,
			Detail: fmt.Sprintf("timeout must be positive: %s", timeout),
			Value:  timeout,
			Err:    pkglogger.ErrNonPositiveSetting,
		}
	}
	return pkglogger.ValidateMaxLogEntryBytes(maxBytes)
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
