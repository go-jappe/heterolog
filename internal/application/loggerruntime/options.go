package loggerruntime

import (
	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// Optionは、constructorへ任意の依存と動作の方針を渡します。
type Option func(*Options)

// Optionsは、constructorが受け取ったOptionを順に適用した結果です。
type Options struct {
	// Spoolは、WithSpoolで渡した再送の設定です。nilの場合は再送のworkerを起動しません。
	Spool *pkgspool.SpoolConfig
	// Routingは、CloudWatch Logsのstream名の時間分割です。
	Routing pkgcloudwatchlogs.StreamRouting
	// Monitorは、WithMemoryPressureMonitorで渡したmemory圧迫の監視です。
	Monitor memorypressure.Monitor
	// TracerとMeterは計装の依存です。ResolveOptionsは、nilを何もしない実装に置き換え、
	// Tracerが始めたspanをcontextから取り出せるように包みます。
	Tracer pkgobserver.Tracer
	Meter  pkgobserver.Meter
	// TelemetryErrorHandlerは、WithTelemetryErrorHandlerで渡した関数です。nilの場合は、
	// telemetryの実装が既定の扱いを選びます。
	TelemetryErrorHandler func(error)
}

// WithSpoolは、同期・非同期loggerのbackendがdurableなspoolを使う場合の再送の設定を
// 渡します。Delivery loggerは再送のworkerを持ちません。
func WithSpool(cfg pkgspool.SpoolConfig) Option {
	return func(o *Options) { o.Spool = &cfg }
}

// WithCloudWatchLogsStreamRoutingは、CloudWatch Logsのstream名の時間分割を選びます。
func WithCloudWatchLogsStreamRouting(routing pkgcloudwatchlogs.StreamRouting) Option {
	return func(o *Options) { o.Routing = routing }
}

// WithObserversは、計装を渡します。nilの依存は何もしない実装を使います。observerの
// providerと、その終了は呼び出し元が受け持ちます。
func WithObservers(tracer pkgobserver.Tracer, meter pkgobserver.Meter) Option {
	return func(o *Options) { o.Tracer, o.Meter = tracer, meter }
}

// WithTelemetryErrorHandlerは、NewTelemetryの配送のerrorを受け取る関数を渡します。
// loggerは使いません。
func WithTelemetryErrorHandler(handler func(error)) Option {
	return func(o *Options) { o.TelemetryErrorHandler = handler }
}

// WithMemoryPressureMonitorは、過負荷時の方針に使う、起動済みのmemory圧迫の監視を
// 渡します。監視の寿命は呼び出し元が受け持ち、loggerは開始・reset・停止をしません。
// 渡した監視の判定は、spool設定のMemoryCriticalRatioなどの閾値に代わります。渡さなければ、
// UseMemoryPressureForOverloadを有効にした各spoolが、その閾値で自分の監視を起動・停止します。
func WithMemoryPressureMonitor(monitor memorypressure.Monitor) Option {
	return func(o *Options) { o.Monitor = monitor }
}

// ResolveOptionsは、optsを順に適用した結果を返します。nilのOptionは無視します。
// 渡された値の検証と既定値は、Builderの実装が受け持ちます。
func ResolveOptions(opts []Option) Options {
	var o Options
	for _, apply := range opts {
		if apply != nil {
			apply(&o)
		}
	}
	o.Tracer = observer.ExposeSpans(o.Tracer)
	o.Meter = observer.EnsureMeter(o.Meter)
	return o
}
