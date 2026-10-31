package loggerruntime

import (
	"github.com/go-jappe/heterolog/internal/application/configview"
	"github.com/go-jappe/heterolog/internal/application/metrics"
)

// Configurationは、このloggerを作った設定を返します。管理用APIが設定の文書を作るための
// もので、稼働中に変えたlevelとsamplingは含めません（OutputSettingsが返します）。設定は
// Newへ渡した値の複製で、Builderが補った既定値は含めません。
func (r *Runtime) Configuration() configview.Configuration {
	return configview.Configuration{Kind: configview.KindLogger, Value: r.config}
}

// Configurationは、このloggerを作った設定を返します。Runtime.Configurationのとおりです。
func (r *AsyncRuntime) Configuration() configview.Configuration {
	return configview.Configuration{Kind: configview.KindAsyncLogger, Value: r.config}
}

// Configurationは、このloggerを作った設定を返します。Runtime.Configurationのとおりです。
func (r *DeliveryRuntime) Configuration() configview.Configuration {
	return configview.Configuration{Kind: configview.KindDeliveryLogger, Value: r.config}
}

// Metricsは、このloggerが記録したmetricsの累積値を返します。利用者がWithObserversで
// Meterを渡したかどうかによらず、loggerは内部のMeterにも記録します。statusの報告が、
// 出力先ごとの送信の結果と所要時間を読むために使います。
func (r *Runtime) Metrics() []metrics.Metric { return r.meter.Collect() }

// Metricsは、このloggerが記録したmetricsの累積値を返します。Runtime.Metricsのとおりです。
func (r *AsyncRuntime) Metrics() []metrics.Metric { return r.meter.Collect() }

// Metricsは、このloggerが記録したmetricsの累積値を返します。Runtime.Metricsのとおりです。
func (r *DeliveryRuntime) Metrics() []metrics.Metric { return r.meter.Collect() }
