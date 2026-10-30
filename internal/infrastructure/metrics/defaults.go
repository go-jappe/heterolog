package metrics

import (
	"slices"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// metricsの配送の既定値です。.env.exampleと文書の値も合わせます。集める間隔と、属性の組の
// 数の上限は、OpenTelemetry SDKの既定値と同じです。OTLP/HTTPの送り先のほかの既定値は、
// traceと共通のdelivery.DefaultOTLPConfigです。
const (
	DefaultExportInterval   = time.Minute
	DefaultQueueSize        = 16
	DefaultCardinalityLimit = 2000

	// DefaultOTLPEndpointは、OpenTelemetryの既定のOTLP/HTTPの受信先です。
	DefaultOTLPEndpoint = "http://localhost:4318/v1/metrics"
)

// defaultHistogramBoundsは、Histogramのbucketの上限の既定値です。heterologのHistogramは
// 秒の長さを記録するため、Prometheusのclientの既定値と同じ、秒に向いた値にします。
var defaultHistogramBounds = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// DefaultHistogramBoundsは、Histogramのbucketの上限の既定値を返します。
func DefaultHistogramBounds() []float64 { return slices.Clone(defaultHistogramBounds) }

// DefaultConfigは、送り先を選んでいないmetricsの既定の設定を返します。
func DefaultConfig() pkgtelemetry.MetricsConfig {
	return pkgtelemetry.MetricsConfig{
		ExportInterval:   DefaultExportInterval,
		QueueSize:        DefaultQueueSize,
		HistogramBounds:  DefaultHistogramBounds(),
		CardinalityLimit: DefaultCardinalityLimit,
		OTLP:             delivery.DefaultOTLPConfig(DefaultOTLPEndpoint),
	}
}
