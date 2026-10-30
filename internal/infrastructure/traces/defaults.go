package traces

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// traceの配送の既定値です。.env.exampleと文書の値も合わせます。待ち行列とbatchの大きさ、
// 送る間隔は、OpenTelemetry SDKのBatchSpanProcessorの既定値と同じです。OTLP/HTTPの
// 送り先のほかの既定値は、metricsと共通のdelivery.DefaultOTLPConfigです。
const (
	DefaultSamplingRatio  = 1.0
	DefaultQueueSize      = 2048
	DefaultBatchSize      = 512
	DefaultExportInterval = 5 * time.Second

	// DefaultOTLPEndpointは、OpenTelemetryの既定のOTLP/HTTPの受信先です。
	DefaultOTLPEndpoint = "http://localhost:4318/v1/traces"
)

// DefaultConfigは、送り先を選んでいないtraceの既定の設定を返します。
func DefaultConfig() pkgtelemetry.TracesConfig {
	return pkgtelemetry.TracesConfig{
		SamplingRatio:  DefaultSamplingRatio,
		QueueSize:      DefaultQueueSize,
		BatchSize:      DefaultBatchSize,
		ExportInterval: DefaultExportInterval,
		OTLP:           delivery.DefaultOTLPConfig(DefaultOTLPEndpoint),
	}
}
