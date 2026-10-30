package traces

import (
	"time"

	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// traceの配送の既定値です。.env.exampleと文書の値も合わせます。待ち行列とbatchの大きさ、
// 送る間隔は、OpenTelemetry SDKのBatchSpanProcessorの既定値と同じです。
const (
	DefaultSamplingRatio  = 1.0
	DefaultQueueSize      = 2048
	DefaultBatchSize      = 512
	DefaultExportInterval = 5 * time.Second

	// DefaultOTLPEndpointは、OpenTelemetryの既定のOTLP/HTTPの受信先です。
	DefaultOTLPEndpoint       = "http://localhost:4318/v1/traces"
	DefaultOTLPCompression    = "gzip"
	DefaultOTLPTimeout        = 10 * time.Second
	DefaultOTLPRetries        = 3
	DefaultOTLPRetryBaseDelay = time.Second
	DefaultOTLPRetryMaxDelay  = 5 * time.Second
)

// DefaultConfigは、送り先を選んでいないtraceの既定の設定を返します。
func DefaultConfig() pkgtelemetry.TracesConfig {
	return pkgtelemetry.TracesConfig{
		SamplingRatio:  DefaultSamplingRatio,
		QueueSize:      DefaultQueueSize,
		BatchSize:      DefaultBatchSize,
		ExportInterval: DefaultExportInterval,
		OTLP: pkgtelemetry.OTLPTracesConfig{
			Endpoint:       DefaultOTLPEndpoint,
			Compression:    DefaultOTLPCompression,
			Timeout:        DefaultOTLPTimeout,
			Retries:        DefaultOTLPRetries,
			RetryBaseDelay: DefaultOTLPRetryBaseDelay,
			RetryMaxDelay:  DefaultOTLPRetryMaxDelay,
		},
	}
}
