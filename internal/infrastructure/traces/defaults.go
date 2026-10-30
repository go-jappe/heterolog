package traces

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// traceの配送の既定値です。.env.exampleと文書の値も合わせます。待ち行列とbatchの大きさ、
// 送る間隔は、OpenTelemetry SDKのBatchSpanProcessorの既定値と同じです。OTLP/HTTPの
// 送り先のほかの既定値は、metricsと共通のdelivery.DefaultHTTPConfigです。
const (
	DefaultSamplingRatio  = 1.0
	DefaultQueueSize      = 2048
	DefaultBatchSize      = 512
	DefaultExportInterval = 5 * time.Second

	// DefaultOTLPEndpointは、OpenTelemetryの既定のOTLP/HTTPの受信先です。
	DefaultOTLPEndpoint = "http://localhost:4318/v1/traces"

	// DefaultOpenSearchEndpointは、OpenSearchの既定の接続先です。
	DefaultOpenSearchEndpoint = "http://localhost:9200"
	// DefaultOpenSearchIndexは、spanを書く既定のindexです。OpenSearch Dashboardsの
	// Trace Analyticsが読むindex pattern（otel-v1-apm-span-*）に合わせます。
	DefaultOpenSearchIndex = "otel-v1-apm-span-heterolog"

	// DefaultClickHouseEndpoint、DefaultClickHouseDatabase、DefaultClickHouseTableは、spanを
	// 書くClickHouseの既定の接続先と表です。OpenTelemetry CollectorのClickHouse exporterの
	// 既定の表（default.otel_traces）に合わせます。DefaultClickHouseUsernameは、ログの出力先と
	// 同じ既定の利用者です。
	DefaultClickHouseEndpoint = "http://localhost:8123"
	DefaultClickHouseDatabase = "default"
	DefaultClickHouseTable    = "otel_traces"
	DefaultClickHouseUsername = "default"
)

// DefaultConfigは、送り先を選んでいないtraceの既定の設定を返します。
func DefaultConfig() pkgtelemetry.TracesConfig {
	return pkgtelemetry.TracesConfig{
		SamplingRatio:  DefaultSamplingRatio,
		QueueSize:      DefaultQueueSize,
		BatchSize:      DefaultBatchSize,
		ExportInterval: DefaultExportInterval,
		OTLP:           delivery.DefaultHTTPConfig(DefaultOTLPEndpoint),
		OpenSearch:     DefaultOpenSearchConfig(),
		ClickHouse:     DefaultClickHouseConfig(),
	}
}

// DefaultClickHouseConfigは、ClickHouseの送り先の既定の設定を返します。送り直しと
// failure storeは、OTLP/HTTPの送り先と同じ既定値です。
func DefaultClickHouseConfig() pkgtelemetry.ClickHouseConfig {
	http := delivery.DefaultHTTPConfig("")
	return pkgtelemetry.ClickHouseConfig{
		Client: pkgclickhouse.Config{
			Endpoint:        DefaultClickHouseEndpoint,
			Database:        DefaultClickHouseDatabase,
			Table:           DefaultClickHouseTable,
			Timeout:         http.Timeout,
			Username:        DefaultClickHouseUsername,
			CompressRequest: http.Compression == "gzip",
		},
		Retries:        http.Retries,
		RetryBaseDelay: http.RetryBaseDelay,
		RetryMaxDelay:  http.RetryMaxDelay,
		FailureStore:   http.FailureStore,
	}
}

// DefaultOpenSearchConfigは、OpenSearchの送り先の既定の設定を返します。送り直しと
// failure storeは、OTLP/HTTPの送り先と同じ既定値です。認証は、ログの出力先と同じく
// basic認証を既定にし、認証情報は補いません。
func DefaultOpenSearchConfig() pkgtelemetry.OpenSearchConfig {
	http := delivery.DefaultHTTPConfig("")
	return pkgtelemetry.OpenSearchConfig{
		Client: pkgopensearch.Config{
			Endpoint:        DefaultOpenSearchEndpoint,
			Timeout:         http.Timeout,
			AuthConfig:      pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeBasic},
			CompressRequest: http.Compression == "gzip",
		},
		Index:          DefaultOpenSearchIndex,
		Retries:        http.Retries,
		RetryBaseDelay: http.RetryBaseDelay,
		RetryMaxDelay:  http.RetryMaxDelay,
		FailureStore:   http.FailureStore,
	}
}
