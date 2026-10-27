package observability

const (
	DefaultTracingProvider     = "noop"
	DefaultTracingExporterType = "none"
	DefaultTracingEndpoint     = "localhost:4317"
	DefaultTracingProtocol     = "grpc" // grpcまたはhttp
)

type ObservabilityConfig struct {
	Tracing TracingConfig
	Metrics MetricsConfig
}

type TracingConfig struct {
	Enabled bool

	// どの実装を使うか（OTelとは限らない）
	Provider string // "otel" | "noop" | "vendorX" など

	// どこへ送るか（OTLPなら共通化できる）
	Exporter TraceExporterConfig
}

type TraceExporterConfig struct {
	Type string // "otlp" | "jaeger" | "zipkin" | "stdout" | "none"

	OTLP *OtlpClientConfig // Type=="otlp" の時だけ使う
}

type OtlpClientConfig struct {
	Endpoint string
	Protocol string // "grpc" | "http"
	Insecure bool
	Headers  map[string]string
}

type MetricsConfig struct {
}
