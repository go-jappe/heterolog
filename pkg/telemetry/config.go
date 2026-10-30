// Package telemetry defines the configuration of heterolog.NewTelemetry, which
// delivers spans from the Tracer of heterolog and from the OpenTelemetry SDK to
// OTLP/HTTP receivers, files and stdout.
package telemetry

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/logger"
)

// Config configures heterolog.NewTelemetry. Start with
// heterolog.DefaultTelemetryConfig, which fills every setting except the
// destinations.
type Config struct {
	// Resource identifies the process in the spans that the Tracer of the
	// Telemetry creates. Spans from the OpenTelemetry SDK keep the resource of
	// their TracerProvider.
	Resource logger.Resource
	Traces   TracesConfig
}

// TraceExporter names a destination of spans.
type TraceExporter string

const (
	// TraceExporterOTLP posts spans to an OTLP/HTTP receiver, such as an
	// OpenTelemetry Collector or Grafana Tempo, in protobuf.
	TraceExporterOTLP TraceExporter = "otlp"
	// TraceExporterFile appends spans to a file as OTLP JSON.
	TraceExporterFile TraceExporter = "file"
	// TraceExporterStdout writes spans to the standard output as OTLP JSON.
	TraceExporterStdout TraceExporter = "stdout"
)

// TracesConfig configures the recording and delivery of spans.
type TracesConfig struct {
	// Exporters lists the destinations of spans. Each destination receives
	// every recorded span and has its own queue, so a slow destination does
	// not delay the others. Without destinations, the Tracer of the Telemetry
	// records nothing.
	Exporters []TraceExporter
	// SamplingRatio is the fraction of new traces that are recorded, from 0 to
	// 1. A span whose context carries a parent span context follows the
	// decision of the parent instead.
	SamplingRatio float64
	// QueueSize is the number of ended spans that each destination keeps
	// while they wait to be sent. When the queue is full, further spans are
	// dropped.
	QueueSize int
	// BatchSize is the largest number of spans that a destination sends at
	// once. It must not exceed QueueSize.
	BatchSize int
	// ExportInterval is the longest time that an ended span waits for its
	// batch to fill before the batch is sent, while the destination is not
	// busy sending an earlier batch.
	ExportInterval time.Duration
	// OTLP configures TraceExporterOTLP.
	OTLP OTLPTracesConfig
	// File configures TraceExporterFile.
	File FileTracesConfig
}

// OTLPTracesConfig configures the OTLP/HTTP destination of spans.
type OTLPTracesConfig struct {
	// Endpoint is the URL that spans are posted to, such as
	// "http://localhost:4318/v1/traces". The path is used as given; /v1/traces
	// is not added. Redirects are not followed.
	Endpoint string
	// Headers are added to each request, such as an Authorization header.
	Headers map[string]string
	// Compression is "gzip" or "none".
	Compression string
	// Timeout limits each request.
	Timeout time.Duration
	// Retries is the number of times that a request that failed temporarily
	// is sent again. As the OTLP specification requires, only the statuses
	// 429, 502, 503, and 504 and failures without a response, such as a
	// refused connection or a timeout, are temporary. 0 does not resend.
	Retries int
	// RetryBaseDelay and RetryMaxDelay set the wait before each resend: the
	// wait doubles from RetryBaseDelay with each resend, up to RetryMaxDelay,
	// and a random wait between half of it and all of it is chosen. The
	// Retry-After of a response is used instead, up to RetryMaxDelay.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// FileTracesConfig configures the file destination of spans.
type FileTracesConfig struct {
	// Path is the file that each batch of spans is appended to as one line of
	// OTLP JSON. Missing directories are created. The file is not rotated.
	Path string
}
