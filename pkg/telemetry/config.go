// Package telemetry defines the configuration of heterolog.NewTelemetry, which
// delivers spans from the Tracer of heterolog and from the OpenTelemetry SDK to
// OTLP/HTTP receivers, files and stdout.
package telemetry

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	"github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
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
	// Retry-After of a response is used instead; when it is longer than
	// RetryMaxDelay, the request is not resent.
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	// CircuitBreaker stops sending for a while after requests keep failing
	// temporarily: when at least 3 failures in a row, each within 15 seconds
	// of the previous one, have lasted a minute, requests fail without being
	// sent for 30 seconds, and then one request is sent to check whether the
	// receiver has recovered. The same breaker as that of the log
	// destinations is used.
	CircuitBreaker bool
	// FailureStore keeps the spans that could not be sent, so that they are
	// resent later or kept for inspection.
	FailureStore TraceFailureStoreConfig
}

// TraceFailureStoreConfig configures where the otlp destination keeps the
// spans that it could not send. It uses the failure stores of the log
// destinations, and each span is kept as one record of OTLP JSON with its
// resource and instrumentation scope.
//
// Spans that failed for a reason that allows a resend, including failures
// whose outcome is unknown, are resent from the spool. Spans that the
// receiver rejected are kept as dead, and spans of a partial success as
// quarantine, without a resend.
type TraceFailureStoreConfig struct {
	// Store selects and configures the stores as for a log destination: with
	// DurableTypeSpool, the spans are kept in a spool and resent; with
	// DurableTypeFileArchive, they are appended to a failure archive; with
	// DurableTypeNone and EnableDegrade, they are written to stdout. An empty
	// DurableType keeps nothing, and the spans are dropped.
	//
	// The failure archive is uploaded to object storage as for a log
	// destination. The spool and the archive that keep the compressed files
	// that could not be uploaded are owned by "telemetry", whatever
	// LoggerContract their settings have (see pkg/objectstorage), so they do
	// not share the directories of the loggers. The internal log notification targets
	// (HandlerConfig) are not used: the errors and warnings of the stores go to
	// the function passed with heterolog.WithTelemetryErrorHandler.
	Store durable.FailureStoreConfig
	// Replay configures the worker that resends the spans in the spool, as
	// for a log destination.
	Replay spoolrunner.BackendSpoolRunnerConfig
	// ReplaySupervisor configures the restarts of that worker.
	ReplaySupervisor spoolsupervisor.RunnerSupervisorConfig
}

// Enabled reports whether Store keeps or writes the spans that could not be
// sent.
func (c TraceFailureStoreConfig) Enabled() bool {
	types := c.Store.DurableType
	return len(types) > 0 && (types[0] != durable.DurableTypeNone || c.Store.EnableDegrade)
}

// usesSpool reports whether Store selects the spool.
func (c TraceFailureStoreConfig) usesSpool() bool {
	for _, durableType := range c.Store.DurableType {
		if durableType == durable.DurableTypeSpool {
			return true
		}
	}
	return false
}

// FileTracesConfig configures the file destination of spans.
type FileTracesConfig struct {
	// Path is the file that each batch of spans is appended to as one line of
	// OTLP JSON. Missing directories are created. The file is not rotated.
	Path string
}
