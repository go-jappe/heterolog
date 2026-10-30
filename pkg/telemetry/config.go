// Package telemetry defines the configuration of heterolog.NewTelemetry, which
// delivers spans and metrics, from the Tracer and the Meter of heterolog and
// from the OpenTelemetry SDK, to OTLP/HTTP receivers, files and stdout.
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
	// Resource identifies the process in the spans and metrics that the Tracer
	// and the Meter of the Telemetry record. Data from the OpenTelemetry SDK
	// keeps the resource of its provider.
	Resource logger.Resource
	Traces   TracesConfig
	Metrics  MetricsConfig
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
	OTLP HTTPConfig
	// File configures TraceExporterFile.
	File FileConfig
}

// HTTPConfig configures a destination that posts spans or metrics over HTTP:
// an OTLP/HTTP receiver, or VictoriaMetrics.
type HTTPConfig struct {
	// Endpoint is the URL that the data is posted to, such as
	// "http://localhost:4318/v1/traces" for spans. The path is used as given;
	// /v1/traces, /v1/metrics or /api/v1/import/prometheus is not added.
	// Redirects are not followed.
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
	// FailureStore keeps the data that could not be sent, so that it is
	// resent later or kept for inspection.
	FailureStore FailureStoreConfig
}

// FailureStoreConfig configures where a destination keeps the spans or
// metrics that it could not send. It uses the failure stores of the log
// destinations, and each span, or each metric with all its data points, is
// kept as one record of OTLP JSON with its resource and instrumentation
// scope.
//
// Data that failed for a reason that allows a resend, including failures
// whose outcome is unknown, is resent from the spool. Data that the receiver
// rejected is kept as dead, and the data of a partial success as quarantine,
// without a resend.
type FailureStoreConfig struct {
	// Store selects and configures the stores as for a log destination: with
	// DurableTypeSpool, the data is kept in a spool and resent; with
	// DurableTypeFileArchive, it is appended to a failure archive; with
	// DurableTypeNone and EnableDegrade, it is written to stdout. An empty
	// DurableType keeps nothing, and the data is dropped.
	//
	// The failure archive is uploaded to object storage as for a log
	// destination. The spool and the archive that keep the compressed files
	// that could not be uploaded are owned by "telemetry", whatever
	// LoggerContract their settings have (see pkg/objectstorage), so they do
	// not share the directories of the loggers. The internal log notification
	// targets (HandlerConfig) are not used: the errors and warnings of the
	// stores go to the function passed with heterolog.WithTelemetryErrorHandler.
	Store durable.FailureStoreConfig
	// Replay configures the worker that resends the data in the spool, as for
	// a log destination.
	Replay spoolrunner.BackendSpoolRunnerConfig
	// ReplaySupervisor configures the restarts of that worker.
	ReplaySupervisor spoolsupervisor.RunnerSupervisorConfig
	// MaxAge is the longest time that data waits in the spool to be resent,
	// measured from the failure that put it there. Older data is kept as dead
	// without being sent, because many receivers, such as Prometheus, reject
	// samples that are too old. 0 resends data of any age.
	MaxAge time.Duration
}

// Enabled reports whether Store keeps or writes the data that could not be
// sent.
func (c FailureStoreConfig) Enabled() bool {
	types := c.Store.DurableType
	return len(types) > 0 && (types[0] != durable.DurableTypeNone || c.Store.EnableDegrade)
}

// usesSpool reports whether Store selects the spool.
func (c FailureStoreConfig) usesSpool() bool {
	for _, durableType := range c.Store.DurableType {
		if durableType == durable.DurableTypeSpool {
			return true
		}
	}
	return false
}

// FileConfig configures the file destination of spans or metrics.
type FileConfig struct {
	// Path is the file that each batch is appended to as one line of OTLP
	// JSON. Missing directories are created. The file is not rotated.
	Path string
}

// MetricExporter names a destination of metrics.
type MetricExporter string

const (
	// MetricExporterOTLP posts metrics to an OTLP/HTTP receiver, such as an
	// OpenTelemetry Collector, Prometheus or VictoriaMetrics, in protobuf.
	MetricExporterOTLP MetricExporter = "otlp"
	// MetricExporterFile appends metrics to a file as OTLP JSON.
	MetricExporterFile MetricExporter = "file"
	// MetricExporterStdout writes metrics to the standard output as OTLP JSON.
	MetricExporterStdout MetricExporter = "stdout"
	// MetricExporterVictoriaMetrics posts metrics to the
	// /api/v1/import/prometheus API of VictoriaMetrics in the Prometheus text
	// format, with the time of each collection.
	MetricExporterVictoriaMetrics MetricExporter = "victoriametrics"
)

// MetricsConfig configures the recording and delivery of metrics.
type MetricsConfig struct {
	// Exporters lists the destinations of metrics. Each destination receives
	// every collection and has its own queue, so a slow destination does not
	// delay the others. Without destinations, the Meter of the Telemetry
	// records nothing.
	Exporters []MetricExporter
	// ExportInterval is the interval at which the values of the Meter are
	// collected and sent.
	ExportInterval time.Duration
	// QueueSize is the number of collections that each destination keeps
	// while they wait to be sent. When the queue is full, further collections
	// are dropped for that destination. The values of the Meter are
	// cumulative, so a later collection still has the totals.
	QueueSize int
	// HistogramBounds are the upper bounds of the buckets of the histograms
	// of the Meter, in ascending order; a value equal to a bound is counted in
	// its bucket. The defaults suit durations in seconds.
	HistogramBounds []float64
	// CardinalityLimit is the largest number of attribute sets that one metric
	// of the Meter keeps. When it is reached, the values of further attribute
	// sets are added to one set with the attribute otel.metric.overflow=true,
	// which counts toward the limit, as in the OpenTelemetry SDK.
	CardinalityLimit int
	// OTLP configures MetricExporterOTLP.
	OTLP HTTPConfig
	// VictoriaMetrics configures MetricExporterVictoriaMetrics. It posts with
	// the same requests, retries, splitting, circuit breaker, and failure store
	// as OTLP; the receiver does not report partial successes.
	VictoriaMetrics HTTPConfig
	// File configures MetricExporterFile.
	File FileConfig
}
