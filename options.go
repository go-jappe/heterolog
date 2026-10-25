package heterolog

import (
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	configspool "github.com/go-jappe/heterolog/internal/config/spool"
	"github.com/go-jappe/heterolog/internal/config/spool/spoollifecycle"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolrunner"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolsupervisor"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/infrastructure/memory/pressure"
	memorymonitor "github.com/go-jappe/heterolog/internal/observer/memory/pressure"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// Option supplies optional dependencies and runtime policies to constructors.
type Option func(*options)

type options struct {
	spool   *SpoolConfig
	routing StreamRouting
	monitor MemoryPressureMonitor
	tracer  Tracer
	meter   Meter
}

// Public aliases allow custom instrumentation without importing internal packages.
type (
	Tracer                = observer.Tracer
	Span                  = observer.Span
	TraceID               = observer.TraceID
	ObserverAttr          = observer.Attr
	Meter                 = observer.Meter
	Counter               = observer.Counter
	Gauge                 = observer.Gauge
	MemoryPressureMonitor = pressure.MemoryPressureMonitor
)

// SpoolConfig configures background replay of durable failure spools.
type SpoolConfig = configspool.SpoolConfig

// SpoolRunnerConfig supplies separate replay settings for each backend.
type SpoolRunnerConfig = spoolrunner.SpoolRunnerConfig

// BackendSpoolRunnerConfig controls one backend's replay worker and lifecycle.
type BackendSpoolRunnerConfig = spoolrunner.BackendSpoolRunnerConfig

// SpoolLoopConfig controls replay polling.
type SpoolLoopConfig = spoolrunner.RunnerConfig

// SpoolWorkerConfig controls replay finalization and the maximum wait for a
// backend-specified Retry-After.
type SpoolWorkerConfig = spoolrunner.WorkerConfig

// SpoolFinalizeConfig bounds finalization retry state.
type SpoolFinalizeConfig = spoolrunner.FinalizeConfig

// SpoolLifecycleConfig controls retention of finalized spool records.
type SpoolLifecycleConfig = spoollifecycle.LifecycleConfig

// SpoolSupervisorConfig controls replay worker restarts.
type SpoolSupervisorConfig = spoolsupervisor.RunnerSupervisorConfig

// StreamRouting selects time partitioning for CloudWatch Logs stream names.
type StreamRouting = streamrouting.Granularity

const (
	StreamRoutingNone    = streamrouting.None
	StreamRoutingMonthly = streamrouting.Monthly
	StreamRoutingDaily   = streamrouting.Daily
	StreamRoutingHourly  = streamrouting.Hourly
)

// ErrInvalidStreamRouting reports that WithCloudWatchLogsStreamRouting received
// a StreamRouting value that is not supported. The constructors return it as an
// *InvalidStreamRoutingError.
var ErrInvalidStreamRouting = streamrouting.ErrInvalidGranularity

// InvalidStreamRoutingError reports the unsupported StreamRouting value in its
// Granularity field. errors.Is matches ErrInvalidStreamRouting.
type InvalidStreamRoutingError = streamrouting.InvalidGranularityError

// WithSpool supplies replay settings when a synchronous or asynchronous
// backend uses durable spooling. Delivery loggers do not own replay workers.
func WithSpool(cfg SpoolConfig) Option {
	return func(o *options) { o.spool = &cfg }
}

// WithCloudWatchLogsStreamRouting selects stream partitioning for CloudWatch.
func WithCloudWatchLogsStreamRouting(routing StreamRouting) Option {
	return func(o *options) { o.routing = routing }
}

// WithObservers supplies instrumentation. Nil dependencies use no-op observers.
// The caller retains ownership of observer providers and their shutdown.
func WithObservers(tracer Tracer, meter Meter) Option {
	return func(o *options) { o.tracer, o.meter = tracer, meter }
}

// WithMemoryPressureMonitor supplies an already-started monitor, such as one
// from NewMemoryPressureMonitor, for overload policies. The caller owns its
// lifetime: loggers neither start, reset, nor stop it.
func WithMemoryPressureMonitor(monitor MemoryPressureMonitor) Option {
	return func(o *options) { o.monitor = monitor }
}

// ErrInvalidMemoryPressureMonitor reports that NewMemoryPressureMonitor rejected
// its provider, policy, or poll interval.
var ErrInvalidMemoryPressureMonitor = memorymonitor.ErrInvalidMonitorConfig

// NewMemoryPressureMonitor returns a monitor to pass to WithMemoryPressureMonitor.
// After Start, it reads provider every pollInterval and evaluates each reading
// with the same rules as memorypressure.CriticalState: a reading is a critical
// candidate when a reliable observation reaches CriticalRatio or
// MinAvailableBytes, and the monitor reports critical once
// ConsecutiveCriticalHits candidates occur in a row (0 or 1 means the first).
//
// The caller owns the monitor. Start reads once before returning and then polls
// until its context ends; cancel that context when no logger uses the monitor.
// If the first read fails, Start returns the error and polling does not begin.
//
// It returns an error wrapping ErrInvalidMemoryPressureMonitor for a nil
// provider, a non-positive poll interval, or a policy that enables neither
// threshold, has a CriticalRatio above 1, or has a negative
// ConsecutiveCriticalHits.
func NewMemoryPressureMonitor(
	provider memorypressure.Provider,
	policy memorypressure.CriticalPolicy,
	pollInterval time.Duration,
) (MemoryPressureMonitor, error) {
	return memorymonitor.New(provider, policy, pollInterval)
}

// DefaultSpoolConfig returns replay settings for every supported backend.
// Purging is disabled so retained records are not deleted implicitly.
func DefaultSpoolConfig() SpoolConfig {
	backend := BackendSpoolRunnerConfig{
		Runner: SpoolLoopConfig{Interval: 100 * time.Millisecond},
		Worker: SpoolWorkerConfig{
			Finalize:      SpoolFinalizeConfig{RetryDelay: time.Second, MaxEntries: 4098},
			MaxRetryAfter: time.Hour,
		},
		LifeCycle: SpoolLifecycleConfig{SizePurgeInterval: time.Minute},
	}
	return SpoolConfig{
		SupervisorConfig: SpoolSupervisorConfig{RestartDelay: time.Second},
		RunnerConfig: SpoolRunnerConfig{
			OpensearchConfig:     backend,
			LokiConfig:           backend,
			CloudWatchLogsConfig: backend,
			ClickHouseConfig:     backend,
		},
	}
}

func resolveOptions(opts []Option) (options, error) {
	o := options{routing: StreamRoutingNone}
	for _, apply := range opts {
		if apply != nil {
			apply(&o)
		}
	}
	o.tracer = observer.EnsureTracer(o.tracer)
	o.meter = observer.EnsureMeter(o.meter)
	return o, o.routing.Validate()
}
