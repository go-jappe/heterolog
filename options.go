package heterolog

import (
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	memorymonitor "github.com/go-jappe/heterolog/internal/observer/memory/pressure"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/memorypressure"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
	"github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	"github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

// Option supplies optional dependencies and runtime policies to constructors.
type Option = loggerruntime.Option

// These aliases name the types for custom instrumentation and memory pressure
// monitoring, so callers need not import pkg/observer or pkg/memorypressure.
type (
	Tracer                = pkgobserver.Tracer
	Span                  = pkgobserver.Span
	TraceID               = pkgobserver.TraceID
	ObserverAttr          = pkgobserver.Attr
	Meter                 = pkgobserver.Meter
	Counter               = pkgobserver.Counter
	Gauge                 = pkgobserver.Gauge
	Histogram             = pkgobserver.Histogram
	MemoryPressureMonitor = memorypressure.Monitor
)

// SpoolConfig configures background replay of durable failure spools.
type SpoolConfig = pkgspool.SpoolConfig

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
type StreamRouting = pkgcloudwatchlogs.StreamRouting

const (
	StreamRoutingNone    = pkgcloudwatchlogs.StreamRoutingNone
	StreamRoutingMonthly = pkgcloudwatchlogs.StreamRoutingMonthly
	StreamRoutingDaily   = pkgcloudwatchlogs.StreamRoutingDaily
	StreamRoutingHourly  = pkgcloudwatchlogs.StreamRoutingHourly
)

// ErrInvalidStreamRouting reports that WithCloudWatchLogsStreamRouting received
// a StreamRouting value that is not supported. The constructors return it as an
// *InvalidStreamRoutingError.
var ErrInvalidStreamRouting = pkgcloudwatchlogs.ErrInvalidStreamRouting

// InvalidStreamRoutingError reports the unsupported StreamRouting value in its
// Granularity field. errors.Is matches ErrInvalidStreamRouting.
type InvalidStreamRoutingError = pkgcloudwatchlogs.InvalidStreamRoutingError

// WithSpool supplies replay settings when a synchronous or asynchronous
// backend uses durable spooling. Delivery loggers do not own replay workers.
func WithSpool(cfg SpoolConfig) Option { return loggerruntime.WithSpool(cfg) }

// WithCloudWatchLogsStreamRouting selects stream partitioning for CloudWatch.
func WithCloudWatchLogsStreamRouting(routing StreamRouting) Option {
	return loggerruntime.WithCloudWatchLogsStreamRouting(routing)
}

// WithObservers supplies instrumentation. Nil dependencies use no-op observers.
// The caller retains ownership of observer providers and their shutdown.
func WithObservers(tracer Tracer, meter Meter) Option {
	return loggerruntime.WithObservers(tracer, meter)
}

// WithMemoryPressureMonitor supplies an already-started monitor, such as one
// from NewMemoryPressureMonitor, for overload policies. The caller owns its
// lifetime: loggers neither start, reset, nor stop it. Its policy replaces the
// MemoryCriticalRatio, MinAvailableMemoryBytes and
// MemoryCriticalConsecutiveHits of the spool settings. Without it, each spool
// that enables UseMemoryPressureForOverload starts and stops its own monitor
// with those settings.
func WithMemoryPressureMonitor(monitor MemoryPressureMonitor) Option {
	return loggerruntime.WithMemoryPressureMonitor(monitor)
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
func DefaultSpoolConfig() SpoolConfig { return bootstrap.DefaultSpoolConfig() }
