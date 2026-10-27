// Package spoolrunner holds the settings of the workers that replay durable
// failure spools.
package spoolrunner

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
)

// SpoolRunnerConfig supplies separate replay settings for each backend.
type SpoolRunnerConfig struct {
	OpensearchConfig     BackendSpoolRunnerConfig
	LokiConfig           BackendSpoolRunnerConfig
	CloudWatchLogsConfig BackendSpoolRunnerConfig
	ClickHouseConfig     BackendSpoolRunnerConfig
}

// BackendSpoolRunnerConfig controls one backend's replay worker and the
// retention of its spool records.
type BackendSpoolRunnerConfig struct {
	Runner RunnerConfig
	// LifeCycle controls the retention of the backend's spool records.
	LifeCycle spoollifecycle.LifecycleConfig
	Worker    WorkerConfig
}

// RunnerConfig controls replay polling.
type RunnerConfig struct {
	// Interval is the wait between the end of one replay pass and the start of
	// the next. It must be positive; zero and negative values are not replaced
	// by a default.
	Interval time.Duration
	// StopOnError stops polling when a replay pass returns an error.
	StopOnError bool
}

// WorkerConfig controls replay finalization and the maximum wait for a
// backend-specified Retry-After.
type WorkerConfig struct {
	Finalize FinalizeConfig
	// MaxRetryAfter limits the replay wait that a backend asks for with
	// Retry-After; a longer wait is shortened to it, with a warning in the
	// internal log. It applies to waits received by regular sends and to waits
	// already saved in the spool. Zero means the default of one hour, for
	// compatibility with configurations without this field, and never means no
	// limit. A negative value is a configuration error.
	MaxRetryAfter time.Duration
}

// FinalizeConfig bounds finalization retry state.
type FinalizeConfig struct {
	// RetryDelay is the wait before retrying a failed state change of a
	// processing file. It must be positive; zero and negative values are not
	// replaced by a default.
	RetryDelay time.Duration
	// MaxEntries is the maximum number of state-change retries kept in memory.
	// It must be positive.
	MaxEntries int
}
