// Package spoolsupervisor holds the settings of the supervisor that restarts
// spool replay workers.
package spoolsupervisor

import "time"

// RunnerSupervisorConfig controls replay worker restarts.
type RunnerSupervisorConfig struct {
	// RestartDelay is the wait after a worker stops abnormally before it runs
	// again. Zero and negative values are configuration errors.
	RestartDelay time.Duration
	// MaxRestarts is the maximum number of restarts; zero means no limit. A
	// negative value is a configuration error.
	MaxRestarts int
}
