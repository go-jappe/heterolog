package fsstore

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidAppendLogRunnerInterval         = errors.New("appendlog runner interval must be positive")
	ErrInvalidAppendLogSupervisorRestartDelay = errors.New("appendlog runner supervisor restart delay must be positive")
	ErrInvalidAppendLogSupervisorMaxRestarts  = errors.New("appendlog runner supervisor max restarts must be positive")
)

// AppendLogRunnerConfig is the execution policy for the periodic loop and the
// optional Supervisor that apply to one appendlog lifecycle Runner.
type AppendLogRunnerConfig struct {
	// Interval is the time to wait before each lifecycle pass starts. A value of
	// zero or less is invalid.
	Interval time.Duration
	// StopOnError specifies whether a single lifecycle failure ends the periodic
	// loop of the Runner. When it is true, the caller receives the error that the
	// Runner returns and manages restarts if needed.
	StopOnError bool
	// Supervisor is the optional way to restart the Runner after it ends.
	Supervisor AppendLogRunnerSupervisorConfig
}

// AppendLogRunnerSupervisorConfig is the optional restart policy for when the
// Runner ends with an error or a panic. When Enabled is false, the Runner does
// not go through the Supervisor.
type AppendLogRunnerSupervisorConfig struct {
	// Enabled, when true, makes Runner.Run run the periodic loop supervised by
	// the Supervisor.
	Enabled bool
	// RestartDelay is the time to wait after the Runner ends before restarting
	// it. A value of zero or less is invalid.
	RestartDelay time.Duration
	// MaxRestarts is the maximum number of restarts that the Supervisor allows. A
	// value of zero or less is invalid.
	MaxRestarts int
}

// ValidateAppendLogRunnerConfig validates the numbers that the Runner and the
// optional Supervisor actually use. StopOnError without a Supervisor is not a
// configuration error, because the caller can handle the end of the Runner. The
// Supervisor values are not validated when Enabled is false, because they are
// not used.
func ValidateAppendLogRunnerConfig(config AppendLogRunnerConfig) error {
	var errs []error
	if config.Interval <= 0 {
		errs = append(errs, fmt.Errorf(
			"%w: %s",
			ErrInvalidAppendLogRunnerInterval,
			config.Interval,
		))
	}
	if config.Supervisor.Enabled {
		if config.Supervisor.RestartDelay <= 0 {
			errs = append(errs, fmt.Errorf(
				"%w: %s",
				ErrInvalidAppendLogSupervisorRestartDelay,
				config.Supervisor.RestartDelay,
			))
		}
		if config.Supervisor.MaxRestarts <= 0 {
			errs = append(errs, fmt.Errorf(
				"%w: %d",
				ErrInvalidAppendLogSupervisorMaxRestarts,
				config.Supervisor.MaxRestarts,
			))
		}
	}
	return errors.Join(errs...)
}
