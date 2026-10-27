package heterolog

import (
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// LogOutputDestination names an output, such as stdout or opensearch.
type LogOutputDestination = pkglogger.LogOutputDestination

// SamplingConfig configures how an output samples entries. See pkg/logger.
type SamplingConfig = pkglogger.SamplingConfig

// OutputSettings are the settings of an output that can change while a logger
// runs: its Level, the lowest severity it records, and its Sampling.
type OutputSettings = pkglogger.OutputSettings

// OutputSettingsChange changes the Level or Sampling of one output. A nil
// field keeps the current value.
type OutputSettingsChange = pkglogger.OutputSettingsChange

// ErrUnknownOutput reports that UpdateOutputSettings named an output that the
// logger does not use.
var ErrUnknownOutput = pkglogger.ErrUnknownOutput

// OutputSettings returns the current settings of each output of the logger.
// Outputs used only for internal log notifications are not included.
func (r *Runtime) OutputSettings() map[LogOutputDestination]OutputSettings {
	return r.impl().OutputSettings()
}

// UpdateOutputSettings changes the level and sampling of outputs while the
// logger runs. It checks every change first and applies none of them when any
// is invalid; an output the logger does not use is reported as
// ErrUnknownOutput, and an invalid value as a pkg/logger ConfigFieldError.
// Changes take effect from the next entry and are not kept after the process
// exits.
func (r *Runtime) UpdateOutputSettings(changes map[LogOutputDestination]OutputSettingsChange) error {
	return r.impl().UpdateOutputSettings(changes)
}

// OutputSettings returns the current settings of each output of the logger,
// like Runtime.OutputSettings.
func (r *AsyncRuntime) OutputSettings() map[LogOutputDestination]OutputSettings {
	return r.impl().OutputSettings()
}

// UpdateOutputSettings changes the level and sampling of outputs while the
// logger runs, like Runtime.UpdateOutputSettings. An asynchronous logger
// applies the settings in effect when it processes an entry.
func (r *AsyncRuntime) UpdateOutputSettings(changes map[LogOutputDestination]OutputSettingsChange) error {
	return r.impl().UpdateOutputSettings(changes)
}
