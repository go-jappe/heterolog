package logger

import "errors"

// OutputSettings are the settings of an output that can change while a logger
// runs.
type OutputSettings struct {
	// Level is the lowest severity that the output records.
	Level LogLevel
	// Sampling configures how the output samples entries.
	Sampling SamplingConfig
}

// OutputSettingsChange changes the Level or Sampling of one output. A nil
// field keeps the current value.
type OutputSettingsChange struct {
	Level    *LogLevel
	Sampling *SamplingConfig
}

// SettingsController is a logger whose output settings can change while it
// runs. OutputSettings returns the current settings of each output, and
// UpdateOutputSettings checks every change before applying any of them.
type SettingsController interface {
	OutputSettings() map[LogOutputDestination]OutputSettings
	UpdateOutputSettings(changes map[LogOutputDestination]OutputSettingsChange) error
}

// ErrUnknownOutput reports a change to an output that the logger does not use.
var ErrUnknownOutput = errors.New("unknown log output")
