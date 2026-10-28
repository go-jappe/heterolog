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

// OutputSettingsChange changes the Level or some Sampling fields of one output.
// A nil field keeps the current value.
type OutputSettingsChange struct {
	Level    *LogLevel
	Sampling *SamplingChange
}

// SamplingChange changes the fields of the SamplingConfig of one output that
// it sets. A nil field keeps the current value, so changes to different
// fields, made at the same time, do not undo each other.
type SamplingChange struct {
	IsSamplingEnabled        *bool
	EnableLoggingByRequestID *bool
	LoggingByRequestIDRate   *int
	SamplingDebugRate        *int
	SamplingInfoRate         *int
	SamplingWarnRate         *int
	SamplingErrorRate        *int
}

// AppliedOutputSettings holds the settings of one output right before and
// right after UpdateOutputSettings changed it. Both are read while no other
// change of the logger can run, so they show exactly what the change did.
type AppliedOutputSettings struct {
	Before OutputSettings
	After  OutputSettings
}

// SettingsController is a logger whose output settings can change while it
// runs. OutputSettings returns the current settings of each output.
// UpdateOutputSettings applies each change to the settings current when it
// runs, checks every result before applying any of them, and returns the
// settings of each changed output right before and after the change.
type SettingsController interface {
	OutputSettings() map[LogOutputDestination]OutputSettings
	UpdateOutputSettings(changes map[LogOutputDestination]OutputSettingsChange) (map[LogOutputDestination]AppliedOutputSettings, error)
}

// ErrUnknownOutput reports a change to an output that the logger does not use.
var ErrUnknownOutput = errors.New("unknown log output")
