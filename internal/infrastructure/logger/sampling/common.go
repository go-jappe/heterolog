package sampling

import "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"

// ProcessLogSampling handles the common sampling logic for all loggers
// Returns a boolean indicating whether the log should be processed and
// whether it was sampled (if applicable)
func ProcessLogSampling(meta *logging.LogEntry, config SamplingRateConfig) (bool, bool) {
	// If sampling is disabled, process all logs without sampling
	if !config.IsSamplingEnabled() {
		return true, false
	}

	// If the log level requires mandatory logging, process without sampling
	if NeedLog(meta, config) {
		return true, false
	}

	// Check if the log should be sampled
	if !ShouldSampling(meta, config) {
		// Skip logs that don't meet sampling criteria
		return false, false
	}

	// Process the log with sampling flag
	return true, true
}

// ApplySamplingLogic applies the common sampling logic and updates the LogEntry
// Returns true if the log should be processed, false if it should be skipped
func ApplySamplingLogic(meta *logging.LogEntry, config SamplingRateConfig) bool {
	shouldProcess, wasSampled := ProcessLogSampling(meta, config)

	if shouldProcess {
		meta.Sampled = wasSampled
	}

	return shouldProcess
}
