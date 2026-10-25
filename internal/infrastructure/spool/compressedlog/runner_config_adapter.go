package compressedlog

import "time"

// RunnerConfigAdapterはrunner設定を値として保持し、補完や検証をせずに返します。
type RunnerConfigAdapter struct {
	config RunnerConfig
}

var _ RunnerConfigProvider = RunnerConfigAdapter{}

func NewRunnerConfigAdapter(config RunnerConfig) RunnerConfigAdapter {
	return RunnerConfigAdapter{config: config}
}

func (a RunnerConfigAdapter) Interval() time.Duration {
	return a.config.Interval
}

func (a RunnerConfigAdapter) PurgeInterval() time.Duration {
	return a.config.PurgeInterval
}
