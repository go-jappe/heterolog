package spoolrunner

import "time"

type RunnerConfig struct {
	Interval    time.Duration
	StopOnError bool
}
