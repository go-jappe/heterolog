package spoolsupervisor

import "time"

type RunnerSupervisorConfig struct {
	RestartDelay time.Duration
	MaxRestarts  int
}
