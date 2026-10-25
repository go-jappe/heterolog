package spool

import (
	"github.com/go-jappe/heterolog/internal/config/spool/spoolrunner"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolsupervisor"
)

type SpoolConfig struct {
	SupervisorConfig spoolsupervisor.RunnerSupervisorConfig
	RunnerConfig     spoolrunner.SpoolRunnerConfig
}
