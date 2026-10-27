package spool

import (
	"github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	"github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

// SpoolConfig configures background replay of durable failure spools.
type SpoolConfig struct {
	SupervisorConfig spoolsupervisor.RunnerSupervisorConfig
	RunnerConfig     spoolrunner.SpoolRunnerConfig
}
