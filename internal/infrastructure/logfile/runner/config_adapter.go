package runner

import (
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

type runnerConfigAdapter struct {
	config pkgfsstore.AppendLogRunnerConfig
}

func (a runnerConfigAdapter) Interval() time.Duration { return a.config.Interval }
func (a runnerConfigAdapter) StopOnError() bool       { return a.config.StopOnError }

type supervisorConfigAdapter struct {
	config pkgfsstore.AppendLogRunnerSupervisorConfig
}

func (a supervisorConfigAdapter) RestartDelay() time.Duration { return a.config.RestartDelay }
func (a supervisorConfigAdapter) MaxRestarts() int            { return a.config.MaxRestarts }

// configuredRunnerDependenciesはtyped configをRunnerのinterface契約とconstructor optionへ
// 変換します。SupervisorはEnabled=trueの場合だけ生成し、各Runnerが独立した再起動回数を持ちます。
func configuredRunnerDependencies(
	config pkgfsstore.AppendLogRunnerConfig,
) (RunnerConfig, []RunnerOption) {
	runnerConfig := runnerConfigAdapter{config: config}
	if !config.Supervisor.Enabled {
		return runnerConfig, nil
	}
	return runnerConfig, []RunnerOption{
		WithSupervisor(supervisorConfigAdapter{config: config.Supervisor}),
	}
}
