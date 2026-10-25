package fsstore

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidAppendLogRunnerInterval         = errors.New("appendlog runner interval must be positive")
	ErrInvalidAppendLogSupervisorRestartDelay = errors.New("appendlog runner supervisor restart delay must be positive")
	ErrInvalidAppendLogSupervisorMaxRestarts  = errors.New("appendlog runner supervisor max restarts must be positive")
)

// AppendLogRunnerConfigは、一つのappendlog lifecycle Runnerに適用する周期loopと
// optional Supervisorの実行方針です。
type AppendLogRunnerConfig struct {
	// Intervalは各lifecycle処理を開始するまでの待機時間です。0以下は不正です。
	Interval time.Duration
	// StopOnErrorは一回のlifecycle失敗でRunnerの周期loopを終了するかを指定します。
	// trueの場合、呼び出し側はRunnerが返すerrorを受け取り、必要なら再起動を管理します。
	StopOnError bool
	// SupervisorはRunner終了後のoptionalな再起動方法です。
	Supervisor AppendLogRunnerSupervisorConfig
}

// AppendLogRunnerSupervisorConfigは、Runnerがerrorまたはpanicで終了した場合の
// optionalな再起動方針です。Enabled=falseの場合、RunnerはSupervisorを経由しません。
type AppendLogRunnerSupervisorConfig struct {
	// Enabledがtrueの場合、Runner.RunはSupervisorの監視下で周期loopを実行します。
	Enabled bool
	// RestartDelayはRunner終了後に再起動するまでの待機時間です。0以下は不正です。
	RestartDelay time.Duration
	// MaxRestartsはSupervisorが許可する最大再起動回数です。0以下は不正です。
	MaxRestarts int
}

// ValidateAppendLogRunnerConfigは、Runnerとoptional Supervisorが実際に使用する数値を
// 検証します。SupervisorなしのStopOnErrorは、Runnerの終了を呼び出し側が扱えるため
// 設定不正にはしません。Enabled=falseのSupervisor値は使用しないため検証しません。
func ValidateAppendLogRunnerConfig(config AppendLogRunnerConfig) error {
	var errs []error
	if config.Interval <= 0 {
		errs = append(errs, fmt.Errorf(
			"%w: %s",
			ErrInvalidAppendLogRunnerInterval,
			config.Interval,
		))
	}
	if config.Supervisor.Enabled {
		if config.Supervisor.RestartDelay <= 0 {
			errs = append(errs, fmt.Errorf(
				"%w: %s",
				ErrInvalidAppendLogSupervisorRestartDelay,
				config.Supervisor.RestartDelay,
			))
		}
		if config.Supervisor.MaxRestarts <= 0 {
			errs = append(errs, fmt.Errorf(
				"%w: %d",
				ErrInvalidAppendLogSupervisorMaxRestarts,
				config.Supervisor.MaxRestarts,
			))
		}
	}
	return errors.Join(errs...)
}
