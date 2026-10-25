package appendlogrunner

import "time"

const (
	DefaultInterval          = 15 * time.Minute
	DefaultStopOnError       = false
	DefaultSupervisorEnabled = false
	DefaultRestartDelay      = time.Second
	DefaultMaxRestarts       = 3
)

// Configはapplication内のappendlog lifecycle Runnerに共通して適用する実行設定です。
// 個々のfile保存先やlifecycle処理内容ではなく、周期loopと再起動方法だけを保持します。
type Config struct {
	Interval    time.Duration
	StopOnError bool
	Supervisor  SupervisorConfig
}

// SupervisorConfigはoptional Supervisorの有効化と再起動方針です。
type SupervisorConfig struct {
	Enabled      bool
	RestartDelay time.Duration
	MaxRestarts  int
}
