package spoolsupervisor

import "time"

const (
	DefaultRestartDelay = time.Second
	DefaultMaxRestart   = 0 // 無制限
)

// spool runnnerを監視し　落ちたら再起動を管理する
type RunnerSupervisorConfig struct {
	// RestartDelay は、Runner が異常停止した後、
	// 次に Runner.Run を呼び直すまでの待機時間。
	//
	// 0以下は設定ミスとして error にする。
	RestartDelay time.Duration

	// MaxRestarts は、最大再起動回数。
	//
	// 0 の場合は無制限。
	// 負数は設定ミスとして error にする。
	MaxRestarts int
}
