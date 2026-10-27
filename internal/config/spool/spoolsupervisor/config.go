package spoolsupervisor

import (
	"time"
)

const (
	DefaultRestartDelay = time.Second
	DefaultMaxRestart   = 0 // 無制限
)
