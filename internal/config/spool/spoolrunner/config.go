package spoolrunner

import (
	"time"
)

const (
	DefaultRunnerInterval = 100 * time.Millisecond
	DefaultStopOnError    = false

	DefaultFinalizeRetryDelay = 1000 * time.Millisecond
	DefaultMaxFinalizeEntries = 4098

	// DefaultMaxRetryAfterは、backendがRetry-Afterで指定した再送待機に適用する上限の既定値です。
	DefaultMaxRetryAfter = time.Hour
)
