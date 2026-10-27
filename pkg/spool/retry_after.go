package spool

import "time"

// RetryAfterClamp reports that a resend wait that a destination specified with
// Retry-After, or an earliest resend time already saved in the spool, was
// shortened to the configured maximum. Operators are notified of the
// shortening.
type RetryAfterClamp struct {
	// Requested is the wait before it was shortened. When a saved earliest
	// resend time was shortened, it is the time from when it was loaded until
	// the saved earliest resend time.
	Requested time.Duration
	// MaxRetryAfter is the maximum that was applied.
	MaxRetryAfter time.Duration
	// NotBefore is the earliest resend time after shortening.
	NotBefore time.Time
	// Stored reports that an earliest resend time saved in the spool was
	// shortened when it was loaded. When false, it reports that a wait
	// specified by a response of the destination was shortened.
	Stored bool
}

// CapRetryAfter returns retryAfter limited to at most maxRetryAfter, and
// whether it was shortened. A maxRetryAfter of 0 or less imposes no limit.
// Callers must pass a configuration value that has already been validated.
func CapRetryAfter(retryAfter, maxRetryAfter time.Duration) (time.Duration, bool) {
	if maxRetryAfter > 0 && retryAfter > maxRetryAfter {
		return maxRetryAfter, true
	}
	return retryAfter, false
}
