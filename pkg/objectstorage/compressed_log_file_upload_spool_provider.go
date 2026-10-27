package objectstorage

import "time"

// CompressedLogFileUploadSpoolConfigProvider supplies the settings of the
// spool that retries uploads of compressed logs. The constructing side decides
// the concrete type of the settings and how they are held. A shared runtime is
// identified by its resolved storage location; the value or pointer of this
// provider is not used as the identifier.
type CompressedLogFileUploadSpoolConfigProvider interface {
	SpoolPath() string
	Target() string
	LoggerContract() string
	RunnerInterval() time.Duration
	LifecycleInterval() time.Duration
	CircuitBreakerOpenCooldown() time.Duration
	// MaxRetryAfter returns the limit of the per-destination retry wait that
	// an upload destination asks for with Retry-After.
	MaxRetryAfter() time.Duration
	DonePurge() CompressedLogFileUploadSpoolPurgeConfigProvider
	DeadPurge() CompressedLogFileUploadSpoolPurgeConfigProvider
	QuarantinePurge() CompressedLogFileUploadSpoolPurgeConfigProvider
}

// CompressedLogFileUploadSpoolPurgeConfigProvider provides the retention
// settings of one state that files reach when their retry processing ends.
type CompressedLogFileUploadSpoolPurgeConfigProvider interface {
	Enabled() bool
	MaxFileAgeDays() int
	MaxDirSize() uint64
}
