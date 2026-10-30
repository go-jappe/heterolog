package objectstorage

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// CompressedLogFileUploadFailureStoreConfig holds the storage method used when
// a direct upload to object storage cannot be completed, for both the
// .log.<codec> files of ordinary logs and the .jsonl.<codec> archives that
// store records whose backend delivery failed.
type CompressedLogFileUploadFailureStoreConfig struct {
	DurableType        []failurestore.DurableType
	DurableStoreConfig CompressedLogFileUploadFailureDurableStoreConfig
}

// CompressedLogFileUploadFailureDurableStoreConfig holds the file archive
// settings of each route. Spool settings are taken from
// CompressedLogFileUploadSpool, and routes with the same storage location
// share a runtime.
type CompressedLogFileUploadFailureDurableStoreConfig struct {
	FileArchiveConfig CompressedLogFileUploadFailureArchiveConfig
}

// CompressedLogFileUploadFailureArchiveConfig configures the file archive,
// which stores a compressed log file as a single file directly under its
// category directory without changing its bytes or its original file name.
// RootPath is the root already resolved down to the compressed log file upload
// failure archive of the target application. LoggerContract separates the
// areas owned by logger, async_logger and delivery_logger, and by telemetry for
// the failure archive of the spans of heterolog.NewTelemetry. The areas of the
// object storage service and of the destination are built from the metadata of
// the destination.
type CompressedLogFileUploadFailureArchiveConfig struct {
	RootPath          string
	LoggerContract    string
	LifecycleInterval time.Duration
	Retryable         CompressedLogFileUploadFailureArchivePurgeConfig
	Permanent         CompressedLogFileUploadFailureArchivePurgeConfig
}

// CompressedLogFileUploadFailureArchivePurgeConfig holds the retention
// settings for one failure category. MaxFileAgeDays is judged by the date in
// the file name, and a file dated on the cutoff day is kept. MaxDirSize is in
// bytes; when it is exceeded, files are deleted oldest first by the date and
// then the numeric index in their names.
type CompressedLogFileUploadFailureArchivePurgeConfig struct {
	Enabled        bool
	MaxFileAgeDays int
	MaxDirSize     uint64
}

// CompressedLogFileUploadSpoolConfig holds the settings that the constructing
// side specifies for the spool that retries uploads of compressed logs. It has
// the storage location, the retry interval, the destination circuit breaker,
// and the retention policy for finished states. It is converted into a
// provider with NewCompressedLogFileUploadSpoolConfigAdapter before it is
// passed to the consuming side. The root for each object storage service is
// determined by combining it with the metadata of the destination.
// LoggerContract separates the spools owned by logger, async_logger and
// delivery_logger, and by telemetry for heterolog.NewTelemetry.
type CompressedLogFileUploadSpoolConfig struct {
	SpoolPath      string
	Target         string
	LoggerContract string

	RunnerInterval             time.Duration
	LifecycleInterval          time.Duration
	CircuitBreakerOpenCooldown time.Duration
	// MaxRetryAfter limits the per-destination retry wait that an upload
	// destination asks for with Retry-After. Zero means the default of one hour
	// and never means no limit. A negative value is a configuration error.
	MaxRetryAfter time.Duration

	Done       CompressedLogFileUploadSpoolPurgeConfig
	Dead       CompressedLogFileUploadSpoolPurgeConfig
	Quarantine CompressedLogFileUploadSpoolPurgeConfig
}

// CompressedLogFileUploadSpoolPurgeConfig applies only to compressed log files
// whose upload processing has become terminal. The active and processing
// states contain data that has not been uploaded, so no purge settings are
// exposed for them.
type CompressedLogFileUploadSpoolPurgeConfig struct {
	Enabled        bool
	MaxFileAgeDays int
	MaxDirSize     uint64
}
