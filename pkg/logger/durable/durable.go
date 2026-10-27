package durable

import (
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// DurableType is an alias that keeps source compatibility with the existing
// logger failure-store API. The compressed log file upload failure side can use
// the same selection values without importing this package.
type DurableType = failurestore.DurableType

// SpillType is a direct evacuation destination used when no durable store is
// used. Only stdout is implemented at present. s3 and nfs are reserved values,
// and selecting one causes a startup error when the failure handler is built.
type SpillType string

type OverloadPolicy string   // policy when a durable store reaches its limit
type SpillDestination string // secondary evacuation destination for when the primary one is overloaded

const (
	// DurableTypeNone explicitly selects no storage method for the failure
	// store. When it is specified alone, neither the spool nor the file
	// archive is used.
	DurableTypeNone        = failurestore.DurableTypeNone
	DurableTypeFileArchive = failurestore.DurableTypeFileArchive
	DurableTypeSpool       = failurestore.DurableTypeSpool

	// SpillTypeNone explicitly selects no direct evacuation destination when
	// DurableTypeNone is selected. When EnableDegrade is true, it is a
	// contradictory configuration and causes a validation error.
	SpillTypeNone SpillType = "none"

	OverloadPolicyDrop    OverloadPolicy = "drop"
	OverloadPolicyDegrade OverloadPolicy = "degrade"

	SpillDestinationS3     SpillDestination = "s3"
	SpillDestinationStdout SpillDestination = "stdout"
	SpillDestinationNFS    SpillDestination = "nfs"
)

// FailureStoreConfig is the durable failure store configuration shared by all
// backends.
type FailureStoreConfig struct {
	// EnableDegrade controls whether failures are written directly to the
	// SpillType destinations. It applies only when DurableTypeNone is selected.
	// Overload handling while a store is in use is controlled separately by
	// each OverloadPolicy.
	EnableDegrade      bool
	MaxConcurrent      int // must be positive when a failure store or direct degrade is enabled
	DurableType        []DurableType
	SpillType          []SpillType // only stdout is implemented; s3/nfs are reserved values that cause a startup error when selected
	DurableStoreConfig DurableStoreConfig
}

// DeliveryFailureStoreConfig is the failure store configuration used after a
// Delivery logger fails to send to its final destination. A Delivery logger
// that sends immediately waits until saving completes and includes the result
// in the DeliveryReport. In a logger that accepts the sends themselves into a
// queue, later flushes use this configuration.
type DeliveryFailureStoreConfig struct {
	// EnableDegrade and SpillType control direct evacuation when
	// DurableTypeNone is selected.
	EnableDegrade     bool
	DurableType       []DurableType
	SpillType         []SpillType // only stdout is implemented; s3/nfs are reserved values that cause a startup error when selected
	FileArchiveConfig DeliveryFileArchiveConfig
}

// DurableStoreConfig is the durable store configuration shared by all
// backends.
type DurableStoreConfig struct {
	FileArchiveConfig FileArchiveConfig
	SpoolConfig       SpoolConfig
}

type SpoolConfig struct {
	SpoolPath string // ex.)/var/lib/{appname}/spool (the /var/lib/ part)
	Target    string // (the appname part)
	Dir       string // ex.)/var/lib/{appname}/spool/{Dir} (the Dir part)

	OverloadPolicyConfig SpoolOverloadPolicyConfig
	HandlerConfig        handler.HandlerConfig
}

// SpoolOverloadPolicyConfig configures how processing is decided on overload.
//
// It holds the thresholds that decide whether retryable failure data may be
// saved to the spool. Its main use is to check, before data for a retry is
// evacuated to the active spool when sending to a backend fails, whether that
// storage itself is overloaded or running short of resources.
//
// Designing each overload threshold so that its zero value disables it makes
// operation easier. Only ActiveBacklogWarnAfterDays is a threshold for backlog
// warnings that does not affect whether data may be saved.
//
// When an enabled condition other than ActiveBacklogWarnAfterDays is hit, the
// data is dropped or evacuated to SpillDestination according to
// OverloadPolicy.
type SpoolOverloadPolicyConfig struct {
	// OverloadPolicy is the course of action when overload is detected.
	//
	// drop:
	//   gives up saving to the spool and discards the data.
	//
	// degrade:
	//   evacuates the data to SpillDestination, outside the spool.
	OverloadPolicy OverloadPolicy

	// SpillDestination is the secondary evacuation destination when
	// OverloadPolicy is degrade. Only stdout is implemented at present.
	//
	// It is the alternative storage used when the spool cannot accept any more
	// data.
	//
	// With degrade, any other value, including the zero value, causes a
	// startup error when the failure handler is built.
	SpillDestination SpillDestination

	// MaxCallsSinceScan rescans the directory statistics, even before
	// StatsScanInterval expires, when the number of overload checks since the
	// previous statistics scan, including the current one, reaches it.
	//
	// It is an upper bound that keeps old statistics from being used on and on
	// even when a large number of save checks run in a short time.
	//
	// 0 disables rescans based on the number of calls.
	MaxCallsSinceScan uint64

	// StatsScanInterval is how long the directory statistics kept for the
	// overload check remain valid. After this period has passed, the
	// statistics are rescanned at the next overload check.
	//
	// When it is 0 or less, the statistics are rescanned every time.
	StatsScanInterval time.Duration

	// MaxDirSize is the maximum total size under the directory. When the total
	// exceeds this value, new retryable data is not saved to the spool.
	//
	// Unit: bytes
	//
	// 0 disables it.
	MaxDirSize uint64

	// ActiveBacklogWarnAfterDays is the age in days of the oldest file in the
	// active directory at which a backlog warning is issued. When the oldest
	// data is older than this many days, a Warn is issued once a day, but this
	// is not treated as overload and new retryable data continues to be saved.
	//
	// Unit: days
	//
	// 0 disables it. A negative value is a configuration error.
	ActiveBacklogWarnAfterDays int

	// MinAvailableDiskSpace is the minimum free disk space to keep on the host
	// or in the execution environment. When the available free disk space
	// falls below this value, new retryable data is not saved to the spool.
	//
	// Unit: bytes
	//
	// 0 disables it.
	MinAvailableDiskSpace uint64

	// UseMemoryPressureForOverload selects whether memory pressure is taken
	// into account in the overload check. When it is true and no monitor is
	// passed with heterolog.WithMemoryPressureMonitor, the spool starts its own
	// monitor, which reads the memory pressure of the operating system every
	// second and judges each reading with MemoryCriticalRatio,
	// MinAvailableMemoryBytes and MemoryCriticalConsecutiveHits; the monitor
	// stops when the logger closes. A monitor passed with
	// heterolog.WithMemoryPressureMonitor applies its own policy instead of
	// these three settings. The logger fails to start when the settings enable
	// no threshold or the memory pressure cannot be read.
	UseMemoryPressureForOverload bool

	// MemoryCriticalRatio is the usage ratio, from 0 to 1, at or above which a
	// memory reading is a critical candidate. For example, with 0.90, a usage
	// of 90% or more is a candidate. 0 or less disables this condition.
	MemoryCriticalRatio float64

	// MinAvailableMemoryBytes is the amount of available memory, in bytes, at
	// or below which a memory reading is a critical candidate. 0 disables this
	// condition.
	MinAvailableMemoryBytes uint64

	// MemoryCriticalConsecutiveHits is how many readings in a row must be
	// critical candidates before memory counts as overloaded, so that a short
	// spike does not stop saving to the spool at once. 0 and 1 mean the first
	// candidate.
	MemoryCriticalConsecutiveHits int
}

// FileArchiveConfig configures the failure archive, the durable store that
// DurableTypeFileArchive selects.
type FileArchiveConfig struct {
	Target                 string
	MaxFileSize            uint64
	FailureFileArchivePath string // ex.)/var/lib/{appname} (an example for Linux)
	Dir                    string // ex.)/var/lib/{appname}/{dir}/
	DirCaseRetryable       string // ex.)/var/lib/{appname}/{dir}/{retryable}
	DirCasePermanent       string // ex.)/var/lib/{appname}/{dir}/{permanent}
	FileNameFormat         string
	FileDateFormat         string
	EnableRotate           bool
	UsageCacheTTL          time.Duration
	// MaxFileBackups, MaxFileAge and MaxDirSize are the limits on the number of
	// kept generations, the number of kept days, and the total bytes of the
	// directory. 0 means no limit, and nothing is deleted by that condition. A
	// nonzero value must be at least 2 for the kept generations, at least 1 for
	// the kept days, and for MaxDirSize at least 10 MiB and at least
	// MaxFileSize. Even without limits, a new failure that occurs while the
	// free space is below OverloadPolicyConfig.MinAvailableDiskSpace is not
	// saved to the archive.
	MaxFileBackups     int
	MaxFileAge         int
	MaxDirSize         uint64
	EnableFileCompress bool
	CompressFormat     fsstore.CompressFormat
	// CompressionCodec is a custom compression format added programmatically.
	// When it is nil, the built-in codec for CompressFormat is used.
	CompressionCodec  fsstore.CompressionCodec
	RotationType      fsstore.FileRotationType // daily weekly monthly
	RotationDayOfWeek time.Weekday
	// MaxWriteRetries is how many times a failed file operation, such as a write
	// or opening the next file at rotation, is retried after the first try. 0
	// tries once without retrying, and a negative value is invalid.
	MaxWriteRetries int
	// FileOperationRetryWaitTime is the wait before each retry.
	FileOperationRetryWaitTime time.Duration
	ObjectStorageUploadConfig  objectstorage.UploadTargetConfig
	ArchiveCheckpointConfig    fsstore.ArchiveCheckpointConfig
	// LifecycleRunnerConfig is the periodic loop and the optional Supervisor
	// configuration of runner mode.
	LifecycleRunnerConfig fsstore.AppendLogRunnerConfig
	// LifecycleMode selects what runs the compress, upload and purge steps
	// after rotation. An empty value is treated as inline, the same as before
	// LifecycleMode was introduced.
	LifecycleMode fsstore.FileLifecycleMode

	OverloadPolicyConfig FileArchiveOverloadPolicyConfig
	HandlerConfig        handler.HandlerConfig

	IsDebug                  bool
	ErrorTraceDepth          int // inherits the error trace setting of the backend logger
	ErrorTraceDepthDebugMode int // inherits the debug-mode error trace setting of the backend logger
}

type DeliveryFileArchiveConfig struct {
	Target                 string
	MaxFileSize            uint64
	FailureFileArchivePath string // ex.)/var/lib/{appname} (an example for Linux)
	Dir                    string // ex.)/var/lib/{appname}/{dir}/
	DirCaseRetryable       string // ex.)/var/lib/{appname}/{dir}/{retryable}
	DirCasePermanent       string // ex.)/var/lib/{appname}/{dir}/{permanent}
	FileNameFormat         string
	FileDateFormat         string
	EnableRotate           bool
	UsageCacheTTL          time.Duration
	// MaxFileBackups, MaxFileAge and MaxDirSize have the same meaning, and the
	// same handling of 0 (no limit), as in FileArchiveConfig.
	MaxFileBackups     int
	MaxFileAge         int
	MaxDirSize         uint64
	EnableFileCompress bool
	CompressFormat     fsstore.CompressFormat
	// CompressionCodec has the same contract as
	// FileArchiveConfig.CompressionCodec.
	CompressionCodec  fsstore.CompressionCodec
	RotationType      fsstore.FileRotationType // daily weekly monthly
	RotationDayOfWeek time.Weekday
	// MaxWriteRetries is how many times a failed file operation, such as a write
	// or opening the next file at rotation, is retried after the first try. 0
	// tries once without retrying, and a negative value is invalid.
	MaxWriteRetries int
	// FileOperationRetryWaitTime is the wait before each retry.
	FileOperationRetryWaitTime time.Duration
	ObjectStorageUploadConfig  objectstorage.UploadTargetConfig
	ArchiveCheckpointConfig    fsstore.ArchiveCheckpointConfig
	// LifecycleRunnerConfig has the same meaning as in FileArchiveConfig.
	LifecycleRunnerConfig fsstore.AppendLogRunnerConfig
	// LifecycleMode has the same meaning, and the same handling of an empty
	// value, as in FileArchiveConfig.
	LifecycleMode fsstore.FileLifecycleMode

	OverloadPolicyConfig FileArchiveOverloadPolicyConfig
	HandlerConfig        handler.HandlerConfig

	IsDebug                  bool
	ErrorTraceDepth          int // inherits the error trace setting of the backend logger
	ErrorTraceDepthDebugMode int // inherits the debug-mode error trace setting of the backend logger
}

// MinFileArchiveAvailableDiskSpace is the smallest value that can be set as
// the free space threshold of the failure archive.
const MinFileArchiveAvailableDiskSpace uint64 = 100 * 1024 * 1024

// FileArchiveOverloadPolicyConfig configures overload detection for the
// failure archive and how failures are evacuated.
type FileArchiveOverloadPolicyConfig struct {
	OverloadPolicy        OverloadPolicy   // drop or degrade
	SpillDestination      SpillDestination // secondary destination when the archive cannot accept failures (degrade); only stdout is implemented
	MinAvailableDiskSpace uint64           // failures are not saved to the archive while free space is below this
}
