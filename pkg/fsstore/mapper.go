package fsstore

import (
	"github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"time"
)

// OverloadPolicy is the policy for log file writes while free disk space is
// below FileArchiveOverloadPolicyConfig.MinAvailableDiskSpace.
type OverloadPolicy string

// SpillDestination is where the degrade policy writes logs instead of the log
// file while free disk space is below its limit.
type SpillDestination string

type Config struct {
	Layout    FsstoreConfig
	Lifecycle FileLifecycleConfig
	Overload  FileArchiveOverloadPolicyConfig
	Duration  DurationConfig
	Retry     RetryConfig
	// CompressionCodec is a custom compression format added programmatically. If
	// it is nil, the built-in codec for Lifecycle.CompressFormat is used.
	CompressionCodec CompressionCodec
}

// ArchiveCheckpointConfig holds the destination of the records of completed log
// file uploads, which inline and runner modes share, and the logical source
// identity before the file role is determined. The factory adds
// default/error/debug or retryable/permanent and a fingerprint of the actual
// file pattern to SourcePrefix to build the final source ID.
type ArchiveCheckpointConfig struct {
	// Directory is the destination dedicated to checkpoints. Links in its parent
	// path are resolved to their targets at construction, but the directory is
	// rejected if it is itself a symbolic link or a junction.
	Directory    string
	SourcePrefix string
	// AutoRecoverMissingCheckpoint, when true, recovers by starting from an empty
	// cursor when the checkpoint of a log series with registered destinations is
	// missing. Logs may be uploaded again, including logs that remain from before
	// a destination was added. false makes startup fail when the checkpoint is
	// missing. Creating it for the first time is allowed. Corrupt JSON,
	// permission errors and problems with the destination registration manifest
	// are not recovered from.
	AutoRecoverMissingCheckpoint bool
}

// FileLifecycleMode identifies what runs compression, object storage upload and
// purge after rotation. The default behavior for an empty value depends on the
// Logger contract that uses it. The normal FileLogger treats it as inline, so
// that it can be used without this setting, and the Delivery FileLogger does
// not start the lifecycle, because it has no inline processing. To make the
// behavior explicit, specify one of the constants below.
type FileLifecycleMode string

const (
	// FileLifecycleModeDisable does not start compression, object storage upload
	// or purge after rotation. The writer still stores logs and rotates files.
	FileLifecycleModeDisable FileLifecycleMode = "disable"
	// FileLifecycleModeInline runs the lifecycle processing in the write and
	// rotation path of the writer.
	FileLifecycleModeInline FileLifecycleMode = "inline"
	// FileLifecycleModeRunner runs the lifecycle processing periodically in a
	// dedicated appendlog lifecycle runner.
	FileLifecycleModeRunner FileLifecycleMode = "runner"
)

// LogFileLifecycleRunnerConfig is the configuration for the lifecycle runner of
// the normal file logger. It is kept apart from the write settings of the
// logger and holds only the settings needed for compress, archive and purge.
type LogFileLifecycleRunnerConfig struct {
	FsstoreConfig             Config
	ObjectStorageUploadConfig objectstorage.LogFileUploadConfig
	ArchiveCheckpointConfig   ArchiveCheckpointConfig
	// RunnerConfig is the execution policy for the periodic loop and the
	// optional Supervisor.
	RunnerConfig AppendLogRunnerConfig
	// HandlerConfig is the destination selector to which the results of the
	// periodic runs are delegated. The handler selector specifies only backend
	// names, and whether a destination dedicated to internal logs exists is
	// determined by the destination settings of the same LoggerContractKind.
	// The selector of each level is kept independently, and purge failures are
	// reported as Error without being downgraded to Warn.
	HandlerConfig handler.HandlerConfig
}

type DeliveryFileLifecycleStoreConfig struct {
	Layout    FsstoreConfig
	Lifecycle DeliveryFileLifecycleConfig
	Duration  DurationConfig
	Retry     RetryConfig
	// CompressionCodec has the same contract as Config.CompressionCodec.
	CompressionCodec CompressionCodec
}

type DeliveryFileWriteConfig struct {
	Layout   FsstoreConfig
	Rotation FileRotationConfig
	Duration DurationConfig
	Retry    RetryConfig
}

// DeliveryFileLifecycleRunnerConfig is the configuration for the lifecycle
// runner of delivery log files. It is kept apart from the write settings of the
// logger and holds only the settings needed for compress, archive and purge.
type DeliveryFileLifecycleRunnerConfig struct {
	FsstoreConfig             DeliveryFileLifecycleStoreConfig
	ObjectStorageUploadConfig objectstorage.LogFileUploadConfig
	ArchiveCheckpointConfig   ArchiveCheckpointConfig
	// RunnerConfig has the same meaning as in LogFileLifecycleRunnerConfig.
	RunnerConfig AppendLogRunnerConfig
	// HandlerConfig has the same per-level contract as in
	// LogFileLifecycleRunnerConfig.
	HandlerConfig handler.HandlerConfig
}

// FsstoreConfig configures the destination and naming rules of a file logger.
// When both error and debug logs are separated, a configuration in which only
// the error and debug logs share a directory other than the default
// destination is not allowed.
type FsstoreConfig struct {
	Target      string // namespace of the log destination
	LogPath     string // parent directory of the {target} directory, ex.) /var/log for /var/log/{target}/
	LogDir      string // subdirectory name for normal logs; when used, it must not be empty or only whitespace
	ErrorLogDir string
	// DebugLogDir is the destination when IsSeparateDebugLogDirEnabled is true.
	// When that is enabled, it must not be empty. When it is disabled, this
	// value is ignored and LogDir is shared.
	DebugLogDir string
	// IsSeparateDebugLogDirEnabled selects whether a directory is specified
	// separately for debug logs. It is independent of
	// IsSeparateDebugLogsEnabled, which writes debug logs to separate files.
	IsSeparateDebugLogDirEnabled bool
	IsSeparateErrorLogsEnabled   bool
	SeparateErrorLogsPrefix      string
	IsSeparateDebugLogsEnabled   bool
	SeparateDebugLogsPrefix      string
	FileNameFormat               string
	FileDateFormat               string
}

type FileLifecycleConfig struct {
	EnableRotate      bool
	RotationType      FileRotationType
	RotationDayOfWeek time.Weekday
	// UsageCacheTTL is how long the estimated usage last measured from the
	// filesystem can be used without measuring it again. The next inline cleanup
	// after the TTL has elapsed measures it again. When the estimate exceeds the
	// limit, it does not wait for the TTL to elapse, unless the last measurement
	// still found the usage over the limit after purging.
	UsageCacheTTL      time.Duration
	MaxFileBackups     int
	MaxFileAge         int
	MaxFileSize        uint64
	MaxDirSize         uint64
	EnableFileCompress bool
	CompressFormat     CompressFormat
	// ErrorPurge and DebugPurge are the retention settings of each separated log
	// series. If one is nil, it inherits MaxFileBackups/MaxFileAge/MaxDirSize of
	// the normal logs. When one is set, all three items are required, and a 0 is
	// not filled in as if it were unset. Only when the series shares a directory
	// with the normal logs does MaxDirSize use the value of the normal logs.
	ErrorPurge *FilePurgeConfig
	DebugPurge *FilePurgeConfig
}

// FilePurgeConfig holds the retention limits of one log series. The number of
// generations and the number of days apply to the series, and the size limit
// applies to the whole destination directory. When the size is exceeded, files
// that have not reached the generation or day limits may also be deleted.
type FilePurgeConfig struct {
	MaxFileBackups int    // number of generations to keep, counted in generations determined by the date and number in the file name, whether compressed or not
	MaxFileAge     int    // number of days to keep
	MaxDirSize     uint64 // size in bytes of the whole directory
}

type FileRotationConfig struct {
	EnableRotate      bool
	RotationType      FileRotationType
	RotationDayOfWeek time.Weekday
	MaxFileSize       uint64
}

type DeliveryFileLifecycleConfig struct {
	EnableRotate       bool
	RotationType       FileRotationType
	RotationDayOfWeek  time.Weekday
	UsageCacheTTL      time.Duration
	MaxFileBackups     int
	MaxFileAge         int
	MaxFileSize        uint64
	MaxDirSize         uint64
	EnableFileCompress bool
	CompressFormat     CompressFormat
	// ErrorPurge and DebugPurge have the same contract for inheritance and
	// directory sharing as in FileLifecycleConfig.
	ErrorPurge *FilePurgeConfig
	DebugPurge *FilePurgeConfig
}

// FileArchiveOverloadPolicyConfig configures how an overload is handled.
// Overload is judged by disk space only.
type FileArchiveOverloadPolicyConfig struct {
	OverloadPolicy        OverloadPolicy   // drop or degrade
	SpillDestination      SpillDestination // evacuation destination for degrade; the normal file logger supports only stdout
	MinAvailableDiskSpace uint64           // normal file writes are not performed when free space falls below this value; 0 disables the check
}

type DurationConfig struct {
	CompressWarnThresholdDuration   time.Duration
	CompressErrorThresholdDuration  time.Duration
	WriteFileWarnThresholdDuration  time.Duration
	WriteFileErrorThresholdDuration time.Duration
	WriteFileLatencyEnabled         bool
}

// RetryConfig holds how failed file operations are retried.
type RetryConfig struct {
	// MaxWriteRetries is how many times a failed file operation, such as a write
	// or opening the next file at rotation, is retried after the first try. 0
	// tries once without retrying, and a negative value is invalid.
	MaxWriteRetries int
	// FileOperationRetryWaitTime is the wait before each retry.
	FileOperationRetryWaitTime time.Duration
}
