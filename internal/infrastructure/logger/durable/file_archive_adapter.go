package durable

import (
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// FileArchiveConfigAdapterは公開設定を、保存・送信・runner・loggerの各利用側の
// 設定interfaceへ変換します。
type FileArchiveConfigAdapter struct {
	target                     string
	lifecycleMode              fsstore.FileLifecycleMode
	rotationType               fsstore.FileRotationType
	rotationDayOfWeek          time.Weekday
	fileNameFormat             string
	fileDateFormat             string
	maxFileSize                uint64
	usageCacheTTL              time.Duration
	maxDirSize                 uint64
	maxFileAge                 int
	maxFileBackups             int
	enableFileCompress         bool
	compressFormat             fsstore.CompressFormat
	compressionCodec           fsstore.CompressionCodec
	archivePath                string
	dir                        string
	dirCaseRetryable           string
	dirCasePermanent           string
	enableRotate               bool
	maxWriteAttempts           int
	fileOperationRetryWaitTime time.Duration
	lifecycleRunnerConfig      fsstore.AppendLogRunnerConfig
	objectStorageUpload        objectstorage.UploadTargetConfig
	archiveCheckpoint          fsstore.ArchiveCheckpointConfig
	overloadPolicyConfig       FileArchiveOverloadPolicyConfigProvider
	handler                    loggerhandler.ConfigProvider
	isDebug                    bool
	errorTraceDepth            int
	errorTraceDepthDebugMode   int
}

var (
	_ FileArchiveConfigProvider               = FileArchiveConfigAdapter{}
	_ failurearchive.CompressionCodecProvider = FileArchiveConfigAdapter{}
)

func NewFileArchiveConfigAdapter(cfg pkgdurable.FileArchiveConfig) FileArchiveConfigAdapter {
	lifecycleMode := cfg.LifecycleMode
	if lifecycleMode == "" {
		lifecycleMode = fsstore.FileLifecycleModeInline
	}
	return FileArchiveConfigAdapter{
		target:                     cfg.Target,
		lifecycleMode:              lifecycleMode,
		rotationType:               cfg.RotationType,
		rotationDayOfWeek:          cfg.RotationDayOfWeek,
		fileNameFormat:             cfg.FileNameFormat,
		fileDateFormat:             cfg.FileDateFormat,
		maxFileSize:                cfg.MaxFileSize,
		usageCacheTTL:              cfg.UsageCacheTTL,
		maxDirSize:                 cfg.MaxDirSize,
		maxFileAge:                 cfg.MaxFileAge,
		maxFileBackups:             cfg.MaxFileBackups,
		enableFileCompress:         cfg.EnableFileCompress,
		compressFormat:             cfg.CompressFormat,
		compressionCodec:           cfg.CompressionCodec,
		archivePath:                cfg.FailureFileArchivePath,
		dir:                        cfg.Dir,
		dirCaseRetryable:           cfg.DirCaseRetryable,
		dirCasePermanent:           cfg.DirCasePermanent,
		enableRotate:               cfg.EnableRotate,
		maxWriteAttempts:           cfg.MaxWriteAttempts,
		fileOperationRetryWaitTime: cfg.FileOperationRetryWaitTime,
		lifecycleRunnerConfig:      cfg.LifecycleRunnerConfig,
		objectStorageUpload:        cfg.ObjectStorageUploadConfig,
		archiveCheckpoint:          cfg.ArchiveCheckpointConfig,
		overloadPolicyConfig:       NewFileArchiveOverloadPolicyConfigAdapter(cfg.OverloadPolicyConfig),
		handler:                    loggerhandler.NewConfigAdapter(cfg.HandlerConfig),
		isDebug:                    cfg.IsDebug,
		errorTraceDepth:            cfg.ErrorTraceDepth,
		errorTraceDepthDebugMode:   cfg.ErrorTraceDepthDebugMode,
	}
}

func (a FileArchiveConfigAdapter) Target() string {
	return a.target
}
func (a FileArchiveConfigAdapter) LifecycleMode() fsstore.FileLifecycleMode {
	return a.lifecycleMode
}
func (a FileArchiveConfigAdapter) RotationType() fsstore.FileRotationType {
	return a.rotationType
}
func (a FileArchiveConfigAdapter) RotationDayOfWeek() time.Weekday {
	return a.rotationDayOfWeek
}
func (a FileArchiveConfigAdapter) FileNameFormat() string {
	return a.fileNameFormat
}
func (a FileArchiveConfigAdapter) FileDateFormat() string {
	return a.fileDateFormat
}
func (a FileArchiveConfigAdapter) MaxFileSize() uint64 {
	return a.maxFileSize
}
func (a FileArchiveConfigAdapter) UsageCacheTTL() time.Duration {
	return a.usageCacheTTL
}
func (a FileArchiveConfigAdapter) MaxDirSize() uint64 {
	return a.maxDirSize
}
func (a FileArchiveConfigAdapter) MaxFileAge() int {
	return a.maxFileAge
}
func (a FileArchiveConfigAdapter) MaxFileBackups() int {
	return a.maxFileBackups
}
func (a FileArchiveConfigAdapter) EnableFileCompress() bool {
	return a.enableFileCompress
}
func (a FileArchiveConfigAdapter) CompressFormat() fsstore.CompressFormat {
	return a.compressFormat
}
func (a FileArchiveConfigAdapter) CompressionCodec() fsstore.CompressionCodec {
	return a.compressionCodec
}
func (a FileArchiveConfigAdapter) ArchivePath() string {
	return a.archivePath
}
func (a FileArchiveConfigAdapter) Dir() string {
	return a.dir
}
func (a FileArchiveConfigAdapter) DirCaseRetryable() string {
	return a.dirCaseRetryable
}
func (a FileArchiveConfigAdapter) DirCasePermanent() string {
	return a.dirCasePermanent
}
func (a FileArchiveConfigAdapter) EnableRotate() bool {
	return a.enableRotate
}
func (a FileArchiveConfigAdapter) MaxWriteAttempts() int {
	return a.maxWriteAttempts
}
func (a FileArchiveConfigAdapter) FileOperationRetryWaitTime() time.Duration {
	return a.fileOperationRetryWaitTime
}
func (a FileArchiveConfigAdapter) LifecycleRunnerConfig() fsstore.AppendLogRunnerConfig {
	return a.lifecycleRunnerConfig
}

func (a FileArchiveConfigAdapter) ObjectStorageUpload() objectstorage.UploadTargetConfig {
	return a.objectStorageUpload
}
func (a FileArchiveConfigAdapter) ArchiveCheckpoint() fsstore.ArchiveCheckpointConfig {
	return a.archiveCheckpoint
}
func (a FileArchiveConfigAdapter) OverloadPolicyConfig() FileArchiveOverloadPolicyConfigProvider {
	return a.overloadPolicyConfig
}

func (a FileArchiveConfigAdapter) Handler() loggerhandler.ConfigProvider { return a.handler }
func (a FileArchiveConfigAdapter) IsDebug() bool                         { return a.isDebug }
func (a FileArchiveConfigAdapter) ErrorTraceDepth() int                  { return a.errorTraceDepth }
func (a FileArchiveConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.errorTraceDepthDebugMode
}

type FileArchiveOverloadPolicyConfigAdapter struct {
	config pkgdurable.FileArchiveOverloadPolicyConfig
}

func NewFileArchiveOverloadPolicyConfigAdapter(cfg pkgdurable.FileArchiveOverloadPolicyConfig) FileArchiveOverloadPolicyConfigAdapter {
	return FileArchiveOverloadPolicyConfigAdapter{config: cfg}
}

func (a FileArchiveOverloadPolicyConfigAdapter) OverloadPolicy() pkgdurable.OverloadPolicy {
	return a.config.OverloadPolicy
}
func (a FileArchiveOverloadPolicyConfigAdapter) SpillDestination() pkgdurable.SpillDestination {
	return a.config.SpillDestination
}
func (a FileArchiveOverloadPolicyConfigAdapter) MinAvailableDiskSpace() uint64 {
	return a.config.MinAvailableDiskSpace
}
