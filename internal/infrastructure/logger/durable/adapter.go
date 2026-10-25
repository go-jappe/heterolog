package durable

import (
	"time"

	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

type FailureStoreConfigAdapter struct {
	enableDegrade bool
	maxConcurrent int
	durableTypes  []pkgdurable.DurableType
	spillTypes    []pkgdurable.SpillType
	durableStore  DurableStoreConfigProvider
}

func NewFailureStoreConfigAdapter(cfg pkgdurable.FailureStoreConfig) FailureStoreConfigAdapter {
	return FailureStoreConfigAdapter{
		enableDegrade: cfg.EnableDegrade,
		maxConcurrent: cfg.MaxConcurrent,
		durableTypes:  cfg.DurableType,
		spillTypes:    cfg.SpillType,
		durableStore:  NewDurableStoreConfigAdapter(cfg.DurableStoreConfig),
	}
}

func (a FailureStoreConfigAdapter) EnableDegrade() bool { return a.enableDegrade }
func (a FailureStoreConfigAdapter) MaxConcurrent() int  { return a.maxConcurrent }
func (a FailureStoreConfigAdapter) DurableType() []pkgdurable.DurableType {
	return a.durableTypes
}
func (a FailureStoreConfigAdapter) SpillType() []pkgdurable.SpillType {
	return a.spillTypes
}
func (a FailureStoreConfigAdapter) DurableStoreConfig() DurableStoreConfigProvider {
	return a.durableStore
}

type DeliveryFailureStoreConfigAdapter struct {
	enableDegrade bool
	durableTypes  []pkgdurable.DurableType
	spillTypes    []pkgdurable.SpillType
	durableStore  DeliveryDurableStoreConfigProvider
}

func NewDeliveryFailureStoreConfigAdapter(cfg pkgdurable.DeliveryFailureStoreConfig) DeliveryFailureStoreConfigAdapter {
	return DeliveryFailureStoreConfigAdapter{
		enableDegrade: cfg.EnableDegrade,
		durableTypes:  cfg.DurableType,
		spillTypes:    cfg.SpillType,
		durableStore:  NewDeliveryDurableStoreConfigAdapter(cfg.FileArchiveConfig),
	}
}

func (a DeliveryFailureStoreConfigAdapter) EnableDegrade() bool { return a.enableDegrade }
func (a DeliveryFailureStoreConfigAdapter) DurableType() []pkgdurable.DurableType {
	return a.durableTypes
}
func (a DeliveryFailureStoreConfigAdapter) SpillType() []pkgdurable.SpillType {
	return a.spillTypes
}
func (a DeliveryFailureStoreConfigAdapter) DeliveryDurableStoreConfig() DeliveryDurableStoreConfigProvider {
	return a.durableStore
}

type DurableStoreConfigAdapter struct {
	fileArchive FileArchiveConfigProvider
	spool       SpoolConfigProvider
}

func NewDurableStoreConfigAdapter(cfg pkgdurable.DurableStoreConfig) DurableStoreConfigAdapter {
	return DurableStoreConfigAdapter{
		fileArchive: NewFileArchiveConfigAdapter(cfg.FileArchiveConfig),
		spool:       NewSpoolConfigAdapter(cfg.SpoolConfig),
	}
}

func (a DurableStoreConfigAdapter) FileArchiveConfig() FileArchiveConfigProvider {
	return a.fileArchive
}
func (a DurableStoreConfigAdapter) SpoolConfig() SpoolConfigProvider {
	return a.spool
}

type DeliveryDurableStoreConfigAdapter struct {
	fileArchive FileArchiveConfigProvider
}

func NewDeliveryDurableStoreConfigAdapter(cfg pkgdurable.DeliveryFileArchiveConfig) DeliveryDurableStoreConfigAdapter {
	return DeliveryDurableStoreConfigAdapter{
		fileArchive: NewFileArchiveConfigAdapter(deliveryFileArchiveConfig(cfg)),
	}
}

func (a DeliveryDurableStoreConfigAdapter) FileArchiveConfig() FileArchiveConfigProvider {
	return a.fileArchive
}

type SpoolConfigAdapter struct {
	spoolPath string
	target    string
	dir       string
	overload  spoolstore.SpoolStoreOverloadPolicyConfigProvider
	handler   loggerhandler.ConfigProvider
}

func NewSpoolConfigAdapter(cfg pkgdurable.SpoolConfig) SpoolConfigAdapter {
	return SpoolConfigAdapter{
		spoolPath: cfg.SpoolPath,
		target:    cfg.Target,
		dir:       cfg.Dir,
		overload:  NewSpoolOverloadPolicyConfigAdapter(cfg.OverloadPolicyConfig),
		handler:   loggerhandler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

func (a SpoolConfigAdapter) SpoolPath() string { return a.spoolPath }
func (a SpoolConfigAdapter) Target() string    { return a.target }
func (a SpoolConfigAdapter) Dir() string       { return a.dir }
func (a SpoolConfigAdapter) OverloadPolicyConfig() spoolstore.SpoolStoreOverloadPolicyConfigProvider {
	return a.overload
}
func (a SpoolConfigAdapter) Handler() loggerhandler.ConfigProvider { return a.handler }

type SpoolOverloadPolicyConfigAdapter struct {
	cfg pkgdurable.SpoolOverloadPolicyConfig
}

func NewSpoolOverloadPolicyConfigAdapter(cfg pkgdurable.SpoolOverloadPolicyConfig) SpoolOverloadPolicyConfigAdapter {
	return SpoolOverloadPolicyConfigAdapter{cfg: cfg}
}

func (a SpoolOverloadPolicyConfigAdapter) OverloadPolicy() pkgdurable.OverloadPolicy {
	return a.cfg.OverloadPolicy
}
func (a SpoolOverloadPolicyConfigAdapter) SpillDestination() pkgdurable.SpillDestination {
	return a.cfg.SpillDestination
}
func (a SpoolOverloadPolicyConfigAdapter) MaxCallsSinceScan() uint64 {
	return a.cfg.MaxCallsSinceScan
}
func (a SpoolOverloadPolicyConfigAdapter) StatsScanInterval() time.Duration {
	return a.cfg.StatsScanInterval
}
func (a SpoolOverloadPolicyConfigAdapter) MaxDirSize() uint64 { return a.cfg.MaxDirSize }
func (a SpoolOverloadPolicyConfigAdapter) ActiveBacklogWarnAfterDays() int {
	return a.cfg.ActiveBacklogWarnAfterDays
}
func (a SpoolOverloadPolicyConfigAdapter) MinAvailableDiskSpace() uint64 {
	return a.cfg.MinAvailableDiskSpace
}
func (a SpoolOverloadPolicyConfigAdapter) UseMemoryPressureForOverload() bool {
	return a.cfg.UseMemoryPressureForOverload
}
func (a SpoolOverloadPolicyConfigAdapter) MemoryCriticalRatio() float64 {
	return a.cfg.MemoryCriticalRatio
}
func (a SpoolOverloadPolicyConfigAdapter) MinAvailableMemoryBytes() uint64 {
	return a.cfg.MinAvailableMemoryBytes
}
func (a SpoolOverloadPolicyConfigAdapter) MemoryCriticalConsecutiveHits() int {
	return a.cfg.MemoryCriticalConsecutiveHits
}

func deliveryFileArchiveConfig(cfg pkgdurable.DeliveryFileArchiveConfig) pkgdurable.FileArchiveConfig {
	return pkgdurable.FileArchiveConfig{
		Target:                     cfg.Target,
		MaxFileSize:                cfg.MaxFileSize,
		FailureFileArchivePath:     cfg.FailureFileArchivePath,
		Dir:                        cfg.Dir,
		DirCaseRetryable:           cfg.DirCaseRetryable,
		DirCasePermanent:           cfg.DirCasePermanent,
		FileNameFormat:             cfg.FileNameFormat,
		FileDateFormat:             cfg.FileDateFormat,
		EnableRotate:               cfg.EnableRotate,
		UsageCacheTTL:              cfg.UsageCacheTTL,
		MaxFileBackups:             cfg.MaxFileBackups,
		MaxFileAge:                 cfg.MaxFileAge,
		MaxDirSize:                 cfg.MaxDirSize,
		EnableFileCompress:         cfg.EnableFileCompress,
		CompressFormat:             cfg.CompressFormat,
		CompressionCodec:           cfg.CompressionCodec,
		RotationType:               cfg.RotationType,
		RotationDayOfWeek:          cfg.RotationDayOfWeek,
		MaxWriteAttempts:           cfg.MaxWriteAttempts,
		FileOperationRetryWaitTime: cfg.FileOperationRetryWaitTime,
		ObjectStorageUploadConfig:  cfg.ObjectStorageUploadConfig,
		ArchiveCheckpointConfig:    cfg.ArchiveCheckpointConfig,
		LifecycleRunnerConfig:      cfg.LifecycleRunnerConfig,
		LifecycleMode:              cfg.LifecycleMode,
		OverloadPolicyConfig:       cfg.OverloadPolicyConfig,
		HandlerConfig:              cfg.HandlerConfig,
		IsDebug:                    cfg.IsDebug,
		ErrorTraceDepth:            cfg.ErrorTraceDepth,
		ErrorTraceDepthDebugMode:   cfg.ErrorTraceDepthDebugMode,
	}
}
