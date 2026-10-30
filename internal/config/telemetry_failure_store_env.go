package config

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/filelifecycle"
	"github.com/go-jappe/heterolog/internal/config/fsstoreconfig"
	"github.com/go-jappe/heterolog/internal/config/internalerror"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/config/spool/spoollifecycle"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolrunner"
	"github.com/go-jappe/heterolog/internal/config/spool/spoolsupervisor"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	pkgspoollifecycle "github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

// telemetryFailureStoreKeysは、traceやmetricsの送り先のfailure storeの環境変数のkeyと、
// 保存先のdirectoryの既定値です。keyの並びは、ログの出力先（HETEROLOG_LOKI_*など）と同じで、
// ログの出力先の名前の位置に、送り先の名前（TRACES_OTLPなど）が入ります。directoryは、
// ログの出力先と同じく、HETEROLOG_SYSTEM_LIB_DIRとHETEROLOG_TARGETを基準に解決します。
type telemetryFailureStoreKeys struct {
	EnableDegrade, MaxConcurrent, DurableTypes, DegradeTypes string

	ArchiveMaxFileSize, ArchiveRotate, ArchiveMaxBackups, ArchiveMaxAge, ArchiveMaxDirSize    string
	ArchiveCompress, ArchiveCompressFormat, ArchiveLifecycleMode, ArchiveDir, ArchiveRotation string
	ArchiveOverloadPolicy, ArchiveSecondaryDestination                                        string
	UploadEnabled, UploadType, UploadS3Bucket, UploadS3Prefix, UploadS3Destinations           string

	SpoolDir, SpoolOverloadPolicy, SpoolSecondaryDestination, SpoolMaxActiveDirSize                  string
	SpoolBacklogWarnAfterDays, SpoolMaxScannedCacheUsage, SpoolCacheLifetime, SpoolUseMemoryPressure string

	RunnerInterval, RunnerStopOnError                                                             string
	PurgeDone, DoneAge, DoneDirSize, PurgeDead, DeadAge, DeadDirSize                              string
	PurgeQuarantine, QuarantineAge, QuarantineDirSize, PurgeInterval, SizeEstimateCache           string
	FullScanEveryNChecks, FullScanMaxInterval, FinalizeRetryDelay, FinalizeEntries, MaxRetryAfter string

	DefaultSpoolDir, DefaultArchiveDir string
}

// TelemetryFailureStoreConfigは、traceやmetricsの送り先のfailure storeと、spoolから送り直す
// 設定です。
type TelemetryFailureStoreConfig struct {
	FailureHandler   failure.FailureHandlerConfig
	Replay           pkgspoolrunner.BackendSpoolRunnerConfig
	ReplaySupervisor pkgspoolsupervisor.RunnerSupervisorConfig
}

// loadTelemetryFailureStoreConfigは、送り先のfailure storeの設定を、keysのkeyから読み込みます。
// traceとmetricsでは、内部ログの通知先（*_HANDLER）は使わないため、読みません。
func loadTelemetryFailureStoreConfig(env pkgenv.Lookup, keys telemetryFailureStoreKeys) (TelemetryFailureStoreConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		keys.EnableDegrade, failure.DefaultEnableDegrade,
		keys.MaxConcurrent, failure.DefaultMaxConcurrent,
		keys.DurableTypes, failure.DefaultDurableTypes,
		keys.DegradeTypes, failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) { return loadTelemetryFailureArchiveConfig(env, keys) },
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) { return loadTelemetryFailureSpoolConfig(env, keys) },
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	result := TelemetryFailureStoreConfig{FailureHandler: cfg}
	var replayErr error
	if containsDurableType(cfg.DurableType, failure.DurableTypeSpool) {
		result.Replay, replayErr = loadTelemetrySpoolRunnerConfig(env, keys)
		maxRestarts, maxRestartsErr := env.GetEnvInt("HETEROLOG_MAX_RESTART_SPOOL_TIMES", spoolsupervisor.DefaultMaxRestart)
		replayErr = errors.Join(replayErr, maxRestartsErr)
		result.ReplaySupervisor = pkgspoolsupervisor.RunnerSupervisorConfig{
			RestartDelay: spoolsupervisor.DefaultRestartDelay,
			MaxRestarts:  maxRestarts,
		}
	}
	return result, errors.Join(selectionErr, fileArchiveErr, spoolErr, replayErr)
}

func loadTelemetryFailureArchiveConfig(env pkgenv.Lookup, keys telemetryFailureStoreKeys) (failure.FileArchiveConfig, error) {
	maxFileSize, maxFileSizeErr := env.GetEnvUint64Scaled(keys.ArchiveMaxFileSize, failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
	rotate, rotateErr := env.GetEnvBool(keys.ArchiveRotate, failure.DefaultFileArchiveRotateEnabled)
	maxBackups, maxBackupsErr := env.GetEnvInt(keys.ArchiveMaxBackups, failure.DefaultFileArchiveMaxBackups)
	maxAge, maxAgeErr := env.GetEnvInt(keys.ArchiveMaxAge, failure.DefaultFileArchiveMaxAge)
	maxDirSize, maxDirSizeErr := env.GetEnvUint64Scaled(keys.ArchiveMaxDirSize, failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
	compress, compressErr := env.GetEnvBool(keys.ArchiveCompress, failure.DefaultFileArchiveCompressEnabled)
	compressFormatKey := keys.ArchiveCompressFormat
	compressFormat, compressFormatErr := loadLogCompressFormat(
		env.GetEnvString(compressFormatKey, string(failure.DefaultFileArchiveCompressFormat)),
		compressFormatKey,
		failure.DefaultFileArchiveCompressFormat,
	)
	upload, uploadErr := loadFailureArchiveObjectStorageUploadConfig(
		env,
		keys.UploadEnabled,
		keys.UploadType,
		keys.UploadS3Bucket,
		keys.UploadS3Prefix,
		keys.UploadS3Destinations,
	)
	return failure.FileArchiveConfig{
		MaxFileSize:                maxFileSize,
		LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString(keys.ArchiveLifecycleMode, string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
		Dir:                        env.GetEnvString(keys.ArchiveDir, keys.DefaultArchiveDir),
		DirCaseRetryable:           failure.DirCaseRetryable,
		DirCasePermanent:           failure.DirCasePermanent,
		EnableRotate:               rotate,
		MaxFileBackups:             maxBackups,
		MaxFileAge:                 maxAge,
		MaxDirSize:                 maxDirSize,
		EnableFileCompress:         compress,
		CompressFormat:             compressFormat,
		FileRotation:               loadLogRotation(env.GetEnvString(keys.ArchiveRotation, string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
		FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
		MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
		FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
		ObjectStorageUploadConfig:  upload,
		OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
			OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString(keys.ArchiveOverloadPolicy, string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
			SpillDestination: failure.ParseSpillDestination(env.GetEnvString(keys.ArchiveSecondaryDestination, string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
		},
		HandlerConfig: internalerror.RoutingConfig{},
	}, errors.Join(maxFileSizeErr, rotateErr, maxBackupsErr, maxAgeErr, maxDirSizeErr, compressErr, compressFormatErr, uploadErr)
}

func loadTelemetryFailureSpoolConfig(env pkgenv.Lookup, keys telemetryFailureStoreKeys) (failure.SpoolConfig, error) {
	maxCalls, maxCallsErr := env.GetEnvUint64(keys.SpoolMaxScannedCacheUsage, failure.DefaultMaxSpoolScannedCacheUsageCount)
	cacheLifetime, cacheLifetimeErr := env.GetEnvDuration(keys.SpoolCacheLifetime, failure.DefaultSpoolCacheLifetime)
	maxDirSize, maxDirSizeErr := env.GetEnvUint64Scaled(keys.SpoolMaxActiveDirSize, failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
	backlogWarnAfterDays, backlogWarnAfterDaysErr := env.GetEnvInt(keys.SpoolBacklogWarnAfterDays, failure.DefaultSpoolActiveBacklogWarnAfterDays)
	useMemoryPressure, useMemoryPressureErr := env.GetEnvBool(keys.SpoolUseMemoryPressure, failure.DefaultUseMemoryPressureForOverload)
	return failure.SpoolConfig{
		Dir: env.GetEnvString(keys.SpoolDir, keys.DefaultSpoolDir),
		OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
			OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString(keys.SpoolOverloadPolicy, string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
			SpillDestination:             failure.ParseSpillDestination(env.GetEnvString(keys.SpoolSecondaryDestination, string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
			MaxCallsSinceScan:            maxCalls,
			StatsScanInterval:            cacheLifetime,
			MaxDirSize:                   maxDirSize,
			ActiveBacklogWarnAfterDays:   backlogWarnAfterDays,
			UseMemoryPressureForOverload: useMemoryPressure,
		},
		HandlerConfig: internalerror.RoutingConfig{},
	}, errors.Join(maxCallsErr, cacheLifetimeErr, maxDirSizeErr, backlogWarnAfterDaysErr, useMemoryPressureErr)
}

func loadTelemetrySpoolRunnerConfig(env pkgenv.Lookup, keys telemetryFailureStoreKeys) (pkgspoolrunner.BackendSpoolRunnerConfig, error) {
	interval, intervalErr := env.GetEnvDurationMillisecond(keys.RunnerInterval, spoolrunner.DefaultRunnerInterval)
	stopOnError, stopOnErrorErr := env.GetEnvBool(keys.RunnerStopOnError, spoolrunner.DefaultStopOnError)
	purgeDone, purgeDoneErr := env.GetEnvBool(keys.PurgeDone, spoollifecycle.DefaultEnablePurgeDone)
	doneAge, doneAgeErr := env.GetEnvInt(keys.DoneAge, spoollifecycle.DefaultDoneMaxFileAge)
	doneDirSize, doneDirSizeErr := env.GetEnvUint64Scaled(keys.DoneDirSize, spoollifecycle.DefaultDoneMaxDirSizeMB, BytesPerMegabyte)
	purgeDead, purgeDeadErr := env.GetEnvBool(keys.PurgeDead, spoollifecycle.DefaultEnablePurgeDead)
	deadAge, deadAgeErr := env.GetEnvInt(keys.DeadAge, spoollifecycle.DefaultDeadMaxFileAges)
	deadDirSize, deadDirSizeErr := env.GetEnvUint64Scaled(keys.DeadDirSize, spoollifecycle.DefaultDeadMaxDirSizes, BytesPerMegabyte)
	purgeQuarantine, purgeQuarantineErr := env.GetEnvBool(keys.PurgeQuarantine, spoollifecycle.DefaultEnablePurgeQuarantine)
	quarantineAge, quarantineAgeErr := env.GetEnvInt(keys.QuarantineAge, spoollifecycle.DefaultQuarantineMaxFileAge)
	quarantineDirSize, quarantineDirSizeErr := env.GetEnvUint64Scaled(keys.QuarantineDirSize, spoollifecycle.DefaultQuarantineMaxDirSize, BytesPerMegabyte)
	purgeInterval, purgeIntervalErr := env.GetEnvDuration(keys.PurgeInterval, spoollifecycle.DefaultSizePurgeInterval)
	sizeEstimateCache, sizeEstimateCacheErr := env.GetEnvBool(keys.SizeEstimateCache, spoollifecycle.DefaultEnableSizeEstimateCache)
	fullScanEveryNChecks, fullScanEveryNChecksErr := env.GetEnvInt(keys.FullScanEveryNChecks, spoollifecycle.DefaultSizeFullScanEveryNChecks)
	fullScanMaxInterval, fullScanMaxIntervalErr := env.GetEnvDuration(keys.FullScanMaxInterval, spoollifecycle.DefaultSizeFullScanMaxInterval)
	finalizeRetryDelay, finalizeRetryDelayErr := env.GetEnvDurationMillisecond(keys.FinalizeRetryDelay, spoolrunner.DefaultFinalizeRetryDelay)
	finalizeEntries, finalizeEntriesErr := env.GetEnvInt(keys.FinalizeEntries, spoolrunner.DefaultMaxFinalizeEntries)
	maxRetryAfter, maxRetryAfterErr := getEnvMaxRetryAfter(env, keys.MaxRetryAfter, spoolrunner.DefaultMaxRetryAfter)
	return pkgspoolrunner.BackendSpoolRunnerConfig{
			Runner: pkgspoolrunner.RunnerConfig{Interval: interval, StopOnError: stopOnError},
			LifeCycle: pkgspoollifecycle.LifecycleConfig{
				EnablePurgeDone:          purgeDone,
				DoneMaxFileAge:           doneAge,
				DoneMaxDirSize:           doneDirSize,
				EnablePurgeDead:          purgeDead,
				DeadMaxFileAge:           deadAge,
				DeadMaxDirSize:           deadDirSize,
				EnablePurgeQuarantine:    purgeQuarantine,
				QuarantineMaxFileAge:     quarantineAge,
				QuarantineMaxDirSize:     quarantineDirSize,
				SizePurgeInterval:        purgeInterval,
				EnableSizeEstimateCache:  sizeEstimateCache,
				SizeFullScanEveryNChecks: fullScanEveryNChecks,
				SizeFullScanMaxInterval:  fullScanMaxInterval,
			},
			Worker: pkgspoolrunner.WorkerConfig{
				Finalize:      pkgspoolrunner.FinalizeConfig{RetryDelay: finalizeRetryDelay, MaxEntries: finalizeEntries},
				MaxRetryAfter: maxRetryAfter,
			},
		}, errors.Join(
			intervalErr, stopOnErrorErr, purgeDoneErr, doneAgeErr, doneDirSizeErr, purgeDeadErr, deadAgeErr, deadDirSizeErr,
			purgeQuarantineErr, quarantineAgeErr, quarantineDirSizeErr, purgeIntervalErr, sizeEstimateCacheErr,
			fullScanEveryNChecksErr, fullScanMaxIntervalErr, finalizeRetryDelayErr, finalizeEntriesErr, maxRetryAfterErr,
		)
}

// telemetryFailureArchiveUploadEnabledは、traceかmetricsのfailure archiveがobject storageへ
// 送るかを返します。送る場合は、loggerと同じ標準のcompressed log upload failure storeの設定を、
// Telemetryの保存先として使います。
func telemetryFailureArchiveUploadEnabled(a AppConfig) bool {
	uploads := func(enabled bool, fh failure.FailureHandlerConfig) bool {
		archive := fh.Fsstore.FileArchiveConfig
		return enabled && containsDurableType(fh.DurableType, failure.DurableTypeFsstore) &&
			filelifecycle.UsesObjectStorageUpload(archive.LifecycleMode) && archive.ObjectStorageUploadConfig.Enabled
	}
	return uploads(a.Traces.Enabled, a.Traces.FailureStore.FailureHandler) ||
		uploads(a.Metrics.Enabled, a.Metrics.FailureStore.FailureHandler) ||
		uploads(a.Metrics.Enabled, a.Metrics.VictoriaMetricsFailureStore.FailureHandler)
}

// tracesOTLPFailureStoreKeysは、traceのotlpの送り先のfailure storeのkeyです。
var tracesOTLPFailureStoreKeys = telemetryFailureStoreKeys{
	EnableDegrade:               "HETEROLOG_ENABLE_TRACES_OTLP_DEGRADE",
	MaxConcurrent:               "HETEROLOG_MAX_TRACES_OTLP_FAILURE_HANDLER_CONCURRENT",
	DurableTypes:                "HETEROLOG_TRACES_OTLP_DURABLE_TYPES",
	DegradeTypes:                "HETEROLOG_TRACES_OTLP_DEGRADE_TYPES",
	ArchiveMaxFileSize:          "HETEROLOG_MAX_TRACES_OTLP_DURABLE_FILE_ARCHIVE_SIZE_MB",
	ArchiveRotate:               "HETEROLOG_TRACES_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE",
	ArchiveMaxBackups:           "HETEROLOG_MAX_TRACES_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS",
	ArchiveMaxAge:               "HETEROLOG_MAX_TRACES_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_AGE",
	ArchiveMaxDirSize:           "HETEROLOG_MAX_TRACES_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB",
	ArchiveCompress:             "HETEROLOG_ENABLE_TRACES_OTLP_DURABLE_FILE_ARCHIVE_COMPRESS",
	ArchiveCompressFormat:       "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
	ArchiveLifecycleMode:        "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
	ArchiveDir:                  "HETEROLOG_TRACES_OTLP_FAILURE_ARCHIVE_DIR",
	ArchiveRotation:             "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_ROTATION",
	ArchiveOverloadPolicy:       "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY",
	ArchiveSecondaryDestination: "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION",
	UploadEnabled:               "HETEROLOG_ENABLE_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
	UploadType:                  "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
	UploadS3Bucket:              "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
	UploadS3Prefix:              "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
	UploadS3Destinations:        "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	SpoolDir:                    "HETEROLOG_TRACES_OTLP_FAILURE_SPOOL_DIR",
	SpoolOverloadPolicy:         "HETEROLOG_TRACES_OTLP_SPOOL_OVERLOAD_POLICY",
	SpoolSecondaryDestination:   "HETEROLOG_TRACES_OTLP_SPOOL_SECONDARY_DESTINATION",
	SpoolMaxActiveDirSize:       "HETEROLOG_MAX_TRACES_OTLP_SPOOL_FILE_ACTIVE_DIR_SIZE_MB",
	SpoolBacklogWarnAfterDays:   "HETEROLOG_TRACES_OTLP_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS",
	SpoolMaxScannedCacheUsage:   "HETEROLOG_MAX_TRACES_OTLP_SPOOL_SCANNED_CACHE_USAGE_COUNT",
	SpoolCacheLifetime:          "HETEROLOG_TRACES_OTLP_SPOOL_CACHE_LIFETIME_SECOND",
	SpoolUseMemoryPressure:      "HETEROLOG_USE_MEMORY_PRESSURE_TRACES_OTLP_SPOOL_OVERLOAD",
	RunnerInterval:              "HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_INTERVAL_MILLISECOND",
	RunnerStopOnError:           "HETEROLOG_ENABLE_TRACES_OTLP_SPOOL_RUNNER_STOP_ON_ERROR",
	PurgeDone:                   "HETEROLOG_ENABLE_PURGE_TRACES_OTLP_SPOOL_DONE",
	DoneAge:                     "HETEROLOG_MAX_TRACES_OTLP_SPOOL_DONE_AGE",
	DoneDirSize:                 "HETEROLOG_MAX_TRACES_OTLP_SPOOL_DONE_DIR_SIZE_MB",
	PurgeDead:                   "HETEROLOG_ENABLE_PURGE_TRACES_OTLP_SPOOL_DEAD",
	DeadAge:                     "HETEROLOG_MAX_TRACES_OTLP_SPOOL_DEAD_AGE",
	DeadDirSize:                 "HETEROLOG_MAX_TRACES_OTLP_SPOOL_DEAD_DIR_SIZE_MB",
	PurgeQuarantine:             "HETEROLOG_ENABLE_PURGE_TRACES_OTLP_SPOOL_QUARANTINE",
	QuarantineAge:               "HETEROLOG_MAX_TRACES_OTLP_SPOOL_QUARANTINE_AGE",
	QuarantineDirSize:           "HETEROLOG_MAX_TRACES_OTLP_SPOOL_QUARANTINE_DIR_SIZE_MB",
	PurgeInterval:               "HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_PURGE_INTERVAL_SECOND",
	SizeEstimateCache:           "HETEROLOG_ENABLE_TRACES_OTLP_SPOOL_SIZE_ESTIMATE_CACHE",
	FullScanEveryNChecks:        "HETEROLOG_TRACES_OTLP_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS",
	FullScanMaxInterval:         "HETEROLOG_MAX_TRACES_OTLP_SPOOL_FULL_SCAN_INTERVAL_SECOND",
	FinalizeRetryDelay:          "HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND",
	FinalizeEntries:             "HETEROLOG_MAX_TRACES_OTLP_SPOOL_FINALIZE_FAILURE_ENTRIES",
	MaxRetryAfter:               "HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND",
	DefaultSpoolDir:             "traces_otlp",
	DefaultArchiveDir:           "traces_otlp_failure_archives",
}

// metricsOTLPFailureStoreKeysは、metricsのotlpの送り先のfailure storeのkeyです。
var metricsOTLPFailureStoreKeys = telemetryFailureStoreKeys{
	EnableDegrade:               "HETEROLOG_ENABLE_METRICS_OTLP_DEGRADE",
	MaxConcurrent:               "HETEROLOG_MAX_METRICS_OTLP_FAILURE_HANDLER_CONCURRENT",
	DurableTypes:                "HETEROLOG_METRICS_OTLP_DURABLE_TYPES",
	DegradeTypes:                "HETEROLOG_METRICS_OTLP_DEGRADE_TYPES",
	ArchiveMaxFileSize:          "HETEROLOG_MAX_METRICS_OTLP_DURABLE_FILE_ARCHIVE_SIZE_MB",
	ArchiveRotate:               "HETEROLOG_METRICS_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE",
	ArchiveMaxBackups:           "HETEROLOG_MAX_METRICS_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS",
	ArchiveMaxAge:               "HETEROLOG_MAX_METRICS_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_AGE",
	ArchiveMaxDirSize:           "HETEROLOG_MAX_METRICS_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB",
	ArchiveCompress:             "HETEROLOG_ENABLE_METRICS_OTLP_DURABLE_FILE_ARCHIVE_COMPRESS",
	ArchiveCompressFormat:       "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
	ArchiveLifecycleMode:        "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
	ArchiveDir:                  "HETEROLOG_METRICS_OTLP_FAILURE_ARCHIVE_DIR",
	ArchiveRotation:             "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_ROTATION",
	ArchiveOverloadPolicy:       "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY",
	ArchiveSecondaryDestination: "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION",
	UploadEnabled:               "HETEROLOG_ENABLE_METRICS_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
	UploadType:                  "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
	UploadS3Bucket:              "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
	UploadS3Prefix:              "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
	UploadS3Destinations:        "HETEROLOG_METRICS_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	SpoolDir:                    "HETEROLOG_METRICS_OTLP_FAILURE_SPOOL_DIR",
	SpoolOverloadPolicy:         "HETEROLOG_METRICS_OTLP_SPOOL_OVERLOAD_POLICY",
	SpoolSecondaryDestination:   "HETEROLOG_METRICS_OTLP_SPOOL_SECONDARY_DESTINATION",
	SpoolMaxActiveDirSize:       "HETEROLOG_MAX_METRICS_OTLP_SPOOL_FILE_ACTIVE_DIR_SIZE_MB",
	SpoolBacklogWarnAfterDays:   "HETEROLOG_METRICS_OTLP_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS",
	SpoolMaxScannedCacheUsage:   "HETEROLOG_MAX_METRICS_OTLP_SPOOL_SCANNED_CACHE_USAGE_COUNT",
	SpoolCacheLifetime:          "HETEROLOG_METRICS_OTLP_SPOOL_CACHE_LIFETIME_SECOND",
	SpoolUseMemoryPressure:      "HETEROLOG_USE_MEMORY_PRESSURE_METRICS_OTLP_SPOOL_OVERLOAD",
	RunnerInterval:              "HETEROLOG_METRICS_OTLP_SPOOL_RUNNER_INTERVAL_MILLISECOND",
	RunnerStopOnError:           "HETEROLOG_ENABLE_METRICS_OTLP_SPOOL_RUNNER_STOP_ON_ERROR",
	PurgeDone:                   "HETEROLOG_ENABLE_PURGE_METRICS_OTLP_SPOOL_DONE",
	DoneAge:                     "HETEROLOG_MAX_METRICS_OTLP_SPOOL_DONE_AGE",
	DoneDirSize:                 "HETEROLOG_MAX_METRICS_OTLP_SPOOL_DONE_DIR_SIZE_MB",
	PurgeDead:                   "HETEROLOG_ENABLE_PURGE_METRICS_OTLP_SPOOL_DEAD",
	DeadAge:                     "HETEROLOG_MAX_METRICS_OTLP_SPOOL_DEAD_AGE",
	DeadDirSize:                 "HETEROLOG_MAX_METRICS_OTLP_SPOOL_DEAD_DIR_SIZE_MB",
	PurgeQuarantine:             "HETEROLOG_ENABLE_PURGE_METRICS_OTLP_SPOOL_QUARANTINE",
	QuarantineAge:               "HETEROLOG_MAX_METRICS_OTLP_SPOOL_QUARANTINE_AGE",
	QuarantineDirSize:           "HETEROLOG_MAX_METRICS_OTLP_SPOOL_QUARANTINE_DIR_SIZE_MB",
	PurgeInterval:               "HETEROLOG_METRICS_OTLP_SPOOL_RUNNER_PURGE_INTERVAL_SECOND",
	SizeEstimateCache:           "HETEROLOG_ENABLE_METRICS_OTLP_SPOOL_SIZE_ESTIMATE_CACHE",
	FullScanEveryNChecks:        "HETEROLOG_METRICS_OTLP_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS",
	FullScanMaxInterval:         "HETEROLOG_MAX_METRICS_OTLP_SPOOL_FULL_SCAN_INTERVAL_SECOND",
	FinalizeRetryDelay:          "HETEROLOG_METRICS_OTLP_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND",
	FinalizeEntries:             "HETEROLOG_MAX_METRICS_OTLP_SPOOL_FINALIZE_FAILURE_ENTRIES",
	MaxRetryAfter:               "HETEROLOG_METRICS_OTLP_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND",
	DefaultSpoolDir:             "metrics_otlp",
	DefaultArchiveDir:           "metrics_otlp_failure_archives",
}

// metricsVictoriaMetricsFailureStoreKeysは、metricsのvictoriametricsの送り先のfailure storeのkeyです。
var metricsVictoriaMetricsFailureStoreKeys = telemetryFailureStoreKeys{
	EnableDegrade:               "HETEROLOG_ENABLE_METRICS_VICTORIAMETRICS_DEGRADE",
	MaxConcurrent:               "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_FAILURE_HANDLER_CONCURRENT",
	DurableTypes:                "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_TYPES",
	DegradeTypes:                "HETEROLOG_METRICS_VICTORIAMETRICS_DEGRADE_TYPES",
	ArchiveMaxFileSize:          "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_SIZE_MB",
	ArchiveRotate:               "HETEROLOG_METRICS_VICTORIAMETRICS_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE",
	ArchiveMaxBackups:           "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS",
	ArchiveMaxAge:               "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_ENABLE_DURABLE_FILE_ARCHIVE_AGE",
	ArchiveMaxDirSize:           "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB",
	ArchiveCompress:             "HETEROLOG_ENABLE_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_COMPRESS",
	ArchiveCompressFormat:       "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT",
	ArchiveLifecycleMode:        "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
	ArchiveDir:                  "HETEROLOG_METRICS_VICTORIAMETRICS_FAILURE_ARCHIVE_DIR",
	ArchiveRotation:             "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_ROTATION",
	ArchiveOverloadPolicy:       "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY",
	ArchiveSecondaryDestination: "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION",
	UploadEnabled:               "HETEROLOG_ENABLE_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
	UploadType:                  "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE",
	UploadS3Bucket:              "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
	UploadS3Prefix:              "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX",
	UploadS3Destinations:        "HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_DESTINATIONS",
	SpoolDir:                    "HETEROLOG_METRICS_VICTORIAMETRICS_FAILURE_SPOOL_DIR",
	SpoolOverloadPolicy:         "HETEROLOG_METRICS_VICTORIAMETRICS_SPOOL_OVERLOAD_POLICY",
	SpoolSecondaryDestination:   "HETEROLOG_METRICS_VICTORIAMETRICS_SPOOL_SECONDARY_DESTINATION",
	SpoolMaxActiveDirSize:       "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_FILE_ACTIVE_DIR_SIZE_MB",
	SpoolBacklogWarnAfterDays:   "HETEROLOG_METRICS_VICTORIAMETRICS_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS",
	SpoolMaxScannedCacheUsage:   "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_SCANNED_CACHE_USAGE_COUNT",
	SpoolCacheLifetime:          "HETEROLOG_METRICS_VICTORIAMETRICS_SPOOL_CACHE_LIFETIME_SECOND",
	SpoolUseMemoryPressure:      "HETEROLOG_USE_MEMORY_PRESSURE_METRICS_VICTORIAMETRICS_SPOOL_OVERLOAD",
	RunnerInterval:              "HETEROLOG_METRICS_VICTORIAMETRICS_SPOOL_RUNNER_INTERVAL_MILLISECOND",
	RunnerStopOnError:           "HETEROLOG_ENABLE_METRICS_VICTORIAMETRICS_SPOOL_RUNNER_STOP_ON_ERROR",
	PurgeDone:                   "HETEROLOG_ENABLE_PURGE_METRICS_VICTORIAMETRICS_SPOOL_DONE",
	DoneAge:                     "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_DONE_AGE",
	DoneDirSize:                 "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_DONE_DIR_SIZE_MB",
	PurgeDead:                   "HETEROLOG_ENABLE_PURGE_METRICS_VICTORIAMETRICS_SPOOL_DEAD",
	DeadAge:                     "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_DEAD_AGE",
	DeadDirSize:                 "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_DEAD_DIR_SIZE_MB",
	PurgeQuarantine:             "HETEROLOG_ENABLE_PURGE_METRICS_VICTORIAMETRICS_SPOOL_QUARANTINE",
	QuarantineAge:               "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_QUARANTINE_AGE",
	QuarantineDirSize:           "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_QUARANTINE_DIR_SIZE_MB",
	PurgeInterval:               "HETEROLOG_METRICS_VICTORIAMETRICS_SPOOL_RUNNER_PURGE_INTERVAL_SECOND",
	SizeEstimateCache:           "HETEROLOG_ENABLE_METRICS_VICTORIAMETRICS_SPOOL_SIZE_ESTIMATE_CACHE",
	FullScanEveryNChecks:        "HETEROLOG_METRICS_VICTORIAMETRICS_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS",
	FullScanMaxInterval:         "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_FULL_SCAN_INTERVAL_SECOND",
	FinalizeRetryDelay:          "HETEROLOG_METRICS_VICTORIAMETRICS_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND",
	FinalizeEntries:             "HETEROLOG_MAX_METRICS_VICTORIAMETRICS_SPOOL_FINALIZE_FAILURE_ENTRIES",
	MaxRetryAfter:               "HETEROLOG_METRICS_VICTORIAMETRICS_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND",
	DefaultSpoolDir:             "metrics_victoriametrics",
	DefaultArchiveDir:           "metrics_victoriametrics_failure_archives",
}
