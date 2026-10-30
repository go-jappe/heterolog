package config

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/failure"
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

// traceの送り先のfailure storeの、保存先のdirectoryの既定値です。ログの出力先と同じく、
// spoolはHETEROLOG_SYSTEM_LIB_DIRとHETEROLOG_TARGETを基準に解決します。
const (
	defaultTracesOTLPFailureSpoolDir   = "traces_otlp"
	defaultTracesOTLPFailureArchiveDir = "traces_otlp_failure_archives"
)

// TracesFailureStoreConfigは、otlpの送り先のfailure storeと、spoolから送り直す設定です。
type TracesFailureStoreConfig struct {
	FailureHandler   failure.FailureHandlerConfig
	Replay           pkgspoolrunner.BackendSpoolRunnerConfig
	ReplaySupervisor pkgspoolsupervisor.RunnerSupervisorConfig
}

// loadTracesFailureStoreConfigは、otlpの送り先のfailure storeの設定を、ログの出力先と同じ
// keyの並び（ログの出力先の名前の位置にTRACES_OTLP）で読み込みます。traceでは、failure archiveのobject storageへのuploadと、
// 内部ログの通知先（*_HANDLER）は使わないため、読みません。
func loadTracesFailureStoreConfig(env pkgenv.Lookup) (TracesFailureStoreConfig, error) {
	cfg, selectionErr := loadFailureHandlerSelection(
		env,
		"HETEROLOG_ENABLE_TRACES_OTLP_DEGRADE", failure.DefaultEnableDegrade,
		"HETEROLOG_MAX_TRACES_OTLP_FAILURE_HANDLER_CONCURRENT", failure.DefaultMaxConcurrent,
		"HETEROLOG_TRACES_OTLP_DURABLE_TYPES", failure.DefaultDurableTypes,
		"HETEROLOG_TRACES_OTLP_DEGRADE_TYPES", failure.DefaultSpillTypes,
	)
	fileArchiveConfig, fileArchiveErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeFsstore),
		func() (failure.FileArchiveConfig, error) { return loadTracesFailureArchiveConfig(env) },
	)
	spoolConfig, spoolErr := loadSelectedConfig(
		containsDurableType(cfg.DurableType, failure.DurableTypeSpool),
		func() (failure.SpoolConfig, error) { return loadTracesFailureSpoolConfig(env) },
	)
	cfg.Fsstore = failure.FailureFileSystemStoreConfig{
		FileArchiveConfig: fileArchiveConfig,
		SpoolConfig:       spoolConfig,
	}
	result := TracesFailureStoreConfig{FailureHandler: cfg}
	var replayErr error
	if containsDurableType(cfg.DurableType, failure.DurableTypeSpool) {
		result.Replay, replayErr = loadTracesSpoolRunnerConfig(env)
		maxRestarts, maxRestartsErr := env.GetEnvInt("HETEROLOG_MAX_RESTART_SPOOL_TIMES", spoolsupervisor.DefaultMaxRestart)
		replayErr = errors.Join(replayErr, maxRestartsErr)
		result.ReplaySupervisor = pkgspoolsupervisor.RunnerSupervisorConfig{
			RestartDelay: spoolsupervisor.DefaultRestartDelay,
			MaxRestarts:  maxRestarts,
		}
	}
	return result, errors.Join(selectionErr, fileArchiveErr, spoolErr, replayErr)
}

func loadTracesFailureArchiveConfig(env pkgenv.Lookup) (failure.FileArchiveConfig, error) {
	maxFileSize, maxFileSizeErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_TRACES_OTLP_DURABLE_FILE_ARCHIVE_SIZE_MB", failure.DefaultFileArchiveMaxSizeMB, BytesPerMegabyte)
	rotate, rotateErr := env.GetEnvBool("HETEROLOG_TRACES_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", failure.DefaultFileArchiveRotateEnabled)
	maxBackups, maxBackupsErr := env.GetEnvInt("HETEROLOG_MAX_TRACES_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", failure.DefaultFileArchiveMaxBackups)
	maxAge, maxAgeErr := env.GetEnvInt("HETEROLOG_MAX_TRACES_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_AGE", failure.DefaultFileArchiveMaxAge)
	maxDirSize, maxDirSizeErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_TRACES_OTLP_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", failure.DefaultFileArchiveMaxDirSizeMB, BytesPerMegabyte)
	compress, compressErr := env.GetEnvBool("HETEROLOG_ENABLE_TRACES_OTLP_DURABLE_FILE_ARCHIVE_COMPRESS", failure.DefaultFileArchiveCompressEnabled)
	compressFormatKey := "HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT"
	compressFormat, compressFormatErr := loadLogCompressFormat(
		env.GetEnvString(compressFormatKey, string(failure.DefaultFileArchiveCompressFormat)),
		compressFormatKey,
		failure.DefaultFileArchiveCompressFormat,
	)
	return failure.FileArchiveConfig{
		MaxFileSize:                maxFileSize,
		LifecycleMode:              logconfig.ParseLogFileLifecycleMode(env.GetEnvString("HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", string(failure.DefaultFileArchiveLifecycleMode)), failure.DefaultFileArchiveLifecycleMode),
		Dir:                        env.GetEnvString("HETEROLOG_TRACES_OTLP_FAILURE_ARCHIVE_DIR", defaultTracesOTLPFailureArchiveDir),
		DirCaseRetryable:           failure.DirCaseRetryable,
		DirCasePermanent:           failure.DirCasePermanent,
		EnableRotate:               rotate,
		MaxFileBackups:             maxBackups,
		MaxFileAge:                 maxAge,
		MaxDirSize:                 maxDirSize,
		EnableFileCompress:         compress,
		CompressFormat:             compressFormat,
		FileRotation:               loadLogRotation(env.GetEnvString("HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_ROTATION", string(failure.DefaultFileArchiveRotation)), failure.DefaultFileArchiveRotation),
		FileRotationDayOfWeek:      failure.DefaultFileArchiveRotationDayOfWeek,
		MaxWriteRetries:            fsstoreconfig.MaxFileOperationRetries,
		FileOperationRetryWaitTime: fsstoreconfig.FileOperationRetryWaitTime,
		OverloadPolicyConfig: failure.FileArchiveOverloadPolicyConfig{
			OverloadPolicy:   failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", string(failure.OverloadPolicyDrop)), failure.OverloadPolicyDrop),
			SpillDestination: failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
		},
		HandlerConfig: internalerror.RoutingConfig{},
	}, errors.Join(maxFileSizeErr, rotateErr, maxBackupsErr, maxAgeErr, maxDirSizeErr, compressErr, compressFormatErr)
}

func loadTracesFailureSpoolConfig(env pkgenv.Lookup) (failure.SpoolConfig, error) {
	maxCalls, maxCallsErr := env.GetEnvUint64("HETEROLOG_MAX_TRACES_OTLP_SPOOL_SCANNED_CACHE_USAGE_COUNT", failure.DefaultMaxSpoolScannedCacheUsageCount)
	cacheLifetime, cacheLifetimeErr := env.GetEnvDuration("HETEROLOG_TRACES_OTLP_SPOOL_CACHE_LIFETIME_SECOND", failure.DefaultSpoolCacheLifetime)
	maxDirSize, maxDirSizeErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_TRACES_OTLP_SPOOL_FILE_ACTIVE_DIR_SIZE_MB", failure.DefaultSpoolActiveMaxDirSizeMB, BytesPerMegabyte)
	backlogWarnAfterDays, backlogWarnAfterDaysErr := env.GetEnvInt("HETEROLOG_TRACES_OTLP_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS", failure.DefaultSpoolActiveBacklogWarnAfterDays)
	useMemoryPressure, useMemoryPressureErr := env.GetEnvBool("HETEROLOG_USE_MEMORY_PRESSURE_TRACES_OTLP_SPOOL_OVERLOAD", failure.DefaultUseMemoryPressureForOverload)
	return failure.SpoolConfig{
		Dir: env.GetEnvString("HETEROLOG_TRACES_OTLP_FAILURE_SPOOL_DIR", defaultTracesOTLPFailureSpoolDir),
		OverloadPolicyConfig: failure.SpoolOverloadPolicyConfig{
			OverloadPolicy:               failure.ParseOverloadPolicy(env.GetEnvString("HETEROLOG_TRACES_OTLP_SPOOL_OVERLOAD_POLICY", string(failure.DefaultSpoolOverloadPolicy)), failure.DefaultSpoolOverloadPolicy),
			SpillDestination:             failure.ParseSpillDestination(env.GetEnvString("HETEROLOG_TRACES_OTLP_SPOOL_SECONDARY_DESTINATION", string(failure.SpillDestinationStdout)), failure.SpillDestinationStdout),
			MaxCallsSinceScan:            maxCalls,
			StatsScanInterval:            cacheLifetime,
			MaxDirSize:                   maxDirSize,
			ActiveBacklogWarnAfterDays:   backlogWarnAfterDays,
			UseMemoryPressureForOverload: useMemoryPressure,
		},
		HandlerConfig: internalerror.RoutingConfig{},
	}, errors.Join(maxCallsErr, cacheLifetimeErr, maxDirSizeErr, backlogWarnAfterDaysErr, useMemoryPressureErr)
}

func loadTracesSpoolRunnerConfig(env pkgenv.Lookup) (pkgspoolrunner.BackendSpoolRunnerConfig, error) {
	interval, intervalErr := env.GetEnvDurationMillisecond("HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_INTERVAL_MILLISECOND", spoolrunner.DefaultRunnerInterval)
	stopOnError, stopOnErrorErr := env.GetEnvBool("HETEROLOG_ENABLE_TRACES_OTLP_SPOOL_RUNNER_STOP_ON_ERROR", spoolrunner.DefaultStopOnError)
	purgeDone, purgeDoneErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_TRACES_OTLP_SPOOL_DONE", spoollifecycle.DefaultEnablePurgeDone)
	doneAge, doneAgeErr := env.GetEnvInt("HETEROLOG_MAX_TRACES_OTLP_SPOOL_DONE_AGE", spoollifecycle.DefaultDoneMaxFileAge)
	doneDirSize, doneDirSizeErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_TRACES_OTLP_SPOOL_DONE_DIR_SIZE_MB", spoollifecycle.DefaultDoneMaxDirSizeMB, BytesPerMegabyte)
	purgeDead, purgeDeadErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_TRACES_OTLP_SPOOL_DEAD", spoollifecycle.DefaultEnablePurgeDead)
	deadAge, deadAgeErr := env.GetEnvInt("HETEROLOG_MAX_TRACES_OTLP_SPOOL_DEAD_AGE", spoollifecycle.DefaultDeadMaxFileAges)
	deadDirSize, deadDirSizeErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_TRACES_OTLP_SPOOL_DEAD_DIR_SIZE_MB", spoollifecycle.DefaultDeadMaxDirSizes, BytesPerMegabyte)
	purgeQuarantine, purgeQuarantineErr := env.GetEnvBool("HETEROLOG_ENABLE_PURGE_TRACES_OTLP_SPOOL_QUARANTINE", spoollifecycle.DefaultEnablePurgeQuarantine)
	quarantineAge, quarantineAgeErr := env.GetEnvInt("HETEROLOG_MAX_TRACES_OTLP_SPOOL_QUARANTINE_AGE", spoollifecycle.DefaultQuarantineMaxFileAge)
	quarantineDirSize, quarantineDirSizeErr := env.GetEnvUint64Scaled("HETEROLOG_MAX_TRACES_OTLP_SPOOL_QUARANTINE_DIR_SIZE_MB", spoollifecycle.DefaultQuarantineMaxDirSize, BytesPerMegabyte)
	purgeInterval, purgeIntervalErr := env.GetEnvDuration("HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_PURGE_INTERVAL_SECOND", spoollifecycle.DefaultSizePurgeInterval)
	sizeEstimateCache, sizeEstimateCacheErr := env.GetEnvBool("HETEROLOG_ENABLE_TRACES_OTLP_SPOOL_SIZE_ESTIMATE_CACHE", spoollifecycle.DefaultEnableSizeEstimateCache)
	fullScanEveryNChecks, fullScanEveryNChecksErr := env.GetEnvInt("HETEROLOG_TRACES_OTLP_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS", spoollifecycle.DefaultSizeFullScanEveryNChecks)
	fullScanMaxInterval, fullScanMaxIntervalErr := env.GetEnvDuration("HETEROLOG_MAX_TRACES_OTLP_SPOOL_FULL_SCAN_INTERVAL_SECOND", spoollifecycle.DefaultSizeFullScanMaxInterval)
	finalizeRetryDelay, finalizeRetryDelayErr := env.GetEnvDurationMillisecond("HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND", spoolrunner.DefaultFinalizeRetryDelay)
	finalizeEntries, finalizeEntriesErr := env.GetEnvInt("HETEROLOG_MAX_TRACES_OTLP_SPOOL_FINALIZE_FAILURE_ENTRIES", spoolrunner.DefaultMaxFinalizeEntries)
	maxRetryAfter, maxRetryAfterErr := getEnvMaxRetryAfter(env, "HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND", spoolrunner.DefaultMaxRetryAfter)
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
