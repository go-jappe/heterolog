package config

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/objectstoragecompressedlog"
	pkgenv "github.com/go-jappe/heterolog/pkg/env"
	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// loadStandardObjectStorageCompressedLogFileUploadFailureStoreはDurableTypeを先に確定し、
// 選択されたstandard subconfigだけをenvironmentから読み込みます。選択自体が不正な場合は
// optional設定を読まず、呼び出し元のValidateConfigに選択errorの報告を委ねます。
func loadStandardObjectStorageCompressedLogFileUploadFailureStore(env pkgenv.Lookup) (objectstoragecompressedlog.FailureStoreConfig, error) {
	defaults := objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig()
	durableTypes := loadDurableTypes(
		env.GetEnvSliceString("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", nil),
		defaults.DurableType,
	)
	cfg := objectstoragecompressedlog.FailureStoreConfig{DurableType: durableTypes}
	if failurestore.ValidateDurableTypes(
		"standard compressed log file upload failure",
		durableTypes,
		failurestore.DurableTypeFileArchive,
		failurestore.DurableTypeSpool,
	) != nil {
		return cfg, nil
	}
	if containsFailureStoreDurableType(durableTypes, failurestore.DurableTypeNone) {
		return cfg, nil
	}

	var errs []error
	if containsFailureStoreDurableType(durableTypes, failurestore.DurableTypeFileArchive) {
		fileArchiveConfig, err := loadStandardObjectStorageCompressedLogFileUploadFailureArchiveConfig(
			env,
			defaults.DurableStoreConfig.FileArchiveConfig,
		)
		cfg.DurableStoreConfig.FileArchiveConfig = fileArchiveConfig
		errs = append(errs, err)
	}
	if containsFailureStoreDurableType(durableTypes, failurestore.DurableTypeSpool) {
		spoolConfig, err := loadStandardObjectStorageCompressedLogFileUploadSpoolConfig(
			env,
			defaults.DurableStoreConfig.SpoolConfig,
		)
		cfg.DurableStoreConfig.SpoolConfig = spoolConfig
		errs = append(errs, err)
	}
	return cfg, errors.Join(errs...)
}

// loadDeliveryObjectStorageCompressedLogFileUploadFailureStoreはDurableTypeを先に確定し、
// 選択されたDelivery subconfigだけをenvironmentから読み込みます。選択自体が不正な場合は
// optional設定を読まず、呼び出し元のValidateConfigに選択errorの報告を委ねます。
func loadDeliveryObjectStorageCompressedLogFileUploadFailureStore(env pkgenv.Lookup) (objectstoragecompressedlog.FailureStoreConfig, error) {
	defaults := objectstoragecompressedlog.DefaultDeliveryCompressedLogFileUploadFailureStoreConfig()
	durableTypes := loadDurableTypes(
		env.GetEnvSliceString("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", nil),
		defaults.DurableType,
	)
	cfg := objectstoragecompressedlog.FailureStoreConfig{DurableType: durableTypes}
	if failurestore.ValidateDurableTypes(
		"delivery compressed log file upload failure",
		durableTypes,
		failurestore.DurableTypeFileArchive,
		failurestore.DurableTypeSpool,
	) != nil {
		return cfg, nil
	}
	if containsFailureStoreDurableType(durableTypes, failurestore.DurableTypeNone) {
		return cfg, nil
	}

	var errs []error
	if containsFailureStoreDurableType(durableTypes, failurestore.DurableTypeFileArchive) {
		fileArchiveConfig, err := loadDeliveryObjectStorageCompressedLogFileUploadFailureArchiveConfig(
			env,
			defaults.DurableStoreConfig.FileArchiveConfig,
		)
		cfg.DurableStoreConfig.FileArchiveConfig = fileArchiveConfig
		errs = append(errs, err)
	}
	if containsFailureStoreDurableType(durableTypes, failurestore.DurableTypeSpool) {
		spoolConfig, err := loadDeliveryObjectStorageCompressedLogFileUploadSpoolConfig(
			env,
			defaults.DurableStoreConfig.SpoolConfig,
		)
		cfg.DurableStoreConfig.SpoolConfig = spoolConfig
		errs = append(errs, err)
	}
	return cfg, errors.Join(errs...)
}

func loadStandardObjectStorageCompressedLogFileUploadFailureArchiveConfig(
	env pkgenv.Lookup,
	defaults objectstoragecompressedlog.FileArchiveConfig,
) (objectstoragecompressedlog.FileArchiveConfig, error) {
	var errs []error
	lifecycleInterval, err := env.GetEnvDuration(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND",
		defaults.LifecycleInterval,
	)
	errs = append(errs, err)
	retryableEnabled, err := env.GetEnvBool(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_RETRYABLE",
		defaults.Retryable.Enabled,
	)
	errs = append(errs, err)
	retryableMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_FILE_AGE_DAYS",
		defaults.Retryable.MaxFileAgeDays,
	)
	errs = append(errs, err)
	retryableMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_DIR_SIZE_GB",
		defaults.Retryable.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)
	permanentEnabled, err := env.GetEnvBool(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_PERMANENT",
		defaults.Permanent.Enabled,
	)
	errs = append(errs, err)
	permanentMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_FILE_AGE_DAYS",
		defaults.Permanent.MaxFileAgeDays,
	)
	errs = append(errs, err)
	permanentMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_DIR_SIZE_GB",
		defaults.Permanent.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)

	return objectstoragecompressedlog.FileArchiveConfig{
		DirName: env.GetEnvString(
			"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_DIR",
			defaults.DirName,
		),
		LifecycleInterval: lifecycleInterval,
		Retryable: objectstoragecompressedlog.PurgeConfig{
			Enabled:        retryableEnabled,
			MaxFileAgeDays: retryableMaxFileAgeDays,
			MaxDirSize:     retryableMaxDirSize,
		},
		Permanent: objectstoragecompressedlog.PurgeConfig{
			Enabled:        permanentEnabled,
			MaxFileAgeDays: permanentMaxFileAgeDays,
			MaxDirSize:     permanentMaxDirSize,
		},
	}, errors.Join(errs...)
}

func loadDeliveryObjectStorageCompressedLogFileUploadFailureArchiveConfig(
	env pkgenv.Lookup,
	defaults objectstoragecompressedlog.FileArchiveConfig,
) (objectstoragecompressedlog.FileArchiveConfig, error) {
	var errs []error
	lifecycleInterval, err := env.GetEnvDuration(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND",
		defaults.LifecycleInterval,
	)
	errs = append(errs, err)
	retryableEnabled, err := env.GetEnvBool(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_RETRYABLE",
		defaults.Retryable.Enabled,
	)
	errs = append(errs, err)
	retryableMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_FILE_AGE_DAYS",
		defaults.Retryable.MaxFileAgeDays,
	)
	errs = append(errs, err)
	retryableMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_DIR_SIZE_GB",
		defaults.Retryable.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)
	permanentEnabled, err := env.GetEnvBool(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_PERMANENT",
		defaults.Permanent.Enabled,
	)
	errs = append(errs, err)
	permanentMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_FILE_AGE_DAYS",
		defaults.Permanent.MaxFileAgeDays,
	)
	errs = append(errs, err)
	permanentMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_DIR_SIZE_GB",
		defaults.Permanent.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)

	return objectstoragecompressedlog.FileArchiveConfig{
		DirName: env.GetEnvString(
			"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_DIR",
			defaults.DirName,
		),
		LifecycleInterval: lifecycleInterval,
		Retryable: objectstoragecompressedlog.PurgeConfig{
			Enabled:        retryableEnabled,
			MaxFileAgeDays: retryableMaxFileAgeDays,
			MaxDirSize:     retryableMaxDirSize,
		},
		Permanent: objectstoragecompressedlog.PurgeConfig{
			Enabled:        permanentEnabled,
			MaxFileAgeDays: permanentMaxFileAgeDays,
			MaxDirSize:     permanentMaxDirSize,
		},
	}, errors.Join(errs...)
}

func loadStandardObjectStorageCompressedLogFileUploadSpoolConfig(
	env pkgenv.Lookup,
	defaults objectstoragecompressedlog.SpoolConfig,
) (objectstoragecompressedlog.SpoolConfig, error) {
	var errs []error
	runnerInterval, err := env.GetEnvDuration(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND",
		defaults.Runner.Interval,
	)
	errs = append(errs, err)
	maxRetryAfter, err := getEnvMaxRetryAfter(
		env,
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_MAX_RETRY_AFTER_SECOND",
		defaults.Runner.MaxRetryAfter,
	)
	errs = append(errs, err)
	circuitBreakerOpenCooldown, err := env.GetEnvDuration(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND",
		defaults.CircuitBreaker.OpenCooldown,
	)
	errs = append(errs, err)
	lifecycleInterval, err := env.GetEnvDuration(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_LIFECYCLE_INTERVAL_SECOND",
		defaults.Lifecycle.Interval,
	)
	errs = append(errs, err)
	doneEnabled, err := env.GetEnvBool(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DONE",
		defaults.Lifecycle.Done.Enabled,
	)
	errs = append(errs, err)
	doneMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_FILE_AGE_DAYS",
		defaults.Lifecycle.Done.MaxFileAgeDays,
	)
	errs = append(errs, err)
	doneMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_DIR_SIZE_GB",
		defaults.Lifecycle.Done.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)
	deadEnabled, err := env.GetEnvBool(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DEAD",
		defaults.Lifecycle.Dead.Enabled,
	)
	errs = append(errs, err)
	deadMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_FILE_AGE_DAYS",
		defaults.Lifecycle.Dead.MaxFileAgeDays,
	)
	errs = append(errs, err)
	deadMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_DIR_SIZE_GB",
		defaults.Lifecycle.Dead.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)
	quarantineEnabled, err := env.GetEnvBool(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_QUARANTINE",
		defaults.Lifecycle.Quarantine.Enabled,
	)
	errs = append(errs, err)
	quarantineMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_FILE_AGE_DAYS",
		defaults.Lifecycle.Quarantine.MaxFileAgeDays,
	)
	errs = append(errs, err)
	quarantineMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_DIR_SIZE_GB",
		defaults.Lifecycle.Quarantine.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)

	return objectstoragecompressedlog.SpoolConfig{
		Runner: objectstoragecompressedlog.RunnerConfig{
			Interval:      runnerInterval,
			MaxRetryAfter: maxRetryAfter,
		},
		CircuitBreaker: objectstoragecompressedlog.CircuitBreakerConfig{
			OpenCooldown: circuitBreakerOpenCooldown,
		},
		Lifecycle: objectstoragecompressedlog.LifecycleConfig{
			Interval: lifecycleInterval,
			Done: objectstoragecompressedlog.PurgeConfig{
				Enabled:        doneEnabled,
				MaxFileAgeDays: doneMaxFileAgeDays,
				MaxDirSize:     doneMaxDirSize,
			},
			Dead: objectstoragecompressedlog.PurgeConfig{
				Enabled:        deadEnabled,
				MaxFileAgeDays: deadMaxFileAgeDays,
				MaxDirSize:     deadMaxDirSize,
			},
			Quarantine: objectstoragecompressedlog.PurgeConfig{
				Enabled:        quarantineEnabled,
				MaxFileAgeDays: quarantineMaxFileAgeDays,
				MaxDirSize:     quarantineMaxDirSize,
			},
		},
	}, errors.Join(errs...)
}

func loadDeliveryObjectStorageCompressedLogFileUploadSpoolConfig(
	env pkgenv.Lookup,
	defaults objectstoragecompressedlog.SpoolConfig,
) (objectstoragecompressedlog.SpoolConfig, error) {
	var errs []error
	runnerInterval, err := env.GetEnvDuration(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND",
		defaults.Runner.Interval,
	)
	errs = append(errs, err)
	maxRetryAfter, err := getEnvMaxRetryAfter(
		env,
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_MAX_RETRY_AFTER_SECOND",
		defaults.Runner.MaxRetryAfter,
	)
	errs = append(errs, err)
	circuitBreakerOpenCooldown, err := env.GetEnvDuration(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND",
		defaults.CircuitBreaker.OpenCooldown,
	)
	errs = append(errs, err)
	lifecycleInterval, err := env.GetEnvDuration(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_LIFECYCLE_INTERVAL_SECOND",
		defaults.Lifecycle.Interval,
	)
	errs = append(errs, err)
	doneEnabled, err := env.GetEnvBool(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DONE",
		defaults.Lifecycle.Done.Enabled,
	)
	errs = append(errs, err)
	doneMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_FILE_AGE_DAYS",
		defaults.Lifecycle.Done.MaxFileAgeDays,
	)
	errs = append(errs, err)
	doneMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_DIR_SIZE_GB",
		defaults.Lifecycle.Done.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)
	deadEnabled, err := env.GetEnvBool(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DEAD",
		defaults.Lifecycle.Dead.Enabled,
	)
	errs = append(errs, err)
	deadMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_FILE_AGE_DAYS",
		defaults.Lifecycle.Dead.MaxFileAgeDays,
	)
	errs = append(errs, err)
	deadMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_DIR_SIZE_GB",
		defaults.Lifecycle.Dead.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)
	quarantineEnabled, err := env.GetEnvBool(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_QUARANTINE",
		defaults.Lifecycle.Quarantine.Enabled,
	)
	errs = append(errs, err)
	quarantineMaxFileAgeDays, err := env.GetEnvInt(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_FILE_AGE_DAYS",
		defaults.Lifecycle.Quarantine.MaxFileAgeDays,
	)
	errs = append(errs, err)
	quarantineMaxDirSize, err := env.GetEnvUint64Scaled(
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_DIR_SIZE_GB",
		defaults.Lifecycle.Quarantine.MaxDirSize/(BytesPerMegabyte*1024),
		BytesPerMegabyte*1024,
	)
	errs = append(errs, err)

	return objectstoragecompressedlog.SpoolConfig{
		Runner: objectstoragecompressedlog.RunnerConfig{
			Interval:      runnerInterval,
			MaxRetryAfter: maxRetryAfter,
		},
		CircuitBreaker: objectstoragecompressedlog.CircuitBreakerConfig{
			OpenCooldown: circuitBreakerOpenCooldown,
		},
		Lifecycle: objectstoragecompressedlog.LifecycleConfig{
			Interval: lifecycleInterval,
			Done: objectstoragecompressedlog.PurgeConfig{
				Enabled:        doneEnabled,
				MaxFileAgeDays: doneMaxFileAgeDays,
				MaxDirSize:     doneMaxDirSize,
			},
			Dead: objectstoragecompressedlog.PurgeConfig{
				Enabled:        deadEnabled,
				MaxFileAgeDays: deadMaxFileAgeDays,
				MaxDirSize:     deadMaxDirSize,
			},
			Quarantine: objectstoragecompressedlog.PurgeConfig{
				Enabled:        quarantineEnabled,
				MaxFileAgeDays: quarantineMaxFileAgeDays,
				MaxDirSize:     quarantineMaxDirSize,
			},
		},
	}, errors.Join(errs...)
}
