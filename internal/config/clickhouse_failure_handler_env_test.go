package config

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	failurecfg "github.com/go-jappe/heterolog/internal/config/failure"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	loggerdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestLoadConfigReadsClickHouseFailureHandlerFullEnvKeysPerScope(t *testing.T) {
	root := t.TempDir()
	setup := []string{
		"HETEROLOG_APP_NAME=clickhouse-failure-full-key-test",
		"HETEROLOG_TARGET=clickhouse-failure-target",
		"HETEROLOG_APP_STATE_DIR=" + filepath.Join(root, "state"),
		"HETEROLOG_SYSTEM_LIB_DIR=" + filepath.Join(root, "lib"),
		"HETEROLOG_SYSTEM_LOG_DIR=" + filepath.Join(root, "log"),
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=clickhouse",
		"HETEROLOG_ASYNC_LOG_TYPE=clickhouse",
		"HETEROLOG_DELIVERY_LOG_TYPE=clickhouse",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_CLICKHOUSE_ENDPOINT=http://sync-clickhouse:8123",
		"HETEROLOG_ASYNC_CLICKHOUSE_ENDPOINT=http://async-clickhouse:8123",
		"HETEROLOG_DELIVERY_CLICKHOUSE_ENDPOINT=http://delivery-clickhouse:8123",
		"HETEROLOG_CLICKHOUSE_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_DEBUG_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_DEBUG_HANDLER=stdout",
		// Deliveryの退避先はfileArchiveだけで、既定値と同じ選択です。
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES=fileArchive",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION=us-east-1",
		"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=test-access-key",
		"HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=test-secret-key",
	}
	// 各行は既定値と異なる値にし、全てのkeyが読込結果に現れるようにします。値の候補が
	// 少ないkeyはLoggerContractKindの間で同じ値になる場合があり、その分離は行ごとの
	// 読込比較で確認します。
	contractLines := []string{
		"HETEROLOG_MAX_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=11",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_MAX_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB=101",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR=sync-archive",
		"HETEROLOG_CLICKHOUSE_DURABLE_RETRYABLE_DIR=sync-retryable",
		"HETEROLOG_CLICKHOUSE_DURABLE_PERMANENT_DIR=sync-permanent",
		"HETEROLOG_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE=false",
		"HETEROLOG_MAX_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS=111",
		"HETEROLOG_MAX_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_AGE=112",
		"HETEROLOG_MAX_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB=113",
		"HETEROLOG_ENABLE_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS=false",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT=gzip",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_ROTATION=monthly",
		"HETEROLOG_ENABLE_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET=sync-bucket",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX=sync/archive",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=degrade",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=nfs",
		"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR=sync-spool",
		"HETEROLOG_CLICKHOUSE_SPOOL_OVERLOAD_POLICY=degrade",
		"HETEROLOG_CLICKHOUSE_SPOOL_SECONDARY_DESTINATION=s3",
		"HETEROLOG_MAX_CLICKHOUSE_SPOOL_SCANNED_CACHE_USAGE_COUNT=121",
		"HETEROLOG_CLICKHOUSE_SPOOL_CACHE_LIFETIME_SECOND=122",
		"HETEROLOG_MAX_CLICKHOUSE_SPOOL_FILE_ACTIVE_DIR_SIZE_MB=123",
		"HETEROLOG_CLICKHOUSE_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS=124",
		"HETEROLOG_USE_MEMORY_PRESSURE_CLICKHOUSE_SPOOL_OVERLOAD=true",

		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=21",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB=201",
		"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_ARCHIVE_DIR=async-archive",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_RETRYABLE_DIR=async-retryable",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_PERMANENT_DIR=async-permanent",
		"HETEROLOG_ASYNC_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE=false",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS=211",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_AGE=212",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB=213",
		"HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS=false",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT=lz4",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_ROTATION=weekly",
		"HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET=async-bucket",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX=async/archive",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=degrade",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=s3",
		"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_SPOOL_DIR=async-spool",
		"HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_OVERLOAD_POLICY=degrade",
		"HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_SECONDARY_DESTINATION=nfs",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_SPOOL_SCANNED_CACHE_USAGE_COUNT=221",
		"HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_CACHE_LIFETIME_SECOND=222",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_SPOOL_FILE_ACTIVE_DIR_SIZE_MB=223",
		"HETEROLOG_ASYNC_CLICKHOUSE_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS=224",
		"HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_CLICKHOUSE_SPOOL_OVERLOAD=true",

		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB=301",
		"HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR=delivery-archive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_RETRYABLE_DIR=delivery-retryable",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_PERMANENT_DIR=delivery-permanent",
		"HETEROLOG_DELIVERY_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE=false",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS=311",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_AGE=312",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB=313",
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS=false",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT=xz",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_ROTATION=monthly",
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET=delivery-bucket",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX=delivery/archive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=degrade",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=nfs",
	}
	lines := append(append([]string(nil), setup...), contractLines...)
	cfg := loadConfigFromTempEnv(t, lines)
	// 全ての行が読み込まれ、その行のLoggerContractKindの設定だけに効くことを確認します。
	assertEveryContractEnvLineChangesItsContract(t, lines, contractLines, map[string]string{
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD": "delivery uploads also select the delivery compressed log upload failure store",
	})

	tests := []struct {
		name string
		got  failurecfg.FailureHandlerConfig
		want clickHouseFailureEnvExpectation
	}{
		{
			name: "sync",
			got:  cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig,
			want: clickHouseFailureEnvExpectation{
				maxConcurrent: 11,
				durableTypes:  []loggerdurable.DurableType{loggerdurable.DurableTypeSpool, loggerdurable.DurableTypeFileArchive},
				archive: clickHouseArchiveEnvExpectation{
					maxFileSize:      101 * BytesPerMegabyte,
					dir:              "sync-archive",
					retryableDir:     "sync-retryable",
					permanentDir:     "sync-permanent",
					enableRotate:     false,
					maxBackups:       111,
					maxAge:           112,
					maxDirSize:       113 * BytesPerMegabyte,
					enableCompress:   false,
					compressFormat:   pkgfsstore.CompressGzip,
					rotation:         pkgfsstore.FileRotationMonthly,
					bucket:           "sync-bucket",
					objectPrefix:     "sync/archive",
					overloadPolicy:   loggerdurable.OverloadPolicyDegrade,
					spillDestination: loggerdurable.SpillDestinationNFS,
				},
				spool: clickHouseSpoolEnvExpectation{
					dir:                        "sync-spool",
					overloadPolicy:             loggerdurable.OverloadPolicyDegrade,
					spillDestination:           loggerdurable.SpillDestinationS3,
					maxCallsSinceScan:          121,
					statsScanInterval:          122 * time.Second,
					maxDirSize:                 123 * BytesPerMegabyte,
					activeBacklogWarnAfterDays: 124,
					useMemoryPressure:          true,
				},
			},
		},
		{
			name: "async",
			got:  cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig,
			want: clickHouseFailureEnvExpectation{
				maxConcurrent: 21,
				durableTypes:  []loggerdurable.DurableType{loggerdurable.DurableTypeSpool, loggerdurable.DurableTypeFileArchive},
				archive: clickHouseArchiveEnvExpectation{
					maxFileSize:      201 * BytesPerMegabyte,
					dir:              "async-archive",
					retryableDir:     "async-retryable",
					permanentDir:     "async-permanent",
					enableRotate:     false,
					maxBackups:       211,
					maxAge:           212,
					maxDirSize:       213 * BytesPerMegabyte,
					enableCompress:   false,
					compressFormat:   pkgfsstore.CompressLz4,
					rotation:         pkgfsstore.FileRotationWeekly,
					bucket:           "async-bucket",
					objectPrefix:     "async/archive",
					overloadPolicy:   loggerdurable.OverloadPolicyDegrade,
					spillDestination: loggerdurable.SpillDestinationS3,
				},
				spool: clickHouseSpoolEnvExpectation{
					dir:                        "async-spool",
					overloadPolicy:             loggerdurable.OverloadPolicyDegrade,
					spillDestination:           loggerdurable.SpillDestinationNFS,
					maxCallsSinceScan:          221,
					statsScanInterval:          222 * time.Second,
					maxDirSize:                 223 * BytesPerMegabyte,
					activeBacklogWarnAfterDays: 224,
					useMemoryPressure:          true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertClickHouseFailureEnvConfig(t, tt.got, tt.want)
		})
	}

	t.Run("delivery", func(t *testing.T) {
		assertDeliveryClickHouseFailureEnvConfig(
			t,
			cfg.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig,
			clickHouseArchiveEnvExpectation{
				maxFileSize:      301 * BytesPerMegabyte,
				dir:              "delivery-archive",
				retryableDir:     "delivery-retryable",
				permanentDir:     "delivery-permanent",
				enableRotate:     false,
				maxBackups:       311,
				maxAge:           312,
				maxDirSize:       313 * BytesPerMegabyte,
				enableCompress:   false,
				compressFormat:   pkgfsstore.CompressXz,
				rotation:         pkgfsstore.FileRotationMonthly,
				bucket:           "delivery-bucket",
				objectPrefix:     "delivery/archive",
				overloadPolicy:   loggerdurable.OverloadPolicyDegrade,
				spillDestination: loggerdurable.SpillDestinationNFS,
			},
		)
	})
}

type clickHouseFailureEnvExpectation struct {
	maxConcurrent int
	durableTypes  []loggerdurable.DurableType
	archive       clickHouseArchiveEnvExpectation
	spool         clickHouseSpoolEnvExpectation
}

type clickHouseArchiveEnvExpectation struct {
	maxFileSize      uint64
	dir              string
	retryableDir     string
	permanentDir     string
	enableRotate     bool
	maxBackups       int
	maxAge           int
	maxDirSize       uint64
	enableCompress   bool
	compressFormat   pkgfsstore.CompressFormat
	rotation         pkgfsstore.FileRotationType
	bucket           string
	objectPrefix     string
	overloadPolicy   loggerdurable.OverloadPolicy
	spillDestination loggerdurable.SpillDestination
}

type clickHouseSpoolEnvExpectation struct {
	dir                        string
	overloadPolicy             loggerdurable.OverloadPolicy
	spillDestination           loggerdurable.SpillDestination
	maxCallsSinceScan          uint64
	statsScanInterval          time.Duration
	maxDirSize                 uint64
	activeBacklogWarnAfterDays int
	useMemoryPressure          bool
}

func assertClickHouseFailureEnvConfig(t *testing.T, got failurecfg.FailureHandlerConfig, want clickHouseFailureEnvExpectation) {
	t.Helper()

	assertComparable(t, "MaxConcurrent", got.MaxConcurrent, want.maxConcurrent)
	assertSlice(t, "DurableType", got.DurableType, want.durableTypes)

	archive := got.Fsstore.FileArchiveConfig
	assertComparable(t, "archive MaxFileSize", archive.MaxFileSize, want.archive.maxFileSize)
	assertComparable(t, "archive Dir", archive.Dir, want.archive.dir)
	assertComparable(t, "archive DirCaseRetryable", archive.DirCaseRetryable, want.archive.retryableDir)
	assertComparable(t, "archive DirCasePermanent", archive.DirCasePermanent, want.archive.permanentDir)
	assertComparable(t, "archive EnableRotate", archive.EnableRotate, want.archive.enableRotate)
	assertComparable(t, "archive MaxFileBackups", archive.MaxFileBackups, want.archive.maxBackups)
	assertComparable(t, "archive MaxFileAge", archive.MaxFileAge, want.archive.maxAge)
	assertComparable(t, "archive MaxDirSize", archive.MaxDirSize, want.archive.maxDirSize)
	assertComparable(t, "archive EnableFileCompress", archive.EnableFileCompress, want.archive.enableCompress)
	assertComparable(t, "archive CompressFormat", archive.CompressFormat, want.archive.compressFormat)
	assertComparable(t, "archive FileRotation", archive.FileRotation, want.archive.rotation)
	assertComparable(t, "archive object storage Enabled", archive.ObjectStorageUploadConfig.Enabled, true)
	assertSlice(t, "archive object storage Targets", archive.ObjectStorageUploadConfig.Targets, []pkgobjectstorage.UploadTarget{pkgobjectstorage.UploadTargetS3})
	assertComparable(t, "archive object storage bucket", archive.ObjectStorageUploadConfig.S3.Bucket, want.archive.bucket)
	assertComparable(t, "archive object storage prefix", archive.ObjectStorageUploadConfig.S3.Prefix, want.archive.objectPrefix)
	assertComparable(t, "archive overload policy", archive.OverloadPolicyConfig.OverloadPolicy, want.archive.overloadPolicy)
	assertComparable(t, "archive spill destination", archive.OverloadPolicyConfig.SpillDestination, want.archive.spillDestination)

	spool := got.Fsstore.SpoolConfig
	assertComparable(t, "spool Dir", spool.Dir, want.spool.dir)
	assertComparable(t, "spool overload policy", spool.OverloadPolicyConfig.OverloadPolicy, want.spool.overloadPolicy)
	assertComparable(t, "spool spill destination", spool.OverloadPolicyConfig.SpillDestination, want.spool.spillDestination)
	assertComparable(t, "spool MaxCallsSinceScan", spool.OverloadPolicyConfig.MaxCallsSinceScan, want.spool.maxCallsSinceScan)
	assertComparable(t, "spool StatsScanInterval", spool.OverloadPolicyConfig.StatsScanInterval, want.spool.statsScanInterval)
	assertComparable(t, "spool MaxDirSize", spool.OverloadPolicyConfig.MaxDirSize, want.spool.maxDirSize)
	assertComparable(t, "spool ActiveBacklogWarnAfterDays", spool.OverloadPolicyConfig.ActiveBacklogWarnAfterDays, want.spool.activeBacklogWarnAfterDays)
	assertComparable(t, "spool UseMemoryPressureForOverload", spool.OverloadPolicyConfig.UseMemoryPressureForOverload, want.spool.useMemoryPressure)
}

func assertDeliveryClickHouseFailureEnvConfig(
	t *testing.T,
	got failurecfg.DeliveryFailureHandlerConfig,
	want clickHouseArchiveEnvExpectation,
) {
	t.Helper()

	assertSlice(t, "delivery DurableType", got.DurableType, []loggerdurable.DurableType{loggerdurable.DurableTypeFileArchive})

	archive := got.FileArchiveConfig
	assertComparable(t, "delivery archive MaxFileSize", archive.MaxFileSize, want.maxFileSize)
	assertComparable(t, "delivery archive Dir", archive.Dir, want.dir)
	assertComparable(t, "delivery archive DirCaseRetryable", archive.DirCaseRetryable, want.retryableDir)
	assertComparable(t, "delivery archive DirCasePermanent", archive.DirCasePermanent, want.permanentDir)
	assertComparable(t, "delivery archive EnableRotate", archive.EnableRotate, want.enableRotate)
	assertComparable(t, "delivery archive MaxFileBackups", archive.MaxFileBackups, want.maxBackups)
	assertComparable(t, "delivery archive MaxFileAge", archive.MaxFileAge, want.maxAge)
	assertComparable(t, "delivery archive MaxDirSize", archive.MaxDirSize, want.maxDirSize)
	assertComparable(t, "delivery archive EnableFileCompress", archive.EnableFileCompress, want.enableCompress)
	assertComparable(t, "delivery archive CompressFormat", archive.CompressFormat, want.compressFormat)
	assertComparable(t, "delivery archive FileRotation", archive.FileRotation, want.rotation)
	assertComparable(t, "delivery archive object storage Enabled", archive.ObjectStorageUploadConfig.Enabled, true)
	assertSlice(t, "delivery archive object storage Targets", archive.ObjectStorageUploadConfig.Targets, []pkgobjectstorage.UploadTarget{pkgobjectstorage.UploadTargetS3})
	assertComparable(t, "delivery archive object storage bucket", archive.ObjectStorageUploadConfig.S3.Bucket, want.bucket)
	assertComparable(t, "delivery archive object storage prefix", archive.ObjectStorageUploadConfig.S3.Prefix, want.objectPrefix)
	assertComparable(t, "delivery archive overload policy", archive.OverloadPolicyConfig.OverloadPolicy, want.overloadPolicy)
	assertComparable(t, "delivery archive spill destination", archive.OverloadPolicyConfig.SpillDestination, want.spillDestination)
}

func assertComparable[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertSlice[T any](t *testing.T, name string, got, want []T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}
