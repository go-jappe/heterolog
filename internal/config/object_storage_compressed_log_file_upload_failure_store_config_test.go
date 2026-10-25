package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/config/objectstoragecompressedlog"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

const objectStorageCompressedLogTestGiB = uint64(1024 * 1024 * 1024)

func enableStandardFileCompressedLogProducer(cfg *AppConfig) {
	cfg.Logger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	store := &cfg.Logger.FileConfig.FileSystemStoreConfig
	store.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	store.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{Enabled: true},
	}
}

func enableAsyncFileCompressedLogProducer(cfg *AppConfig) {
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	store := &cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig
	store.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	store.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{Enabled: true},
	}
}

func enableDeliveryFileCompressedLogProducer(cfg *AppConfig) {
	cfg.DeliveryLogger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	store := &cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig
	store.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	store.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{Enabled: true},
	}
}

func enableStandardOpenSearchFailureArchiveUpload(cfg *AppConfig) {
	cfg.Logger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	store := &cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
		FailureHandlerConfig
	store.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive}
	store.Fsstore.FileArchiveConfig.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	store.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig.Enabled = true
}

func enableDeliveryOpenSearchFailureArchiveUpload(cfg *AppConfig) {
	cfg.DeliveryLogger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	store := &cfg.DeliveryLogger.DeliveryOpenSearchConfig.
		DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig
	store.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive}
	store.FileArchiveConfig.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	store.FileArchiveConfig.ObjectStorageUploadConfig.Enabled = true
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreUsesStandardDefaults(t *testing.T) {
	clearObjectStorageCompressedLogFileUploadFailureStoreEnv(t)
	appCfg := AppConfig{}
	enableStandardFileCompressedLogProducer(&appCfg)

	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v", err)
	}
	assertObjectStorageCompressedLogFileUploadFailureStoreDefaults(t, "standard", cfg.Standard, false)
	if !reflect.DeepEqual(cfg.Delivery, objectstoragecompressedlog.FailureStoreConfig{}) {
		t.Fatalf("Delivery = %+v, want unused zero value", cfg.Delivery)
	}
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreUsesDeliveryDefaults(t *testing.T) {
	clearObjectStorageCompressedLogFileUploadFailureStoreEnv(t)
	appCfg := AppConfig{}
	enableDeliveryFileCompressedLogProducer(&appCfg)

	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v", err)
	}
	if !reflect.DeepEqual(cfg.Standard, objectstoragecompressedlog.FailureStoreConfig{}) {
		t.Fatalf("Standard = %+v, want unused zero value", cfg.Standard)
	}
	assertObjectStorageCompressedLogFileUploadFailureStoreDefaults(t, "delivery", cfg.Delivery, true)
}

// TestLoadObjectStorageCompressedLogFileUploadFailureStoreEnablesRetainedFailurePurgeWhenExplicitは、
// 既定では削除しないretryable・permanent・dead・quarantineも、明示した場合は
// 保持期間・容量による削除を有効にできることを検証します。
func TestLoadObjectStorageCompressedLogFileUploadFailureStoreEnablesRetainedFailurePurgeWhenExplicit(t *testing.T) {
	clearObjectStorageCompressedLogFileUploadFailureStoreEnv(t)
	for _, prefix := range []string{"", "DELIVERY_"} {
		for _, key := range []string{
			"ARCHIVE_ENABLE_PURGE_RETRYABLE",
			"ARCHIVE_ENABLE_PURGE_PERMANENT",
			"SPOOL_ENABLE_PURGE_DEAD",
			"SPOOL_ENABLE_PURGE_QUARANTINE",
		} {
			t.Setenv("HETEROLOG_"+prefix+"OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_"+key, "true")
		}
	}
	appCfg := AppConfig{}
	enableStandardFileCompressedLogProducer(&appCfg)
	enableDeliveryFileCompressedLogProducer(&appCfg)
	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v", err)
	}
	for name, store := range map[string]objectstoragecompressedlog.FailureStoreConfig{
		"standard": cfg.Standard,
		"delivery": cfg.Delivery,
	} {
		archive := store.DurableStoreConfig.FileArchiveConfig
		lifecycle := store.DurableStoreConfig.SpoolConfig.Lifecycle
		for purgeName, enabled := range map[string]bool{
			"archive retryable": archive.Retryable.Enabled,
			"archive permanent": archive.Permanent.Enabled,
			"spool dead":        lifecycle.Dead.Enabled,
			"spool quarantine":  lifecycle.Quarantine.Enabled,
		} {
			if !enabled {
				t.Errorf("%s %s purge = false, want explicitly enabled", name, purgeName)
			}
		}
	}
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreReadsOnlySelectedSpool(t *testing.T) {
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "spool")
	// fileArchiveが未選択なら、その残存不正値は起動を妨げません。
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND", "invalid")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "7")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_LIFECYCLE_INTERVAL_SECOND", "9")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND", "11")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DONE", "false")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_FILE_AGE_DAYS", "17")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_DIR_SIZE_GB", "19")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DEAD", "true")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_FILE_AGE_DAYS", "23")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_DIR_SIZE_GB", "29")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_QUARANTINE", "true")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_FILE_AGE_DAYS", "31")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_DIR_SIZE_GB", "37")

	appCfg := AppConfig{}
	enableAsyncFileCompressedLogProducer(&appCfg)
	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v", err)
	}

	if !reflect.DeepEqual(
		cfg.Standard.DurableType,
		[]failurestore.DurableType{failurestore.DurableTypeSpool},
	) {
		t.Fatalf("DurableType = %v, want spool", cfg.Standard.DurableType)
	}
	if cfg.Standard.DurableStoreConfig.FileArchiveConfig != (objectstoragecompressedlog.FileArchiveConfig{}) {
		t.Fatalf(
			"unselected FileArchiveConfig = %+v, want zero value",
			cfg.Standard.DurableStoreConfig.FileArchiveConfig,
		)
	}
	spool := cfg.Standard.DurableStoreConfig.SpoolConfig
	if spool.Runner.Interval != 7*time.Second ||
		spool.Lifecycle.Interval != 9*time.Second ||
		spool.CircuitBreaker.OpenCooldown != 11*time.Second {
		t.Fatalf("standard spool runner/circuit breaker = %+v, want configured values", spool)
	}
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, "done", spool.Lifecycle.Done, false, 17, 19*objectStorageCompressedLogTestGiB)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, "dead", spool.Lifecycle.Dead, true, 23, 29*objectStorageCompressedLogTestGiB)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, "quarantine", spool.Lifecycle.Quarantine, true, 31, 37*objectStorageCompressedLogTestGiB)
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreReadsOnlySelectedFileArchive(t *testing.T) {
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "fileArchive")
	// spoolが未選択なら、その残存不正値は起動を妨げません。
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "invalid")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND", "invalid")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_DIR", "compressed_log_archive")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND", "17")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_RETRYABLE", "true")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_FILE_AGE_DAYS", "41")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_DIR_SIZE_GB", "43")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_PERMANENT", "false")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_FILE_AGE_DAYS", "47")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_DIR_SIZE_GB", "53")

	appCfg := AppConfig{}
	enableStandardFileCompressedLogProducer(&appCfg)
	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v", err)
	}

	if cfg.Standard.DurableStoreConfig.SpoolConfig != (objectstoragecompressedlog.SpoolConfig{}) {
		t.Fatalf(
			"unselected SpoolConfig = %+v, want zero value",
			cfg.Standard.DurableStoreConfig.SpoolConfig,
		)
	}
	archive := cfg.Standard.DurableStoreConfig.FileArchiveConfig
	if archive.DirName != "compressed_log_archive" || archive.LifecycleInterval != 17*time.Second {
		t.Fatalf("FileArchiveConfig = %+v, want configured identity and interval", archive)
	}
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, "retryable", archive.Retryable, true, 41, 43*objectStorageCompressedLogTestGiB)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, "permanent", archive.Permanent, false, 47, 53*objectStorageCompressedLogTestGiB)
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreNoneIgnoresSubconfigEnvironment(t *testing.T) {
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "none")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND", "invalid")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "invalid")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND", "invalid")

	appCfg := AppConfig{}
	enableStandardFileCompressedLogProducer(&appCfg)
	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("none selection read unused subconfig: %v", err)
	}
	want := objectstoragecompressedlog.FailureStoreConfig{
		DurableType: []failurestore.DurableType{failurestore.DurableTypeNone},
	}
	if !reflect.DeepEqual(cfg.Standard, want) {
		t.Fatalf("Standard = %+v, want %+v", cfg.Standard, want)
	}
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreIgnoresUnusedContractEnvironment(t *testing.T) {
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "unknown")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "invalid")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "unknown")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "invalid")

	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), AppConfig{})
	if err != nil {
		t.Fatalf("unused compressed log file upload failure store setting produced error: %v", err)
	}
	if !reflect.DeepEqual(cfg, objectstoragecompressedlog.Config{}) {
		t.Fatalf("config = %+v, want zero value", cfg)
	}
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreIgnoresInactiveProducerEnvironment(
	t *testing.T,
) {
	tests := []struct {
		name      string
		configure func(*AppConfig)
	}{
		{
			name: "unselected sync file logger",
			configure: func(cfg *AppConfig) {
				enableStandardFileCompressedLogProducer(cfg)
				cfg.Logger.LogOutputDestinations = nil
			},
		},
		{
			name: "unselected async file logger",
			configure: func(cfg *AppConfig) {
				enableAsyncFileCompressedLogProducer(cfg)
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = nil
			},
		},
		{
			name: "disabled sync file lifecycle",
			configure: func(cfg *AppConfig) {
				enableStandardFileCompressedLogProducer(cfg)
				cfg.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode =
					pkgfsstore.FileLifecycleModeDisable
			},
		},
		{
			name: "sync file target is not applicable",
			configure: func(cfg *AppConfig) {
				enableStandardFileCompressedLogProducer(cfg)
				upload := &cfg.Logger.FileConfig.FileSystemStoreConfig.
					ObjectStorageUploadConfig
				upload.Default.Enabled = false
				upload.Error.Enabled = true
			},
		},
		{
			name: "unselected backend failure archive upload",
			configure: func(cfg *AppConfig) {
				enableStandardOpenSearchFailureArchiveUpload(cfg)
				cfg.Logger.LogOutputDestinations = nil
			},
		},
		{
			name: "unselected delivery file logger",
			configure: func(cfg *AppConfig) {
				enableDeliveryFileCompressedLogProducer(cfg)
				cfg.DeliveryLogger.LogOutputDestinations = nil
			},
		},
		{
			name: "delivery file lifecycle has no upload owner",
			configure: func(cfg *AppConfig) {
				enableDeliveryFileCompressedLogProducer(cfg)
				cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.
					LifecycleMode = pkgfsstore.FileLifecycleModeInline
			},
		},
		{
			name: "delivery file target is not applicable",
			configure: func(cfg *AppConfig) {
				enableDeliveryFileCompressedLogProducer(cfg)
				upload := &cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.
					ObjectStorageUploadConfig
				upload.Default.Enabled = false
				upload.Debug.Enabled = true
			},
		},
		{
			name: "disabled delivery backend failure archive lifecycle",
			configure: func(cfg *AppConfig) {
				enableDeliveryOpenSearchFailureArchiveUpload(cfg)
				cfg.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.
					FailureHandlerConfig.FileArchiveConfig.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "unknown")
			t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "unknown")

			var appCfg AppConfig
			tt.configure(&appCfg)
			cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
			if err != nil {
				t.Fatalf("inactive producer read stale failure-store environment: %v", err)
			}
			if !reflect.DeepEqual(cfg, objectstoragecompressedlog.Config{}) {
				t.Fatalf("config = %+v, want unused zero value", cfg)
			}
		})
	}
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreReadsOnlyRequiredContract(t *testing.T) {
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "unknown")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "spool")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "41")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND", "43")

	appCfg := AppConfig{}
	enableDeliveryFileCompressedLogProducer(&appCfg)
	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("unused standard compressed log file setting produced error: %v", err)
	}
	if got := cfg.Delivery.DurableStoreConfig.SpoolConfig.Runner.Interval; got != 41*time.Second {
		t.Fatalf("Delivery spool Runner.Interval = %s, want 41s", got)
	}
	if got := cfg.Delivery.DurableStoreConfig.SpoolConfig.CircuitBreaker.OpenCooldown; got != 43*time.Second {
		t.Fatalf("Delivery spool CircuitBreaker.OpenCooldown = %s, want 43s", got)
	}
}

func TestBackendFailureArchiveUploadLoadsContractFailureStoreWithoutFileLogger(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*AppConfig)
		contract  pkglogger.LoggerContractKind
	}{
		{name: "sync", configure: enableStandardOpenSearchFailureArchiveUpload, contract: pkglogger.LoggerContractSync},
		{name: "async", configure: func(cfg *AppConfig) {
			enableStandardOpenSearchFailureArchiveUpload(cfg)
			cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig = cfg.Logger.OpenSearchConfig
			cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = cfg.Logger.LogOutputDestinations
			cfg.Logger.LogOutputDestinations = nil
		}, contract: pkglogger.LoggerContractAsyncBestEffort},
		{name: "delivery", configure: enableDeliveryOpenSearchFailureArchiveUpload, contract: pkglogger.LoggerContractDelivery},
	} {
		t.Run(test.name, func(t *testing.T) {
			var cfg AppConfig
			test.configure(&cfg)
			selectedKey := "HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES"
			unusedKey := "HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES"
			if test.contract == pkglogger.LoggerContractDelivery {
				selectedKey, unusedKey = unusedKey, selectedKey
			}
			t.Setenv(selectedKey, "fileArchive")
			t.Setenv(unusedKey, "unknown")
			loaded, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			selected, unused := loaded.Standard, loaded.Delivery
			if test.contract == pkglogger.LoggerContractDelivery {
				selected, unused = unused, selected
			}
			if !reflect.DeepEqual(selected.DurableType, []failurestore.DurableType{failurestore.DurableTypeFileArchive}) || !reflect.DeepEqual(unused, objectstoragecompressedlog.FailureStoreConfig{}) {
				t.Fatalf("loaded failure stores = %+v, want only %s fileArchive settings", loaded, test.contract)
			}
		})
	}
}

func TestMappedCompressedUploadsUseSameSpoolSettingsPerLoggerContract(t *testing.T) {
	var cfg AppConfig
	cfg.RuntimeEnv.Cloud = &configruntime.CloudInfo{}
	cfg.RuntimeConfig.Target = "app"
	cfg.FsstoreConfig.SpoolPath = t.TempDir()
	cfg.CompressedLogFileUploadFailureStore.Standard = objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig()
	cfg.CompressedLogFileUploadFailureStore.Delivery = objectstoragecompressedlog.DefaultDeliveryCompressedLogFileUploadFailureStoreConfig()
	enableStandardFileCompressedLogProducer(&cfg)
	enableStandardOpenSearchFailureArchiveUpload(&cfg)
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile, pkglogger.LogOutputOpenSearch, pkglogger.LogOutputLoki}
	cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig = cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig
	cfg.AsyncBestEffortMultiLoggerConfig.FileConfig = cfg.Logger.FileConfig
	cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig = cfg.Logger.OpenSearchConfig
	cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig = cfg.Logger.LokiConfig
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = cfg.Logger.LogOutputDestinations
	enableDeliveryFileCompressedLogProducer(&cfg)
	enableDeliveryOpenSearchFailureArchiveUpload(&cfg)
	cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile, pkglogger.LogOutputOpenSearch, pkglogger.LogOutputLoki}
	cfg.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.FailureHandlerConfig = cfg.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig
	syncCfg := ToLoggerConfig(cfg)
	asyncCfg := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryCfg := ToDeliveryLoggerConfig(cfg)
	for _, test := range []struct {
		name     string
		file     objectstorage.LogFileUploadConfig
		backends []objectstorage.UploadTargetConfig
	}{
		{name: "sync", file: syncCfg.File.ObjectStorageUploadConfig, backends: []objectstorage.UploadTargetConfig{
			syncCfg.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
			syncCfg.Loki.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		}},
		{name: "async", file: asyncCfg.File.ObjectStorageUploadConfig, backends: []objectstorage.UploadTargetConfig{
			asyncCfg.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
			asyncCfg.Loki.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		}},
		{name: "delivery", file: deliveryCfg.File.LifecycleRunnerSettings.ObjectStorageUploadConfig, backends: []objectstorage.UploadTargetConfig{
			deliveryCfg.OpenSearch.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
			deliveryCfg.Loki.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			shared := test.file.CompressedLogFileUploadSpool
			if shared == nil || test.file.Default.CompressedLogFileUploadSpool == nil ||
				compressedLogSpoolSettings(test.file.Default.CompressedLogFileUploadSpool) != compressedLogSpoolSettings(shared) {
				t.Fatal("ordinary file upload has different spool settings")
			}
			for _, upload := range test.backends {
				if upload.CompressedLogFileUploadSpool == nil ||
					compressedLogSpoolSettings(upload.CompressedLogFileUploadSpool) != compressedLogSpoolSettings(shared) {
					t.Fatal("backend archive uses different spool settings for the same logger")
				}
			}
		})
	}
	if syncCfg.File.ObjectStorageUploadConfig.CompressedLogFileUploadSpool.LoggerContract() == asyncCfg.File.ObjectStorageUploadConfig.CompressedLogFileUploadSpool.LoggerContract() {
		t.Fatal("different logger contracts map to the same spool contract")
	}
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreMapsEveryDeliveryEnvironmentKey(
	t *testing.T,
) {
	// Delivery loaderがStandard用keyを誤って参照しても検出できるよう、異なる有効値を設定します。
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_DIR", "standard_archive")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND", "101")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_RETRYABLE", "false")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_FILE_AGE_DAYS", "103")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_DIR_SIZE_GB", "107")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_PERMANENT", "true")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_FILE_AGE_DAYS", "109")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_DIR_SIZE_GB", "113")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "127")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_LIFECYCLE_INTERVAL_SECOND", "131")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND", "137")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DONE", "true")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_FILE_AGE_DAYS", "139")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_DIR_SIZE_GB", "149")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DEAD", "false")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_FILE_AGE_DAYS", "151")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_DIR_SIZE_GB", "157")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_QUARANTINE", "true")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_FILE_AGE_DAYS", "163")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_DIR_SIZE_GB", "167")

	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "fileArchive,spool")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_DIR", "delivery_archive")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND", "17")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_RETRYABLE", "true")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_FILE_AGE_DAYS", "19")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_DIR_SIZE_GB", "23")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_PERMANENT", "false")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_FILE_AGE_DAYS", "29")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_DIR_SIZE_GB", "31")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "37")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_LIFECYCLE_INTERVAL_SECOND", "41")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND", "43")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DONE", "false")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_FILE_AGE_DAYS", "47")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_DIR_SIZE_GB", "53")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DEAD", "true")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_FILE_AGE_DAYS", "59")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_DIR_SIZE_GB", "61")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_QUARANTINE", "false")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_FILE_AGE_DAYS", "67")
	t.Setenv("HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_DIR_SIZE_GB", "71")

	appCfg := AppConfig{}
	enableDeliveryFileCompressedLogProducer(&appCfg)
	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v", err)
	}
	if !reflect.DeepEqual(cfg.Standard, objectstoragecompressedlog.FailureStoreConfig{}) {
		t.Fatalf("Standard = %+v, want unused zero value", cfg.Standard)
	}
	if !reflect.DeepEqual(
		cfg.Delivery.DurableType,
		[]failurestore.DurableType{
			failurestore.DurableTypeFileArchive,
			failurestore.DurableTypeSpool,
		},
	) {
		t.Fatalf("Delivery DurableType = %v, want fileArchive and spool", cfg.Delivery.DurableType)
	}

	archive := cfg.Delivery.DurableStoreConfig.FileArchiveConfig
	if archive.DirName != "delivery_archive" || archive.LifecycleInterval != 17*time.Second {
		t.Fatalf("Delivery FileArchiveConfig = %+v, want configured identity and interval", archive)
	}
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(
		t,
		"delivery archive retryable",
		archive.Retryable,
		true,
		19,
		23*objectStorageCompressedLogTestGiB,
	)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(
		t,
		"delivery archive permanent",
		archive.Permanent,
		false,
		29,
		31*objectStorageCompressedLogTestGiB,
	)

	spool := cfg.Delivery.DurableStoreConfig.SpoolConfig
	if spool.Runner.Interval != 37*time.Second ||
		spool.Lifecycle.Interval != 41*time.Second ||
		spool.CircuitBreaker.OpenCooldown != 43*time.Second {
		t.Fatalf("Delivery SpoolConfig = %+v, want configured intervals", spool)
	}
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(
		t,
		"delivery spool done",
		spool.Lifecycle.Done,
		false,
		47,
		53*objectStorageCompressedLogTestGiB,
	)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(
		t,
		"delivery spool dead",
		spool.Lifecycle.Dead,
		true,
		59,
		61*objectStorageCompressedLogTestGiB,
	)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(
		t,
		"delivery spool quarantine",
		spool.Lifecycle.Quarantine,
		false,
		67,
		71*objectStorageCompressedLogTestGiB,
	)
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreRejectsInvalidSelection(t *testing.T) {
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "none,spool")

	appCfg := AppConfig{}
	enableStandardFileCompressedLogProducer(&appCfg)
	_, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	var typeErr *failurestore.DurableTypeError
	if !errors.As(err, &typeErr) || typeErr.Type != failurestore.DurableTypeNone ||
		!errors.Is(err, failurestore.ErrDurableTypeNoneNotAlone) {
		t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v, want selection validation", err)
	}
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreValidatesSelectedSpool(t *testing.T) {
	for value, wantCooldown := range map[string]time.Duration{"0": 0, "-1": -time.Second} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "spool")
			t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND", value)

			appCfg := AppConfig{}
			enableStandardFileCompressedLogProducer(&appCfg)
			_, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
			var settingErr *objectstoragecompressedlog.SettingError
			if !errors.As(err, &settingErr) || settingErr.Contract != "standard" ||
				settingErr.Setting != objectstoragecompressedlog.SettingSpoolCircuitBreakerOpenCooldown ||
				settingErr.Value != wantCooldown ||
				!errors.Is(settingErr, objectstoragecompressedlog.ErrNonPositiveSetting) {
				t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v, want cooldown validation", err)
			}
		})
	}
}

func TestLoadObjectStorageCompressedLogFileUploadFailureStoreIsIndependentOfRecordSpool(t *testing.T) {
	// compressed log file spoolはrecord spool supervisorとは別のcoordinatorで動くため、
	// 未使用のgeneric supervisor値が不正でも起動を妨げません。
	t.Setenv("HETEROLOG_MAX_RESTART_SPOOL_TIMES", "not-an-integer")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES", "spool")
	t.Setenv("HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND", "59")

	appCfg := AppConfig{}
	enableStandardFileCompressedLogProducer(&appCfg)
	cfg, err := loadObjectStorageCompressedLogFileUploadFailureStoreConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("loadObjectStorageCompressedLogFileUploadFailureStore() error = %v", err)
	}
	standard := cfg.Standard
	if got := standard.DurableStoreConfig.SpoolConfig.Runner.Interval; got != 59*time.Second {
		t.Fatalf("standard object storage spool interval = %s, want 59s", got)
	}
	recordSpool, err := loadSpoolConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("loadSpoolConfig() read compressed-log-only supervisor environment: %v", err)
	}
	if recordSpool.SupervisorConfig.MaxRestarts != 0 {
		t.Fatalf(
			"record spool MaxRestarts = %d, want inactive zero value",
			recordSpool.SupervisorConfig.MaxRestarts,
		)
	}
}

func TestFileLoggerMappersAttachObjectStorageCompressedLogFileUploadFailureStoreByContract(t *testing.T) {
	appCfg := AppConfig{}
	appCfg.RuntimeConfig.Target = "heterolog"
	appCfg.FsstoreConfig.AppStatePath = "/var/lib"
	appCfg.FsstoreConfig.SpoolPath = "/var/lib"
	appCfg.CompressedLogFileUploadFailureStore.Standard =
		objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig()
	appCfg.CompressedLogFileUploadFailureStore.Delivery =
		objectstoragecompressedlog.DefaultDeliveryCompressedLogFileUploadFailureStoreConfig()
	appCfg.CompressedLogFileUploadFailureStore.Standard.DurableStoreConfig.
		SpoolConfig.CircuitBreaker.OpenCooldown = 4 * time.Minute
	appCfg.CompressedLogFileUploadFailureStore.Delivery.DurableStoreConfig.
		SpoolConfig.CircuitBreaker.OpenCooldown = 6 * time.Minute
	appCfg.CompressedLogFileUploadFailureStore.Standard.DurableStoreConfig.
		SpoolConfig.Runner.MaxRetryAfter = 40 * time.Minute
	appCfg.CompressedLogFileUploadFailureStore.Delivery.DurableStoreConfig.
		SpoolConfig.Runner.MaxRetryAfter = 70 * time.Minute
	appCfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig.Default.Enabled = true
	appCfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig.Default.Targets =
		[]objectstorage.UploadTarget{objectstorage.UploadTargetS3}
	appCfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.
		ObjectStorageUploadConfig.Default.Enabled = true
	appCfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.
		ObjectStorageUploadConfig.Default.Targets =
		[]objectstorage.UploadTarget{objectstorage.UploadTargetS3}
	appCfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.
		ObjectStorageUploadConfig.Default.Enabled = true
	appCfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.
		ObjectStorageUploadConfig.Default.Targets =
		[]objectstorage.UploadTarget{objectstorage.UploadTargetS3}

	tests := []struct {
		name              string
		config            objectstorage.LogFileUploadConfig
		contract          pkglogger.LoggerContractKind
		wantOpenCooldown  time.Duration
		wantMaxRetryAfter time.Duration
	}{
		{
			name:              "logger",
			config:            NewFileLoggerConfig(&appCfg).ObjectStorageUploadConfig,
			contract:          pkglogger.LoggerContractSync,
			wantOpenCooldown:  4 * time.Minute,
			wantMaxRetryAfter: 40 * time.Minute,
		},
		{
			name:              "async logger",
			config:            NewAsyncFileLoggerConfig(&appCfg).ObjectStorageUploadConfig,
			contract:          pkglogger.LoggerContractAsyncBestEffort,
			wantOpenCooldown:  4 * time.Minute,
			wantMaxRetryAfter: 40 * time.Minute,
		},
		{
			name: "delivery logger",
			config: NewDeliveryFileLoggerConfig(&appCfg).LifecycleRunnerSettings.
				ObjectStorageUploadConfig,
			contract:          pkglogger.LoggerContractDelivery,
			wantOpenCooldown:  6 * time.Minute,
			wantMaxRetryAfter: 70 * time.Minute,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			failureCfg := test.config.Default.CompressedLogFileUploadFailureStore
			if !reflect.DeepEqual(
				failureCfg.DurableType,
				[]failurestore.DurableType{
					failurestore.DurableTypeFileArchive,
					failurestore.DurableTypeSpool,
				},
			) {
				t.Fatalf("DurableType = %v, want spool and fileArchive", failureCfg.DurableType)
			}
			archive := failureCfg.DurableStoreConfig.FileArchiveConfig
			wantArchiveRoot := filepath.Join(
				"/var/lib",
				"heterolog",
				objectstoragecompressedlog.DefaultCompressedLogFileUploadFailureDirName,
			)
			if archive.RootPath != wantArchiveRoot ||
				archive.LoggerContract != test.contract.String() {
				t.Fatalf(
					"mapped archive = %+v, want root %q contract %q",
					archive,
					wantArchiveRoot,
					test.contract,
				)
			}
			spool := test.config.CompressedLogFileUploadSpool
			if spool == nil {
				t.Fatal("mapped shared spool is nil")
			}
			if spool.SpoolPath() != "/var/lib" || spool.Target() != "heterolog" ||
				spool.LoggerContract() != test.contract.String() ||
				spool.CircuitBreakerOpenCooldown() != test.wantOpenCooldown ||
				spool.MaxRetryAfter() != test.wantMaxRetryAfter {
				t.Fatalf(
					"mapped spool = %+v, want /var/lib/heterolog/%s cooldown=%s max retry-after=%s",
					spool,
					test.contract,
					test.wantOpenCooldown,
					test.wantMaxRetryAfter,
				)
			}
		})
	}
}

func TestFileLoggerSpoolSettingsSurviveDedicatedAndRunnerCopies(t *testing.T) {
	appCfg := AppConfig{}
	appCfg.RuntimeConfig.Target = "shared-spool"
	appCfg.FsstoreConfig.SpoolPath = t.TempDir()
	appCfg.CompressedLogFileUploadFailureStore.Standard = objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig()
	appCfg.CompressedLogFileUploadFailureStore.Delivery = objectstoragecompressedlog.DefaultDeliveryCompressedLogFileUploadFailureStoreConfig()
	for _, upload := range []*objectstorage.LogFileUploadConfig{
		&appCfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
		&appCfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
		&appCfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
	} {
		upload.Enabled = true
		for _, target := range []*objectstorage.UploadTargetConfig{&upload.Default, &upload.Error, &upload.Debug} {
			target.Enabled = true
			target.Targets = []objectstorage.UploadTarget{objectstorage.UploadTargetS3}
		}
	}
	override := logconfig.InternalLogDestinationConfig{FileLogDir: "dedicated-internal-logs"}
	usesFile := func(target pkglogger.LogOutputDestination) bool { return target == pkglogger.LogOutputFile }
	for _, test := range []struct {
		name string
		base *pkglogger.FileLoggerConfig
	}{
		{"sync", NewFileLoggerConfig(&appCfg)},
		{"async", NewAsyncFileLoggerConfig(&appCfg)},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.base.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
			shared := test.base.ObjectStorageUploadConfig.CompressedLogFileUploadSpool
			if shared == nil {
				t.Fatal("mapped shared spool is nil")
			}
			dedicated := newInternalLogDestinations(override, usesFile, pkglogger.LoggerConfig{File: test.base})
			if dedicated == nil || dedicated.File == nil {
				t.Fatal("dedicated file config is missing")
			}
			for _, provider := range []objectstorage.CompressedLogFileUploadSpoolConfigProvider{
				dedicated.File.ObjectStorageUploadConfig.CompressedLogFileUploadSpool,
				test.base.LifecycleRunnerConfig().ObjectStorageUploadConfig.CompressedLogFileUploadSpool,
				dedicated.File.LifecycleRunnerConfig().ObjectStorageUploadConfig.CompressedLogFileUploadSpool,
			} {
				if provider == nil || compressedLogSpoolSettings(provider) != compressedLogSpoolSettings(shared) {
					t.Fatal("dedicated or runner config changed the spool settings")
				}
			}
		})
	}
	t.Run("delivery", func(t *testing.T) {
		base := NewDeliveryFileLoggerConfig(&appCfg)
		base.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
		shared := base.LifecycleRunnerSettings.ObjectStorageUploadConfig.CompressedLogFileUploadSpool
		if shared == nil {
			t.Fatal("mapped shared spool is nil")
		}
		dedicated := newDeliveryInternalLogDestinations(override, usesFile, pkglogger.DeliveryLoggerConfig{File: base})
		if dedicated == nil || dedicated.File == nil {
			t.Fatal("dedicated delivery file config is missing")
		}
		for _, provider := range []objectstorage.CompressedLogFileUploadSpoolConfigProvider{
			dedicated.File.LifecycleRunnerSettings.ObjectStorageUploadConfig.CompressedLogFileUploadSpool,
			base.LifecycleRunnerConfig().ObjectStorageUploadConfig.CompressedLogFileUploadSpool,
			dedicated.File.LifecycleRunnerConfig().ObjectStorageUploadConfig.CompressedLogFileUploadSpool,
		} {
			if provider == nil || compressedLogSpoolSettings(provider) != compressedLogSpoolSettings(shared) {
				t.Fatal("dedicated or runner delivery config changed the spool settings")
			}
		}
	})
}

func TestFileLoggerMappersShareArchivePurgeSettingsAcrossLogRoles(t *testing.T) {
	appCfg := AppConfig{}
	appCfg.RuntimeConfig.Target = "heterolog"
	appCfg.FsstoreConfig.AppStatePath = t.TempDir()
	standardArchive := objectstoragecompressedlog.FileArchiveConfig{
		DirName:           "standard-upload-failures",
		LifecycleInterval: 3 * time.Minute,
		Retryable: objectstoragecompressedlog.PurgeConfig{
			Enabled: true, MaxFileAgeDays: 111, MaxDirSize: 12 * objectStorageCompressedLogTestGiB,
		},
		Permanent: objectstoragecompressedlog.PurgeConfig{
			Enabled: false, MaxFileAgeDays: 222, MaxDirSize: 23 * objectStorageCompressedLogTestGiB,
		},
	}
	deliveryArchive := objectstoragecompressedlog.FileArchiveConfig{
		DirName:           "delivery-upload-failures",
		LifecycleInterval: 7 * time.Minute,
		Retryable: objectstoragecompressedlog.PurgeConfig{
			Enabled: false, MaxFileAgeDays: 333, MaxDirSize: 34 * objectStorageCompressedLogTestGiB,
		},
		Permanent: objectstoragecompressedlog.PurgeConfig{
			Enabled: true, MaxFileAgeDays: 444, MaxDirSize: 45 * objectStorageCompressedLogTestGiB,
		},
	}
	appCfg.CompressedLogFileUploadFailureStore.Standard.DurableType =
		[]failurestore.DurableType{failurestore.DurableTypeFileArchive}
	appCfg.CompressedLogFileUploadFailureStore.Standard.DurableStoreConfig.FileArchiveConfig = standardArchive
	appCfg.CompressedLogFileUploadFailureStore.Delivery.DurableType =
		[]failurestore.DurableType{failurestore.DurableTypeFileArchive}
	appCfg.CompressedLogFileUploadFailureStore.Delivery.DurableStoreConfig.FileArchiveConfig = deliveryArchive
	for _, upload := range []*objectstorage.LogFileUploadConfig{
		&appCfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
		&appCfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
		&appCfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
	} {
		upload.Enabled = true
		for _, target := range []*objectstorage.UploadTargetConfig{&upload.Default, &upload.Error, &upload.Debug} {
			target.Enabled = true
			target.Targets = []objectstorage.UploadTarget{objectstorage.UploadTargetS3}
		}
	}

	// 同じarchive保存先を共有する通常・error・debugには、無効な分類の保持値も含め、
	// contractの共通設定が配られます。利用登録時の設定比較には依存しません。
	for _, test := range []struct {
		contract pkglogger.LoggerContractKind
		upload   objectstorage.LogFileUploadConfig
		archive  objectstoragecompressedlog.FileArchiveConfig
	}{
		{pkglogger.LoggerContractSync, NewFileLoggerConfig(&appCfg).ObjectStorageUploadConfig, standardArchive},
		{pkglogger.LoggerContractAsyncBestEffort, NewAsyncFileLoggerConfig(&appCfg).ObjectStorageUploadConfig, standardArchive},
		{pkglogger.LoggerContractDelivery, NewDeliveryFileLoggerConfig(&appCfg).LifecycleRunnerSettings.ObjectStorageUploadConfig, deliveryArchive},
	} {
		t.Run(test.contract.String(), func(t *testing.T) {
			want := objectstorage.CompressedLogFileUploadFailureArchiveConfig{
				RootPath:          filepath.Join(appCfg.FsstoreConfig.AppStatePath, appCfg.RuntimeConfig.Target, test.archive.DirName),
				LoggerContract:    test.contract.String(),
				LifecycleInterval: test.archive.LifecycleInterval,
				Retryable: objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
					Enabled:        test.archive.Retryable.Enabled,
					MaxFileAgeDays: test.archive.Retryable.MaxFileAgeDays,
					MaxDirSize:     test.archive.Retryable.MaxDirSize,
				},
				Permanent: objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
					Enabled:        test.archive.Permanent.Enabled,
					MaxFileAgeDays: test.archive.Permanent.MaxFileAgeDays,
					MaxDirSize:     test.archive.Permanent.MaxDirSize,
				},
			}
			for _, role := range []struct {
				name   string
				upload objectstorage.UploadTargetConfig
			}{
				{"normal", test.upload.Default},
				{"error", test.upload.Error},
				{"debug", test.upload.Debug},
			} {
				t.Run(role.name, func(t *testing.T) {
					got := role.upload.CompressedLogFileUploadFailureStore.DurableStoreConfig.FileArchiveConfig
					if got != want {
						t.Fatalf("mapped archive = %+v, want shared contract settings %+v", got, want)
					}
				})
			}
		})
	}
}

func TestBackendFailureArchiveMappersAttachCompressedLogFileUploadFailureStore(t *testing.T) {
	appCfg := AppConfig{}
	appCfg.CompressedLogFileUploadFailureStore.Standard =
		objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig()
	appCfg.CompressedLogFileUploadFailureStore.Delivery =
		objectstoragecompressedlog.DefaultDeliveryCompressedLogFileUploadFailureStoreConfig()

	enableStandardOpenSearchFailureArchiveUpload(&appCfg)
	syncFailure := appCfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
		FailureHandlerConfig
	syncFailure.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig.CompressedLogFileUploadFailureStore =
		objectstorage.CompressedLogFileUploadFailureStoreConfig{
			DurableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
		}
	syncMapped := newFailureStoreConfig(
		&appCfg,
		checkpointSourceLoggerOpenSearch,
		pkglogger.LoggerContractSync,
		syncFailure,
		appCfg.Logger.OpenSearchConfig.LogBodyConfig,
	)
	syncUpload := syncMapped.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig
	if !syncUpload.Enabled {
		t.Fatal("sync backend failure archive direct upload was disabled by mapping")
	}
	if !reflect.DeepEqual(syncUpload.CompressedLogFileUploadFailureStore.DurableType, appCfg.CompressedLogFileUploadFailureStore.Standard.DurableType) {
		t.Fatalf(
			"sync backend failure archive upload CompressedLogFileUploadFailureStore = %+v, want shared standard settings",
			syncUpload.CompressedLogFileUploadFailureStore,
		)
	}

	enableDeliveryOpenSearchFailureArchiveUpload(&appCfg)
	deliveryFailure := appCfg.DeliveryLogger.DeliveryOpenSearchConfig.
		DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig
	deliveryFailure.FileArchiveConfig.ObjectStorageUploadConfig.CompressedLogFileUploadFailureStore =
		objectstorage.CompressedLogFileUploadFailureStoreConfig{
			DurableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
		}
	deliveryMapped := newDeliveryFailureStoreConfig(
		&appCfg,
		checkpointSourceDeliveryLoggerOpenSearch,
		deliveryFailure,
		appCfg.DeliveryLogger.DeliveryOpenSearchConfig.LogBodyConfig,
	)
	deliveryUpload := deliveryMapped.FileArchiveConfig.ObjectStorageUploadConfig
	if !deliveryUpload.Enabled {
		t.Fatal("delivery backend failure archive direct upload was disabled by mapping")
	}
	if !reflect.DeepEqual(
		deliveryUpload.CompressedLogFileUploadFailureStore.DurableType,
		appCfg.CompressedLogFileUploadFailureStore.Delivery.DurableType,
	) {
		t.Fatalf(
			"delivery backend failure archive upload CompressedLogFileUploadFailureStore = %+v, want shared delivery settings",
			deliveryUpload.CompressedLogFileUploadFailureStore,
		)
	}
	for _, upload := range []objectstorage.UploadTargetConfig{syncUpload, deliveryUpload} {
		if upload.CompressedLogFileUploadSpool == nil {
			t.Fatal("backend failure archive upload has no spool settings provider")
		}
		if upload.CompressedLogFileUploadSpool.LoggerContract() != upload.CompressedLogFileUploadFailureStore.DurableStoreConfig.FileArchiveConfig.LoggerContract {
			t.Fatal("archive and spool settings have different logger contracts")
		}
	}
}

func TestObjectStorageCompressedLogFileUploadFailureStoreMapperLeavesUnselectedSubconfigZero(t *testing.T) {
	appCfg := AppConfig{}
	appCfg.RuntimeConfig.Target = "target"
	appCfg.FsstoreConfig.AppStatePath = "/state"
	appCfg.FsstoreConfig.SpoolPath = "/spool"
	appCfg.CompressedLogFileUploadFailureStore.Standard =
		objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig()
	appCfg.CompressedLogFileUploadFailureStore.Standard.DurableType =
		[]failurestore.DurableType{failurestore.DurableTypeFileArchive}

	upload := attachCompressedLogFileUploadFailureStoreForLogFileTargets(
		&appCfg,
		objectstorage.LogFileUploadConfig{Enabled: true, Default: objectstorage.UploadTargetConfig{Enabled: true}},
		pkglogger.LoggerContractSync,
	)
	if upload.CompressedLogFileUploadSpool != nil {
		t.Fatalf(
			"unselected mapped shared spool = %+v, want nil",
			upload.CompressedLogFileUploadSpool,
		)
	}
}

func TestCompressedLogFailureStoreMapperKeepsBackendSelectionOnUploadTargets(t *testing.T) {
	appCfg := AppConfig{}
	appCfg.RuntimeConfig.Target = "target"
	appCfg.FsstoreConfig.AppStatePath = "/state"
	appCfg.FsstoreConfig.SpoolPath = "/spool"
	appCfg.CompressedLogFileUploadFailureStore.Standard =
		objectstoragecompressedlog.DefaultStandardCompressedLogFileUploadFailureStoreConfig()
	upload := objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		},
	}
	baseline := attachCompressedLogFileUploadFailureStoreForLogFileTargets(&appCfg, upload, pkglogger.LoggerContractSync)
	upload.Default.Targets = []objectstorage.UploadTarget{objectstorage.UploadTargetS3, "future-storage"}
	upload.Error = objectstorage.UploadTargetConfig{
		Enabled: true,
		Targets: []objectstorage.UploadTarget{"future-storage"},
	}
	appCfg.Logger.FileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled = true
	mapped := attachCompressedLogFileUploadFailureStoreForLogFileTargets(&appCfg, upload, pkglogger.LoggerContractSync)
	if !reflect.DeepEqual(mapped.Default.Targets, upload.Default.Targets) ||
		!reflect.DeepEqual(mapped.Error.Targets, upload.Error.Targets) {
		t.Fatal("failure store mapping changed per-role backend selection")
	}
	if mapped.CompressedLogFileUploadSpool == nil ||
		compressedLogSpoolSettings(mapped.CompressedLogFileUploadSpool) != compressedLogSpoolSettings(baseline.CompressedLogFileUploadSpool) {
		t.Fatalf("shared spool settings = %+v, want unchanged %+v", mapped.CompressedLogFileUploadSpool, baseline.CompressedLogFileUploadSpool)
	}
	for _, role := range []objectstorage.UploadTargetConfig{mapped.Default, mapped.Error} {
		if !reflect.DeepEqual(role.CompressedLogFileUploadFailureStore, baseline.Default.CompressedLogFileUploadFailureStore) {
			t.Fatalf("failure store settings = %+v, want shared backend-independent settings %+v", role.CompressedLogFileUploadFailureStore, baseline.Default.CompressedLogFileUploadFailureStore)
		}
	}
}

func TestPublicEnvExampleExposesObjectStorageCompressedLogFileUploadFailureStoreDefaults(t *testing.T) {
	wantValues := map[string]string{
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES":                                       "fileArchive,spool",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_DIR":                                         "compressed_log_file_upload_failures",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND":                   "60",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_RETRYABLE":                      "false",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_FILE_AGE_DAYS":                 "365",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_DIR_SIZE_GB":                   "100",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_PERMANENT":                      "false",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_FILE_AGE_DAYS":                 "365",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_DIR_SIZE_GB":                   "100",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES":                              "fileArchive,spool",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_DIR":                                "compressed_log_file_upload_failures",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_LIFECYCLE_INTERVAL_SECOND":          "60",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_RETRYABLE":             "false",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_FILE_AGE_DAYS":        "730",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_RETRYABLE_MAX_DIR_SIZE_GB":          "500",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_ENABLE_PURGE_PERMANENT":             "false",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_FILE_AGE_DAYS":        "730",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_ARCHIVE_PERMANENT_MAX_DIR_SIZE_GB":          "500",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND":                        "5",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_LIFECYCLE_INTERVAL_SECOND":                     "60",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND":          "300",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DONE":                             "true",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_FILE_AGE_DAYS":                        "30",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_DIR_SIZE_GB":                          "20",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DEAD":                             "false",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_FILE_AGE_DAYS":                        "365",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_DIR_SIZE_GB":                          "100",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_QUARANTINE":                       "false",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_FILE_AGE_DAYS":                  "365",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_DIR_SIZE_GB":                    "100",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RUNNER_INTERVAL_SECOND":               "5",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_LIFECYCLE_INTERVAL_SECOND":            "60",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_CIRCUIT_BREAKER_OPEN_COOLDOWN_SECOND": "300",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DONE":                    "true",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_FILE_AGE_DAYS":               "90",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DONE_MAX_DIR_SIZE_GB":                 "100",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_DEAD":                    "false",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_FILE_AGE_DAYS":               "730",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_DEAD_MAX_DIR_SIZE_GB":                 "500",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_ENABLE_PURGE_QUARANTINE":              "false",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_FILE_AGE_DAYS":         "730",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_QUARANTINE_MAX_DIR_SIZE_GB":           "500",
	}
	deprecatedKeys := []string{
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RETRY_BASE_DELAY_SECOND",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RETRY_MAX_DELAY_SECOND",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RETRY_BACKOFF_MULTIPLIER",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RETRY_BASE_DELAY_SECOND",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RETRY_MAX_DELAY_SECOND",
		"HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_SPOOL_RETRY_BACKOFF_MULTIPLIER",
	}

	values := readPublicEnvExample(t)
	for key, want := range wantValues {
		got, ok := values[key]
		if !ok {
			t.Errorf(".env.example is missing %s", key)
			continue
		}
		if got != want {
			t.Errorf(".env.example %s = %q, want %q", key, got, want)
		}
	}
	for _, key := range deprecatedKeys {
		if _, ok := values[key]; ok {
			t.Errorf(".env.example still exposes removed %s", key)
		}
	}
}

// clearObjectStorageCompressedLogFileUploadFailureStoreEnvは、開発者の環境に残る
// compressed log file upload failure storeの設定をtest中だけ空にします。pkg/envは
// 空文字を未設定として扱うため、shellの値に依存せずloaderの既定値を検証できます。
func clearObjectStorageCompressedLogFileUploadFailureStoreEnv(t *testing.T) {
	t.Helper()

	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_") ||
			strings.HasPrefix(key, "HETEROLOG_DELIVERY_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_") {
			t.Setenv(key, "")
		}
	}
}

func assertObjectStorageCompressedLogFileUploadFailureStoreDefaults(
	t *testing.T,
	name string,
	cfg objectstoragecompressedlog.FailureStoreConfig,
	delivery bool,
) {
	t.Helper()
	wantTypes := []failurestore.DurableType{
		failurestore.DurableTypeFileArchive,
		failurestore.DurableTypeSpool,
	}
	if !reflect.DeepEqual(cfg.DurableType, wantTypes) {
		t.Errorf("%s DurableType = %v, want %v", name, cfg.DurableType, wantTypes)
	}
	archive := cfg.DurableStoreConfig.FileArchiveConfig
	if archive.DirName != objectstoragecompressedlog.DefaultCompressedLogFileUploadFailureDirName ||
		archive.LifecycleInterval != time.Minute {
		t.Errorf("%s FileArchiveConfig = %+v, want default identity and interval", name, archive)
	}
	spool := cfg.DurableStoreConfig.SpoolConfig
	if spool.Runner.Interval != 5*time.Second ||
		spool.Lifecycle.Interval != time.Minute ||
		spool.CircuitBreaker.OpenCooldown != 5*time.Minute {
		t.Errorf("%s SpoolConfig = %+v, want default runner/circuit breaker", name, spool)
	}
	// upload済みのdoneだけを既定で削除します。uploadできずに保持しているretryable・
	// permanent・dead・quarantineは、明示しない限り削除しません。保持期間・容量の既定値は
	// 削除を有効にした場合に使います。
	if delivery {
		assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" archive retryable", archive.Retryable, false, 730, 500*objectStorageCompressedLogTestGiB)
		assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" archive permanent", archive.Permanent, false, 730, 500*objectStorageCompressedLogTestGiB)
		assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" spool done", spool.Lifecycle.Done, true, 90, 100*objectStorageCompressedLogTestGiB)
		assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" spool dead", spool.Lifecycle.Dead, false, 730, 500*objectStorageCompressedLogTestGiB)
		assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" spool quarantine", spool.Lifecycle.Quarantine, false, 730, 500*objectStorageCompressedLogTestGiB)
		return
	}
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" archive retryable", archive.Retryable, false, 365, 100*objectStorageCompressedLogTestGiB)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" archive permanent", archive.Permanent, false, 365, 100*objectStorageCompressedLogTestGiB)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" spool done", spool.Lifecycle.Done, true, 30, 20*objectStorageCompressedLogTestGiB)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" spool dead", spool.Lifecycle.Dead, false, 365, 100*objectStorageCompressedLogTestGiB)
	assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(t, name+" spool quarantine", spool.Lifecycle.Quarantine, false, 365, 100*objectStorageCompressedLogTestGiB)
}

func compressedLogSpoolSettings(provider objectstorage.CompressedLogFileUploadSpoolConfigProvider) objectstorage.CompressedLogFileUploadSpoolConfig {
	purgeSettings := func(config objectstorage.CompressedLogFileUploadSpoolPurgeConfigProvider) objectstorage.CompressedLogFileUploadSpoolPurgeConfig {
		return objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
			Enabled: config.Enabled(), MaxFileAgeDays: config.MaxFileAgeDays(), MaxDirSize: config.MaxDirSize(),
		}
	}
	return objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath: provider.SpoolPath(), Target: provider.Target(), LoggerContract: provider.LoggerContract(),
		RunnerInterval: provider.RunnerInterval(), LifecycleInterval: provider.LifecycleInterval(),
		CircuitBreakerOpenCooldown: provider.CircuitBreakerOpenCooldown(),
		Done:                       purgeSettings(provider.DonePurge()), Dead: purgeSettings(provider.DeadPurge()),
		Quarantine: purgeSettings(provider.QuarantinePurge()),
	}
}

func assertObjectStorageCompressedLogFileUploadFailurePurgeConfig(
	t *testing.T,
	name string,
	cfg objectstoragecompressedlog.PurgeConfig,
	wantEnabled bool,
	wantAge int,
	wantSize uint64,
) {
	t.Helper()
	if cfg.Enabled != wantEnabled || cfg.MaxFileAgeDays != wantAge || cfg.MaxDirSize != wantSize {
		t.Errorf(
			"%s = %+v, want enabled=%t age=%d size=%d",
			name,
			cfg,
			wantEnabled,
			wantAge,
			wantSize,
		)
	}
}
