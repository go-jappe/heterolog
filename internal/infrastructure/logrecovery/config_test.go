package logrecovery

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/storageowner"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRecoverySpecsUseInternalFileOwnerAndSkipBaseTemplate(t *testing.T) {
	for _, contract := range []struct {
		option, prefix string
		kind           pkglogger.LoggerContractKind
	}{
		{"sync", "", pkglogger.LoggerContractSync},
		{"async", "ASYNC_", pkglogger.LoggerContractAsyncBestEffort},
		{"delivery", "DELIVERY_", pkglogger.LoggerContractDelivery},
	} {
		t.Run(contract.option, func(t *testing.T) {
			cfg := loadRecoveryTestConfig(t, map[string]string{
				"HETEROLOG_" + contract.prefix + "LOG_TYPE":                      "stdout",
				"HETEROLOG_" + contract.prefix + "STDOUT_LOGGER_ERROR_HANDLER":   "file",
				"HETEROLOG_" + contract.prefix + "LOG_INTERNAL_LOG_FILE_LOG_DIR": "internal-only",
			})
			specs, err := recoverySpecs(testConfigs(cfg), Options{LoggerContract: contract.kind, Scope: RecoveryScopeAll})
			require.NoError(t, err)
			require.Len(t, specs, 1)
			require.Equal(t, "file-logger/internal", specs[0].name)
			spoolOwners := compressedlog.NewRootRuntimeBuilder()
			t.Cleanup(func() { require.NoError(t, spoolOwners.Close()) })
			lifecycles, err := specs[0].build(spoolOwners, nil)
			require.NoError(t, err)
			require.Len(t, lifecycles, 1)
			directory, err := lifecycles[0].DirectoryPath()
			require.NoError(t, err)
			require.Equal(t, filepath.Join(cfg.FsstoreConfig.LogPath, "service", "internal-only"), directory)
			require.NoError(t, lifecycles[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes())
			require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.LogPath, "service", contract.option))
		})
	}
}

func TestRecoverySpecsExcludeUnselectedBackendFailureArchives(t *testing.T) {
	archive := durable.FailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeFileArchive}}
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		OpenSearch:            &pkglogger.OpenSearchConfig{OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{FailureStoreConfig: archive}},
	}
	require.Empty(t, logRecoverySpecs(cfg, RecoveryScopeAll))
	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	specs := logRecoverySpecs(cfg, RecoveryScopeAll)
	require.Len(t, specs, 1)
	require.Equal(t, "log-backend-failure-archive/opensearch", specs[0].name)
	require.Empty(t, logRecoverySpecs(cfg, RecoveryScopeFileLogger))
	cfg.OpenSearch.FailureStoreConfig.DurableType = []durable.DurableType{durable.DurableTypeSpool}
	require.Empty(t, logRecoverySpecs(cfg, RecoveryScopeLogBackendFailureArchive))

	delivery := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		OpenSearch: &pkglogger.DeliveryOpenSearchConfig{DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
			FailureStoreConfig: durable.DeliveryFailureStoreConfig{DurableType: []durable.DurableType{durable.DurableTypeFileArchive}},
		}},
	}
	require.Empty(t, deliveryRecoverySpecs(delivery, RecoveryScopeAll))
	delivery.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	require.Len(t, deliveryRecoverySpecs(delivery, RecoveryScopeLogBackendFailureArchive), 1)
}

func TestRecoverySpecsPreserveMappedCheckpointIDsForAllContracts(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, nil)
	upload := objectstorage.LogFileUploadConfig{Enabled: true, Default: objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{Region: "ap-northeast-1", Endpoint: "https://s3.example.test", AccessKeyID: "test-key", SecretAccessKey: "test-secret", Bucket: "test-archive"},
	}}
	cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig = upload
	cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig = upload
	cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig = upload
	for _, contract := range []struct {
		option, source string
		kind           pkglogger.LoggerContractKind
	}{
		{"sync", "logger_file", pkglogger.LoggerContractSync},
		{"async", "async_logger_file", pkglogger.LoggerContractAsyncBestEffort},
		{"delivery", "delivery_logger_file", pkglogger.LoggerContractDelivery},
	} {
		t.Run(contract.option, func(t *testing.T) {
			var layout pkgfsstore.FsstoreConfig
			var mappedCheckpoint pkgfsstore.ArchiveCheckpointConfig
			switch contract.kind {
			case pkglogger.LoggerContractSync:
				mapped := config.NewFileLoggerConfig(&cfg)
				layout, mappedCheckpoint = mapped.FsstoreConfig.Layout, mapped.ArchiveCheckpointConfig
			case pkglogger.LoggerContractAsyncBestEffort:
				mapped := config.NewAsyncFileLoggerConfig(&cfg)
				layout, mappedCheckpoint = mapped.FsstoreConfig.Layout, mapped.ArchiveCheckpointConfig
			case pkglogger.LoggerContractDelivery:
				mapped := config.NewDeliveryFileLoggerConfig(&cfg)
				layout, mappedCheckpoint = mapped.FsstoreConfig.Layout, mapped.LifecycleRunnerSettings.ArchiveCheckpointConfig
			}
			require.Equal(t, contract.source, mappedCheckpoint.SourcePrefix)
			specs, err := recoverySpecs(testConfigs(cfg), Options{LoggerContract: contract.kind, Scope: RecoveryScopeFileLogger})
			require.NoError(t, err)
			require.Len(t, specs, 1)
			require.Equal(t, mappedCheckpoint.Directory, specs[0].checkpointDirectory)
			require.NoError(t, checkpoint.PrepareDirectory(specs[0].checkpointDirectory))
			spoolOwners := compressedlog.NewRootRuntimeBuilder()
			t.Cleanup(func() { require.NoError(t, spoolOwners.Close()) })
			lifecycles, err := specs[0].build(spoolOwners, nil)
			require.NoError(t, err)
			require.Len(t, lifecycles, 1)
			directory, err := lifecycles[0].DirectoryPath()
			require.NoError(t, err)
			require.Equal(t, filepath.Join(layout.LogPath, layout.Target, layout.LogDir), directory)
			sourceID := checkpoint.ArchiveCheckpointSourceID(mappedCheckpoint.SourcePrefix, "default", logpattern.FileNamePattern{Dir: directory, Target: layout.Target, Extension: "log"})
			require.FileExists(t, filepath.Join(mappedCheckpoint.Directory, sourceID+".log-file-uploads.json"))
			require.NoError(t, lifecycles[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes())
		})
	}
	entries, err := os.ReadDir(filepath.Join(cfg.FsstoreConfig.AppStatePath, "service", "archive-checkpoints"))
	require.NoError(t, err)
	require.NotEmpty(t, entries)
}

// Telemetryの復旧対象は、送り先に選び、failure archiveへ保存する送り先だけです。spoolだけを
// 使う送り先と、送り先に選んでいない設定は、通常の起動と同じく所有者ではありません。
func TestTelemetryRecoverySpecsSelectTheArchivesOfTheDestinationsInUse(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, map[string]string{
		"HETEROLOG_ENABLE_TRACES":                               "true",
		"HETEROLOG_TRACES_TYPE":                                 "otlp,clickhouse,file",
		"HETEROLOG_TRACES_OTLP_DURABLE_TYPES":                   "fileArchive",
		"HETEROLOG_TRACES_CLICKHOUSE_DURABLE_TYPES":             "spool",
		"HETEROLOG_TRACES_OPENSEARCH_DURABLE_TYPES":             "fileArchive",
		"HETEROLOG_ENABLE_METRICS":                              "true",
		"HETEROLOG_METRICS_TYPE":                                "victoriametrics",
		"HETEROLOG_METRICS_VICTORIAMETRICS_DURABLE_TYPES":       "spool,fileArchive",
		"HETEROLOG_METRICS_OTLP_DURABLE_TYPES":                  "fileArchive",
		"HETEROLOG_TRACES_OTLP_FAILURE_ARCHIVE_DIR":             "span_archives",
		"HETEROLOG_METRICS_VICTORIAMETRICS_FAILURE_ARCHIVE_DIR": "metric_archives",
	})
	for _, scope := range []RecoveryScope{RecoveryScopeAll, RecoveryScopeTelemetryFailureArchive} {
		specs, err := recoverySpecs(testConfigs(cfg), Options{LoggerContract: storageowner.Telemetry, Scope: scope})
		require.NoError(t, err)
		var names []string
		for _, spec := range specs {
			names = append(names, spec.name)
		}
		require.Equal(t, []string{"telemetry-failure-archive/otlp_traces", "telemetry-failure-archive/victoriametrics"}, names)
	}
	// loggerの系列は、Telemetryの設定にはありません。
	specs, err := recoverySpecs(testConfigs(cfg), Options{LoggerContract: storageowner.Telemetry, Scope: RecoveryScopeFileLogger})
	require.NoError(t, err)
	require.Empty(t, specs)

	specs, err = recoverySpecs(testConfigs(cfg), Options{LoggerContract: storageowner.Telemetry, Scope: RecoveryScopeAll})
	require.NoError(t, err)
	for index, dir := range []string{"span_archives", "metric_archives"} {
		spoolOwners := compressedlog.NewRootRuntimeBuilder()
		lifecycles, err := specs[index].build(spoolOwners, nil)
		require.NoError(t, err)
		// retryableとpermanentのdirectoryごとに一つずつです。
		require.Len(t, lifecycles, 2)
		for _, lifecycle := range lifecycles {
			directory, err := lifecycle.DirectoryPath()
			require.NoError(t, err)
			require.Contains(t, directory, string(filepath.Separator)+dir+string(filepath.Separator))
			require.NoError(t, lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes())
		}
		require.NoError(t, spoolOwners.Close())
	}
}

// loggerOwnedSpoolは、圧縮ログのspoolの設定のうち、所有者だけをsync loggerにします。
type loggerOwnedSpool struct {
	objectstorage.CompressedLogFileUploadSpoolConfigProvider
}

func (loggerOwnedSpool) LoggerContract() string { return pkglogger.LoggerContractSync.String() }

// uploadできなかった圧縮fileの保存先は、通常の起動と同じく、設定にかかわらずTelemetryが
// 所有します。checkpointも、通常の起動と同じ保存先を使います。
func TestTelemetryRecoverySpecsKeepTheUploadStorageOfTheTelemetry(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, map[string]string{
		"HETEROLOG_ENABLE_TRACES":                                                   "true",
		"HETEROLOG_TRACES_OTLP_DURABLE_TYPES":                                       "fileArchive",
		"HETEROLOG_ENABLE_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD":   "true",
		"HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET":       "spans",
		"HETEROLOG_TRACES_OTLP_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE":     "s3",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION":                                        "us-east-1",
		"HETEROLOG_OBJECT_STORAGE_COMPRESSED_LOG_FILE_UPLOAD_FAILURE_DURABLE_TYPES": "fileArchive,spool",
	})
	mapped := config.MapTelemetryConfig(cfg)
	upload := &mapped.Traces.OTLP.FailureStore.Store.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig
	require.NotNil(t, upload.CompressedLogFileUploadSpool)
	upload.CompressedLogFileUploadSpool = loggerOwnedSpool{upload.CompressedLogFileUploadSpool}
	upload.CompressedLogFileUploadFailureStore.DurableStoreConfig.FileArchiveConfig.LoggerContract = pkglogger.LoggerContractSync.String()

	specs := telemetryRecoverySpecs(mapped, RecoveryScopeAll)
	require.Len(t, specs, 1)
	require.NotEmpty(t, specs[0].spoolConfigs)
	for _, spool := range specs[0].spoolConfigs {
		require.Equal(t, storageowner.Telemetry.String(), spool.LoggerContract())
	}
	checkpointConfig := mapped.Traces.OTLP.FailureStore.Store.DurableStoreConfig.FileArchiveConfig.ArchiveCheckpointConfig
	require.NotEmpty(t, checkpointConfig.Directory)
	require.Equal(t, checkpointConfig.Directory, specs[0].checkpointDirectory)
}

func TestCheckScopeOwner(t *testing.T) {
	loggers := []pkglogger.LoggerContractKind{pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery}
	for _, owner := range loggers {
		for _, scope := range []RecoveryScope{RecoveryScopeFileLogger, RecoveryScopeLogBackendFailureArchive, RecoveryScopeAll} {
			require.NoError(t, CheckScopeOwner(owner, scope), "%s/%s", owner, scope)
		}
		require.ErrorIs(t, CheckScopeOwner(owner, RecoveryScopeTelemetryFailureArchive), ErrRecoveryScopeNotOwned)
	}
	for _, scope := range []RecoveryScope{RecoveryScopeTelemetryFailureArchive, RecoveryScopeAll} {
		require.NoError(t, CheckScopeOwner(storageowner.Telemetry, scope), "%s", scope)
	}
	for _, scope := range []RecoveryScope{RecoveryScopeFileLogger, RecoveryScopeLogBackendFailureArchive} {
		require.ErrorIs(t, CheckScopeOwner(storageowner.Telemetry, scope), ErrRecoveryScopeNotOwned)
	}
}

// 所有者が持たない系列を選んだ場合は、設定を変換する前に拒否し、保存先を作りません。
func TestRecoverRejectsAScopeThatTheOwnerDoesNotHave(t *testing.T) {
	cfg := loadRecoveryTestConfig(t, nil)
	report, err := Recover(context.Background(), testConfigs(cfg), Options{LoggerContract: storageowner.Telemetry, Scope: RecoveryScopeFileLogger})
	require.ErrorIs(t, err, ErrRecoveryScopeNotOwned)
	require.Empty(t, report.Directories)
	require.Equal(t, []string{err.Error()}, report.Errors)
	require.NoDirExists(t, cfg.FsstoreConfig.LogPath)
	require.NoDirExists(t, cfg.FsstoreConfig.AppStatePath)
}
