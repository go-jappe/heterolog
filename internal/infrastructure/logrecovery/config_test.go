package logrecovery

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
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
