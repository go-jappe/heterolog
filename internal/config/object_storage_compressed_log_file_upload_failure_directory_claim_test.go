package config

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/objectstoragecompressedlog"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestStorageDirectoryClaimsObjectStorageCompressedLogFileUploadFailureRootsPerActiveContract(t *testing.T) {
	cfg := newObjectStorageCompressedLogFileUploadFailureDirectoryClaimConfig(t)
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.DeliveryLogger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	configureRunnerFileArchiveUpload(
		&cfg.Logger.FileConfig.FileSystemStoreConfig,
		"sync-log",
	)
	configureRunnerFileArchiveUpload(
		&cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig,
		"async-log",
	)
	configureRunnerDeliveryFileArchiveUpload(
		&cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig,
		"delivery-log",
	)
	cfg.CompressedLogFileUploadFailureStore.Standard =
		objectStorageCompressedLogFileUploadFailureStoreForClaims(
			failurestore.DurableTypeFileArchive,
			failurestore.DurableTypeSpool,
		)
	cfg.CompressedLogFileUploadFailureStore.Delivery =
		objectStorageCompressedLogFileUploadFailureStoreForClaims(
			failurestore.DurableTypeFileArchive,
			failurestore.DurableTypeSpool,
		)

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}

	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync,
		pkglogger.LoggerContractAsyncBestEffort,
		pkglogger.LoggerContractDelivery,
	} {
		archiveSource := objectStorageCompressedLogFileUploadFailureDirectoryClaimSource(contract, "file_archive.s3")
		archiveClaims := storageClaimsBySource(claims, archiveSource)
		if len(archiveClaims) != 1 {
			t.Fatalf("%s claims = %+v, want exactly one; all=%+v", archiveSource, archiveClaims, claims)
		}
		wantArchive, err := canonicalStoragePath(filepath.Join(
			cfg.FsstoreConfig.AppStatePath,
			cfg.RuntimeConfig.Target,
			"compressed-log-archives",
			"s3",
			contract.String(),
		))
		if err != nil {
			t.Fatalf("canonicalStoragePath(archive) error = %v", err)
		}
		if got := archiveClaims[0].path; got != wantArchive {
			t.Fatalf("%s path = %q, want %q", archiveSource, got, wantArchive)
		}

		spoolSource := objectStorageCompressedLogFileUploadFailureDirectoryClaimSource(contract, "spool.s3")
		spoolClaims := storageClaimsBySource(claims, spoolSource)
		if len(spoolClaims) != 1 {
			t.Fatalf("%s claims = %+v, want exactly one; all=%+v", spoolSource, spoolClaims, claims)
		}
		wantSpool, err := canonicalStoragePath(filepath.Join(
			cfg.FsstoreConfig.SpoolPath,
			cfg.RuntimeConfig.Target,
			"spool",
			"objectstorage",
			"s3",
			contract.String(),
		))
		if err != nil {
			t.Fatalf("canonicalStoragePath(spool) error = %v", err)
		}
		if got := spoolClaims[0].path; got != wantSpool {
			t.Fatalf("%s path = %q, want %q", spoolSource, got, wantSpool)
		}
	}
	if err := validateStorageDirectoryClaims(claims); err != nil {
		t.Fatalf("distinct object storage compressed log file roots collision error = %v", err)
	}
}

func TestStorageDirectoryClaimsIncludeBackendFailureArchiveCompressedUploads(
	t *testing.T,
) {
	cfg := newObjectStorageCompressedLogFileUploadFailureDirectoryClaimConfig(t)
	cfg.Logger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	syncFailure := &cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
		FailureHandlerConfig
	configureRunnerFailureArchive(syncFailure)
	syncFailure.Fsstore.FileArchiveConfig.Dir = "sync-backend-archive"

	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
	asyncFailure := &cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.
		LokiFlushManagerConfig.FailureHandlerConfig
	configureRunnerFailureArchive(asyncFailure)
	asyncFailure.Fsstore.FileArchiveConfig.Dir = "async-backend-archive"

	cfg.DeliveryLogger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputClickHouse}
	deliveryFailure := &cfg.DeliveryLogger.DeliveryClickHouseConfig.
		DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig
	deliveryFailure.DurableType =
		[]pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive}
	deliveryFailure.FileArchiveConfig.Dir = "delivery-backend-archive"
	deliveryFailure.FileArchiveConfig.DirCaseRetryable = "retryable"
	deliveryFailure.FileArchiveConfig.DirCasePermanent = "permanent"
	deliveryFailure.FileArchiveConfig.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	deliveryFailure.FileArchiveConfig.ObjectStorageUploadConfig.Enabled = true
	deliveryFailure.FileArchiveConfig.ObjectStorageUploadConfig.Targets =
		[]objectstorage.UploadTarget{objectstorage.UploadTargetS3}

	cfg.CompressedLogFileUploadFailureStore.Standard =
		objectStorageCompressedLogFileUploadFailureStoreForClaims(failurestore.DurableTypeSpool)
	cfg.CompressedLogFileUploadFailureStore.Delivery =
		objectStorageCompressedLogFileUploadFailureStoreForClaims(failurestore.DurableTypeSpool)

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
	} {
		source := objectStorageCompressedLogFileUploadFailureDirectoryClaimSource(contract, "spool.s3")
		if got := storageClaimsBySource(claims, source); len(got) != 1 {
			t.Fatalf("backend failure archive upload claims = %+v, want one %s", got, source)
		}
	}
	if err := validateStorageDirectoryClaims(claims); err != nil {
		t.Fatalf("backend failure archive claims produced a directory collision: %v", err)
	}
}

func TestStorageDirectoryClaimsObjectStorageCompressedLogFileUploadFailureUsesOnlySelectedDurableType(
	t *testing.T,
) {
	tests := []struct {
		name        string
		durableType []failurestore.DurableType
		wantArchive bool
		wantSpool   bool
	}{
		{
			name:        "file archive only",
			durableType: []failurestore.DurableType{failurestore.DurableTypeFileArchive},
			wantArchive: true,
		},
		{
			name:        "spool only",
			durableType: []failurestore.DurableType{failurestore.DurableTypeSpool},
			wantSpool:   true,
		},
		{
			name:        "none",
			durableType: []failurestore.DurableType{failurestore.DurableTypeNone},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newObjectStorageCompressedLogFileUploadFailureDirectoryClaimConfig(t)
			cfg.Logger.LogOutputDestinations =
				pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
			configureRunnerFileArchiveUpload(
				&cfg.Logger.FileConfig.FileSystemStoreConfig,
				"sync-log",
			)
			cfg.CompressedLogFileUploadFailureStore.Standard =
				objectStorageCompressedLogFileUploadFailureStoreForClaims(tt.durableType...)

			claims, err := cfg.collectStorageDirectoryClaims()
			if err != nil {
				t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
			}
			archiveClaims := storageClaimsBySource(
				claims,
				objectStorageCompressedLogFileUploadFailureDirectoryClaimSource(
					pkglogger.LoggerContractSync,
					"file_archive.s3",
				),
			)
			if got := len(archiveClaims); (got == 1) != tt.wantArchive {
				t.Fatalf("file archive claims = %+v, wantArchive=%t", archiveClaims, tt.wantArchive)
			}
			spoolClaims := storageClaimsBySource(
				claims,
				objectStorageCompressedLogFileUploadFailureDirectoryClaimSource(
					pkglogger.LoggerContractSync,
					"spool.s3",
				),
			)
			if got := len(spoolClaims); (got == 1) != tt.wantSpool {
				t.Fatalf("spool claims = %+v, wantSpool=%t", spoolClaims, tt.wantSpool)
			}
		})
	}
}

func TestStorageDirectoryClaimsIgnoreInactiveObjectStorageCompressedLogFileUploadFailureProducers(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AppConfig)
	}{
		{
			name: "file logger is not selected",
			configure: func(cfg *AppConfig) {
				configureRunnerFileArchiveUpload(
					&cfg.Logger.FileConfig.FileSystemStoreConfig,
					"unused-log",
				)
			},
		},
		{
			name: "file lifecycle is disabled",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				configureRunnerFileArchiveUpload(
					&cfg.Logger.FileConfig.FileSystemStoreConfig,
					"disabled-lifecycle-log",
				)
				cfg.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode =
					pkgfsstore.FileLifecycleModeDisable
			},
		},
		{
			name: "only an inapplicable file class is enabled",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				store := &cfg.Logger.FileConfig.FileSystemStoreConfig
				store.LogDir = "default-log"
				store.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
				store.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
					Enabled: true,
					Error:   objectstorage.UploadTargetConfig{Enabled: true},
				}
			},
		},
		{
			name: "backend is not selected",
			configure: func(cfg *AppConfig) {
				configureRunnerFailureArchive(
					&cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
						FailureHandlerConfig,
				)
			},
		},
		{
			name: "selected backend does not select its file archive",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
				failureCfg := &cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
					FailureHandlerConfig
				configureRunnerFailureArchive(failureCfg)
				failureCfg.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
				failureCfg.Fsstore.SpoolConfig.Dir = "record-spool"
			},
		},
		{
			name: "selected backend file archive lifecycle is disabled",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
				failureCfg := &cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
					FailureHandlerConfig
				configureRunnerFailureArchive(failureCfg)
				failureCfg.Fsstore.FileArchiveConfig.LifecycleMode =
					pkgfsstore.FileLifecycleModeDisable
			},
		},
		{
			name: "selected backend file archive upload is disabled",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
				failureCfg := &cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
					FailureHandlerConfig
				configureRunnerFailureArchive(failureCfg)
				failureCfg.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig.Enabled = false
			},
		},
		{
			name: "delivery file lifecycle has no upload owner",
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				configureRunnerDeliveryFileArchiveUpload(
					&cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig,
					"delivery-log",
				)
				cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleMode =
					pkgfsstore.FileLifecycleModeInline
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newObjectStorageCompressedLogFileUploadFailureDirectoryClaimConfig(t)
			cfg.CompressedLogFileUploadFailureStore.Standard =
				objectStorageCompressedLogFileUploadFailureStoreForClaims(
					failurestore.DurableTypeFileArchive,
					failurestore.DurableTypeSpool,
				)
			cfg.CompressedLogFileUploadFailureStore.Delivery =
				objectStorageCompressedLogFileUploadFailureStoreForClaims(
					failurestore.DurableTypeFileArchive,
					failurestore.DurableTypeSpool,
				)
			tt.configure(&cfg)

			claims, err := cfg.collectStorageDirectoryClaims()
			if err != nil {
				t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
			}
			for _, claim := range claims {
				if strings.Contains(claim.source, ".object_storage_compressed_log.") {
					t.Fatalf("inactive producer created compressed log file claim %+v; all=%+v", claim, claims)
				}
			}
		})
	}
}

func TestStorageDirectoryOwnershipRejectsObjectStorageCompressedLogFileUploadFailureArchiveOverlap(t *testing.T) {
	cfg := newObjectStorageCompressedLogFileUploadFailureDirectoryClaimConfig(t)
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputFile,
		pkglogger.LogOutputOpenSearch,
	}
	configureRunnerFileArchiveUpload(
		&cfg.Logger.FileConfig.FileSystemStoreConfig,
		"sync-log",
	)
	failureCfg := &cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
		FailureHandlerConfig
	configureRunnerFailureArchive(failureCfg)
	failureCfg.Fsstore.FileArchiveConfig.Dir = "shared-archive"
	cfg.CompressedLogFileUploadFailureStore.Standard =
		objectStorageCompressedLogFileUploadFailureStoreForClaims(failurestore.DurableTypeFileArchive)
	cfg.CompressedLogFileUploadFailureStore.Standard.DurableStoreConfig.
		FileArchiveConfig.DirName = "shared-archive"

	err := validateStorageDirectoryOwnership(cfg)
	requireStorageDirectoryCollision(
		t,
		err,
		"logger.opensearch.failure_archive",
		"logger.object_storage_compressed_log.file_archive.s3",
	)
}

func TestStorageDirectoryOwnershipRejectsObjectStorageCompressedLogFileUploadSpoolOverlap(t *testing.T) {
	cfg := newObjectStorageCompressedLogFileUploadFailureDirectoryClaimConfig(t)
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputFile,
		pkglogger.LogOutputOpenSearch,
	}
	configureRunnerFileArchiveUpload(
		&cfg.Logger.FileConfig.FileSystemStoreConfig,
		"sync-log",
	)
	recordSpool := &cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
		FailureHandlerConfig
	recordSpool.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
	recordSpool.Fsstore.SpoolConfig.Dir = "objectstorage"
	cfg.CompressedLogFileUploadFailureStore.Standard =
		objectStorageCompressedLogFileUploadFailureStoreForClaims(failurestore.DurableTypeSpool)

	err := validateStorageDirectoryOwnership(cfg)
	requireStorageDirectoryCollision(
		t,
		err,
		"logger.opensearch.failure_spool",
		"logger.object_storage_compressed_log.spool.s3",
	)
}

func newObjectStorageCompressedLogFileUploadFailureDirectoryClaimConfig(t *testing.T) AppConfig {
	t.Helper()
	root := t.TempDir()
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "compressed-log-claim-test"
	cfg.FsstoreConfig.AppStatePath = filepath.Join(root, "state")
	cfg.FsstoreConfig.SpoolPath = filepath.Join(root, "system-lib")
	cfg.FsstoreConfig.LogPath = filepath.Join(root, "logs")
	return cfg
}

func objectStorageCompressedLogFileUploadFailureStoreForClaims(
	types ...failurestore.DurableType,
) objectstoragecompressedlog.FailureStoreConfig {
	return objectstoragecompressedlog.FailureStoreConfig{
		DurableType: types,
		DurableStoreConfig: objectstoragecompressedlog.DurableStoreConfig{
			FileArchiveConfig: objectstoragecompressedlog.FileArchiveConfig{
				DirName: "compressed-log-archives",
			},
		},
	}
}
