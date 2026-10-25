package config

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/appstatepath"
)

func TestStorageDirectoryClaimsAddsOneSharedArchiveCheckpointDirectory(t *testing.T) {
	root := t.TempDir()
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "checkpoint-owner"
	cfg.FsstoreConfig.AppStatePath = filepath.Join(root, "state")
	cfg.FsstoreConfig.LogPath = filepath.Join(root, "logs")
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.DeliveryLogger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}

	configureRunnerFileArchiveUpload(
		&cfg.Logger.FileConfig.FileSystemStoreConfig,
		"sync",
	)
	configureRunnerFileArchiveUpload(
		&cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig,
		"async",
	)
	configureRunnerDeliveryFileArchiveUpload(
		&cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig,
		"delivery",
	)

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}
	checkpointClaims := storageClaimsBySource(claims, archiveCheckpointDirectoryClaimSource)
	if len(checkpointClaims) != 1 {
		t.Fatalf("archive checkpoint claims = %+v, want exactly one; all=%+v", checkpointClaims, claims)
	}
	wantPath, err := canonicalStoragePath(appstatepath.ArchiveCheckpointDirectory(
		cfg.FsstoreConfig.AppStatePath,
		cfg.RuntimeConfig.Target,
	))
	if err != nil {
		t.Fatalf("canonicalStoragePath() error = %v", err)
	}
	if got := checkpointClaims[0].path; got != wantPath {
		t.Fatalf("archive checkpoint claim path = %q, want %q", got, wantPath)
	}
	if err := validateStorageDirectoryClaims(claims); err != nil {
		t.Fatalf("shared checkpoint directory self-collision error = %v, want nil", err)
	}
}

func TestStorageDirectoryOwnershipRejectsArchiveCheckpointOverlap(t *testing.T) {
	root := t.TempDir()
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "checkpoint-collision"
	cfg.FsstoreConfig.AppStatePath = root
	cfg.FsstoreConfig.LogPath = root
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	configureRunnerFileArchiveUpload(
		&cfg.Logger.FileConfig.FileSystemStoreConfig,
		appstatepath.ArchiveCheckpointDirectoryName,
	)

	err := validateStorageDirectoryOwnership(cfg)
	requireStorageDirectoryCollision(t, err, archiveCheckpointDirectoryClaimSource, "logger.file.default")
}

func TestStorageDirectoryClaimsAlwaysAddsReservedArchiveCheckpointDirectory(t *testing.T) {
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "reserved-checkpoint"
	cfg.FsstoreConfig.AppStatePath = filepath.Join(t.TempDir(), "state")

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}
	checkpointClaims := storageClaimsBySource(claims, archiveCheckpointDirectoryClaimSource)
	if len(checkpointClaims) != 1 {
		t.Fatalf("archive checkpoint claims = %+v, want exactly one", checkpointClaims)
	}
}

func TestStorageDirectoryClaimsAddsArchiveCheckpointForSelectedFailureArchive(t *testing.T) {
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "failure-checkpoint"
	cfg.FsstoreConfig.AppStatePath = filepath.Join(t.TempDir(), "state")
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
	configureRunnerFailureArchive(
		&cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig,
	)

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}
	if got := storageClaimsBySource(claims, archiveCheckpointDirectoryClaimSource); len(got) != 1 {
		t.Fatalf("selected failure archive checkpoint claims = %+v, want one; all=%+v", got, claims)
	}
}

func configureRunnerFileArchiveUpload(
	store *logconfig.FileSystemStoreConfig,
	logDir string,
) {
	store.LogDir = logDir
	store.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	store.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{
				objectstorage.UploadTargetS3,
			},
		},
	}
}

func configureRunnerDeliveryFileArchiveUpload(
	store *logconfig.DeliveryFileSystemStoreConfig,
	logDir string,
) {
	store.LogDir = logDir
	store.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	store.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{
				objectstorage.UploadTargetS3,
			},
		},
	}
}

func configureRunnerFailureArchive(cfg *failure.FailureHandlerConfig) {
	cfg.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive}
	cfg.Fsstore.FileArchiveConfig.Dir = "failure-archive"
	cfg.Fsstore.FileArchiveConfig.DirCaseRetryable = "retryable"
	cfg.Fsstore.FileArchiveConfig.DirCasePermanent = "permanent"
	cfg.Fsstore.FileArchiveConfig.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	cfg.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig.Enabled = true
	cfg.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig.Targets =
		[]objectstorage.UploadTarget{objectstorage.UploadTargetS3}
}

func storageClaimsBySource(
	claims []storageDirectoryClaim,
	source string,
) []storageDirectoryClaim {
	matched := make([]storageDirectoryClaim, 0, 1)
	for _, claim := range claims {
		if claim.source == source {
			matched = append(matched, claim)
		}
	}
	return matched
}
