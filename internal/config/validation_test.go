package config

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/failure"
	fsstorespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"

	"github.com/stretchr/testify/require"
)

func TestValidateStorageDirectoryClaims(t *testing.T) {
	root := t.TempDir()

	tests := []struct {
		name    string
		claims  []storageDirectoryClaim
		wantErr bool
	}{
		{
			name: "distinct sibling directories",
			claims: []storageDirectoryClaim{
				{source: "logger.loki.archive", path: filepath.Join(root, "loki")},
				{source: "logger.clickhouse.archive", path: filepath.Join(root, "clickhouse")},
			},
		},
		{
			name: "same backend owners cannot share spool root",
			claims: []storageDirectoryClaim{
				{source: "logger.loki.spool", path: filepath.Join(root, "loki")},
				{source: "async_logger.loki.spool", path: filepath.Join(root, "loki")},
			},
			wantErr: true,
		},
		{
			name: "different backends share spool root",
			claims: []storageDirectoryClaim{
				{source: "logger.loki.spool", path: filepath.Join(root, "shared")},
				{source: "logger.clickhouse.spool", path: filepath.Join(root, "shared")},
			},
			wantErr: true,
		},
		{
			name: "independent managed directories are nested",
			claims: []storageDirectoryClaim{
				{source: "logger.file", path: filepath.Join(root, "managed")},
				{source: "logger.loki.archive", path: filepath.Join(root, "managed", "archive")},
			},
			wantErr: true,
		},
		{
			name: "archive directories are nested",
			claims: []storageDirectoryClaim{
				{source: "logger.loki.archive", path: filepath.Join(root, "archive")},
				{source: "logger.clickhouse.archive", path: filepath.Join(root, "archive", "clickhouse")},
			},
			wantErr: true,
		},
		{
			name: "spool directories are nested",
			claims: []storageDirectoryClaim{
				{source: "logger.loki.spool", path: filepath.Join(root, "spool")},
				{source: "async_logger.loki.spool", path: filepath.Join(root, "spool", "async")},
			},
			wantErr: true,
		},
		{
			name: "archive and spool share exact directory",
			claims: []storageDirectoryClaim{
				{source: "logger.loki.archive", path: filepath.Join(root, "shared")},
				{source: "logger.loki.spool", path: filepath.Join(root, "shared")},
			},
			wantErr: true,
		},
		{
			name: "archive contains spool directory",
			claims: []storageDirectoryClaim{
				{source: "logger.loki.archive", path: filepath.Join(root, "cross")},
				{source: "logger.loki.spool", path: filepath.Join(root, "cross", "spool")},
			},
			wantErr: true,
		},
		{
			name: "distinct spool directories",
			claims: []storageDirectoryClaim{
				{source: "logger.loki.spool", path: filepath.Join(root, "spool-a")},
				{source: "async_logger.loki.spool", path: filepath.Join(root, "spool-b")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStorageDirectoryClaims(tt.claims)
			if tt.wantErr && err == nil {
				t.Fatal("validateStorageDirectoryClaims() error = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validateStorageDirectoryClaims() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateStorageDirectoryOwnershipJoinsCollectionAndCollisionErrors(t *testing.T) {
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "storage-validation-error-aggregation"
	cfg.FsstoreConfig.LogPath = t.TempDir()
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputFile,
		pkglogger.LogOutputLoki,
	}
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.Logger.FileConfig.FileSystemStoreConfig.LogDir = "shared"
	cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LogDir = "shared"
	cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig =
		failure.FailureHandlerConfig{
			DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
			Fsstore: failure.FailureFileSystemStoreConfig{
				SpoolConfig: failure.SpoolConfig{Dir: "loki"},
			},
		}

	err := validateStorageDirectoryOwnership(cfg)
	if err == nil {
		t.Fatal("validateStorageDirectoryOwnership() error = nil, want joined errors")
	}
	var resolutionErr *StorageDirectoryResolutionError
	if !errors.As(err, &resolutionErr) || resolutionErr.Directory != "logger.loki failure spool" ||
		!errors.Is(resolutionErr, fsstorespool.ErrEmptySpoolPath) {
		t.Fatalf("validateStorageDirectoryOwnership() error = %v, want empty spool path for logger.loki", err)
	}
	requireStorageDirectoryCollision(t, err, "logger.file.default", "async_logger.file.default")
}

func TestStorageDirectoryClaimsIncludesHandlerOnlyInternalLogFileOverride(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*AppConfig)
		wantSource string
		wantDir    string
	}{
		{
			name: "sync",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.Logger.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				cfg.Logger.StdoutConfig.HandlerConfig.OnErrorConfig = []string{"file"}
				cfg.Logger.FileConfig.FileSystemStoreConfig.LogDir = "sync-handler-only"
				cfg.Logger.InternalLogDestinationConfig.FileLogDir = "sync-internal-log"
			},
			wantSource: "logger.internal_log.file.default",
			wantDir:    "sync-internal-log",
		},
		{
			name: "async",
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				cfg.AsyncBestEffortMultiLoggerConfig.StdoutConfig.
					HandlerConfig.OnErrorConfig = []string{"file"}
				cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.
					FileSystemStoreConfig.LogDir = "async-handler-only"
				cfg.AsyncBestEffortMultiLoggerConfig.
					InternalLogDestinationConfig.FileLogDir = "async-internal-log"
			},
			wantSource: "async_logger.internal_log.file.default",
			wantDir:    "async-internal-log",
		},
		{
			name: "delivery",
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.DeliveryLogger.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				cfg.DeliveryLogger.DeliveryStdoutConfig.
					HandlerConfig.OnErrorConfig = []string{"file"}
				cfg.DeliveryLogger.DeliveryFileConfig.
					FileSystemStoreConfig.LogDir = "delivery-handler-only"
				cfg.DeliveryLogger.InternalLogDestinationConfig.
					FileLogDir = "delivery-internal-log"
			},
			wantSource: "delivery_logger.internal_log.file.default",
			wantDir:    "delivery-internal-log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := AppConfig{}
			cfg.RuntimeConfig.Target = "handler-only-internal-log"
			logPath := filepath.Join(t.TempDir(), "logs")
			cfg.FsstoreConfig.LogPath = logPath
			tt.configure(&cfg)

			claims, err := cfg.collectStorageDirectoryClaims()
			if err != nil {
				t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
			}
			var internalLogClaim *storageDirectoryClaim
			for index := range claims {
				if claims[index].source == tt.wantSource {
					internalLogClaim = &claims[index]
					break
				}
			}
			if internalLogClaim == nil {
				t.Fatalf("storage claims = %+v, want source %q", claims, tt.wantSource)
			}
			for _, claim := range claims {
				if strings.HasPrefix(claim.source, strings.TrimSuffix(tt.wantSource, ".internal_log.file.default")+".file.") {
					t.Fatalf(
						"storage claims = %+v, handler-only dedicated file must not start an unused base owner",
						claims,
					)
				}
			}
			wantPath, err := canonicalStoragePath(filepath.Join(
				logPath,
				cfg.RuntimeConfig.Target,
				tt.wantDir,
			))
			if err != nil {
				t.Fatalf("canonicalStoragePath() error = %v", err)
			}
			if internalLogClaim.path != wantPath {
				t.Fatalf(
					"internal-log file claim path = %q, want effective path %q",
					internalLogClaim.path,
					wantPath,
				)
			}
			if err := validateStorageDirectoryClaims(claims); err != nil {
				t.Fatalf("validateStorageDirectoryClaims() error = %v", err)
			}
		})
	}
}

func TestStorageDirectoryClaimsDoesNotAddInternalLogFileOwnerWithoutOverride(t *testing.T) {
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "shared-internal-log-file"
	cfg.FsstoreConfig.LogPath = filepath.Join(t.TempDir(), "logs")
	cfg.Logger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	cfg.Logger.HandlerTargetDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.Logger.StdoutConfig.HandlerConfig.OnErrorConfig = []string{"file"}
	cfg.Logger.FileConfig.FileSystemStoreConfig.LogDir = "shared"

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}
	if !hasStorageDirectoryClaimSource(claims, "logger.file.default") {
		t.Fatalf("storage claims = %+v, want shared base file owner", claims)
	}
	for _, claim := range claims {
		if strings.Contains(claim.source, ".internal_log.file") {
			t.Fatalf("storage claims = %+v, must not contain a dedicated internal-log owner", claims)
		}
	}
}

func TestStorageDirectoryClaimsUsesEffectiveInternalLogFileLayout(t *testing.T) {
	baseRoot := filepath.Join(t.TempDir(), "base")
	internalRoot := filepath.Join(t.TempDir(), "internal")
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "base-target"
	cfg.FsstoreConfig.LogPath = baseRoot
	cfg.Logger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	cfg.Logger.HandlerTargetDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
	cfg.Logger.StdoutConfig.HandlerConfig.OnErrorConfig = []string{"file"}
	cfg.Logger.FileConfig.FileSystemStoreConfig.LogDir = "base-logs"
	cfg.Logger.FileConfig.FileSystemStoreConfig.ErrorLogDir = "base-errors"
	cfg.Logger.FileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled = true
	cfg.Logger.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled = true
	cfg.Logger.InternalLogDestinationConfig.FileLogPath = internalRoot
	cfg.Logger.InternalLogDestinationConfig.FileTarget = "internal-target"
	cfg.Logger.InternalLogDestinationConfig.FileLogDir = "internal-logs"
	cfg.Logger.InternalLogDestinationConfig.FileErrorLogDir = "internal-errors"

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}
	gotBySource := make(map[string]string, len(claims))
	for _, claim := range claims {
		gotBySource[claim.source] = claim.path
	}
	wants := map[string]string{
		"logger.internal_log.file.default+debug": filepath.Join(
			internalRoot,
			"internal-target",
			"internal-logs",
		),
		"logger.internal_log.file.error": filepath.Join(
			internalRoot,
			"internal-target",
			"internal-errors",
		),
	}
	for source, rawPath := range wants {
		wantPath, err := canonicalStoragePath(rawPath)
		if err != nil {
			t.Fatalf("canonicalStoragePath(%q) error = %v", rawPath, err)
		}
		if got := gotBySource[source]; got != wantPath {
			t.Fatalf("%s claim path = %q, want %q; claims=%+v", source, got, wantPath, claims)
		}
	}
	if err := validateStorageDirectoryClaims(claims); err != nil {
		t.Fatalf("validateStorageDirectoryClaims() error = %v", err)
	}
}

func TestStorageDirectoryClaimsResolvesInternalDebugDirectory(t *testing.T) {
	for _, tt := range []struct {
		name     string
		base     string
		override string
		want     string
	}{
		{name: "unspecified follows internal normal directory", want: "internal-logs"},
		{name: "inherits explicit base debug directory", base: "base-debug", want: "base-debug"},
		{name: "explicit internal debug directory", base: "base-debug", override: "internal-debug", want: "internal-debug"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := AppConfig{}
			cfg.RuntimeConfig.Target = "app"
			cfg.FsstoreConfig.LogPath = t.TempDir()
			cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
			cfg.Logger.HandlerTargetDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
			cfg.Logger.StdoutConfig.HandlerConfig.OnDebugConfig = []string{"file"}
			cfg.Logger.FileConfig.FileSystemStoreConfig.LogDir = "base-logs"
			cfg.Logger.FileConfig.FileSystemStoreConfig.DebugLogDir = tt.base
			cfg.Logger.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogDirEnabled = tt.base != ""
			cfg.Logger.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled = true
			cfg.Logger.InternalLogDestinationConfig.FileLogDir = "internal-logs"
			cfg.Logger.InternalLogDestinationConfig.FileDebugLogDir = tt.override
			claims, err := cfg.collectStorageDirectoryClaims()
			if err != nil {
				t.Fatal(err)
			}
			wantSource := "logger.internal_log.file.debug"
			if tt.want == "internal-logs" {
				wantSource = "logger.internal_log.file.default+debug"
			}
			matches := storageClaimsBySource(claims, wantSource)
			wantPath, err := canonicalStoragePath(filepath.Join(cfg.FsstoreConfig.LogPath, "app", tt.want))
			if err != nil {
				t.Fatalf("canonical debug directory: %v", err)
			}
			if len(matches) != 1 || matches[0].path != wantPath {
				t.Fatalf("debug directory claims = %+v, want %s at %s", claims, wantSource, wantPath)
			}
		})
	}
}

func TestValidateApplicationConfigRejectsInternalLogFileOverrideCollision(t *testing.T) {
	tests := []struct {
		name        string
		configure   func(*AppConfig)
		wantSources [2]string
	}{
		{
			name: "sync",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{
						pkglogger.LogOutputStdout,
						pkglogger.LogOutputFile,
					}
				cfg.Logger.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				cfg.Logger.StdoutConfig.HandlerConfig.OnErrorConfig = []string{"file"}
				cfg.Logger.FileConfig.FileSystemStoreConfig.LogDir = "shared"
				cfg.Logger.InternalLogDestinationConfig.FileLogDir = "shared/internal"
			},
			wantSources: [2]string{
				"logger.file.default",
				"logger.internal_log.file.default",
			},
		},
		{
			name: "async",
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
					pkglogger.LogOutputDestinations{
						pkglogger.LogOutputStdout,
						pkglogger.LogOutputFile,
					}
				cfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				cfg.AsyncBestEffortMultiLoggerConfig.StdoutConfig.
					HandlerConfig.OnErrorConfig = []string{"file"}
				cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.
					FileSystemStoreConfig.LogDir = "shared"
				cfg.AsyncBestEffortMultiLoggerConfig.
					InternalLogDestinationConfig.FileLogDir = "shared/internal"
			},
			wantSources: [2]string{
				"async_logger.file.default",
				"async_logger.internal_log.file.default",
			},
		},
		{
			name: "delivery",
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{
						pkglogger.LogOutputStdout,
						pkglogger.LogOutputFile,
					}
				cfg.DeliveryLogger.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputFile}
				cfg.DeliveryLogger.DeliveryStdoutConfig.
					HandlerConfig.OnErrorConfig = []string{"file"}
				cfg.DeliveryLogger.DeliveryFileConfig.
					FileSystemStoreConfig.LogDir = "shared/internal"
				cfg.DeliveryLogger.InternalLogDestinationConfig.
					FileLogDir = "shared"
			},
			wantSources: [2]string{
				"delivery_logger.file.default",
				"delivery_logger.internal_log.file.default",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := AppConfig{}
			cfg.RuntimeConfig.Target = "internal-log-file-collision"
			cfg.FsstoreConfig.LogPath = filepath.Join(t.TempDir(), "logs")
			tt.configure(&cfg)

			err := ValidateApplicationConfig(cfg)
			requireStorageDirectoryCollision(t, err, tt.wantSources[0], tt.wantSources[1])
		})
	}
}

func TestLoadConfigRejectsFileArchiveTerminalDirectoryCollision(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki",
		"HETEROLOG_LOKI_DURABLE_TYPES=fileArchive",
		"HETEROLOG_LOKI_DURABLE_RETRYABLE_DIR=failed",
		"HETEROLOG_LOKI_DURABLE_PERMANENT_DIR=failed",
	))

	requireStorageDirectoryCollision(
		t,
		err,
		"logger.loki.failure_archive.retryable",
		"logger.loki.failure_archive.permanent",
	)
}

func TestLoadConfigRejectsSharedArchiveRootAcrossBackendsWithDistinctTerminalDirectories(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki,clickhouse",
		"HETEROLOG_LOKI_DURABLE_TYPES=fileArchive",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=fileArchive",
		"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR=shared-archive",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR=shared-archive",
		"HETEROLOG_LOKI_DURABLE_RETRYABLE_DIR=loki-retryable",
		"HETEROLOG_LOKI_DURABLE_PERMANENT_DIR=loki-permanent",
		"HETEROLOG_CLICKHOUSE_DURABLE_RETRYABLE_DIR=clickhouse-retryable",
		"HETEROLOG_CLICKHOUSE_DURABLE_PERMANENT_DIR=clickhouse-permanent",
	))

	requireStorageDirectoryCollision(
		t,
		err,
		"logger.loki.failure_archive",
		"logger.clickhouse.failure_archive",
	)
}

func TestLoadConfigRejectsSharedArchiveRootAcrossLoggerContractsWithDistinctTerminalDirectories(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki",
		"HETEROLOG_ASYNC_LOG_TYPE=loki",
		"HETEROLOG_LOKI_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES=fileArchive",
		"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR=shared-loki-archive",
		"HETEROLOG_ASYNC_LOKI_FAILURE_ARCHIVE_DIR=shared-loki-archive",
		"HETEROLOG_LOKI_DURABLE_RETRYABLE_DIR=sync-retryable",
		"HETEROLOG_LOKI_DURABLE_PERMANENT_DIR=sync-permanent",
		"HETEROLOG_ASYNC_LOKI_DURABLE_RETRYABLE_DIR=async-retryable",
		"HETEROLOG_ASYNC_LOKI_DURABLE_PERMANENT_DIR=async-permanent",
	))

	requireStorageDirectoryCollision(
		t,
		err,
		"logger.loki.failure_archive",
		"async_logger.loki.failure_archive",
	)
}

func TestLoadConfigRejectsSharedArchiveDirectoryAcrossAllLoggerContracts(t *testing.T) {
	tests := []struct {
		name    string
		backend string
		env     []string
	}{
		{
			name:    "opensearch",
			backend: "opensearch",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
				"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
				"HETEROLOG_DELIVERY_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
				"HETEROLOG_OPENSEARCH_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_OPENSEARCH_DURABLE_PERMANENT_DIR=permanent",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_PERMANENT_DIR=permanent",
			},
		},
		{
			name:    "loki",
			backend: "loki",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=loki",
				"HETEROLOG_ASYNC_LOG_TYPE=loki",
				"HETEROLOG_DELIVERY_LOG_TYPE=loki",
				"HETEROLOG_LOKI_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES=fileArchive",
				"HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES=fileArchive",
				"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR=shared-loki-archive",
				"HETEROLOG_ASYNC_LOKI_FAILURE_ARCHIVE_DIR=shared-loki-archive",
				"HETEROLOG_DELIVERY_LOKI_FAILURE_ARCHIVE_DIR=shared-loki-archive",
				"HETEROLOG_LOKI_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_LOKI_DURABLE_PERMANENT_DIR=permanent",
				"HETEROLOG_ASYNC_LOKI_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_ASYNC_LOKI_DURABLE_PERMANENT_DIR=permanent",
				"HETEROLOG_DELIVERY_LOKI_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_DELIVERY_LOKI_DURABLE_PERMANENT_DIR=permanent",
			},
		},
		{
			name:    "cloudwatchlogs",
			backend: "cloudwatchlogs",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_ASYNC_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_DELIVERY_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
				"HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
				"HETEROLOG_DELIVERY_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
				"HETEROLOG_CLOUDWATCH_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_CLOUDWATCH_DURABLE_PERMANENT_DIR=permanent",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_PERMANENT_DIR=permanent",
				"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_PERMANENT_DIR=permanent",
			},
		},
		{
			name:    "clickhouse",
			backend: "clickhouse",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=clickhouse",
				"HETEROLOG_ASYNC_LOG_TYPE=clickhouse",
				"HETEROLOG_DELIVERY_LOG_TYPE=clickhouse",
				"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES=fileArchive",
				"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES=fileArchive",
				"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR=shared-clickhouse-archive",
				"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_ARCHIVE_DIR=shared-clickhouse-archive",
				"HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR=shared-clickhouse-archive",
				"HETEROLOG_CLICKHOUSE_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_CLICKHOUSE_DURABLE_PERMANENT_DIR=permanent",
				"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_PERMANENT_DIR=permanent",
				"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_RETRYABLE_DIR=retryable",
				"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_PERMANENT_DIR=permanent",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, tt.env...))
			// 全てのlogger契約が同じarchive rootを共有するため、全ての組が衝突します。
			sync := "logger." + tt.backend + ".failure_archive"
			async := "async_logger." + tt.backend + ".failure_archive"
			delivery := "delivery_logger." + tt.backend + ".failure_archive"
			requireStorageDirectoryCollision(t, err, sync, async)
			requireStorageDirectoryCollision(t, err, sync, delivery)
			requireStorageDirectoryCollision(t, err, async, delivery)
		})
	}
}

func TestLoadConfigRejectsSanitizedSpoolCollisionAcrossBackends(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki,clickhouse",
		"HETEROLOG_LOKI_DURABLE_TYPES=spool",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=spool",
		"HETEROLOG_LOKI_FAILURE_SPOOL_DIR=shared/backend",
		"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR=shared_backend",
	))

	requireStorageDirectoryCollision(t, err, "logger.loki.failure_spool", "logger.clickhouse.failure_spool")
}

func TestLoadConfigRejectsSharedSpoolAcrossScopesForDifferentDestinations(t *testing.T) {
	tests := []struct {
		name    string
		backend string
		env     []string
	}{
		{
			name:    "opensearch",
			backend: "opensearch",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=spool",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=spool",
				"HETEROLOG_OPENSEARCH_FAILURE_SPOOL_DIR=shared-opensearch-spool",
				"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_SPOOL_DIR=shared-opensearch-spool",
				"HETEROLOG_OPENSEARCH_ENDPOINT=https://sync-opensearch.example",
				"HETEROLOG_ASYNC_OPENSEARCH_ENDPOINT=https://async-opensearch.example",
			},
		},
		{
			name:    "cloudwatchlogs",
			backend: "cloudwatchlogs",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_ASYNC_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_CLOUDWATCH_DURABLE_TYPES=spool",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES=spool",
				"HETEROLOG_CLOUDWATCH_FAILURE_SPOOL_DIR=shared-cloudwatch-spool",
				"HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_SPOOL_DIR=shared-cloudwatch-spool",
				"HETEROLOG_CLOUDWATCH_ENDPOINT=https://sync-cloudwatch.example",
				"HETEROLOG_ASYNC_CLOUDWATCH_ENDPOINT=https://async-cloudwatch.example",
			},
		},
		{
			name:    "clickhouse",
			backend: "clickhouse",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=clickhouse",
				"HETEROLOG_ASYNC_LOG_TYPE=clickhouse",
				"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=spool",
				"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES=spool",
				"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR=shared-clickhouse-spool",
				"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_SPOOL_DIR=shared-clickhouse-spool",
				"HETEROLOG_CLICKHOUSE_ENDPOINT=http://sync-clickhouse.example:8123",
				"HETEROLOG_ASYNC_CLICKHOUSE_ENDPOINT=http://async-clickhouse.example:8123",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, tt.env...))
			requireStorageDirectoryCollision(
				t,
				err,
				"logger."+tt.backend+".failure_spool",
				"async_logger."+tt.backend+".failure_spool",
			)
		})
	}
}

func TestLoadConfigRejectsSharedLokiSpoolForSameDestination(t *testing.T) {
	const endpoint = "http://127.0.0.1:3100"

	_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki",
		"HETEROLOG_ASYNC_LOG_TYPE=loki",
		"HETEROLOG_LOKI_DURABLE_TYPES=spool",
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES=spool",
		"HETEROLOG_LOKI_FAILURE_SPOOL_DIR=shared-loki",
		"HETEROLOG_ASYNC_LOKI_FAILURE_SPOOL_DIR=shared-loki",
		"HETEROLOG_LOKI_ENDPOINT="+endpoint,
		"HETEROLOG_ASYNC_LOKI_ENDPOINT="+endpoint,
	))

	requireStorageDirectoryCollision(
		t,
		err,
		"logger.loki.failure_spool",
		"async_logger.loki.failure_spool",
	)
}

func TestLoadConfigRejectsSharedLokiSpoolForDifferentEndpoints(t *testing.T) {
	_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki",
		"HETEROLOG_ASYNC_LOG_TYPE=loki",
		"HETEROLOG_LOKI_DURABLE_TYPES=spool",
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES=spool",
		"HETEROLOG_LOKI_FAILURE_SPOOL_DIR=shared-loki",
		"HETEROLOG_ASYNC_LOKI_FAILURE_SPOOL_DIR=shared-loki",
		"HETEROLOG_LOKI_ENDPOINT=http://127.0.0.1:3100",
		"HETEROLOG_ASYNC_LOKI_ENDPOINT=http://127.0.0.1:3200",
	))

	requireStorageDirectoryCollision(t, err, "logger.loki.failure_spool", "async_logger.loki.failure_spool")
}

// TestLoadConfigIgnoresStoragePathSettingsForDisabledBackendsは、未選択backendに残った
// 衝突する保存先設定が起動を妨げないことを検証します。「Ignores」「Allows」系のtestは、
// Rejects系と同じくdirectory所有権を検証するValidateApplicationConfigとmapperを通します。
// LoadConfigだけでは所有権の衝突を検出しないためです。
func TestLoadConfigIgnoresStoragePathSettingsForDisabledBackends(t *testing.T) {
	cfg := loadAcceptedStorageLayout(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=stdout",
		"HETEROLOG_LOKI_FAILURE_SPOOL_DIR=unused/shared",
		"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR=unused_shared",
		"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR=unused-archive",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR=unused-archive",
	))
	requireStorageLayoutMapped(t, cfg, pkglogger.LoggerContractSync)
}

func TestLoadConfigIgnoresDurableDirectoriesWhenDurableTypeIsNone(t *testing.T) {
	cfg := loadAcceptedStorageLayout(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki,clickhouse",
		"HETEROLOG_LOKI_DURABLE_TYPES=none",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=none",
		"HETEROLOG_LOKI_FAILURE_SPOOL_DIR=shared/backend",
		"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR=shared_backend",
		"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR=shared-archive",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR=shared-archive",
	))
	requireStorageLayoutMapped(t, cfg, pkglogger.LoggerContractSync)
}

func TestLoadConfigIgnoresDirectoriesForUnselectedDurableTypes(t *testing.T) {
	tests := []struct {
		name string
		env  []string
	}{
		{
			name: "opensearch sync archive is unselected",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=spool",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
				"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
			},
		},
		{
			name: "opensearch async archive is unselected",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=spool",
				"HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
				"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
			},
		},
		{
			name: "opensearch spool is unselected",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR=sync-opensearch-archive",
				"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR=async-opensearch-archive",
				"HETEROLOG_OPENSEARCH_FAILURE_SPOOL_DIR=shared-opensearch-spool",
				"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_SPOOL_DIR=shared-opensearch-spool",
				"HETEROLOG_OPENSEARCH_ENDPOINT=https://sync-opensearch.example",
				"HETEROLOG_ASYNC_OPENSEARCH_ENDPOINT=https://async-opensearch.example",
			},
		},
		{
			name: "cloudwatch sync archive is unselected",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_ASYNC_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_CLOUDWATCH_DURABLE_TYPES=spool",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
				"HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
			},
		},
		{
			name: "cloudwatch async archive is unselected",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_ASYNC_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES=spool",
				"HETEROLOG_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
				"HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
			},
		},
		{
			name: "cloudwatch spool is unselected",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_ASYNC_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_CLOUDWATCH_FAILURE_ARCHIVE_DIR=sync-cloudwatch-archive",
				"HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_ARCHIVE_DIR=async-cloudwatch-archive",
				"HETEROLOG_CLOUDWATCH_FAILURE_SPOOL_DIR=shared-cloudwatch-spool",
				"HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_SPOOL_DIR=shared-cloudwatch-spool",
				"HETEROLOG_CLOUDWATCH_ENDPOINT=https://sync-cloudwatch.example",
				"HETEROLOG_ASYNC_CLOUDWATCH_ENDPOINT=https://async-cloudwatch.example",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := append(storageBackendCredentialEnv(), tt.env...)
			cfg := loadAcceptedStorageLayout(t, storagePathValidationEnv(t, env...))
			requireStorageLayoutMapped(
				t,
				cfg,
				pkglogger.LoggerContractSync,
				pkglogger.LoggerContractAsyncBestEffort,
			)
		})
	}
}

func TestLoadConfigIgnoresDirectorySettingsForDisabledAsyncAndDeliveryScopes(t *testing.T) {
	tests := []struct {
		name string
		env  []string
	}{
		{
			name: "opensearch",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
				"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
				"HETEROLOG_DELIVERY_OPENSEARCH_FAILURE_ARCHIVE_DIR=shared-opensearch-archive",
			},
		},
		{
			name: "cloudwatchlogs",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_ASYNC_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_DELIVERY_LOG_TYPE=cloudwatchlogs",
				"HETEROLOG_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
				"HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
				"HETEROLOG_DELIVERY_CLOUDWATCH_FAILURE_ARCHIVE_DIR=shared-cloudwatch-archive",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := append(storageBackendCredentialEnv(), tt.env...)
			cfg := loadAcceptedStorageLayout(t, storagePathValidationEnv(t, env...))
			// async/deliveryは無効なので、有効なsync loggerだけをmapperで検証します。
			requireStorageLayoutMapped(t, cfg, pkglogger.LoggerContractSync)
		})
	}
}

func TestLoadConfigRejectsSharedFileLogDirectoryAcrossScopes(t *testing.T) {
	tests := []struct {
		name        string
		env         []string
		wantSources [2]string
	}{
		{
			name: "sync default and async default",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=file",
				"HETEROLOG_ASYNC_LOG_TYPE=file",
				"HETEROLOG_LOG_DIR=shared-sync-async",
				"HETEROLOG_ASYNC_LOG_DIR=shared-sync-async",
				"HETEROLOG_ERROR_LOG_DIR=sync-error-logs",
				"HETEROLOG_ASYNC_ERROR_LOG_DIR=async-error-logs",
			},
			wantSources: [2]string{"logger.file.default", "async_logger.file.default"},
		},
		{
			name: "sync default and delivery default",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=file",
				"HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_LOG_DIR=shared-sync-delivery",
				"HETEROLOG_DELIVERY_LOG_DIR=shared-sync-delivery",
				"HETEROLOG_ERROR_LOG_DIR=sync-error-logs",
				"HETEROLOG_DELIVERY_ERROR_LOG_DIR=delivery-error-logs",
			},
			wantSources: [2]string{"logger.file.default", "delivery_logger.file.default"},
		},
		{
			name: "async default and delivery default",
			env: []string{
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_ASYNC_LOG_TYPE=file",
				"HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_ASYNC_LOG_DIR=shared-async-delivery",
				"HETEROLOG_DELIVERY_LOG_DIR=shared-async-delivery",
				"HETEROLOG_ASYNC_ERROR_LOG_DIR=async-error-logs",
				"HETEROLOG_DELIVERY_ERROR_LOG_DIR=delivery-error-logs",
			},
			wantSources: [2]string{"async_logger.file.default", "delivery_logger.file.default"},
		},
		{
			name: "sync error and async default",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=file",
				"HETEROLOG_ASYNC_LOG_TYPE=file",
				"HETEROLOG_LOG_DIR=sync-normal-logs",
				"HETEROLOG_ERROR_LOG_DIR=shared-cross-role",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS=true",
				"HETEROLOG_ASYNC_LOG_DIR=shared-cross-role",
				"HETEROLOG_ASYNC_ERROR_LOG_DIR=async-error-logs",
			},
			wantSources: [2]string{"logger.file.error", "async_logger.file.default"},
		},
		{
			name: "sync debug and async default",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true", "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_LOG_TYPE=file", "HETEROLOG_ASYNC_LOG_TYPE=file",
				"HETEROLOG_LOG_DIR=sync-normal-logs", "HETEROLOG_DEBUG_LOG_DIR=shared-debug",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS=true",
				"HETEROLOG_ASYNC_LOG_DIR=shared-debug",
			},
			wantSources: [2]string{"logger.file.debug", "async_logger.file.default"},
		},
		{
			name: "async debug and delivery error",
			env: []string{
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true", "HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_ASYNC_LOG_TYPE=file", "HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_ASYNC_DEBUG_LOG_DIR=shared-debug", "HETEROLOG_DELIVERY_ERROR_LOG_DIR=shared-debug",
				"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true",
				"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOGS=true",
				"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_ERROR_LOGS=true",
			},
			wantSources: [2]string{"async_logger.file.debug", "delivery_logger.file.error"},
		},
		{
			name: "delivery debug and sync default",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true", "HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=file", "HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_LOG_DIR=shared-debug", "HETEROLOG_DELIVERY_DEBUG_LOG_DIR=shared-debug",
				"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true",
				"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOGS=true",
			},
			wantSources: [2]string{"delivery_logger.file.debug", "logger.file.default"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, tt.env...))
			requireStorageDirectoryCollision(t, err, tt.wantSources[0], tt.wantSources[1])
		})
	}
}

func TestLoadConfigAllowsFileLogRolesToShareDirectoryWithinScope(t *testing.T) {
	tests := []struct {
		name       string
		env        []string
		wantSource string
	}{
		{
			name: "sync",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_LOG_TYPE=file",
				"HETEROLOG_LOG_DIR=shared-sync/../shared-sync",
				"HETEROLOG_ERROR_LOG_DIR=shared-sync",
				"HETEROLOG_DEBUG_LOG_DIR=shared-sync",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS=true",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS=true",
			},
			wantSource: "logger.file.default+error+debug",
		},
		{
			name: "async",
			env: []string{
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ASYNC_LOG_TYPE=file",
				"HETEROLOG_ASYNC_LOG_DIR=shared-async",
				"HETEROLOG_ASYNC_ERROR_LOG_DIR=shared-async",
				"HETEROLOG_ASYNC_DEBUG_LOG_DIR=shared-async",
				"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true",
				"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_ERROR_LOGS=true",
				"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOGS=true",
			},
			wantSource: "async_logger.file.default+error+debug",
		},
		{
			name: "delivery",
			env: []string{
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_DELIVERY_LOG_DIR=shared-delivery",
				"HETEROLOG_DELIVERY_ERROR_LOG_DIR=shared-delivery",
				"HETEROLOG_DELIVERY_DEBUG_LOG_DIR=shared-delivery",
				"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true",
				"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_ERROR_LOGS=true",
				"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOGS=true",
			},
			wantSource: "delivery_logger.file.default+error+debug",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t, tt.env...))

			claims, err := cfg.collectStorageDirectoryClaims()
			if err != nil {
				t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
			}
			fileClaims := storageClaimsBySource(claims, tt.wantSource)
			if len(fileClaims) != 1 {
				t.Fatalf("file directory claims for %q = %+v, want one; all=%+v", tt.wantSource, fileClaims, claims)
			}
		})
	}
}

func TestValidateApplicationConfigFileLogDirectorySharing(t *testing.T) {
	for _, contract := range []struct {
		name    string
		prefix  string
		enabled string
		source  string
	}{
		{name: "sync", enabled: "HETEROLOG_ENABLE_LOGGING", source: "logger"},
		{name: "async", prefix: "ASYNC_", enabled: "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING", source: "async_logger"},
		{name: "delivery", prefix: "DELIVERY_", enabled: "HETEROLOG_ENABLE_DELIVERY_LOGGING", source: "delivery_logger"},
	} {
		for _, internal := range []bool{false, true} {
			scope := contract.name
			if internal {
				scope += " internal"
			}
			for _, tt := range []struct {
				name         string
				logDir       string
				errorDir     string
				debugDir     string
				disableError bool
				disableDebug bool
				wantErr      bool
			}{
				{name: "error and debug cannot share without default", logDir: "normal", errorDir: "special", debugDir: "special", wantErr: true},
				{name: "normalized error and debug cannot share without default", logDir: "normal", errorDir: "special", debugDir: "other/../special", wantErr: true},
				{name: "default and debug share", logDir: "normal", errorDir: "error", debugDir: "normal"},
				{name: "default and error share", logDir: "normal", errorDir: "normal", debugDir: "debug"},
				{name: "all share after normalization", logDir: "normal", errorDir: "other/../normal", debugDir: "normal"},
				{name: "all separate", logDir: "normal", errorDir: "error", debugDir: "debug"},
				{name: "directory separation disabled shares default", logDir: "normal", errorDir: "error"},
				{name: "disabled error directory ignored", logDir: "normal", errorDir: "special", debugDir: "special", disableError: true},
				{name: "disabled debug directory ignored", logDir: "normal", errorDir: "special", debugDir: "special", disableDebug: true},
			} {
				t.Run(scope+"/"+tt.name, func(t *testing.T) {
					prefix := contract.prefix
					lines := []string{
						contract.enabled + "=true",
						"HETEROLOG_ENABLE_" + prefix + "LOG_FILE_SEPARATE_ERROR_LOGS=true",
						"HETEROLOG_ENABLE_" + prefix + "LOG_FILE_SEPARATE_DEBUG_LOGS=true",
					}
					var directoryPrefix, wantSource string
					if internal {
						lines = append(lines,
							"HETEROLOG_"+prefix+"LOG_TYPE=stdout",
							"HETEROLOG_"+prefix+"STDOUT_LOGGER_ERROR_HANDLER=file",
						)
						directoryPrefix = prefix + "LOG_INTERNAL_LOG_FILE_"
						wantSource = contract.source + ".internal_log.file"
					} else {
						lines = append(lines, "HETEROLOG_"+prefix+"LOG_TYPE=file")
						directoryPrefix = prefix
						wantSource = contract.source + ".file"
					}
					lines = append(lines,
						"HETEROLOG_"+directoryPrefix+"LOG_DIR="+tt.logDir,
						"HETEROLOG_"+directoryPrefix+"ERROR_LOG_DIR="+tt.errorDir,
						"HETEROLOG_"+directoryPrefix+"DEBUG_LOG_DIR="+tt.debugDir,
					)
					if tt.debugDir != "" {
						if internal {
							lines = append(lines, "HETEROLOG_"+directoryPrefix+"SEPARATE_DEBUG_LOG_DIR=true")
						} else {
							lines = append(lines, "HETEROLOG_ENABLE_"+prefix+"LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true")
						}
					}
					if tt.disableError {
						lines = append(lines, "HETEROLOG_ENABLE_"+prefix+"LOG_FILE_SEPARATE_ERROR_LOGS=false")
					}
					if tt.disableDebug {
						lines = append(lines, "HETEROLOG_ENABLE_"+prefix+"LOG_FILE_SEPARATE_DEBUG_LOGS=false")
					}
					_, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t, lines...))
					if tt.wantErr {
						var sharedErr *SharedRoleLogDirectoryError
						if !errors.As(err, &sharedErr) || sharedErr.Source != wantSource ||
							filepath.Base(sharedErr.Directory) != "special" ||
							filepath.Base(sharedErr.DefaultDirectory) != "normal" {
							t.Fatalf("directory sharing error = %v, want %s error/debug directory special shared without normal", err, wantSource)
						}
					} else if err != nil {
						t.Fatalf("allowed directory layout was rejected: %v", err)
					}
				})
			}
		}
	}
}

func TestLoadConfigDoesNotCreateClaimsForDisabledSeparateFileWriters(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_LOG_DIR=default-only",
		"HETEROLOG_ERROR_LOG_DIR=unused-error-directory",
		"HETEROLOG_DEBUG_LOG_DIR=unused-debug-directory",
		"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS=false",
		"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS=false",
	))

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}
	if got := storageClaimsBySource(claims, "logger.file.default"); len(got) != 1 {
		t.Fatalf("default file directory claims = %+v, want one; all=%+v", got, claims)
	}
}

func TestLoadConfigAllowsSharedArchiveBaseWithDistinctFinalDirectories(t *testing.T) {
	cfg := loadAcceptedStorageLayout(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki,clickhouse",
		"HETEROLOG_LOKI_DURABLE_TYPES=fileArchive",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=fileArchive",
		"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR=loki-archive",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR=clickhouse-archive",
		"HETEROLOG_LOKI_DURABLE_RETRYABLE_DIR=retryable",
		"HETEROLOG_CLICKHOUSE_DURABLE_RETRYABLE_DIR=retryable",
		"HETEROLOG_LOKI_DURABLE_PERMANENT_DIR=permanent",
		"HETEROLOG_CLICKHOUSE_DURABLE_PERMANENT_DIR=permanent",
	))
	requireStorageLayoutMapped(t, cfg, pkglogger.LoggerContractSync)
}

func TestLoadConfigAllowsSameArchiveAndSpoolDirWhenResolvedPathsDiffer(t *testing.T) {
	const sharedDir = "clickhouse"

	cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=clickhouse",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR="+sharedDir,
		"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR="+sharedDir,
		"HETEROLOG_CLICKHOUSE_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_DEBUG_HANDLER=stdout",
	))

	failureConfig := cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig
	if got := failureConfig.Fsstore.FileArchiveConfig.Dir; got != sharedDir {
		t.Fatalf("ClickHouse failure archive dir = %q, want %q", got, sharedDir)
	}
	if got := failureConfig.Fsstore.SpoolConfig.Dir; got != sharedDir {
		t.Fatalf("ClickHouse failure spool dir = %q, want %q", got, sharedDir)
	}

	archiveClaim, err := cfg.newArchiveDirectoryClaim(
		"logger.clickhouse",
		failureConfig.Fsstore.FileArchiveConfig,
	)
	if err != nil {
		t.Fatalf("resolve ClickHouse failure archive directories: %v", err)
	}
	spoolClaim, err := cfg.newSpoolDirectoryClaim(
		"logger.clickhouse",
		failureConfig.Fsstore.SpoolConfig,
	)
	if err != nil {
		t.Fatalf("resolve ClickHouse failure spool directory: %v", err)
	}

	if storagePathsOverlap(archiveClaim.path, spoolClaim.path) {
		t.Fatalf(
			"same archive/spool dir name resolved to overlapping paths: archive=%q spool=%q",
			archiveClaim.path,
			spoolClaim.path,
		)
	}
}

func TestLoadConfigDoesNotClaimDeliverySpoolAndPreservesTypeForMapperValidation(t *testing.T) {
	base := filepath.Join(t.TempDir(), "shared-storage")

	cfg, err := loadConfigErrorFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_APP_STATE_DIR="+base,
		"HETEROLOG_SYSTEM_LIB_DIR="+base,
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_DELIVERY_LOG_TYPE=clickhouse",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR=spool",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_RETRYABLE_DIR=shared-store",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_PERMANENT_DIR=permanent",
		"DELIVERY_CLICKHOUSE_FAILURE_SPOOL_DIR=shared-store",
	))
	if err != nil {
		t.Fatalf("LoadConfig() returned error for ignored Delivery ClickHouse spool settings: %v", err)
	}
	assertDurableTypes(
		t,
		cfg.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig.DurableType,
		"spool",
		"fileArchive",
	)
}

func TestLoadConfigReadsOpenSearchFailureSpoolDir(t *testing.T) {
	const want = "custom-opensearch-spool"

	cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=opensearch",
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES=spool",
		"HETEROLOG_OPENSEARCH_FAILURE_SPOOL_DIR="+want,
	))

	got := cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig.Fsstore.SpoolConfig.Dir
	if got != want {
		t.Fatalf("OpenSearch failure spool dir = %q, want %q", got, want)
	}
}

// TestLoadConfigAcceptsFixtureEnvStorageLayoutは、公開例から生成したfixtureの.envを
// applicationと同じHETEROLOG_ENV_PATH境界で読み込み、application全体の設定検証とmapperを通します。
// fixtureは全LoggerContractKindで主要backendを選択するため、公開例に掲載した保存先が
// backendを有効にしても衝突しないことを確認できます。開発者の.envは読み込みません。
// fixtureの.env.exampleは同じ公開例から生成した同等のfileなので、別途検証しません。
// 公開例そのものの既定構成はTestPublicExampleMapsWithoutCredentialsが検証します。
func TestLoadConfigAcceptsFixtureEnvStorageLayout(t *testing.T) {
	// fixtureは一時directoryに作るため、Windowsで一時directoryを決める環境変数を残したまま
	// 先に作成し、その後に実行中のprocessの設定項目だけを外します。
	envPath := filepath.Join(configFixtureRootForTest(t), ".env")
	testutil.UnsetConfigEnvironment(t)
	if _, err := os.Stat(envPath); err != nil {
		t.Fatalf("stat fixture .env: %v", err)
	}
	if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
		t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
	}

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with fixture .env storage layout: %v", err)
	}
	if err := ValidateApplicationConfig(cfg); err != nil {
		t.Fatalf("ValidateApplicationConfig() with fixture .env storage layout: %v", err)
	}
	if _, err := MapLoggerConfig(cfg); err != nil {
		t.Fatalf("MapLoggerConfig() with fixture .env: %v", err)
	}
	if _, err := MapAsyncLoggerConfig(cfg); err != nil {
		t.Fatalf("MapAsyncLoggerConfig() with fixture .env: %v", err)
	}
	if _, err := MapDeliveryLoggerConfig(cfg); err != nil {
		t.Fatalf("MapDeliveryLoggerConfig() with fixture .env: %v", err)
	}
}

func storagePathValidationEnv(t *testing.T, overrides ...string) []string {
	t.Helper()

	root := t.TempDir()
	lines := []string{
		"HETEROLOG_TARGET=storage-path-validation",
		"HETEROLOG_SYSTEM_LOG_DIR=" + filepath.Join(root, "logs"),
		"HETEROLOG_SYSTEM_LIB_DIR=" + filepath.Join(root, "spool"),
		"HETEROLOG_APP_STATE_DIR=" + filepath.Join(root, "archive"),
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
	}

	for _, override := range overrides {
		key, _, ok := strings.Cut(override, "=")
		if !ok {
			t.Fatalf("invalid env override %q", override)
		}

		replaced := false
		for i, line := range lines {
			lineKey, _, lineOK := strings.Cut(line, "=")
			if lineOK && lineKey == key {
				lines[i] = override
				replaced = true
				break
			}
		}
		if !replaced {
			lines = append(lines, override)
		}
	}

	return lines
}

// loadAcceptedStorageLayoutは、LoadConfigに加えてdirectory所有権を含む
// ValidateApplicationConfigを通過したAppConfigを返します。
func loadAcceptedStorageLayout(t *testing.T, lines []string) AppConfig {
	t.Helper()

	cfg, err := loadConfigErrorFromTempEnv(t, lines)
	require.NoError(t, err)
	return cfg
}

// storageBackendCredentialEnvは、保存先以外の理由でmapperが拒否しないよう、
// OpenSearchとCloudWatch Logsのsync/async用dummy認証情報を返します。
func storageBackendCredentialEnv() []string {
	var lines []string
	for _, prefix := range []string{"", "ASYNC_"} {
		lines = append(lines,
			"HETEROLOG_"+prefix+"OPENSEARCH_USERNAME=storage-test-user",
			"HETEROLOG_"+prefix+"OPENSEARCH_PASSWORD=storage-test-password",
			"HETEROLOG_"+prefix+"CLOUDWATCH_AWS_ACCESS_KEY_ID=storage-test-access-key",
			"HETEROLOG_"+prefix+"CLOUDWATCH_AWS_SECRET_ACCESS_KEY=storage-test-secret-key",
		)
	}
	return lines
}

// requireStorageLayoutMappedは、有効なLoggerContractKindごとに検証付きmapperが
// 設定を受理することを確認します。
func requireStorageLayoutMapped(t *testing.T, cfg AppConfig, contracts ...pkglogger.LoggerContractKind) {
	t.Helper()

	for _, contract := range contracts {
		require.NoError(t, mapLogDirectoryTestConfig(cfg, contract), contract.DisplayName())
	}
}

// requireStorageDirectoryCollisionは、errが所有単位sourceAとsourceBの保存先directoryの衝突を
// 含むことを確認します。順序は問いません。sourceが"logger.file.default"のように役割を含む場合、
// 同じdirectoryを共有して"logger.file.default+debug"にまとめられた所有単位とも一致します。
func requireStorageDirectoryCollision(t *testing.T, err error, sourceA, sourceB string) *StorageDirectoryCollisionError {
	t.Helper()
	if err == nil {
		t.Fatal("storage directory validation error = nil, want collision error")
	}
	collisions := errortree.All[*StorageDirectoryCollisionError](err)
	for _, collision := range collisions {
		if (storageClaimSourceMatches(collision.LeftSource, sourceA) && storageClaimSourceMatches(collision.RightSource, sourceB)) ||
			(storageClaimSourceMatches(collision.LeftSource, sourceB) && storageClaimSourceMatches(collision.RightSource, sourceA)) {
			return collision
		}
	}
	t.Fatalf("storage directory validation error = %v, want collision between %s and %s", err, sourceA, sourceB)
	return nil
}

// storageClaimSourceMatchesは、所有単位の名前sourceがwantと一致するかを返します。sourceは
// 同じdirectoryを共有する役割を"+"で連結した"logger.file.default+debug"の形を取り得るため、
// wantが"logger.file.default"なら、同じ出力先で役割defaultを含む所有単位と一致させます。
func storageClaimSourceMatches(source, want string) bool {
	if source == want {
		return true
	}
	owner, role, ok := cutLast(want, ".")
	if !ok {
		return false
	}
	sourceOwner, roles, ok := cutLast(source, ".")
	return ok && sourceOwner == owner && slices.Contains(strings.Split(roles, "+"), role)
}

// cutLastは、sの最後のsepの前後を返します。sepがなければfalseです。
func cutLast(s, sep string) (before, after string, found bool) {
	if index := strings.LastIndex(s, sep); index >= 0 {
		return s[:index], s[index+len(sep):], true
	}
	return s, "", false
}
