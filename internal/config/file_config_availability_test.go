package config

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/config/runtime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestLoggerUsesInternalHandlerTargetForEachEnabledBackend(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*logconfig.LoggerConfig)
	}{
		{
			name: "stdout logger",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.StdoutConfig.HandlerConfig.OnErrorConfig = []string{" FILE "}
			},
		},
		{
			name: "slack logger",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputSlack}
				cfg.SlackConfig.HandlerConfig.OnWarnConfig = []string{"file"}
			},
		},
		{
			name: "opensearch logger",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
				cfg.OpenSearchConfig.OpenSearchLoggerBaseConfig.HandlerConfig.OnInfoConfig = []string{"file"}
			},
		},
		{
			name: "loki logger",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
				cfg.LokiConfig.LokiLoggerBaseConfig.HandlerConfig.OnDebugConfig = []string{"file"}
			},
		},
		{
			name: "cloudwatch logs logger",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}
				cfg.CloudWatchLogsConfig.CloudWatchLogsLoggerBaseConfig.HandlerConfig.OnErrorConfig =
					[]string{"file"}
			},
		},
		{
			name: "clickhouse logger",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputClickHouse}
				cfg.ClickHouseConfig.ClickHouseLoggerBaseConfig.HandlerConfig.OnWarnConfig = []string{"file"}
			},
		},
		{
			name: "opensearch failure archive",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
				failureCfg := &cfg.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig
				failureCfg.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive}
				failureCfg.Fsstore.FileArchiveConfig.HandlerConfig.OnErrorConfig = []string{"file"}
			},
		},
		{
			name: "loki failure spool",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
				failureCfg := &cfg.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig
				failureCfg.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
				failureCfg.Fsstore.SpoolConfig.HandlerConfig.OnWarnConfig = []string{"file"}
			},
		},
		{
			name: "cloudwatch logs failure archive",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}
				failureCfg := &cfg.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig
				failureCfg.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive}
				failureCfg.Fsstore.FileArchiveConfig.HandlerConfig.OnInfoConfig = []string{"file"}
			},
		},
		{
			name: "clickhouse failure spool",
			configure: func(cfg *logconfig.LoggerConfig) {
				cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputClickHouse}
				failureCfg := &cfg.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig
				failureCfg.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
				failureCfg.Fsstore.SpoolConfig.HandlerConfig.OnDebugConfig = []string{"file"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg logconfig.LoggerConfig
			tt.configure(&cfg)

			if !loggerUsesInternalHandlerTarget(cfg, pkglogger.LogOutputFile) {
				t.Fatal("loggerUsesInternalHandlerTarget() = false, want true")
			}
		})
	}
}

func TestLoggerUsesInternalHandlerTargetIgnoresUnselectedBackend(t *testing.T) {
	cfg := logconfig.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
	}
	cfg.ClickHouseConfig.ClickHouseLoggerBaseConfig.HandlerConfig.OnErrorConfig = []string{"file"}

	if loggerUsesInternalHandlerTarget(cfg, pkglogger.LogOutputFile) {
		t.Fatal("loggerUsesInternalHandlerTarget() = true for an unselected backend")
	}
}

// 通常の出力先にfileを含めなくても、有効なloggerまたはfailure storeのinternal logが
// fileへ委譲される場合は、同じ`.env`からfile設定を読み、runtime mapperまで渡す。
// 一方、通常FileLoggerの起動条件であるLogOutputDestinationsにはfileを追加しない。
func TestLoadConfigKeepsFileConfigForInternalFileHandler(t *testing.T) {
	tests := []struct {
		name             string
		env              []string
		loaded           func(AppConfig) (pkglogger.LogOutputDestinations, pkglogger.LogOutputDestinations, uint64)
		mapped           func(AppConfig) (*pkglogger.FileLoggerConfig, pkglogger.LogOutputDestinations)
		wantMaxSizeBytes uint64
		wantClaimSource  string
	}{
		{
			name: "sync logger handler",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_LOG_TYPE=stdout",
				"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER=file",
				"HETEROLOG_MAX_LOG_SIZE_MB=37",
				"HETEROLOG_LOG_DIR=sync-handler-only",
				"HETEROLOG_ERROR_LOG_DIR=sync-handler-only-error",
				"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=false",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS=false",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS=false",
			},
			loaded: func(cfg AppConfig) (pkglogger.LogOutputDestinations, pkglogger.LogOutputDestinations, uint64) {
				return cfg.Logger.HandlerTargetDestinations,
					cfg.Logger.LogOutputDestinations,
					cfg.Logger.FileConfig.FileSystemStoreConfig.MaxFileSize
			},
			mapped: func(cfg AppConfig) (*pkglogger.FileLoggerConfig, pkglogger.LogOutputDestinations) {
				mapped := ToLoggerConfig(cfg)
				return mapped.File, mapped.LogOutputDestinations
			},
			wantMaxSizeBytes: 37 * BytesPerMegabyte,
			wantClaimSource:  "logger.file.default",
		},
		{
			name: "async logger handler",
			env: []string{
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ASYNC_LOG_TYPE=slack",
				"HETEROLOG_ASYNC_SLACK_LOGGER_DEBUG_HANDLER=file",
				"HETEROLOG_MAX_ASYNC_LOG_SIZE_MB=41",
				"HETEROLOG_ASYNC_LOG_DIR=async-handler-only",
				"HETEROLOG_ASYNC_ERROR_LOG_DIR=async-handler-only-error",
				"HETEROLOG_ENABLE_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD=false",
				"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_ERROR_LOGS=false",
				"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOGS=false",
			},
			loaded: func(cfg AppConfig) (pkglogger.LogOutputDestinations, pkglogger.LogOutputDestinations, uint64) {
				return cfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
					cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
					cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.MaxFileSize
			},
			mapped: func(cfg AppConfig) (*pkglogger.FileLoggerConfig, pkglogger.LogOutputDestinations) {
				mapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
				return mapped.File, mapped.LogOutputDestinations
			},
			wantMaxSizeBytes: 41 * BytesPerMegabyte,
			wantClaimSource:  "async_logger.file.default",
		},
		{
			name: "enabled failure spool handler",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=spool",
				"HETEROLOG_OPENSEARCH_SPOOL_FILE_WARN_HANDLER=file",
				"HETEROLOG_MAX_LOG_SIZE_MB=43",
				"HETEROLOG_LOG_DIR=failure-handler-only",
				"HETEROLOG_ERROR_LOG_DIR=failure-handler-only-error",
				"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=false",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS=false",
				"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS=false",
			},
			loaded: func(cfg AppConfig) (pkglogger.LogOutputDestinations, pkglogger.LogOutputDestinations, uint64) {
				return cfg.Logger.HandlerTargetDestinations,
					cfg.Logger.LogOutputDestinations,
					cfg.Logger.FileConfig.FileSystemStoreConfig.MaxFileSize
			},
			mapped: func(cfg AppConfig) (*pkglogger.FileLoggerConfig, pkglogger.LogOutputDestinations) {
				mapped := ToLoggerConfig(cfg)
				return mapped.File, mapped.LogOutputDestinations
			},
			wantMaxSizeBytes: 43 * BytesPerMegabyte,
			wantClaimSource:  "logger.file.default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t, append([]string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
			}, tt.env...)...))

			handlerTargets, loadedDestinations, gotMaxSize := tt.loaded(cfg)
			if !handlerTargets.Has(pkglogger.LogOutputFile) {
				t.Fatalf("HandlerTargetDestinations = %v, want file", handlerTargets)
			}
			if loadedDestinations.Has(pkglogger.LogOutputFile) {
				t.Fatalf("loaded destinations = %v, must not enable normal file output", loadedDestinations)
			}
			if gotMaxSize != tt.wantMaxSizeBytes {
				t.Fatalf("loaded file MaxFileSize = %d, want %d", gotMaxSize, tt.wantMaxSizeBytes)
			}

			mappedFile, mappedDestinations := tt.mapped(cfg)
			if mappedFile == nil {
				t.Fatal("mapped File config = nil, want handler-available config")
			}
			if mappedDestinations.Has(pkglogger.LogOutputFile) {
				t.Fatalf("mapped destinations = %v, must not enable normal file output", mappedDestinations)
			}
			if got := mappedFile.FsstoreConfig.Lifecycle.MaxFileSize; got != tt.wantMaxSizeBytes {
				t.Fatalf("mapped file MaxFileSize = %d, want %d", got, tt.wantMaxSizeBytes)
			}

			claims, err := cfg.collectStorageDirectoryClaims()
			if err != nil {
				t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
			}
			if !hasStorageDirectoryClaimSource(claims, tt.wantClaimSource) {
				t.Fatalf("storage claims = %+v, want source %q", claims, tt.wantClaimSource)
			}
		})
	}
}

// DurableTypeで選択されていないdurable実装のhandler設定は、
// runtimeで呼ばれないためfile設定の可用性を要求しません。
func TestLoadLoggerConfigIgnoresInactiveFailureStoreFileHandlers(t *testing.T) {
	tests := []struct {
		name string
		env  []string
	}{
		{
			name: "durable store not selected",
			env: []string{
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=none",
				"HETEROLOG_OPENSEARCH_SPOOL_FILE_ERROR_HANDLER=file",
			},
		},
		{
			name: "spool handler configured but archive selected",
			env: []string{
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_OPENSEARCH_SPOOL_FILE_ERROR_HANDLER=file",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envLines := []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
			}
			envLines = append(envLines, tt.env...)
			cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t, envLines...))

			if cfg.Logger.HandlerTargetDestinations.Has(pkglogger.LogOutputFile) {
				t.Fatalf(
					"HandlerTargetDestinations = %v, must not contain file for inactive failure handler",
					cfg.Logger.HandlerTargetDestinations,
				)
			}
			if mapped := ToLoggerConfig(cfg); mapped.File != nil {
				t.Fatal("mapped File config is non-nil for inactive failure handler")
			}
		})
	}
}

// 手組みAppConfigでも、通常出力先にfileを指定していればmapperとdirectory claimの
// 対象になります。HandlerTargetDestinationsはfallback専用時だけ必要です。
func TestManualFileDestinationDoesNotRequireHandlerTarget(t *testing.T) {
	tests := []struct {
		name        string
		configure   func(*AppConfig)
		mappedFile  func(AppConfig) *pkglogger.FileLoggerConfig
		claimSource string
	}{
		{
			name: "sync",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputFile}
				cfg.Logger.FileConfig.FileSystemStoreConfig.LogDir = "manual-sync"
				cfg.Logger.FileConfig.FileSystemStoreConfig.ErrorLogDir = "manual-sync-error"
			},
			mappedFile: func(cfg AppConfig) *pkglogger.FileLoggerConfig {
				return ToLoggerConfig(cfg).File
			},
			claimSource: "logger.file.default",
		},
		{
			name: "async",
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
					pkglogger.LogOutputDestinations{logconfig.LogOutputFile}
				cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LogDir = "manual-async"
				cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.ErrorLogDir = "manual-async-error"
			},
			mappedFile: func(cfg AppConfig) *pkglogger.FileLoggerConfig {
				return ToAsyncBestEffortMultiLoggerConfig(cfg).File
			},
			claimSource: "async_logger.file.default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg AppConfig
			cfg.RuntimeEnv.Cloud = &runtime.CloudInfo{}
			cfg.RuntimeConfig.Target = "manual-file-config"
			cfg.FsstoreConfig.LogPath = filepath.Join(t.TempDir(), "logs")
			tt.configure(&cfg)

			if fileCfg := tt.mappedFile(cfg); fileCfg == nil {
				t.Fatal("mapped File config = nil, want non-nil")
			}

			claims, err := cfg.collectStorageDirectoryClaims()
			if err != nil {
				t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
			}
			if !hasStorageDirectoryClaimSource(claims, tt.claimSource) {
				t.Fatalf("storage claims = %+v, want source %q", claims, tt.claimSource)
			}
		})
	}
}

func hasStorageDirectoryClaimSource(
	claims []storageDirectoryClaim,
	want string,
) bool {
	for _, claim := range claims {
		if claim.source == want {
			return true
		}
	}
	return false
}
