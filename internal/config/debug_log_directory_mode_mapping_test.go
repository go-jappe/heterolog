package config

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestDebugLogDirectoryModeMappingAndClaims(t *testing.T) {
	shared, separate := false, true
	tests := []struct {
		name              string
		baseSeparate      bool
		baseDebugDir      string
		targetRoot        bool
		disableDebugFiles bool
		override          logconfig.InternalLogDestinationConfig
		wantDedicated     bool
		wantSeparate      bool
		wantRawDebug      string
		wantDebugDir      string
		wantError         bool
	}{
		{
			name: "shared mode ignores stale base debug path", baseDebugDir: "stale-debug",
			wantRawDebug: "stale-debug", wantDebugDir: "base-logs",
		},
		{
			name: "shared mode allows target root with stale debug path", baseDebugDir: "stale-debug",
			targetRoot: true, wantRawDebug: "stale-debug", wantDebugDir: ".",
		},
		{
			name: "shared mode ignores stale override alone", baseDebugDir: "stale-debug",
			override:     logconfig.InternalLogDestinationConfig{FileDebugLogDir: "stale-internal-debug"},
			wantRawDebug: "stale-debug", wantDebugDir: "base-logs",
		},
		{
			name: "explicit shared override with stale path is unchanged", baseDebugDir: "stale-debug",
			override: logconfig.InternalLogDestinationConfig{
				FileSeparateDebugLogDirEnabled: &shared, FileDebugLogDir: "stale-internal-debug",
			},
			wantRawDebug: "stale-debug", wantDebugDir: "base-logs",
		},
		{
			name: "identical normal directory override is unchanged", baseDebugDir: "stale-debug",
			override:     logconfig.InternalLogDestinationConfig{FileLogDir: "base-logs"},
			wantRawDebug: "stale-debug", wantDebugDir: "base-logs",
		},
		{
			name: "shared mode follows final internal normal directory", baseDebugDir: "stale-debug",
			override: logconfig.InternalLogDestinationConfig{
				FileLogDir: "internal-logs", FileDebugLogDir: "stale-internal-debug",
			},
			wantDedicated: true, wantRawDebug: "stale-internal-debug", wantDebugDir: "internal-logs",
		},
		{
			name: "nil switch inherits separate mode and raw directory", baseSeparate: true, baseDebugDir: "base-debug",
			override:      logconfig.InternalLogDestinationConfig{FileLogDir: "internal-logs"},
			wantDedicated: true, wantSeparate: true, wantRawDebug: "base-debug", wantDebugDir: "base-debug",
		},
		{
			name: "false alone disables inherited separate directory", baseSeparate: true, baseDebugDir: "base-debug",
			override:      logconfig.InternalLogDestinationConfig{FileSeparateDebugLogDirEnabled: &shared},
			wantDedicated: true, wantRawDebug: "base-debug", wantDebugDir: "base-logs",
		},
		{
			name: "false ignores debug override and uses final normal directory", baseSeparate: true, baseDebugDir: "base-debug",
			override: logconfig.InternalLogDestinationConfig{
				FileSeparateDebugLogDirEnabled: &shared,
				FileLogDir:                     "internal-logs", FileDebugLogDir: "stale-internal-debug",
			},
			wantDedicated: true, wantRawDebug: "stale-internal-debug", wantDebugDir: "internal-logs",
		},
		{
			name: "true alone activates inherited raw directory", baseDebugDir: "base-debug",
			override:      logconfig.InternalLogDestinationConfig{FileSeparateDebugLogDirEnabled: &separate},
			wantDedicated: true, wantSeparate: true, wantRawDebug: "base-debug", wantDebugDir: "base-debug",
		},
		{
			name: "true uses explicitly overridden debug directory",
			override: logconfig.InternalLogDestinationConfig{
				FileSeparateDebugLogDirEnabled: &separate, FileDebugLogDir: "internal-debug",
			},
			wantDedicated: true, wantSeparate: true, wantRawDebug: "internal-debug", wantDebugDir: "internal-debug",
		},
		{
			name: "identical explicit separate switch and directory are unchanged", baseSeparate: true, baseDebugDir: "base-debug",
			override: logconfig.InternalLogDestinationConfig{
				FileSeparateDebugLogDirEnabled: &separate, FileDebugLogDir: "base-debug",
			},
			wantSeparate: true, wantRawDebug: "base-debug", wantDebugDir: "base-debug",
		},
		{
			name:          "true cannot inherit the resolved normal directory",
			override:      logconfig.InternalLogDestinationConfig{FileSeparateDebugLogDirEnabled: &separate},
			wantDedicated: true, wantSeparate: true, wantError: true,
		},
		{
			name: "true cannot use whitespace debug directory", baseDebugDir: " \t ",
			override:      logconfig.InternalLogDestinationConfig{FileSeparateDebugLogDirEnabled: &separate},
			wantDedicated: true, wantSeparate: true, wantRawDebug: " \t ", wantDebugDir: " \t ", wantError: true,
		},
		{
			name: "true requires debug directory even without separate debug files", disableDebugFiles: true,
			override:      logconfig.InternalLogDestinationConfig{FileSeparateDebugLogDirEnabled: &separate},
			wantDedicated: true, wantSeparate: true, wantError: true,
		},
	}
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
	} {
		for _, tt := range tests {
			t.Run(string(contract)+"/"+tt.name, func(t *testing.T) {
				cfg := mapperValidationTestConfig()
				cfg.RuntimeConfig.Target = "app"
				cfg.FsstoreConfig.LogPath = filepath.Join(t.TempDir(), "logs")
				cfg.FsstoreConfig.AppStatePath = t.TempDir()
				baseStore := logconfig.FileSystemStoreConfig{
					LogDir: "base-logs", DebugLogDir: tt.baseDebugDir,
					IsSeparateDebugLogDirEnabled: tt.baseSeparate,
					IsSeparateDebugLogsEnabled:   !tt.disableDebugFiles,
					LifecycleMode:                pkgfsstore.FileLifecycleModeDisable,
				}
				if tt.targetRoot {
					baseStore.LogDir = "."
				}
				configureDebugDirectoryModeContract(&cfg, contract, baseStore, tt.override, !tt.wantDedicated)
				if tt.targetRoot {
					var err error
					switch contract {
					case pkglogger.LoggerContractSync:
						_, err = MapLoggerConfig(cfg)
					case pkglogger.LoggerContractAsyncBestEffort:
						_, err = MapAsyncLoggerConfig(cfg)
					case pkglogger.LoggerContractDelivery:
						_, err = MapDeliveryLoggerConfig(cfg)
					}
					if err != nil {
						t.Fatalf("shared target-root mapping failed: %v", err)
					}
				}
				baseLayout, internalLayout := debugDirectoryModeMappedLayouts(cfg, contract)
				if baseLayout.IsSeparateDebugLogDirEnabled != tt.baseSeparate || baseLayout.DebugLogDir != tt.baseDebugDir {
					t.Fatalf("base mapping lost raw switch or directory: %+v", baseLayout)
				}
				if (internalLayout != nil) != tt.wantDedicated {
					t.Fatalf("dedicated internal layout = %+v, want dedicated %t", internalLayout, tt.wantDedicated)
				}
				layout := baseLayout
				if internalLayout != nil {
					layout = *internalLayout
				}
				if layout.IsSeparateDebugLogDirEnabled != tt.wantSeparate || layout.DebugLogDir != tt.wantRawDebug {
					t.Fatalf("effective raw layout = %+v, want separate %t and raw debug dir %q", layout, tt.wantSeparate, tt.wantRawDebug)
				}
				resolved := appendlog.NewFsstoreConfigAdapter(layout)
				if got := resolved.DebugLogDir(); got != tt.wantDebugDir {
					t.Fatalf("resolved debug directory = %q, want %q", got, tt.wantDebugDir)
				}
				claims, err := cfg.collectStorageDirectoryClaims()
				if tt.wantError {
					if !errors.Is(err, pkgfsstore.ErrEmptyDebugLogDirectory) {
						t.Fatalf("claims error = %v, want %v", err, pkgfsstore.ErrEmptyDebugLogDirectory)
					}
					return
				}
				if err != nil {
					t.Fatalf("collect claims: %v", err)
				}
				source := loggerOutputSource(contract, logconfig.LogOutputFile)
				if tt.wantDedicated {
					source = loggerInternalLogOutputSource(contract, logconfig.LogOutputFile)
				}
				if tt.wantDebugDir == resolved.LogDir() {
					source += ".default+debug"
				} else {
					source += ".debug"
				}
				debugClaims := storageClaimsBySource(claims, source)
				wantPath, err := canonicalStoragePath(filepath.Join(cfg.FsstoreConfig.LogPath, "app", tt.wantDebugDir))
				if err != nil {
					t.Fatalf("canonical debug directory: %v", err)
				}
				if len(debugClaims) != 1 || debugClaims[0].path != wantPath {
					t.Fatalf("claims = %+v, want one %s at %s", claims, source, wantPath)
				}
				if err := validateStorageDirectoryClaims(claims); err != nil {
					t.Fatalf("effective layout produced a false collision: %v", err)
				}
			})
		}
	}
}

func configureDebugDirectoryModeContract(
	cfg *AppConfig,
	contract pkglogger.LoggerContractKind,
	store logconfig.FileSystemStoreConfig,
	override logconfig.InternalLogDestinationConfig,
	normalSelected bool,
) {
	destinations := pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	if normalSelected {
		destinations = append(destinations, pkglogger.LogOutputFile)
	}
	switch contract {
	case pkglogger.LoggerContractSync:
		configureSyncInternalLogTarget(cfg, pkglogger.LogOutputFile)
		cfg.Logger.LogOutputDestinations = destinations
		cfg.Logger.FileConfig.FileSystemStoreConfig = store
		cfg.Logger.InternalLogDestinationConfig = override
	case pkglogger.LoggerContractAsyncBestEffort:
		configureAsyncInternalLogTarget(cfg, pkglogger.LogOutputFile)
		cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations = destinations
		cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig = store
		cfg.AsyncBestEffortMultiLoggerConfig.InternalLogDestinationConfig = override
	case pkglogger.LoggerContractDelivery:
		configureDeliveryInternalLogTarget(cfg, pkglogger.LogOutputFile)
		cfg.DeliveryLogger.LogOutputDestinations = destinations
		cfg.DeliveryLogger.FileSystemStoreConfig = logconfig.DeliveryFileSystemStoreConfig{
			LogDir: store.LogDir, DebugLogDir: store.DebugLogDir,
			IsSeparateDebugLogDirEnabled: store.IsSeparateDebugLogDirEnabled,
			IsSeparateDebugLogsEnabled:   store.IsSeparateDebugLogsEnabled,
			LifecycleMode:                store.LifecycleMode,
		}
		cfg.DeliveryLogger.InternalLogDestinationConfig = override
	}
}

func debugDirectoryModeMappedLayouts(
	cfg AppConfig,
	contract pkglogger.LoggerContractKind,
) (pkgfsstore.FsstoreConfig, *pkgfsstore.FsstoreConfig) {
	if contract == pkglogger.LoggerContractDelivery {
		mapped := ToDeliveryLoggerConfig(cfg)
		if mapped.InternalLogDestinations != nil && mapped.InternalLogDestinations.File != nil {
			return mapped.File.FsstoreConfig.Layout, &mapped.InternalLogDestinations.File.FsstoreConfig.Layout
		}
		return mapped.File.FsstoreConfig.Layout, nil
	}
	var mapped pkglogger.LoggerConfig
	if contract == pkglogger.LoggerContractAsyncBestEffort {
		mapped = ToAsyncBestEffortMultiLoggerConfig(cfg).ToLoggerConfig()
	} else {
		mapped = ToLoggerConfig(cfg)
	}
	if mapped.InternalLogDestinations != nil && mapped.InternalLogDestinations.File != nil {
		return mapped.File.FsstoreConfig.Layout, &mapped.InternalLogDestinations.File.FsstoreConfig.Layout
	}
	return mapped.File.FsstoreConfig.Layout, nil
}
