package config

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLogDirectoryMappingValidatesEffectiveInternalDestination(t *testing.T) {
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
	} {
		for _, tc := range []struct {
			name, baseDir, wantDir string
			override               logconfig.InternalLogDestinationConfig
			normal, wantError      bool
		}{
			{name: "normal empty", normal: true, wantError: true},
			{name: "normal whitespace", baseDir: " \t ", normal: true, wantError: true},
			{name: "normal target root", baseDir: ".", wantDir: ".", normal: true},
			{name: "internal inherits valid directory", baseDir: "base-logs", wantDir: "base-logs"},
			{name: "internal inherits empty directory", wantError: true},
			{name: "dedicated internal inherits empty directory", override: logconfig.InternalLogDestinationConfig{FileTarget: "internal-app"}, wantError: true},
			{name: "dedicated internal inherits valid directory", baseDir: "base-logs", override: logconfig.InternalLogDestinationConfig{FileTarget: "internal-app"}, wantDir: "base-logs"},
			{name: "dedicated internal rejects whitespace", baseDir: "base-logs", override: logconfig.InternalLogDestinationConfig{FileLogDir: " \t "}, wantError: true},
			{name: "valid internal ignores unused empty base", override: logconfig.InternalLogDestinationConfig{FileLogDir: "internal-logs"}, wantDir: "internal-logs"},
		} {
			t.Run(contract.String()+"/"+tc.name, func(t *testing.T) {
				cfg := mapperValidationTestConfig()
				cfg.RuntimeConfig.Target = "app"
				cfg.FsstoreConfig.LogPath = filepath.Join(t.TempDir(), "logs")
				cfg.FsstoreConfig.AppStatePath = t.TempDir()
				configureDebugDirectoryModeContract(&cfg, contract, logconfig.FileSystemStoreConfig{
					LogDir: tc.baseDir, LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
				}, tc.override, tc.normal)
				mappingErr := mapLogDirectoryTestConfig(cfg, contract)
				_, claimsErr := cfg.collectStorageDirectoryClaims()
				if tc.wantError {
					require.ErrorIs(t, mappingErr, pkgfsstore.ErrEmptyLogDirectory)
					require.ErrorIs(t, claimsErr, pkgfsstore.ErrEmptyLogDirectory)
					return
				}
				require.NoError(t, mappingErr)
				require.NoError(t, claimsErr)
				layout, internal := debugDirectoryModeMappedLayouts(cfg, contract)
				if internal != nil {
					layout = *internal
				}
				require.Equal(t, tc.wantDir, layout.LogDir)
			})
		}
	}
}

func TestLogDirectoryEnvironmentDefaultsAndWhitespace(t *testing.T) {
	for _, contract := range []struct {
		kind               pkglogger.LoggerContractKind
		prefix, defaultDir string
	}{
		{kind: pkglogger.LoggerContractSync, defaultDir: logconfig.LogDirCaseFile},
		{kind: pkglogger.LoggerContractAsyncBestEffort, prefix: "ASYNC_", defaultDir: logconfig.AsyncLogDirCaseFile},
		{kind: pkglogger.LoggerContractDelivery, prefix: "DELIVERY_", defaultDir: logconfig.DeliveryLogDirCaseFile},
	} {
		for _, route := range []string{"normal", "internal", "unused"} {
			for _, tc := range []struct {
				name, value string
				set, reject bool
			}{
				{name: "missing"},
				{name: "empty", set: true},
				{name: "whitespace", value: " \t ", set: true, reject: true},
				{name: "target root", value: ".", set: true},
			} {
				t.Run(contract.kind.String()+"/"+route+"/"+tc.name, func(t *testing.T) {
					lines := []string{
						"HETEROLOG_ENABLE_LOGGING=true", "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true", "HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
						"HETEROLOG_LOG_TYPE=stdout", "HETEROLOG_ASYNC_LOG_TYPE=stdout", "HETEROLOG_DELIVERY_LOG_TYPE=stdout",
						"HETEROLOG_" + contract.prefix + "LOG_FILE_LIFECYCLE_MODE=disable",
						"HETEROLOG_ENABLE_" + contract.prefix + "LOG_FILE_SEPARATE_ERROR_LOGS=false",
						"HETEROLOG_ENABLE_" + contract.prefix + "LOG_FILE_SEPARATE_DEBUG_LOGS=false",
					}
					key := "HETEROLOG_" + contract.prefix + "LOG_DIR"
					want := contract.defaultDir
					if route == "internal" {
						lines = append(lines, "HETEROLOG_"+contract.prefix+"STDOUT_LOGGER_ERROR_HANDLER=file", key+"=base-logs")
						key = "HETEROLOG_" + contract.prefix + "LOG_INTERNAL_LOG_FILE_LOG_DIR"
						want = "base-logs"
					} else if route == "normal" {
						lines = append(lines, "HETEROLOG_"+contract.prefix+"LOG_TYPE=file")
					}
					if tc.set {
						lines = append(lines, key+"='"+tc.value+"'")
						if route == "unused" {
							lines = append(lines, "HETEROLOG_"+contract.prefix+"LOG_INTERNAL_LOG_FILE_LOG_DIR='"+tc.value+"'")
						}
						if tc.value != "" {
							want = tc.value
						}
					}
					cfg := loadConfigFromTempEnv(t, lines)
					err := mapLogDirectoryTestConfig(cfg, contract.kind)
					_, claimsErr := cfg.collectStorageDirectoryClaims()
					if tc.reject && route != "unused" {
						require.ErrorIs(t, err, pkgfsstore.ErrEmptyLogDirectory)
						require.ErrorIs(t, claimsErr, pkgfsstore.ErrEmptyLogDirectory)
						return
					}
					require.NoError(t, err)
					require.NoError(t, claimsErr)
					if route == "unused" {
						return
					}
					layout, internal := debugDirectoryModeMappedLayouts(cfg, contract.kind)
					if internal != nil {
						layout = *internal
					}
					require.Equal(t, want, layout.LogDir)
				})
			}
		}
	}
}

func mapLogDirectoryTestConfig(cfg AppConfig, contract pkglogger.LoggerContractKind) error {
	switch contract {
	case pkglogger.LoggerContractSync:
		_, err := MapLoggerConfig(cfg)
		return err
	case pkglogger.LoggerContractAsyncBestEffort:
		_, err := MapAsyncLoggerConfig(cfg)
		return err
	default:
		_, err := MapDeliveryLoggerConfig(cfg)
		return err
	}
}
