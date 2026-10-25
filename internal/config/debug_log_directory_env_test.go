package config

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/pkg/env"
)

func TestFileDebugDirectoryEnvironmentConfig(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)

	for _, contract := range debugDirectoryEnvLoaders() {
		t.Run(contract.name, func(t *testing.T) {
			for _, test := range []struct {
				name            string
				value           *string
				debugDir        string
				separateFiles   string
				wantSeparateDir bool
			}{
				{name: "missing ignores stale path", debugDir: "stale-debug", separateFiles: "true"},
				{name: "empty ignores stale path", value: stringPointer(""), debugDir: "stale-debug", separateFiles: "true"},
				{name: "false ignores stale path", value: stringPointer("false"), debugDir: "stale-debug", separateFiles: "true"},
				{name: "true preserves path", value: stringPointer("true"), debugDir: "debug", separateFiles: "true", wantSeparateDir: true},
				{name: "true keeps empty path for mapping validation", value: stringPointer("true"), separateFiles: "true", wantSeparateDir: true},
				{name: "true keeps whitespace for mapping validation", value: stringPointer("true"), debugDir: " \t ", separateFiles: "true", wantSeparateDir: true},
				{name: "directory switch is independent of file switch", value: stringPointer("true"), debugDir: "debug", separateFiles: "false", wantSeparateDir: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					if test.value != nil {
						t.Setenv(contract.switchKey, *test.value)
					}
					t.Setenv(contract.dirKey, test.debugDir)
					t.Setenv(contract.filesKey, test.separateFiles)
					cfg, err := contract.load()
					if err != nil {
						t.Fatal(err)
					}
					if cfg.IsSeparateDebugLogDirEnabled != test.wantSeparateDir {
						t.Fatalf("directory separation = %v, want %v", cfg.IsSeparateDebugLogDirEnabled, test.wantSeparateDir)
					}
					if cfg.DebugLogDir != test.debugDir {
						t.Fatalf("raw debug path = %q, want %q", cfg.DebugLogDir, test.debugDir)
					}
					if cfg.IsSeparateDebugLogsEnabled != (test.separateFiles == "true") {
						t.Fatalf("file separation = %v, want %s", cfg.IsSeparateDebugLogsEnabled, test.separateFiles)
					}
				})
			}
		})
	}
}

func TestFileDebugDirectoryEnvironmentErrorsAreAggregated(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	for _, contract := range debugDirectoryEnvLoaders() {
		t.Run(contract.name, func(t *testing.T) {
			t.Setenv(contract.switchKey, "perhaps")
			t.Setenv(contract.filesKey, "false")
			t.Setenv(contract.maxSizeKey, "many")
			_, err := contract.load()
			requireEnvironmentKeyError(t, err, contract.switchKey, env.ErrNotBoolean)
			requireEnvironmentKeyError(t, err, contract.maxSizeKey, env.ErrNotInteger)
		})
	}
}

func TestInternalLogDebugDirectoryEnvironmentOverride(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	for _, contract := range internalDebugDirectoryEnvLoaders() {
		t.Run(contract.name, func(t *testing.T) {
			for _, test := range []struct {
				name      string
				value     *string
				want      *bool
				wantError bool
			}{
				{name: "missing inherits"},
				{name: "empty inherits", value: stringPointer("")},
				{name: "false overrides", value: stringPointer("false"), want: boolPointer(false)},
				{name: "true overrides", value: stringPointer("true"), want: boolPointer(true)},
				{name: "standard true alias", value: stringPointer("on"), want: boolPointer(true)},
				{name: "standard false alias", value: stringPointer("off"), want: boolPointer(false)},
				{name: "invalid value", value: stringPointer("perhaps"), wantError: true},
				{name: "whitespace is invalid", value: stringPointer(" "), wantError: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					if test.value != nil {
						t.Setenv(contract.switchKey, *test.value)
					}
					t.Setenv(contract.dirKey, "raw-debug")
					cfg, err := contract.load(processEnvironmentForTest(), false)
					if test.wantError {
						requireEnvironmentKeyError(t, err, contract.switchKey, env.ErrNotBoolean)
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					got := cfg.FileSeparateDebugLogDirEnabled
					if (got == nil) != (test.want == nil) || got != nil && *got != *test.want {
						t.Fatalf("directory override = %v, want %v", got, test.want)
					}
					if cfg.FileDebugLogDir != "raw-debug" {
						t.Fatalf("raw debug path = %q, want raw-debug", cfg.FileDebugLogDir)
					}
				})
			}
		})
	}
}

func TestInternalLogDebugDirectoryEnvironmentErrorsAreAggregated(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	for _, contract := range internalDebugDirectoryEnvLoaders() {
		t.Run(contract.name, func(t *testing.T) {
			t.Setenv(contract.switchKey, "perhaps")
			t.Setenv(contract.labelsKey, "not-json")
			_, err := contract.load(processEnvironmentForTest(), true)
			requireEnvironmentKeyError(t, err, contract.switchKey, env.ErrNotBoolean)
			requireEnvironmentKeyError(t, err, contract.labelsKey, env.ErrInvalidJSON)
		})
	}
}

func debugDirectoryEnvLoaders() []struct {
	name, switchKey, dirKey, filesKey, maxSizeKey string
	load                                          func() (logconfig.FileSystemStoreConfig, error)
} {
	return []struct {
		name, switchKey, dirKey, filesKey, maxSizeKey string
		load                                          func() (logconfig.FileSystemStoreConfig, error)
	}{
		{"sync", "HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", "HETEROLOG_DEBUG_LOG_DIR", "HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS", "HETEROLOG_MAX_LOG_SIZE_MB", func() (logconfig.FileSystemStoreConfig, error) {
			cfg, err := loadFileConfig(processEnvironmentForTest())
			return cfg.FileSystemStoreConfig, err
		}},
		{"async", "HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", "HETEROLOG_ASYNC_DEBUG_LOG_DIR", "HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOGS", "HETEROLOG_MAX_ASYNC_LOG_SIZE_MB", func() (logconfig.FileSystemStoreConfig, error) {
			cfg, err := loadAsyncFileConfig(processEnvironmentForTest())
			return cfg.FileSystemStoreConfig, err
		}},
		{"delivery", "HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", "HETEROLOG_DELIVERY_DEBUG_LOG_DIR", "HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOGS", "HETEROLOG_MAX_DELIVERY_LOG_SIZE_MB", func() (logconfig.FileSystemStoreConfig, error) {
			cfg, err := loadDeliveryFileConfig(processEnvironmentForTest())
			return logconfig.FileSystemStoreConfig{
				DebugLogDir:                  cfg.FileSystemStoreConfig.DebugLogDir,
				IsSeparateDebugLogDirEnabled: cfg.FileSystemStoreConfig.IsSeparateDebugLogDirEnabled,
				IsSeparateDebugLogsEnabled:   cfg.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
			}, err
		}},
	}
}

func internalDebugDirectoryEnvLoaders() []struct {
	name, switchKey, dirKey, labelsKey string
	load                               func(env.Lookup, bool) (logconfig.InternalLogDestinationConfig, error)
} {
	return []struct {
		name, switchKey, dirKey, labelsKey string
		load                               func(env.Lookup, bool) (logconfig.InternalLogDestinationConfig, error)
	}{
		{"sync", "HETEROLOG_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", "HETEROLOG_LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR", "HETEROLOG_LOG_INTERNAL_LOG_LOKI_LABELS", loadLoggerInternalLogDestinationConfig},
		{"async", "HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", "HETEROLOG_ASYNC_LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR", "HETEROLOG_ASYNC_LOG_INTERNAL_LOG_LOKI_LABELS", loadAsyncLoggerInternalLogDestinationConfig},
		{"delivery", "HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR", "HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR", "HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_LABELS", loadDeliveryLoggerInternalLogDestinationConfig},
	}
}

func stringPointer(value string) *string { return &value }
func boolPointer(value bool) *bool       { return &value }
