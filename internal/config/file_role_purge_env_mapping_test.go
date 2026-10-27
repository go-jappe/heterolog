package config

import (
	"strconv"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

type fileRolePurgeLoaderContract struct {
	name   string
	prefix string
	load   func() (fsstore.FilePurgeConfig, *fsstore.FilePurgeConfig, *fsstore.FilePurgeConfig, error)
}

func fileRolePurgeLoaderContracts() []fileRolePurgeLoaderContract {
	return []fileRolePurgeLoaderContract{
		{
			name: "sync",
			load: func() (fsstore.FilePurgeConfig, *fsstore.FilePurgeConfig, *fsstore.FilePurgeConfig, error) {
				cfg, err := loadFileConfig(processEnvironmentForTest())
				store := cfg.FileSystemStoreConfig
				return fsstore.FilePurgeConfig{MaxFileBackups: store.MaxFileBackups, MaxFileAge: store.MaxFileAge, MaxDirSize: store.MaxDirSize}, store.ErrorPurge, store.DebugPurge, err
			},
		},
		{
			name: "async", prefix: "ASYNC_",
			load: func() (fsstore.FilePurgeConfig, *fsstore.FilePurgeConfig, *fsstore.FilePurgeConfig, error) {
				cfg, err := loadAsyncFileConfig(processEnvironmentForTest())
				store := cfg.FileSystemStoreConfig
				return fsstore.FilePurgeConfig{MaxFileBackups: store.MaxFileBackups, MaxFileAge: store.MaxFileAge, MaxDirSize: store.MaxDirSize}, store.ErrorPurge, store.DebugPurge, err
			},
		},
		{
			name: "delivery", prefix: "DELIVERY_",
			load: func() (fsstore.FilePurgeConfig, *fsstore.FilePurgeConfig, *fsstore.FilePurgeConfig, error) {
				cfg, err := loadDeliveryFileConfig(processEnvironmentForTest())
				store := cfg.FileSystemStoreConfig
				return fsstore.FilePurgeConfig{MaxFileBackups: store.MaxFileBackups, MaxFileAge: store.MaxFileAge, MaxDirSize: store.MaxDirSize}, store.ErrorPurge, store.DebugPurge, err
			},
		},
	}
}

func TestLoadFileRolePurgeInheritsCommonSettingsPerField(t *testing.T) {
	common := fsstore.FilePurgeConfig{MaxFileBackups: 71, MaxFileAge: 19, MaxDirSize: 13 * BytesPerMegabyte}
	tests := []struct {
		name      string
		values    map[string]string
		wantError fsstore.FilePurgeConfig
		wantDebug fsstore.FilePurgeConfig
	}{
		{name: "missing", wantError: common, wantDebug: common},
		{
			name: "empty",
			values: map[string]string{
				"ERROR_LOG_FILE_BACKUPS": "", "ERROR_LOG_FILE_AGE": "", "ERROR_LOG_FILE_DIR_SIZE_MB": "",
				"DEBUG_LOG_FILE_BACKUPS": "", "DEBUG_LOG_FILE_AGE": "", "DEBUG_LOG_FILE_DIR_SIZE_MB": "",
			},
			wantError: common, wantDebug: common,
		},
		{
			name: "partial overrides",
			values: map[string]string{
				"ERROR_LOG_FILE_BACKUPS": "3", "DEBUG_LOG_FILE_AGE": "2", "DEBUG_LOG_FILE_DIR_SIZE_MB": "7",
			},
			wantError: fsstore.FilePurgeConfig{MaxFileBackups: 3, MaxFileAge: 19, MaxDirSize: 13 * BytesPerMegabyte},
			wantDebug: fsstore.FilePurgeConfig{MaxFileBackups: 71, MaxFileAge: 2, MaxDirSize: 7 * BytesPerMegabyte},
		},
		{
			name: "explicit zero does not inherit",
			values: map[string]string{
				"ERROR_LOG_FILE_BACKUPS": "0", "ERROR_LOG_FILE_AGE": "0", "ERROR_LOG_FILE_DIR_SIZE_MB": "0",
				"DEBUG_LOG_FILE_BACKUPS": "0", "DEBUG_LOG_FILE_AGE": "0", "DEBUG_LOG_FILE_DIR_SIZE_MB": "0",
			},
		},
		{
			name: "negative retention does not inherit",
			values: map[string]string{
				"ERROR_LOG_FILE_BACKUPS": "-1", "ERROR_LOG_FILE_AGE": "-2",
				"DEBUG_LOG_FILE_BACKUPS": "-3", "DEBUG_LOG_FILE_AGE": "-4",
			},
			wantError: fsstore.FilePurgeConfig{MaxFileBackups: -1, MaxFileAge: -2, MaxDirSize: 13 * BytesPerMegabyte},
			wantDebug: fsstore.FilePurgeConfig{MaxFileBackups: -3, MaxFileAge: -4, MaxDirSize: 13 * BytesPerMegabyte},
		},
	}
	for _, contract := range fileRolePurgeLoaderContracts() {
		for _, tt := range tests {
			t.Run(contract.name+"/"+tt.name, func(t *testing.T) {
				restoreProcessEnvAfterTest(t)
				clearProcessEnvForTest(t)
				t.Setenv("HETEROLOG_MAX_"+contract.prefix+"LOG_FILE_BACKUPS", "71")
				t.Setenv("HETEROLOG_MAX_"+contract.prefix+"LOG_FILE_AGE", "19")
				t.Setenv("HETEROLOG_MAX_"+contract.prefix+"LOG_FILE_DIR_SIZE_MB", "13")
				for suffix, value := range tt.values {
					t.Setenv("HETEROLOG_MAX_"+contract.prefix+suffix, value)
				}
				gotCommon, errorPurge, debugPurge, err := contract.load()
				if err != nil {
					t.Fatalf("load file config: %v", err)
				}
				if gotCommon != common {
					t.Fatalf("common purge = %+v, want %+v", gotCommon, common)
				}
				assertFileRolePurge(t, "error", errorPurge, tt.wantError)
				assertFileRolePurge(t, "debug", debugPurge, tt.wantDebug)
			})
		}
		t.Run(contract.name+"/defaults", func(t *testing.T) {
			restoreProcessEnvAfterTest(t)
			clearProcessEnvForTest(t)
			gotCommon, errorPurge, debugPurge, err := contract.load()
			if err != nil {
				t.Fatalf("load file config: %v", err)
			}
			assertFileRolePurge(t, "error", errorPurge, gotCommon)
			assertFileRolePurge(t, "debug", debugPurge, gotCommon)
		})
	}
}

func TestLoadFileRolePurgeRetainsNumericErrors(t *testing.T) {
	for _, contract := range fileRolePurgeLoaderContracts() {
		for _, role := range []string{"ERROR", "DEBUG"} {
			for _, suffix := range []string{"BACKUPS", "AGE", "DIR_SIZE_MB"} {
				key := "HETEROLOG_MAX_" + contract.prefix + role + "_LOG_FILE_" + suffix
				values := []string{"invalid", "18446744073709551616"}
				if suffix == "DIR_SIZE_MB" {
					values = append(values, "-1", "18446744073709551615")
				}
				for _, value := range values {
					t.Run(key+"/"+value, func(t *testing.T) {
						restoreProcessEnvAfterTest(t)
						clearProcessEnvForTest(t)
						t.Setenv(key, value)
						_, _, _, err := contract.load()
						requireEnvironmentKeyError(t, err, key, nil)
					})
				}
			}
		}
	}
}

func TestMapFileRolePurgePreservesContractsAndInternalOverrides(t *testing.T) {
	lines := []string{
		"HETEROLOG_ENABLE_LOGGING=true", "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true", "HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=file,stdout", "HETEROLOG_ASYNC_LOG_TYPE=file,stdout", "HETEROLOG_DELIVERY_LOG_TYPE=file,stdout",
	}
	for i, prefix := range []string{"", "ASYNC_", "DELIVERY_"} {
		// ここではerrorとdebugの両roleがmain directoryを共有します。下のinternal logの上書きで
		// 再び別directoryに分かれるため、mappingは各roleに設定した容量を保持しなければなりません。
		for _, dir := range []string{"LOG_DIR", "ERROR_LOG_DIR", "DEBUG_LOG_DIR"} {
			lines = append(lines, "HETEROLOG_"+prefix+dir+"="+prefix+"logs")
		}
		lines = append(lines,
			"HETEROLOG_ENABLE_"+prefix+"LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true",
			"HETEROLOG_MAX_"+prefix+"LOG_SIZE_MB=10",
			"HETEROLOG_"+prefix+"STDOUT_LOGGER_ERROR_HANDLER=file",
			"HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_FILE_LOG_DIR="+prefix+"internal",
			"HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_FILE_ERROR_LOG_DIR="+prefix+"internal-error",
			"HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR="+prefix+"internal-debug",
			"HETEROLOG_MAX_"+prefix+"LOG_FILE_BACKUPS="+strconv.Itoa(11+i),
			"HETEROLOG_MAX_"+prefix+"LOG_FILE_AGE="+strconv.Itoa(21+i),
			"HETEROLOG_MAX_"+prefix+"LOG_FILE_DIR_SIZE_MB="+strconv.Itoa(31+i),
			"HETEROLOG_MAX_"+prefix+"ERROR_LOG_FILE_BACKUPS="+strconv.Itoa(41+i),
			"HETEROLOG_MAX_"+prefix+"ERROR_LOG_FILE_AGE="+strconv.Itoa(51+i),
			"HETEROLOG_MAX_"+prefix+"ERROR_LOG_FILE_DIR_SIZE_MB="+strconv.Itoa(61+i),
			"HETEROLOG_MAX_"+prefix+"DEBUG_LOG_FILE_BACKUPS="+strconv.Itoa(71+i),
			"HETEROLOG_MAX_"+prefix+"DEBUG_LOG_FILE_AGE="+strconv.Itoa(81+i),
			"HETEROLOG_MAX_"+prefix+"DEBUG_LOG_FILE_DIR_SIZE_MB="+strconv.Itoa(91+i),
		)
	}
	cfg := loadConfigFromTempEnv(t, lines)
	for _, state := range []string{"configured", "nil"} {
		t.Run(state, func(t *testing.T) {
			loaded := cfg
			if state != "configured" {
				var purge *fsstore.FilePurgeConfig
				loaded.Logger.FileSystemStoreConfig.ErrorPurge = purge
				loaded.Logger.FileSystemStoreConfig.DebugPurge = purge
				loaded.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.ErrorPurge = purge
				loaded.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.DebugPurge = purge
				loaded.DeliveryLogger.FileSystemStoreConfig.ErrorPurge = purge
				loaded.DeliveryLogger.FileSystemStoreConfig.DebugPurge = purge
			}
			syncCfg, err := MapLoggerConfig(loaded)
			if err != nil {
				t.Fatalf("MapLoggerConfig: %v", err)
			}
			asyncCfg, err := MapAsyncLoggerConfig(loaded)
			if err != nil {
				t.Fatalf("MapAsyncLoggerConfig: %v", err)
			}
			deliveryCfg, err := MapDeliveryLoggerConfig(loaded)
			if err != nil {
				t.Fatalf("MapDeliveryLoggerConfig: %v", err)
			}
			if syncCfg.InternalLogDestinations == nil || syncCfg.InternalLogDestinations.File == nil ||
				asyncCfg.InternalLogDestinations == nil || asyncCfg.InternalLogDestinations.File == nil ||
				deliveryCfg.InternalLogDestinations == nil || deliveryCfg.InternalLogDestinations.File == nil {
				t.Fatal("mapping did not create all three internal log file destinations")
			}
			for i, contract := range []struct {
				name                         string
				loadedError, loadedDebug     *fsstore.FilePurgeConfig
				mappedError, mappedDebug     *fsstore.FilePurgeConfig
				internalError, internalDebug *fsstore.FilePurgeConfig
			}{
				{
					"sync", loaded.Logger.FileSystemStoreConfig.ErrorPurge, loaded.Logger.FileSystemStoreConfig.DebugPurge,
					syncCfg.File.FsstoreConfig.Lifecycle.ErrorPurge, syncCfg.File.FsstoreConfig.Lifecycle.DebugPurge,
					syncCfg.InternalLogDestinations.File.FsstoreConfig.Lifecycle.ErrorPurge, syncCfg.InternalLogDestinations.File.FsstoreConfig.Lifecycle.DebugPurge,
				},
				{
					"async", loaded.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.ErrorPurge, loaded.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.DebugPurge,
					asyncCfg.File.FsstoreConfig.Lifecycle.ErrorPurge, asyncCfg.File.FsstoreConfig.Lifecycle.DebugPurge,
					asyncCfg.InternalLogDestinations.File.FsstoreConfig.Lifecycle.ErrorPurge, asyncCfg.InternalLogDestinations.File.FsstoreConfig.Lifecycle.DebugPurge,
				},
				{
					"delivery", loaded.DeliveryLogger.FileSystemStoreConfig.ErrorPurge, loaded.DeliveryLogger.FileSystemStoreConfig.DebugPurge,
					deliveryCfg.File.LifecycleRunnerSettings.ErrorPurge, deliveryCfg.File.LifecycleRunnerSettings.DebugPurge,
					deliveryCfg.InternalLogDestinations.File.LifecycleRunnerSettings.ErrorPurge, deliveryCfg.InternalLogDestinations.File.LifecycleRunnerSettings.DebugPurge,
				},
			} {
				if contract.mappedError != contract.loadedError || contract.internalError != contract.loadedError ||
					contract.mappedDebug != contract.loadedDebug || contract.internalDebug != contract.loadedDebug {
					t.Fatalf("%s mapping changed configured purge pointers", contract.name)
				}
				if state == "configured" {
					assertFileRolePurge(t, contract.name+" error", contract.mappedError, fsstore.FilePurgeConfig{
						MaxFileBackups: 41 + i, MaxFileAge: 51 + i, MaxDirSize: uint64(61+i) * BytesPerMegabyte,
					})
					assertFileRolePurge(t, contract.name+" debug", contract.mappedDebug, fsstore.FilePurgeConfig{
						MaxFileBackups: 71 + i, MaxFileAge: 81 + i, MaxDirSize: uint64(91+i) * BytesPerMegabyte,
					})
				}
			}
		})
	}
}

func TestFileRolePurgeMappingDoesNotReplaceExplicitZero(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true", "HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true", "HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=file", "HETEROLOG_ASYNC_LOG_TYPE=file", "HETEROLOG_DELIVERY_LOG_TYPE=file",
		"HETEROLOG_MAX_ERROR_LOG_FILE_BACKUPS=0", "HETEROLOG_MAX_ERROR_LOG_FILE_AGE=0", "HETEROLOG_MAX_ERROR_LOG_FILE_DIR_SIZE_MB=0",
		"HETEROLOG_MAX_DEBUG_LOG_FILE_BACKUPS=0", "HETEROLOG_MAX_DEBUG_LOG_FILE_AGE=0", "HETEROLOG_MAX_DEBUG_LOG_FILE_DIR_SIZE_MB=0",
		"HETEROLOG_MAX_ASYNC_ERROR_LOG_FILE_BACKUPS=0", "HETEROLOG_MAX_ASYNC_ERROR_LOG_FILE_AGE=0", "HETEROLOG_MAX_ASYNC_ERROR_LOG_FILE_DIR_SIZE_MB=0",
		"HETEROLOG_MAX_ASYNC_DEBUG_LOG_FILE_BACKUPS=0", "HETEROLOG_MAX_ASYNC_DEBUG_LOG_FILE_AGE=0", "HETEROLOG_MAX_ASYNC_DEBUG_LOG_FILE_DIR_SIZE_MB=0",
		"HETEROLOG_MAX_DELIVERY_ERROR_LOG_FILE_BACKUPS=0", "HETEROLOG_MAX_DELIVERY_ERROR_LOG_FILE_AGE=0", "HETEROLOG_MAX_DELIVERY_ERROR_LOG_FILE_DIR_SIZE_MB=0",
		"HETEROLOG_MAX_DELIVERY_DEBUG_LOG_FILE_BACKUPS=0", "HETEROLOG_MAX_DELIVERY_DEBUG_LOG_FILE_AGE=0", "HETEROLOG_MAX_DELIVERY_DEBUG_LOG_FILE_DIR_SIZE_MB=0",
	})
	// 各constructorは、値の意味を検証する前にmapします。共通の上限が正の値のままでも、
	// 明示した0は変更されずにその検証まで届かなければなりません。
	syncCfg := NewFileLoggerConfig(&cfg)
	asyncCfg := NewAsyncFileLoggerConfig(&cfg)
	deliveryCfg := NewDeliveryFileLoggerConfig(&cfg)
	for name, purge := range map[string]*fsstore.FilePurgeConfig{
		"sync error":     syncCfg.FsstoreConfig.Lifecycle.ErrorPurge,
		"sync debug":     syncCfg.FsstoreConfig.Lifecycle.DebugPurge,
		"async error":    asyncCfg.FsstoreConfig.Lifecycle.ErrorPurge,
		"async debug":    asyncCfg.FsstoreConfig.Lifecycle.DebugPurge,
		"delivery error": deliveryCfg.LifecycleRunnerSettings.ErrorPurge,
		"delivery debug": deliveryCfg.LifecycleRunnerSettings.DebugPurge,
	} {
		assertFileRolePurge(t, name, purge, fsstore.FilePurgeConfig{})
	}
}

func TestPublicEnvExampleDocumentsOptionalFileRolePurgeKeys(t *testing.T) {
	values := readPublicEnvExample(t)
	for _, prefix := range []string{"", "ASYNC_", "DELIVERY_"} {
		for _, role := range []string{"ERROR", "DEBUG"} {
			for _, suffix := range []string{"BACKUPS", "AGE", "DIR_SIZE_MB"} {
				key := "HETEROLOG_MAX_" + prefix + role + "_LOG_FILE_" + suffix
				if value, exists := values[key]; !exists || value != "" {
					t.Errorf(".env.example key %s = %q (exists: %t), want blank inheritance default", key, value, exists)
				}
			}
		}
	}
	for key := range values {
		if strings.Contains(key, "INTERNAL_LOG") && (strings.HasSuffix(key, "FILE_BACKUPS") ||
			strings.HasSuffix(key, "FILE_AGE") || strings.HasSuffix(key, "FILE_DIR_SIZE_MB")) {
			t.Errorf(".env.example declares independent internal log purge key %s", key)
		}
	}
}

func assertFileRolePurge(t *testing.T, name string, got *fsstore.FilePurgeConfig, want fsstore.FilePurgeConfig) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("%s purge = %+v, want %+v", name, got, want)
	}
}
