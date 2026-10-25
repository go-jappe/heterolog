package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

func TestLogFileLifecyclesManageConfiguredDebugDirectory(t *testing.T) {
	for _, tt := range []struct {
		name               string
		errorDir           string
		debugDir           string
		separated          bool
		directorySeparated bool
		wantDebugDir       string
		wantDirectoryCount int
	}{
		{name: "directory separation disabled", separated: true, wantDebugDir: "logs", wantDirectoryCount: 2},
		{name: "disabled ignores stale directory", debugDir: "old_debug_logs", separated: true, wantDebugDir: "logs", wantDirectoryCount: 2},
		{name: "independent", debugDir: "debug_logs", separated: true, directorySeparated: true, wantDebugDir: "debug_logs", wantDirectoryCount: 3},
		{name: "all shared", errorDir: "logs", debugDir: "logs", separated: true, directorySeparated: true, wantDebugDir: "logs", wantDirectoryCount: 1},
		{name: "default and error shared", errorDir: "logs", debugDir: "debug_logs", separated: true, directorySeparated: true, wantDebugDir: "debug_logs", wantDirectoryCount: 2},
		{name: "default and debug shared", debugDir: "./logs", separated: true, directorySeparated: true, wantDebugDir: "logs", wantDirectoryCount: 2},
		{name: "file separation disabled", debugDir: "debug_logs", directorySeparated: true, wantDirectoryCount: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
			cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}
			cfg.FsstoreConfig.Layout.ErrorLogDir = "error_logs"
			if tt.errorDir != "" {
				cfg.FsstoreConfig.Layout.ErrorLogDir = tt.errorDir
			}
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
			cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
			cfg.FsstoreConfig.Layout.DebugLogDir = tt.debugDir
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = tt.directorySeparated
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = tt.separated
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug_"
			cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
			cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
			cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 10
			cfg.RunnerConfig.Interval = time.Minute
			lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
			require.NoError(t, err)
			require.Len(t, lifecycles, tt.wantDirectoryCount)
			wantDirectories := map[string]string{"": "logs", "error_": cfg.FsstoreConfig.Layout.ErrorLogDir}
			if tt.separated {
				wantDirectories["debug_"] = tt.wantDebugDir
			} else {
				require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, tt.debugDir))
			}
			managers := fileSeriesManagersForLifecycles(lifecycles)
			require.Len(t, managers, len(wantDirectories))
			var inactivePaths, activePaths []string
			for _, manager := range managers {
				pattern := manager.FileNamePattern()
				wantDir, exists := wantDirectories[pattern.Prefix]
				require.True(t, exists)
				require.Equal(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, wantDir), pattern.Dir)
				inactive := filepath.Join(pattern.Dir, pattern.Prefix+pattern.Target+"_"+currentLogDate()+"_1.log")
				active := filepath.Join(pattern.Dir, pattern.Prefix+pattern.Target+"_"+currentLogDate()+"_2.log")
				require.NoError(t, os.WriteFile(inactive, []byte("inactive log"), 0o600))
				require.NoError(t, os.WriteFile(active, []byte("active log"), 0o600))
				inactivePaths = append(inactivePaths, inactive)
				activePaths = append(activePaths, active)
			}
			for _, lifecycle := range lifecycles {
				report, err := lifecycle.RunOnce(context.Background())
				require.NoError(t, err)
				require.Len(t, report.Compressed, len(lifecycle.fileSeriesManagers))
			}
			for _, path := range inactivePaths {
				require.NoFileExists(t, path)
				require.FileExists(t, path+pkgfsstore.ExtensionCompressGzip)
			}
			for _, path := range activePaths {
				require.FileExists(t, path)
			}
			if !tt.directorySeparated && tt.debugDir != "" {
				require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, tt.debugDir))
			}
		})
	}
}

func TestLifecycleFactoriesValidateErrorDebugDirectorySharingBeforeSideEffects(t *testing.T) {
	for _, tt := range []struct {
		name         string
		debugDir     string
		disableError bool
		disableDebug bool
		wantError    bool
	}{
		{name: "shared error and debug", debugDir: "shared", wantError: true},
		{name: "normalized shared error and debug", debugDir: "unused/../shared", wantError: true},
		{name: "error disabled", debugDir: "shared", disableError: true},
		{name: "debug disabled", debugDir: "shared", disableDebug: true},
		{name: "debug directory separation disabled"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, factory := range []string{
				"log lifecycles",
				"delivery lifecycles",
			} {
				t.Run(factory, func(t *testing.T) {
					cfg := configuredCheckpointFactoryTestConfig(t)
					cfg.RunnerConfig.Interval = time.Minute
					cfg.FsstoreConfig.Layout.ErrorLogDir = "shared"
					cfg.FsstoreConfig.Layout.DebugLogDir = tt.debugDir
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = tt.debugDir != ""
					cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = !tt.disableError
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = !tt.disableDebug
					cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
					cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug_"
					deliveryCfg := pkgfsstore.DeliveryFileLifecycleRunnerConfig{
						FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
							Layout: cfg.FsstoreConfig.Layout,
							Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
								UsageCacheTTL:  time.Hour,
								RotationType:   pkgfsstore.FileRotationDaily,
								MaxFileBackups: cfg.FsstoreConfig.Lifecycle.MaxFileBackups,
								MaxFileAge:     cfg.FsstoreConfig.Lifecycle.MaxFileAge,
								MaxFileSize:    cfg.FsstoreConfig.Lifecycle.MaxFileSize,
								MaxDirSize:     cfg.FsstoreConfig.Lifecycle.MaxDirSize,
							},
						},
						ObjectStorageUploadConfig: cfg.ObjectStorageUploadConfig,
						ArchiveCheckpointConfig:   cfg.ArchiveCheckpointConfig,
						RunnerConfig:              cfg.RunnerConfig,
					}
					var err error
					switch factory {
					case "log lifecycles":
						_, err = NewLogFileLifecycles(LogFileOptions{Config: cfg})
					case "delivery lifecycles":
						_, err = NewDeliveryFileLifecycles(DeliveryFileOptions{Config: deliveryCfg})
					}
					if tt.wantError {
						var conflict *RoleDirectoryConflictError
						require.ErrorAs(t, err, &conflict)
						require.ErrorIs(t, err, ErrRoleDirectoryConflict)
						require.Equal(t, [2]string{"error", "debug"}, conflict.SharedRoles)
						require.Equal(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, "shared"), conflict.SharedDirectory)
						require.Equal(t, "default", conflict.OtherRole)
						require.Equal(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, "logs"), conflict.OtherDirectory)
						entries, readErr := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
						require.NoError(t, readErr)
						require.Empty(t, entries, "rejected layout must not create log or checkpoint directories")
					} else {
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestDebugArchiveCheckpointIdentityUsesIndependentDirectory(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug_"
	cfg.FsstoreConfig.Layout.DebugLogDir = "debug_logs"
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
	cfg.ObjectStorageUploadConfig.Debug = cfg.ObjectStorageUploadConfig.Default
	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, lifecycles, 2)
	for _, manager := range fileSeriesManagersForLifecycles(lifecycles) {
		if manager.FileNamePattern().Prefix != "debug_" {
			continue
		}
		pattern := logpattern.FileNamePattern{
			Dir:       filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, "debug_logs"),
			Prefix:    "debug_",
			Target:    cfg.FsstoreConfig.Layout.Target,
			Extension: "log",
		}
		require.Equal(t, pattern, manager.FileNamePattern())
		require.Equal(t, archiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, string(fileClassDebug), pattern), checkpointStoreForFactoryTest(t, manager).SourceID())
		pattern.Dir = filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, "logs")
		require.NotEqual(t, archiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, string(fileClassDebug), pattern), checkpointStoreForFactoryTest(t, manager).SourceID())
		return
	}
	t.Fatal("debug file series manager was not created")
}
