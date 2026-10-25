package lifecycle

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

func TestLifecycleFactoriesRejectEmptyExplicitDebugDirectoryBeforeCreatingFiles(t *testing.T) {
	for _, splitFiles := range []bool{false, true} {
		for _, path := range []string{"", " \t\n"} {
			for _, factory := range []string{
				"log lifecycles",
				"delivery lifecycles",
			} {
				t.Run(fmt.Sprintf("%s/files=%t/path=%q", factory, splitFiles, path), func(t *testing.T) {
					cfg := configuredCheckpointFactoryTestConfig(t)
					cfg.RunnerConfig.Interval = time.Minute
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
					cfg.FsstoreConfig.Layout.DebugLogDir = path
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = splitFiles
					deliveryCfg := pkgfsstore.DeliveryFileLifecycleRunnerConfig{
						FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
							Layout:    cfg.FsstoreConfig.Layout,
							Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{UsageCacheTTL: time.Hour},
						},
						RunnerConfig: cfg.RunnerConfig, ArchiveCheckpointConfig: cfg.ArchiveCheckpointConfig,
						ObjectStorageUploadConfig: cfg.ObjectStorageUploadConfig,
					}
					var err error
					switch factory {
					case "log lifecycles":
						_, err = NewLogFileLifecycles(LogFileOptions{Config: cfg})
					case "delivery lifecycles":
						_, err = NewDeliveryFileLifecycles(DeliveryFileOptions{Config: deliveryCfg})
					}
					require.ErrorIs(t, err, pkgfsstore.ErrEmptyDebugLogDirectory)
					entries, err := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
					require.NoError(t, err)
					require.Empty(t, entries, "invalid configuration must not create log or checkpoint files")
				})
			}
		}
	}
}

func TestLifecycleFactoriesAllowSharedDebugFilesInTargetRoot(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		t.Run(fmt.Sprintf("delivery=%t", delivery), func(t *testing.T) {
			cfg := configuredCheckpointFactoryTestConfig(t)
			cfg.FsstoreConfig.Layout.LogDir = "."
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = false
			cfg.FsstoreConfig.Layout.DebugLogDir = "stale-debug-directory"
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "debug_"
			cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
			cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}
			var lifecycles []*DirectoryLifecycle
			var err error
			if delivery {
				lifecycles, err = NewDeliveryFileLifecycles(DeliveryFileOptions{
					Config: pkgfsstore.DeliveryFileLifecycleRunnerConfig{
						FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
							Layout: cfg.FsstoreConfig.Layout,
							Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
								UsageCacheTTL:  time.Hour,
								MaxFileBackups: cfg.FsstoreConfig.Lifecycle.MaxFileBackups,
								MaxFileAge:     cfg.FsstoreConfig.Lifecycle.MaxFileAge,
								MaxFileSize:    cfg.FsstoreConfig.Lifecycle.MaxFileSize,
								MaxDirSize:     cfg.FsstoreConfig.Lifecycle.MaxDirSize,
							},
						},
					},
				})
			} else {
				lifecycles, err = NewLogFileLifecycles(LogFileOptions{Config: cfg})
			}
			require.NoError(t, err)
			require.Len(t, lifecycles, 1)
			managers := fileSeriesManagersForLifecycles(lifecycles)
			require.Len(t, managers, 2)
			targetDir := filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target)
			for _, manager := range managers {
				require.Equal(t, targetDir, manager.FileNamePattern().Dir)
			}
			require.NoDirExists(t, filepath.Join(targetDir, cfg.FsstoreConfig.Layout.DebugLogDir))
		})
	}
}
