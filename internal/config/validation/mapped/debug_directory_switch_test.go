package mappedvalidation

import (
	"fmt"
	"os"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestMappedDebugDirectorySwitchValidation(t *testing.T) {
	for _, source := range []string{"logger", "async logger", "delivery logger"} {
		for _, internal := range []bool{false, true} {
			for _, mode := range []pkgfsstore.FileLifecycleMode{
				pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable,
			} {
				if source == "delivery logger" && mode == pkgfsstore.FileLifecycleModeInline {
					continue
				}
				for _, splitFiles := range []bool{false, true} {
					for _, tc := range []struct {
						name    string
						enabled bool
						path    string
						wantErr bool
					}{
						{name: "enabled empty", enabled: true, wantErr: true},
						{name: "enabled whitespace", enabled: true, path: " \t\n", wantErr: true},
						{name: "enabled valid", enabled: true, path: "debug"},
						{name: "disabled empty"},
						{name: "disabled stale whitespace", path: " \t\n"},
					} {
						name := fmt.Sprintf("%s/internal=%t/%s/files=%t/%s", source, internal, mode, splitFiles, tc.name)
						t.Run(name, func(t *testing.T) {
							cfg := mappedRolePurgeFileConfig()
							cfg.LifecycleMode = mode
							cfg.LifecycleRunnerSettings.Interval = time.Minute
							cfg.FsstoreConfig.Layout.LogPath = t.TempDir()
							cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = tc.enabled
							cfg.FsstoreConfig.Layout.DebugLogDir = tc.path
							cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = splitFiles
							err := validateMappedRolePurgeDestination(source, internal, cfg)
							if tc.wantErr {
								label := source + " file"
								if internal {
									label = source + " internal log file"
								}
								requireConfigError(t, err, label, ConfigSectionDirectory, pkgfsstore.ErrEmptyDebugLogDirectory)
							} else {
								require.NoError(t, err)
							}
							entries, err := os.ReadDir(cfg.FsstoreConfig.Layout.LogPath)
							require.NoError(t, err)
							require.Empty(t, entries)
						})
					}
				}
			}
		}
	}
}

func TestMappedDebugDirectorySwitchIgnoresUnusedBaseBackend(t *testing.T) {
	fileCfg := mappedRolePurgeFileConfig()
	fileCfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
	fileCfg.FsstoreConfig.Layout.DebugLogDir = ""
	require.NoError(t, ValidateSyncLoggerConfig("logger", pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Stdout:                &pkglogger.StdoutLoggerConfig{}, File: fileCfg,
	}))
	require.NoError(t, ValidateDeliveryLoggerConfig("delivery logger", pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout},
		Stdout:                &pkglogger.DeliveryStdoutLoggerConfig{}, File: mappedRolePurgeDeliveryConfig(fileCfg),
	}))
}
