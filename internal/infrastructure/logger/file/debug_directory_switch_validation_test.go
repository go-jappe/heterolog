package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestFileConstructorsRejectEmptyExplicitDebugDirectoryBeforeCreatingFiles(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{
		pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable,
	} {
		for _, splitFiles := range []bool{false, true} {
			for _, path := range []string{"", " \t\n"} {
				for _, factory := range []string{"sync set", "sync default", "sync error", "sync debug", "delivery set", "delivery default", "delivery error", "delivery debug"} {
					if mode == pkgfsstore.FileLifecycleModeInline && factory[:4] != "sync" {
						continue
					}
					t.Run(fmt.Sprintf("%s/%s/files=%t/path=%q", factory, mode, splitFiles, path), func(t *testing.T) {
						root := t.TempDir()
						cfg := newTestFileLoggerConfig(root, "logs", "empty-debug-directory")
						cfg.LifecycleMode = mode
						cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
						cfg.FsstoreConfig.Layout.DebugLogDir = path
						cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = splitFiles
						deliveryCfg := newTestDeliveryFileLoggerConfig(root, "logs", "empty-debug-directory")
						deliveryCfg.LifecycleMode = mode
						deliveryCfg.FsstoreConfig.Layout = cfg.FsstoreConfig.Layout
						var err error
						switch factory {
						case "sync set":
							set, createErr := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
							err = createErr
							if set != nil {
								require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set))
							}
						case "sync default", "sync error", "sync debug":
							role := map[string]loggerType{"sync default": DefaultLogger, "sync error": ErrorLogger, "sync debug": DebugLogger}[factory]
							logger, createErr := newFileLoggerBase(context.Background(), NewConfigAdapter(cfg), role, InternalHandlers{}, false, nil, nil, nil)
							err = createErr
							if logger != nil {
								require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
							}
						case "delivery set":
							set, createErr := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
							err = createErr
							if set != nil {
								require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set))
							}
						default:
							constructor := NewDeliveryFileLogger
							if factory == "delivery error" {
								constructor = NewDeliveryErrorFileLogger
							}
							if factory == "delivery debug" {
								constructor = NewDeliveryDebugFileLogger
							}
							logger, createErr := constructor(context.Background(), NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
							err = createErr
							if logger != nil {
								require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
							}
						}
						require.ErrorIs(t, err, pkgfsstore.ErrEmptyDebugLogDirectory)
						entries, err := os.ReadDir(root)
						require.NoError(t, err)
						require.Empty(t, entries, "invalid configuration must not create log or checkpoint files")
					})
				}
			}
		}
	}
}

func TestFileDirectoryValidationUsesExplicitDirectoryFlagFromCustomProvider(t *testing.T) {
	for _, path := range []string{"", " \t"} {
		layout := &appendlog.FsstoreConfigAdapter{
			LogDirConfig: "logs", DebugLogDirConfig: path, IsSeparateDebugLogsEnabledConfig: true,
		}
		require.NoError(t, validateFileLoggerDirectoryLayout(layout))
		layout.IsSeparateDebugLogDirEnabledConfig = true
		require.ErrorIs(t, validateFileLoggerDirectoryLayout(layout), pkgfsstore.ErrEmptyDebugLogDirectory)
		layout.IsSeparateDebugLogsEnabledConfig = false
		require.ErrorIs(t, validateFileLoggerDirectoryLayout(layout), pkgfsstore.ErrEmptyDebugLogDirectory)
	}
}

func TestFileConstructorsAllowSharedDebugFilesInTargetRoot(t *testing.T) {
	for _, delivery := range []bool{false, true} {
		for _, role := range []loggerType{DefaultLogger, DebugLogger} {
			t.Run(fmt.Sprintf("delivery=%t/%s", delivery, role), func(t *testing.T) {
				root := resolvedTempDirForTest(t)
				cfg := newTestFileLoggerConfig(root, ".", "target-root")
				cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
				cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = false
				cfg.FsstoreConfig.Layout.DebugLogDir = "stale-debug-directory"
				var activePath string
				if delivery {
					deliveryCfg := newTestDeliveryFileLoggerConfig(root, ".", "target-root")
					deliveryCfg.LifecycleMode = cfg.LifecycleMode
					deliveryCfg.FsstoreConfig.Layout = cfg.FsstoreConfig.Layout
					constructor := NewDeliveryFileLogger
					if role == DebugLogger {
						constructor = NewDeliveryDebugFileLogger
					}
					logger, err := constructor(context.Background(), NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
					require.NoError(t, err)
					closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
					activePath = logger.Fsstore.ActiveFilePath
				} else {
					logger, err := newFileLoggerBase(context.Background(), NewConfigAdapter(cfg), role, InternalHandlers{}, false, nil, nil, nil)
					require.NoError(t, err)
					closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
					activePath = logger.Fsstore.ActiveFilePath
				}
				targetDir := filepath.Join(root, cfg.FsstoreConfig.Layout.Target)
				require.Equal(t, targetDir, filepath.Dir(activePath))
				require.NoDirExists(t, filepath.Join(targetDir, cfg.FsstoreConfig.Layout.DebugLogDir))
			})
		}
	}
}
