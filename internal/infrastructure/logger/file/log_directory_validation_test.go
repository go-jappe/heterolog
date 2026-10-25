package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFileConstructorsRejectEmptyLogDirectoryBeforeInitialization(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{
		pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable,
	} {
		for _, logDir := range []string{"", " \t\r\n"} {
			for _, factory := range []string{
				"sync set", "sync default", "sync error", "sync debug",
				"delivery set", "delivery default", "delivery debug",
			} {
				if mode == pkgfsstore.FileLifecycleModeInline && factory[:4] != "sync" {
					continue // Delivery writer does not support inline lifecycle.
				}
				t.Run(fmt.Sprintf("%s/%s/logDir=%q", factory, mode, logDir), func(t *testing.T) {
					root := t.TempDir()
					cfg := newTestFileLoggerConfig(root, "logs", "empty-log-directory")
					cfg.LifecycleMode = mode
					cfg.FsstoreConfig.Layout.LogDir = logDir
					deliveryCfg := newTestDeliveryFileLoggerConfig(root, "logs", "empty-log-directory")
					deliveryCfg.LifecycleMode = mode
					deliveryCfg.FsstoreConfig.Layout = cfg.FsstoreConfig.Layout

					// Validation must precede even acquiring ownership of the destination.
					lease, err := directorylease.Acquire("test.log-directory-validation", root)
					require.NoError(t, err)
					t.Cleanup(lease.Release)
					switch factory {
					case "sync set":
						set, createErr := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
						err = createErr
						if set != nil {
							closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
						}
					case "sync default", "sync error", "sync debug":
						role := map[string]loggerType{"sync default": DefaultLogger, "sync error": ErrorLogger, "sync debug": DebugLogger}[factory]
						logger, createErr := newFileLoggerBase(context.Background(), NewConfigAdapter(cfg), role, InternalHandlers{}, false, nil, nil, nil)
						err = createErr
						if logger != nil {
							closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
						}
					case "delivery set":
						set, createErr := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
						err = createErr
						if set != nil {
							closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
						}
					default:
						constructor := NewDeliveryFileLogger
						if factory == "delivery debug" {
							constructor = NewDeliveryDebugFileLogger
						}
						logger, createErr := constructor(context.Background(), NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
						err = createErr
						if logger != nil {
							closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
						}
					}
					require.ErrorIs(t, err, pkgfsstore.ErrEmptyLogDirectory)
					require.NotErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
					entries, readErr := os.ReadDir(root)
					require.NoError(t, readErr)
					require.Empty(t, entries, "invalid LogDir must not create log or checkpoint directories or files")
				})
			}
		}
	}
}

func TestDeliveryRoleConstructorsAllowUnusedEmptyLogDirectory(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable} {
		for _, logDir := range []string{"", " \t\r\n"} {
			for _, role := range []loggerType{ErrorLogger, DebugLogger} {
				t.Run(fmt.Sprintf("%s/%s/logDir=%q", role, mode, logDir), func(t *testing.T) {
					root := resolvedTempDirForTest(t)
					cfg := newTestDeliveryFileLoggerConfig(root, "logs", "unused-log-directory")
					cfg.LifecycleMode = mode
					cfg.FsstoreConfig.Layout.LogDir = logDir
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
					cfg.FsstoreConfig.Layout.DebugLogDir = "debug-logs"
					constructor := NewDeliveryErrorFileLogger
					directory := cfg.FsstoreConfig.Layout.ErrorLogDir
					level := pkglogger.LogLevelError
					if role == DebugLogger {
						constructor = NewDeliveryDebugFileLogger
						directory = cfg.FsstoreConfig.Layout.DebugLogDir
						level = pkglogger.LogLevelDebug
					}
					logger, err := constructor(context.Background(), NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
					require.NoError(t, err)
					closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
					require.Equal(t, filepath.Join(root, cfg.FsstoreConfig.Layout.Target, directory), filepath.Dir(logger.Fsstore.ActiveFilePath))
					entry := &logging.LogEntry{Level: level, Message: "unused ordinary LogDir does not affect this role"}
					_, err = logger.HandleLogEntry(context.Background(), entry)
					require.NoError(t, err)
					assertLogFileContains(t, logger.Fsstore.ActiveFilePath, entry.Message)
				})
			}
		}
	}
}
