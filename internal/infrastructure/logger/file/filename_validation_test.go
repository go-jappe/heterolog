package file

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFileLoggerRejectsColonBeforeFileCreationAndDirectoryLease(t *testing.T) {
	for _, field := range []string{"target", "filename format", "date format", "error prefix", "debug prefix"} {
		for _, mode := range []pkgfsstore.FileLifecycleMode{
			pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable,
		} {
			for _, factory := range []string{"sync set", "sync default", "sync error", "sync debug", "delivery set", "delivery default", "delivery error", "delivery debug"} {
				if mode == pkgfsstore.FileLifecycleModeInline && factory[:4] != "sync" {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", field, mode, factory), func(t *testing.T) {
					root := t.TempDir()
					cfg := newTestFileLoggerConfig(root, "logs", "colon-validation")
					cfg.LifecycleMode = mode
					layout := &cfg.FsstoreConfig.Layout
					var want pkgfsstore.LogFileNameComponentError
					switch field {
					case "target":
						layout.Target = "target:stream"
						want = pkgfsstore.LogFileNameComponentError{Field: "target", Value: layout.Target}
					case "filename format":
						layout.FileNameFormat = "/%s%s_%s_%d.log:stream"
						want = pkgfsstore.LogFileNameComponentError{Field: "file name format", Value: layout.FileNameFormat}
					case "date format":
						layout.FileDateFormat = "20060102:1504"
						want = pkgfsstore.LogFileNameComponentError{Field: "file date format", Value: layout.FileDateFormat}
					case "error prefix":
						layout.SeparateErrorLogsPrefix = "error:"
						want = pkgfsstore.LogFileNameComponentError{Field: "error log prefix", Value: layout.SeparateErrorLogsPrefix}
					case "debug prefix":
						layout.SeparateDebugLogsPrefix = "debug:"
						want = pkgfsstore.LogFileNameComponentError{Field: "debug log prefix", Value: layout.SeparateDebugLogsPrefix}
					}
					deliveryCfg := newTestDeliveryFileLoggerConfig(root, "logs", "colon-validation")
					deliveryCfg.LifecycleMode = mode
					deliveryCfg.FsstoreConfig.Layout = *layout

					// 親を先に予約し、ファイル名の検証がlease取得より先であることも確認します。
					lease, err := directorylease.Acquire("test.filename-validation", root)
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
						if factory == "delivery error" {
							constructor = NewDeliveryErrorFileLogger
						}
						if factory == "delivery debug" {
							constructor = NewDeliveryDebugFileLogger
						}
						logger, createErr := constructor(context.Background(), NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
						err = createErr
						if logger != nil {
							closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
						}
					}
					require.ErrorIs(t, err, pkgfsstore.ErrInvalidLogFileNameComponent)
					var nameErr *pkgfsstore.LogFileNameComponentError
					require.ErrorAs(t, err, &nameErr)
					require.Equal(t, want, *nameErr)
					require.NotErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
					entries, readErr := os.ReadDir(root)
					require.NoError(t, readErr)
					require.Empty(t, entries, "invalid names must not create directories, ordinary files, or alternate streams")
				})
			}
		}
	}
}

func TestFileLoggerAllowsColonInDisabledPrefixes(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{
		pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner, pkgfsstore.FileLifecycleModeDisable,
	} {
		for _, delivery := range []bool{false, true} {
			if delivery && mode == pkgfsstore.FileLifecycleModeInline {
				continue
			}
			t.Run(fmt.Sprintf("%s/delivery=%t", mode, delivery), func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				cfg := newTestFileLoggerConfig(root, "logs", "disabled-prefix")
				cfg.LifecycleMode = mode
				cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
				cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "unused:error"
				cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "unused:debug"
				entry := &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "ordinary log remains writable"}
				if delivery {
					deliveryCfg := newTestDeliveryFileLoggerConfig(root, "logs", "disabled-prefix")
					deliveryCfg.LifecycleMode = mode
					deliveryCfg.FsstoreConfig.Layout = cfg.FsstoreConfig.Layout
					set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
					require.NoError(t, err)
					closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
					logger, err := set.NewFileLogger(ctx)
					require.NoError(t, err)
					errorLogger, err := set.NewErrorFileLogger(ctx)
					require.NoError(t, err)
					debugLogger, err := set.NewDebugFileLogger(ctx)
					require.NoError(t, err)
					require.Same(t, logger, errorLogger)
					require.Same(t, logger, debugLogger)
					_, err = logger.HandleLogEntry(ctx, entry)
					require.NoError(t, err)
					assertLogFileContains(t, logger.Fsstore.ActiveFilePath, entry.Message)
				} else {
					set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
					require.NoError(t, err)
					closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
					logger, err := set.NewFileLogger(ctx)
					require.NoError(t, err)
					errorLogger, err := set.NewErrorFileLogger(ctx)
					require.NoError(t, err)
					debugLogger, err := set.NewDebugFileLogger(ctx)
					require.NoError(t, err)
					require.Same(t, logger, errorLogger)
					require.Same(t, logger, debugLogger)
					require.NoError(t, logger.HandleLogEntry(ctx, entry))
					assertLogFileContains(t, logger.Fsstore.ActiveFilePath, entry.Message)
				}
			})
		}
	}
}

func TestFileLoggerDoesNotTreatDriveColonInDirectoryConfigAsFileNameComponent(t *testing.T) {
	// ドライブ指定の':'はファイル名部品ではないため、LinuxでもWindowsでもcolon検証の対象外です。
	// 結合後のpathの扱いはOSごとに異なるため、requireDrivePathDirectoryValidationで確認します
	// （Windows以外は受け付け、Windowsはファイル名部品以外の理由で拒否します）。
	// 存在しないドライブを作成しないよう、ここではconstructorの共通検証だけを実行します。
	cfg := newTestFileLoggerConfig(`C:\logger-output`, `D:\ordinary-logs`, "drive-path")
	cfg.FsstoreConfig.Layout.ErrorLogDir = `E:\error-logs`
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
	cfg.FsstoreConfig.Layout.DebugLogDir = `F:\debug-logs`
	requireDrivePathDirectoryValidation(t, validateFileLoggerConfigProvider(NewConfigAdapter(cfg)))
	deliveryCfg := newTestDeliveryFileLoggerConfig("", "", "drive-path")
	deliveryCfg.FsstoreConfig.Layout = cfg.FsstoreConfig.Layout
	requireDrivePathDirectoryValidation(t, validateDeliveryFileLoggerConfigProvider(NewDeliveryFileLoggerConfigAdapter(deliveryCfg), DefaultLogger))
}
