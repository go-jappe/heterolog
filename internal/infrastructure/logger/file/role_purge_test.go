package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestInlineRolePurgeUsesEachSeriesRetentionInSharedDirectory(t *testing.T) {
	for _, operation := range []string{"age", "generation"} {
		t.Run(operation, func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "role-retention")
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
			cfg.FsstoreConfig.Layout.ErrorLogDir = "unused/../logs"
			cfg.FsstoreConfig.Layout.DebugLogDir = "./logs"
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
			cfg.FsstoreConfig.Lifecycle.MaxFileAge = 30
			cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 6
			cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
			cfg.FsstoreConfig.Lifecycle.ErrorPurge = &pkgfsstore.FilePurgeConfig{MaxFileAge: 3, MaxFileBackups: 2, MaxDirSize: 20 << 20}
			cfg.FsstoreConfig.Lifecycle.DebugPurge = &pkgfsstore.FilePurgeConfig{MaxFileAge: 10, MaxFileBackups: 4, MaxDirSize: 30 << 20}
			errorOverride := *cfg.FsstoreConfig.Lifecycle.ErrorPurge
			debugOverride := *cfg.FsstoreConfig.Lifecycle.DebugPurge
			set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
			defaultLogger, err := set.NewFileLogger(context.Background())
			require.NoError(t, err)
			errorLogger, err := set.NewErrorFileLogger(context.Background())
			require.NoError(t, err)
			debugLogger, err := set.NewDebugFileLogger(context.Background())
			require.NoError(t, err)
			loggers := []*FileLogger{defaultLogger, errorLogger, debugLogger}
			now := time.Now().UTC().Truncate(24 * time.Hour).Add(12 * time.Hour)
			paths := make([][]string, len(loggers))
			for role, logger := range loggers {
				logger.Clock = pkgclock.NewFixedClock(now)
				require.Equal(t, uint64(100<<20), logger.inlinePurge.config.MaxDirSize())
				require.Same(t, defaultLogger.inlinePurge.directorySizeTracker, logger.inlinePurge.directorySizeTracker)
				ages := []int{7, 20}
				if operation == "generation" {
					ages = []int{1, 1, 1, 1, 1, 1}
				}
				for index, age := range ages {
					modified := now.AddDate(0, 0, -age).Add(time.Duration(index) * time.Minute)
					paths[role] = append(paths[role], writeRolePurgeFile(t, logger, modified, index+1, 1))
				}
			}
			for role, logger := range loggers {
				require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
				for sibling := role + 1; sibling < len(loggers); sibling++ {
					for _, path := range paths[sibling] {
						require.FileExists(t, path, "purge must not apply one role's retention to a sibling")
					}
				}
			}
			for role, logger := range loggers {
				for index, path := range paths[role] {
					removed := role == 1 || (role == 2 && index == 1)
					if operation == "generation" {
						removed = index < len(paths[role])-[]int{6, 2, 4}[role]
					}
					if removed {
						require.NoFileExists(t, path)
					} else {
						require.FileExists(t, path)
					}
				}
				require.FileExists(t, logger.Fsstore.ActiveFilePath)
			}
			require.Equal(t, errorOverride, *cfg.FsstoreConfig.Lifecycle.ErrorPurge)
			require.Equal(t, debugOverride, *cfg.FsstoreConfig.Lifecycle.DebugPurge)
		})
	}
}

func TestInlineRolePurgeUsesPhysicalDirectoryCapacity(t *testing.T) {
	for _, role := range []loggerType{ErrorLogger, DebugLogger} {
		for _, shared := range []bool{false, true} {
			for _, override := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/shared=%t/override=%t", role, shared, override), func(t *testing.T) {
					cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "role-capacity")
					cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
					cfg.FsstoreConfig.Lifecycle.MaxFileAge = 30
					cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 6
					cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
					directory := "independent"
					if shared {
						directory = "unused/../logs"
					}
					var purge *pkgfsstore.FilePurgeConfig
					if override {
						purge = &pkgfsstore.FilePurgeConfig{MaxFileAge: 30, MaxFileBackups: 6, MaxDirSize: 10 << 20}
					}
					cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = role == ErrorLogger
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = role == DebugLogger
					if role == ErrorLogger {
						cfg.FsstoreConfig.Layout.ErrorLogDir = directory
						cfg.FsstoreConfig.Lifecycle.ErrorPurge = purge
					} else {
						cfg.FsstoreConfig.Layout.DebugLogDir = directory
						cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = directory != ""
						cfg.FsstoreConfig.Lifecycle.DebugPurge = purge
					}
					set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
					var logger *FileLogger
					var err error
					if role == ErrorLogger {
						logger, err = set.NewErrorFileLogger(context.Background())
					} else {
						logger, err = set.NewDebugFileLogger(context.Background())
					}
					require.NoError(t, err)
					path := writeRolePurgeFile(t, logger, time.Now().UTC().AddDate(0, 0, -1), 1, 12<<20)
					require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
					if override && !shared {
						require.NoFileExists(t, path)
					} else {
						require.FileExists(t, path)
					}
					require.FileExists(t, logger.Fsstore.ActiveFilePath)
				})
			}
		}
	}
}

func TestInlineRolePurgeKeepsAdapterResolvedProviders(t *testing.T) {
	for _, override := range []string{"none", "error", "debug"} {
		t.Run(override, func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "resolved-role-purge")
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
			cfg.FsstoreConfig.Layout.ErrorLogDir = "errors"
			cfg.FsstoreConfig.Layout.DebugLogDir = "debug"
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
			cfg.FsstoreConfig.Lifecycle.MaxFileAge = 30
			cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 6
			cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
			cfg.FsstoreConfig.Lifecycle.ErrorPurge = nil
			cfg.FsstoreConfig.Lifecycle.DebugPurge = nil
			purge := &pkgfsstore.FilePurgeConfig{MaxFileAge: 3, MaxFileBackups: 2, MaxDirSize: 20 << 20}
			if override == "error" {
				cfg.FsstoreConfig.Lifecycle.ErrorPurge = purge
			} else if override == "debug" {
				cfg.FsstoreConfig.Lifecycle.DebugPurge = purge
			}
			adapter := NewConfigAdapter(cfg)
			set := newTestFileLoggerSet(t, adapter, nil, nil)
			now := time.Now().UTC().Truncate(24 * time.Hour).Add(12 * time.Hour)
			for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
				logger, err := set.fileLogger(context.Background(), role)
				require.NoError(t, err)
				resolved := adapter.Fsstore().Purge()
				if role == ErrorLogger {
					resolved = adapter.Fsstore().ErrorPurge()
				} else if role == DebugLogger {
					resolved = adapter.Fsstore().DebugPurge()
				}
				// runtime側でnilの補完やproviderの複製をせず、adapterの確定値を保持します。
				require.NotNil(t, resolved)
				require.Same(t, resolved, logger.inlinePurge.config)
				if string(role) != override {
					require.Same(t, adapter.Fsstore().Purge(), logger.inlinePurge.config)
				}
				logger.Clock = pkgclock.NewFixedClock(now)
				path := writeRolePurgeFile(t, logger, now.AddDate(0, 0, -7), 1, 1)
				require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
				if string(role) == override {
					require.NoFileExists(t, path)
				} else {
					require.FileExists(t, path)
				}
			}
		})
	}
}

func TestLoggerRejectsInvalidRolePurgeBeforeSideEffects(t *testing.T) {
	for _, role := range []loggerType{ErrorLogger, DebugLogger} {
		for _, invalid := range []string{"age", "backups", "capacity"} {
			t.Run(string(role)+"/"+invalid, func(t *testing.T) {
				root := t.TempDir()
				cfg := newTestFileLoggerConfig(root, "logs", "invalid-role-purge")
				cfg.FsstoreConfig.Layout.DebugLogDir = "debug"
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
				purge := &pkgfsstore.FilePurgeConfig{MaxFileAge: 30, MaxFileBackups: 6, MaxDirSize: 20 << 20}
				var wantErr error
				switch invalid {
				case "age":
					purge.MaxFileAge = 0
					wantErr = infrafsstore.ErrInvalidMaxFileAge
				case "backups":
					purge.MaxFileBackups = 1
					wantErr = infrafsstore.ErrInvalidMaxFileBackups
				case "capacity":
					purge.MaxDirSize = 0
					wantErr = infrafsstore.ErrInvalidMaxDirSize
				}
				if role == ErrorLogger {
					cfg.FsstoreConfig.Lifecycle.ErrorPurge = purge
				} else {
					cfg.FsstoreConfig.Lifecycle.DebugPurge = purge
				}
				set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
				if set != nil {
					closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
				}
				var purgeErr *appendlog.PurgeConfigError
				require.ErrorAs(t, err, &purgeErr)
				require.Equal(t, appendlog.PurgeRole(role), purgeErr.Role)
				require.ErrorIs(t, purgeErr, wantErr)
				entries, err := os.ReadDir(root)
				require.NoError(t, err)
				require.Empty(t, entries)
			})
		}
	}
}

func TestAsyncFileRolePurgeUsesConfiguredLifecycle(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner} {
		for _, role := range []loggerType{ErrorLogger, DebugLogger} {
			t.Run(string(mode)+"/"+string(role), func(t *testing.T) {
				cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "async-role-purge")
				cfg.LifecycleMode = mode
				cfg.FsstoreConfig.Layout.ErrorLogDir = "logs"
				cfg.FsstoreConfig.Layout.DebugLogDir = "logs"
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
				cfg.FsstoreConfig.Lifecycle.MaxFileAge = 30
				cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 6
				cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
				purge := &pkgfsstore.FilePurgeConfig{MaxFileAge: 3, MaxFileBackups: 2, MaxDirSize: 10 << 20}
				if role == ErrorLogger {
					cfg.FsstoreConfig.Lifecycle.ErrorPurge = purge
				} else {
					cfg.FsstoreConfig.Lifecycle.DebugPurge = purge
				}
				set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
				defaultLogger, err := set.NewFileLogger(context.Background())
				require.NoError(t, err)
				var logger *FileLogger
				if role == ErrorLogger {
					logger, err = set.NewErrorFileLogger(context.Background())
				} else {
					logger, err = set.NewDebugFileLogger(context.Background())
				}
				require.NoError(t, err)
				modified := time.Now().UTC().AddDate(0, 0, -7)
				defaultPath := writeRolePurgeFile(t, defaultLogger, modified, 1, 1)
				rolePath := writeRolePurgeFile(t, logger, modified, 1, 1)
				async, err := multi.NewAsyncBestEffortMultiLogger(rolePurgeAsyncConfig{}, logger)
				require.NoError(t, err)
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "async Close", func() closerForTest { return async }, true)
				if role == ErrorLogger {
					async.Error(context.Background(), "async retention trigger")
				} else {
					async.Debug(context.Background(), "async retention trigger")
				}
				require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "async Close", async))
				assertLogFileContains(t, logger.Fsstore.ActiveFilePath, "async retention trigger")
				if mode == pkgfsstore.FileLifecycleModeRunner {
					require.Nil(t, logger.inlinePurge)
					require.FileExists(t, rolePath, "runner mode must wait for external lifecycle execution")
					runners, err := logfilerunner.NewLogFileRunners(logfilelifecycle.LogFileOptions{
						Config: pkgfsstore.LogFileLifecycleRunnerConfig{FsstoreConfig: cfg.FsstoreConfig},
					}, logfilerunner.StaticRunnerConfig{IntervalValue: time.Minute})
					require.NoError(t, err)
					for _, runner := range runners {
						_, err := runner.RunOnce(context.Background())
						require.NoError(t, err)
					}
				} else {
					// 書き込みが受理したbackgroundのcleanupが終わってから、削除結果を確認します。
					waitBackgroundForTest(t, logger)
					require.False(t, logger.inlinePurge.cleanupRunning.Load())
				}
				require.NoFileExists(t, rolePath)
				require.FileExists(t, defaultPath)
			})
		}
	}

}

func TestLoggerRolePurgeIgnoresUnusedOverrides(t *testing.T) {
	for _, mode := range []pkgfsstore.FileLifecycleMode{pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeDisable} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "unused-role-purge")
			cfg.LifecycleMode = mode
			cfg.FsstoreConfig.Lifecycle.ErrorPurge = &pkgfsstore.FilePurgeConfig{}
			cfg.FsstoreConfig.Lifecycle.DebugPurge = &pkgfsstore.FilePurgeConfig{}
			if mode == pkgfsstore.FileLifecycleModeInline {
				cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
			}
			set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
			logger, err := set.NewErrorFileLogger(context.Background())
			require.NoError(t, err)
			if mode == pkgfsstore.FileLifecycleModeInline {
				defaultLogger, err := set.NewFileLogger(context.Background())
				require.NoError(t, err)
				require.Same(t, defaultLogger, logger)
				require.Equal(t, cfg.FsstoreConfig.Lifecycle.MaxFileAge, logger.inlinePurge.config.MaxFileAge())
			} else {
				require.Nil(t, logger.inlinePurge)
			}
		})
	}
}

type rolePurgeAsyncConfig struct{}

func (rolePurgeAsyncConfig) Timeout() time.Duration { return time.Second }
func (rolePurgeAsyncConfig) MaxParallel() int       { return 1 }
func (rolePurgeAsyncConfig) MaxLogEntryBytes() int  { return pkglogger.DefaultMaxLogEntryBytes }

func writeRolePurgeFile(t *testing.T, logger *FileLogger, modified time.Time, index int, size int64) string {
	t.Helper()
	pattern := logger.Fsstore.FileName
	path := filepath.Join(pattern.Dir, fmt.Sprintf("%s%s_%s_%d.log", pattern.Prefix, pattern.Target, modified.Format("20060102"), index))
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	require.NoError(t, os.Truncate(path, size))
	require.NoError(t, os.Chtimes(path, modified, modified))
	return path
}
