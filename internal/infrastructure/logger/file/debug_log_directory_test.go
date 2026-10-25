package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

func TestLoggerSetsWriteDebugToConfiguredDirectory(t *testing.T) {
	for _, tt := range []struct {
		name               string
		debugDir           string
		separated          bool
		directorySeparated bool
		wantDir            string
	}{
		{name: "directory separation disabled", separated: true, wantDir: "logs"},
		{name: "disabled ignores stale directory", debugDir: "old_debug_logs", separated: true, wantDir: "logs"},
		{name: "explicit shared", debugDir: "logs", separated: true, directorySeparated: true, wantDir: "logs"},
		{name: "independent", debugDir: "debug_logs", separated: true, directorySeparated: true, wantDir: "debug_logs"},
		{name: "file separation disabled", debugDir: "debug_logs", directorySeparated: true, wantDir: "logs"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("sync", func(t *testing.T) {
				cfg := newTestFileLoggerConfig(resolvedTempDirForTest(t), "logs", "debug-directory")
				cfg.FsstoreConfig.Layout.DebugLogDir = tt.debugDir
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = tt.directorySeparated
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = tt.separated
				cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
				set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
				defaultLogger, err := set.NewFileLogger(context.Background())
				require.NoError(t, err)
				debugLogger, err := set.NewDebugFileLogger(context.Background())
				require.NoError(t, err)
				require.NoError(t, defaultLogger.testInfo(context.Background(), "default entry"))
				require.NoError(t, debugLogger.testDebug(context.Background(), "debug entry"))
				require.Equal(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "debug-directory", tt.wantDir), filepath.Dir(debugLogger.Fsstore.ActiveFilePath))
				assertLogFileContains(t, debugLogger.Fsstore.ActiveFilePath, "debug entry")
				if !tt.directorySeparated && tt.debugDir != "" {
					require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "debug-directory", tt.debugDir))
				}
				if tt.separated {
					assertLogFileNotContains(t, defaultLogger.Fsstore.ActiveFilePath, "debug entry")
				} else {
					require.Same(t, defaultLogger, debugLogger)
					require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "debug-directory", tt.debugDir))
				}
				if tt.wantDir == "logs" {
					require.Same(t, defaultLogger.lifecycleGuard, debugLogger.lifecycleGuard)
				} else {
					require.NotSame(t, defaultLogger.lifecycleGuard, debugLogger.lifecycleGuard)
					require.NotSame(t, defaultLogger.inlinePurge.directorySizeTracker, debugLogger.inlinePurge.directorySizeTracker)
					debugLogger.lifecycleGuard.WithProtectedPaths(func(paths map[string]struct{}) {
						require.Contains(t, paths, debugLogger.Fsstore.ActiveFilePath)
						require.NotContains(t, paths, defaultLogger.Fsstore.ActiveFilePath)
					})
				}
			})
			t.Run("delivery", func(t *testing.T) {
				cfg := newTestDeliveryFileLoggerConfig(resolvedTempDirForTest(t), "logs", "debug-directory")
				cfg.FsstoreConfig.Layout.DebugLogDir = tt.debugDir
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = tt.directorySeparated
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = tt.separated
				set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
				require.NoError(t, err)
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
				defaultLogger, err := set.NewFileLogger(context.Background())
				require.NoError(t, err)
				debugLogger, err := set.NewDebugFileLogger(context.Background())
				require.NoError(t, err)
				report, err := defaultLogger.testInfo(context.Background(), "default entry")
				requireDeliverySucceeded(t, report, err)
				report, err = debugLogger.testDebug(context.Background(), "debug entry")
				requireDeliverySucceeded(t, report, err)
				require.Equal(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "debug-directory", tt.wantDir), filepath.Dir(debugLogger.Fsstore.ActiveFilePath))
				assertLogFileContains(t, debugLogger.Fsstore.ActiveFilePath, "debug entry")
				if !tt.directorySeparated && tt.debugDir != "" {
					require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "debug-directory", tt.debugDir))
				}
				if tt.separated {
					assertLogFileNotContains(t, defaultLogger.Fsstore.ActiveFilePath, "debug entry")
				} else {
					require.Same(t, defaultLogger, debugLogger)
					require.NoDirExists(t, filepath.Join(cfg.FsstoreConfig.Layout.LogPath, "debug-directory", tt.debugDir))
				}
			})
		})
	}
}

func TestLoggerSetReservesIndependentDebugDirectoryBeforeOpeningRole(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "debug-lease")
	cfg.FsstoreConfig.Layout.DebugLogDir = "debug_logs"
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
	set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
	debugDir := filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, "debug_logs")
	require.NoDirExists(t, debugDir)
	lease, err := directorylease.Acquire("debug-directory-contender", debugDir)
	if lease != nil {
		lease.Release()
	}
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set))
	lease, err = directorylease.Acquire("debug-directory-after-close", debugDir)
	require.NoError(t, err)
	lease.Release()
}

func TestLoggerSetChecksDebugDirectoryCollisions(t *testing.T) {
	t.Run("nested directory", func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "debug-nested")
		cfg.FsstoreConfig.Layout.DebugLogDir = filepath.Join("logs", "debug")
		cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = filepath.Join("logs", "debug") != ""
		set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		if set != nil {
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
		}
		require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	})
	t.Run("same prefix in independent directories", func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "debug-prefix")
		cfg.FsstoreConfig.Layout.DebugLogDir = "debug_logs"
		cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
		cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = ""
		set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
		_, err := set.NewDebugFileLogger(context.Background())
		require.NoError(t, err)
	})
	t.Run("same prefix in error directory", func(t *testing.T) {
		cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "debug-collision")
		cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
		cfg.FsstoreConfig.Layout.DebugLogDir = cfg.FsstoreConfig.Layout.ErrorLogDir
		cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = cfg.FsstoreConfig.Layout.ErrorLogDir != ""
		cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix
		set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		if set != nil {
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
		}
		requireLoggerRolesShareFiles(t, err, ErrorLogger, DebugLogger)
	})
}

func TestStandaloneDeliveryDebugLoggerOwnsIndependentDirectory(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(resolvedTempDirForTest(t), "logs", "standalone-debug-directory")
	cfg.FsstoreConfig.Layout.DebugLogDir = "debug_logs"
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
	adapter := NewDeliveryFileLoggerConfigAdapter(cfg)
	defaultLogger, err := NewDeliveryFileLogger(context.Background(), adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "defaultLogger Close", func() closerForTest { return defaultLogger }, true)
	debugLogger, err := NewDeliveryDebugFileLogger(context.Background(), adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "debugLogger Close", func() closerForTest { return debugLogger }, true)
	report, err := debugLogger.testDebug(context.Background(), "standalone debug entry")
	requireDeliverySucceeded(t, report, err)
	debugDir := filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, "debug_logs")
	require.Equal(t, debugDir, filepath.Dir(debugLogger.Fsstore.ActiveFilePath))
	lease, err := directorylease.Acquire("standalone-debug-contender", debugDir)
	if lease != nil {
		lease.Release()
	}
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
}

func TestUploadProtectionResolvesRoleDirectoryWhenPrefixesMatch(t *testing.T) {
	for _, enabledRole := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		t.Run(string(enabledRole), func(t *testing.T) {
			cfg := newTestFileLoggerConfig(resolvedTempDirForTest(t), "logs", "directory-upload")
			cfg.FsstoreConfig.Layout.DebugLogDir = "debug_logs"
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
			cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = ""
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = ""
			cfg.ObjectStorageUploadConfig.Enabled = true
			cfg.ObjectStorageUploadConfig.Default.Enabled = enabledRole == DefaultLogger
			cfg.ObjectStorageUploadConfig.Error.Enabled = enabledRole == ErrorLogger
			cfg.ObjectStorageUploadConfig.Debug.Enabled = enabledRole == DebugLogger
			logger := &FileLogger{config: NewConfigAdapter(cfg)}
			directories, err := resolveLoggerDirectoryPaths(logger.config.Fsstore().Layout())
			require.NoError(t, err)
			logger.directoryPaths = directories
			for role, directory := range map[loggerType]string{
				DefaultLogger: cfg.FsstoreConfig.Layout.LogDir,
				ErrorLogger:   cfg.FsstoreConfig.Layout.ErrorLogDir,
				DebugLogger:   cfg.FsstoreConfig.Layout.DebugLogDir,
			} {
				pattern := logpattern.FileNamePattern{
					Dir:       filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, directory),
					Target:    cfg.FsstoreConfig.Layout.Target,
					Extension: "log",
				}
				path := filepath.Join(pattern.Dir, pattern.Target+"_20260820_1.log.gz")
				require.Equal(t, role == enabledRole, logger.requiresUploadProtection(path, newPurgeFileNames(pattern)), "role %s", role)
			}
		})
	}
}

func TestRecoveryUsesDebugPatternsOnlyInConfiguredDirectory(t *testing.T) {
	cfg := newTestFileLoggerConfig(resolvedTempDirForTest(t), "logs", "directory-recovery")
	cfg.FsstoreConfig.Layout.DebugLogDir = cfg.FsstoreConfig.Layout.LogDir
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = cfg.FsstoreConfig.Layout.LogDir != ""
	logger := &FileLogger{config: NewConfigAdapter(cfg)}
	directories, err := resolveLoggerDirectoryPaths(logger.config.Fsstore().Layout())
	require.NoError(t, err)
	logger.directoryPaths = directories
	for _, tt := range []struct {
		directory string
		prefix    string
		want      []string
	}{
		{directory: "logs", want: []string{"", "debug_"}},
		{directory: cfg.FsstoreConfig.Layout.ErrorLogDir, prefix: "error_", want: []string{"error_"}},
	} {
		t.Run(tt.directory, func(t *testing.T) {
			current := logpattern.FileNamePattern{
				Dir:       filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, tt.directory),
				Prefix:    tt.prefix,
				Target:    cfg.FsstoreConfig.Layout.Target,
				Extension: "log",
			}
			patterns := logger.configuredPhysicalRawArchivePatterns(current)
			var prefixes []string
			for _, pattern := range patterns {
				require.Equal(t, pathidentity.AbsoluteOrCleanPath(current.Dir), pathidentity.AbsoluteOrCleanPath(pattern.Dir))
				prefixes = append(prefixes, pattern.Prefix)
			}
			require.ElementsMatch(t, tt.want, prefixes)
			require.NoDirExists(t, current.Dir)
		})
	}
}

func TestLoggerSetsValidateErrorDebugDirectorySharing(t *testing.T) {
	for _, tt := range []struct {
		name         string
		defaultDir   string
		errorDir     string
		debugDir     string
		disableError bool
		disableDebug bool
		relativeRoot bool
		wantError    bool
	}{
		{name: "all shared", defaultDir: "logs", errorDir: "logs", debugDir: "logs"},
		{name: "default and debug shared", defaultDir: "logs", errorDir: "errors", debugDir: "logs"},
		{name: "default and error shared", defaultDir: "logs", errorDir: "logs", debugDir: "debug"},
		{name: "all separate", defaultDir: "logs", errorDir: "errors", debugDir: "debug"},
		{name: "debug fallback", defaultDir: "logs", errorDir: "errors"},
		{name: "all shared through normalization", defaultDir: "unused/../shared", errorDir: "shared", debugDir: "./shared"},
		{name: "error and debug shared", defaultDir: "logs", errorDir: "shared", debugDir: "shared", wantError: true},
		{name: "error and debug shared through normalization", defaultDir: "logs", errorDir: "shared", debugDir: "unused/../shared", wantError: true},
		{name: "relative root", defaultDir: "logs", errorDir: "shared", debugDir: "./shared", relativeRoot: true, wantError: true},
		{name: "error disabled", defaultDir: "logs", errorDir: "shared", debugDir: "shared", disableError: true},
		{name: "debug disabled", defaultDir: "logs", errorDir: "shared", debugDir: "shared", disableDebug: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, kind := range []string{"file", "delivery"} {
				t.Run(kind, func(t *testing.T) {
					root := resolvedTempDirForTest(t)
					logPath := root
					if tt.relativeRoot {
						// Windowsではpackageの作業directoryと一時directoryが別のdriveにある場合があるため、
						// rootと同じvolumeの一時directoryを作業directoryにして相対pathを作ります。
						cwd := resolvedTempDirForTest(t)
						t.Chdir(cwd)
						relative, err := filepath.Rel(cwd, root)
						require.NoError(t, err)
						logPath = relative
					}
					cfg := newTestFileLoggerConfig(logPath, tt.defaultDir, "directory-sharing")
					cfg.FsstoreConfig.Layout.ErrorLogDir = tt.errorDir
					cfg.FsstoreConfig.Layout.DebugLogDir = tt.debugDir
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = tt.debugDir != ""
					cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = !tt.disableError
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = !tt.disableDebug
					var set interface{ Close() error }
					var err error
					if kind == "delivery" {
						deliveryCfg := newTestDeliveryFileLoggerConfig(logPath, tt.defaultDir, "directory-sharing")
						deliveryCfg.FsstoreConfig.Layout = cfg.FsstoreConfig.Layout
						set, err = NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
					} else {
						set, err = NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
					}
					if tt.wantError {
						require.Nil(t, set)
						var sharing *ErrorDebugDirectorySharingError
						require.ErrorAs(t, err, &sharing)
						// errorは正規化済みidentityを保持するため、期待値もOSごとのcaseを揃えて比べます。
						require.Equal(t, ErrorDebugDirectorySharingError{
							SharedDirectory:  pathidentity.ComparisonKey(filepath.Join(root, "directory-sharing", "shared")),
							DefaultDirectory: pathidentity.ComparisonKey(filepath.Join(root, "directory-sharing", "logs")),
						}, *sharing)
						lease, leaseErr := directorylease.Acquire("rejected-layout", root)
						require.NoError(t, leaseErr)
						lease.Release()
					} else {
						require.NoError(t, err)
						require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set))
					}
					entries, err := os.ReadDir(root)
					require.NoError(t, err)
					require.Empty(t, entries, "layout validation must precede directory and file creation")
				})
			}
		})
	}
}

func TestStandaloneDeliveryLoggersRejectErrorDebugOnlySharing(t *testing.T) {
	for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		t.Run(string(role), func(t *testing.T) {
			root := t.TempDir()
			cfg := newTestDeliveryFileLoggerConfig(root, "logs", "standalone-layout")
			cfg.FsstoreConfig.Layout.DebugLogDir = cfg.FsstoreConfig.Layout.ErrorLogDir
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = cfg.FsstoreConfig.Layout.ErrorLogDir != ""
			logger, err := newStandaloneDeliveryFileLogger(context.Background(), NewDeliveryFileLoggerConfigAdapter(cfg), role, nil, nil)
			require.Nil(t, logger)
			var sharing *ErrorDebugDirectorySharingError
			require.ErrorAs(t, err, &sharing)
			entries, err := os.ReadDir(root)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestLoggerSetDebugDirectoryDoesNotShareDirectoryPurgeBudget(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "debug-directory-budget")
	cfg.FsstoreConfig.Layout.DebugLogDir = "debug_logs"
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
	defaultLogger, err := set.NewFileLogger(context.Background())
	require.NoError(t, err)
	debugLogger, err := set.NewDebugFileLogger(context.Background())
	require.NoError(t, err)
	debugArchive := filepath.Join(debugLogger.Fsstore.FileName.Dir, "unrelated_20260913_1.log")
	require.NoError(t, os.WriteFile(debugArchive, nil, 0o600))
	require.NoError(t, os.Truncate(debugArchive, int64(cfg.FsstoreConfig.Lifecycle.MaxDirSize)+1))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), defaultLogger))
	require.FileExists(t, debugArchive)
	require.NoError(t, purgeFileLoggerForTest(context.Background(), debugLogger))
	require.NoFileExists(t, debugArchive)
	require.FileExists(t, debugLogger.Fsstore.ActiveFilePath)
}
