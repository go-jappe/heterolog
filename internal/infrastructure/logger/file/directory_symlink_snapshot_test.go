package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestFileLoggerPinsSymlinkRootAcrossLazyRolesAndRotation(t *testing.T) {
	ctx := context.Background()
	realRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	replacementRoot := t.TempDir()
	alias := filepath.Join(t.TempDir(), "root-alias")
	testutil.Symlink(t, realRoot, alias)
	const target = "physical-log-root"
	directory := filepath.Join(realRoot, target, "logs")
	cfg := newTestFileLoggerConfig(alias, "logs", target)
	cfg.FsstoreConfig.Layout.ErrorLogDir = "logs"
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	require.NoDirExists(t, directory)
	logger, err := set.NewFileLogger(ctx)
	require.NoError(t, err)
	require.Equal(t, directory, logger.Fsstore.FileName.Dir)
	require.Same(t, appendlog.DirectoryGuardForPath(directory), logger.lifecycleGuard)
	require.Same(t, appendlog.DirectorySizeTrackerForPath(directory), logger.inlinePurge.directorySizeTracker)

	// lazy roleも最初に予約したdirectoryを使い、差し替えられたaliasを辿りません。
	require.NoError(t, os.Remove(alias))
	testutil.Symlink(t, replacementRoot, alias)
	errorLogger, err := set.NewErrorFileLogger(ctx)
	require.NoError(t, err)
	debugLogger, err := set.NewDebugFileLogger(ctx)
	require.NoError(t, err)
	for _, roleLogger := range []*FileLogger{errorLogger, debugLogger} {
		require.Equal(t, directory, roleLogger.Fsstore.FileName.Dir)
		require.Same(t, logger.lifecycleGuard, roleLogger.lifecycleGuard)
		require.Same(t, logger.inlinePurge.directorySizeTracker, roleLogger.inlinePurge.directorySizeTracker)
	}
	patterns := logger.configuredPhysicalRawArchivePatterns(logger.Fsstore.FileName)
	require.Len(t, patterns, 3)
	for _, pattern := range patterns {
		require.Equal(t, directory, pattern.Dir)
	}
	// uploader未生成roleのpurge保護も、configのalias文字列ではなく予約済み実体と比較します。
	uploadCfg := cfg
	uploadCfg.ObjectStorageUploadConfig.Enabled = true
	uploadCfg.ObjectStorageUploadConfig.Error.Enabled = true
	protection := &FileLogger{config: NewConfigAdapter(uploadCfg), directoryPaths: logger.directoryPaths}
	errorArchiveName := "error_" + target + "_20260101_1.log.gz"
	names := newPurgeFileNames(logger.Fsstore.FileName)
	require.True(t, protection.requiresUploadProtection(filepath.Join(directory, errorArchiveName), names))
	require.False(t, protection.requiresUploadProtection(filepath.Join(replacementRoot, target, "logs", errorArchiveName), names))

	remote := &recordingArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
	oldPath := fillActiveFileLoggerForRotation(t, logger)
	require.NoError(t, logger.HandleLogEntry(ctx, &logging.LogEntry{
		OccurredAt: time.Now(), Level: pkglogger.LogLevelInfo, Message: "rotate in the pinned physical directory",
	}))
	waitBackgroundForTest(t, logger)
	require.NotEqual(t, oldPath, logger.Fsstore.ActiveFilePath)
	require.Equal(t, directory, filepath.Dir(logger.Fsstore.ActiveFilePath))
	require.Equal(t, []string{oldPath + pkgfsstore.ExtensionCompressGzip}, archiveUploadInputPaths(remote.inputsSnapshot()))
	require.NoFileExists(t, oldPath)
	require.NoFileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
	got, err := logger.GetFileSystemStoreDirectoryPath()
	require.NoError(t, err)
	require.Equal(t, directory, got)
	require.NoDirExists(t, filepath.Join(replacementRoot, target))
}

func TestDeliveryLoggerSetPinsSymlinkRootBeforeLazyOpenAndRotation(t *testing.T) {
	ctx := context.Background()
	realRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	replacementRoot := t.TempDir()
	alias := filepath.Join(t.TempDir(), "root-alias")
	testutil.Symlink(t, realRoot, alias)
	const target = "physical-delivery-root"
	directory := filepath.Join(realRoot, target, "logs")
	cfg := newTestDeliveryFileLoggerConfig(alias, "logs", target)
	cfg.FsstoreConfig.Layout.ErrorLogDir = "logs"
	cfg.FsstoreConfig.Rotation.EnableRotate = true
	set, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	require.NoDirExists(t, directory)
	require.NoError(t, os.Remove(alias))
	// 予約後にaliasがdanglingになっても、未生成roleは予約時の実体pathを使います。
	testutil.Symlink(t, filepath.Join(replacementRoot, "missing"), alias)

	logger, err := set.NewFileLogger(ctx)
	require.NoError(t, err)
	errorLogger, err := set.NewErrorFileLogger(ctx)
	require.NoError(t, err)
	debugLogger, err := set.NewDebugFileLogger(ctx)
	require.NoError(t, err)
	for _, roleLogger := range []*DeliveryFileLogger{logger, errorLogger, debugLogger} {
		require.Equal(t, directory, roleLogger.Fsstore.FileName.Dir)
		require.Same(t, appendlog.DirectoryGuardForPath(directory), roleLogger.lifecycleGuard)
	}
	oldPath := logger.Fsstore.ActiveFilePath
	_, err = logger.Fsstore.File.Write(make([]byte, cfg.FsstoreConfig.Rotation.MaxFileSize+1))
	require.NoError(t, err)
	report, err := logger.testInfo(ctx, "delivery rotation in the pinned physical directory")
	requireDeliverySucceeded(t, report, err)
	require.NotEqual(t, oldPath, logger.Fsstore.ActiveFilePath)
	require.Equal(t, directory, filepath.Dir(logger.Fsstore.ActiveFilePath))
	got, err := logger.GetFileSystemStoreDirectoryPath()
	require.NoError(t, err)
	require.Equal(t, directory, got)
	require.NoDirExists(t, filepath.Join(replacementRoot, target))
}

func TestLoggerSetsValidatePhysicalRoleAliases(t *testing.T) {
	for _, separateDefault := range []bool{false, true} {
		for _, contract := range []string{"file", "delivery"} {
			name := contract + "/all-shared"
			if separateDefault {
				name = contract + "/only-error-debug-shared"
			}
			t.Run(name, func(t *testing.T) {
				root := t.TempDir()
				const target = "role-aliases"
				shared := filepath.Join(root, target, "shared")
				require.NoError(t, os.MkdirAll(shared, 0o750))
				alias := filepath.Join(root, target, "shared-alias")
				testutil.Symlink(t, shared, alias)
				cfg := newTestFileLoggerConfig(root, "shared-alias", target)
				cfg.FsstoreConfig.Layout.ErrorLogDir = "shared"
				cfg.FsstoreConfig.Layout.DebugLogDir = "shared"
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
				if separateDefault {
					cfg.FsstoreConfig.Layout.LogDir = "default"
					cfg.FsstoreConfig.Layout.DebugLogDir = "shared-alias"
				}
				var set interface{ Close() error }
				var err error
				if contract == "file" {
					set, err = NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
				} else {
					deliveryCfg := newTestDeliveryFileLoggerConfig(root, cfg.FsstoreConfig.Layout.LogDir, target)
					deliveryCfg.FsstoreConfig.Layout = cfg.FsstoreConfig.Layout
					set, err = NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(deliveryCfg), nil, nil)
				}
				if separateDefault {
					var sharing *ErrorDebugDirectorySharingError
					require.ErrorAs(t, err, &sharing)
					require.Nil(t, set)
					require.NoDirExists(t, filepath.Join(root, target, "default"))
				} else {
					require.NoError(t, err)
					require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set))
				}
			})
		}
	}
}

func TestInlineRolePurgeUsesResolvedAliasDirectoryCapacity(t *testing.T) {
	for _, role := range []loggerType{ErrorLogger, DebugLogger} {
		for _, configuredLimit := range []uint64{0, 10 << 20} {
			t.Run(fmt.Sprintf("%s/limit=%d", role, configuredLimit), func(t *testing.T) {
				root, err := filepath.EvalSymlinks(t.TempDir())
				require.NoError(t, err)
				const target = "alias-role-capacity"
				directory := filepath.Join(root, target, "logs")
				require.NoError(t, os.MkdirAll(directory, 0o750))
				alias := filepath.Join(root, target, "role-alias")
				testutil.Symlink(t, directory, alias)
				cfg := newTestFileLoggerConfig(root, "logs", target)
				cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
				cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = role == ErrorLogger
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = role == DebugLogger
				override := &pkgfsstore.FilePurgeConfig{MaxFileAge: 3, MaxFileBackups: 2, MaxDirSize: configuredLimit}
				if role == ErrorLogger {
					cfg.FsstoreConfig.Layout.ErrorLogDir = "role-alias"
					cfg.FsstoreConfig.Lifecycle.ErrorPurge = override
				} else {
					cfg.FsstoreConfig.Layout.DebugLogDir = "role-alias"
					cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
					cfg.FsstoreConfig.Lifecycle.DebugPurge = override
				}
				set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
				require.NoError(t, err, "shared directory must use the default limit even when the role limit is invalid")
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
				// lazy生成時の再解決で共有判定が変わらないことも確認します。
				require.NoError(t, os.Remove(alias))
				testutil.Symlink(t, t.TempDir(), alias)
				var logger *FileLogger
				if role == ErrorLogger {
					logger, err = set.NewErrorFileLogger(context.Background())
				} else {
					logger, err = set.NewDebugFileLogger(context.Background())
				}
				require.NoError(t, err)
				require.Equal(t, directory, logger.Fsstore.FileName.Dir)
				require.Equal(t, uint64(100<<20), logger.inlinePurge.config.MaxDirSize())
				require.Equal(t, 3, logger.inlinePurge.config.MaxFileAge())
				require.Equal(t, 2, logger.inlinePurge.config.MaxFileBackups())
				require.Equal(t, configuredLimit, override.MaxDirSize, "input config must not be mutated")
			})
		}
	}
}
