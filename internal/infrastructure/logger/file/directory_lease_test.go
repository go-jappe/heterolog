package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestLoggerSetDirectoryLeaseDistinguishesInstancesWithSameScopeLabel(t *testing.T) {
	root := t.TempDir()
	cfg := newTestFileLoggerConfig(root, "shared", "lease-owner")
	// default/error/debugはprefixで同じdirectoryを共有する一つの取得単位です。
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
	scopeLabel := directorylease.ScopeLabel("logger.file")

	first, err := NewLoggerSetWithDirectoryScopeLabel(
		scopeLabel,
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	require.NotZero(t, first.directoryLease.ID())

	defaultLogger, err := first.NewFileLogger(context.Background())
	require.NoError(t, err)
	errorLogger, err := first.NewErrorFileLogger(context.Background())
	require.NoError(t, err)
	debugLogger, err := first.NewDebugFileLogger(context.Background())
	require.NoError(t, err)
	require.Equal(t, filepath.Dir(defaultLogger.Fsstore.ActiveFilePath), filepath.Dir(errorLogger.Fsstore.ActiveFilePath))
	require.Equal(t, filepath.Dir(defaultLogger.Fsstore.ActiveFilePath), filepath.Dir(debugLogger.Fsstore.ActiveFilePath))

	second, err := NewLoggerSetWithDirectoryScopeLabel(
		scopeLabel,
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	var conflict *directorylease.ConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, scopeLabel, conflict.RequestedScopeLabel)
	require.Equal(t, scopeLabel, conflict.ExistingScopeLabel)
	require.NotZero(t, conflict.RequestedAttemptID)
	require.NotEqual(t, first.directoryLease.ID(), conflict.RequestedAttemptID)
	require.Equal(t, first.directoryLease.ID(), conflict.ExistingLeaseID)

	firstLeaseID := first.directoryLease.ID()
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "first Close", first))
	replacement, err := NewLoggerSetWithDirectoryScopeLabel(
		scopeLabel,
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	require.NotEqual(t, firstLeaseID, replacement.directoryLease.ID())
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "replacement Close", replacement))
}

func TestLoggerSetDirectoryLeaseRejectsParentChildRolesAtomically(t *testing.T) {
	root := t.TempDir()
	cfg := newTestFileLoggerConfig(root, "logs", "nested-role-owner")
	cfg.FsstoreConfig.Layout.ErrorLogDir = filepath.Join("logs", "error")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.Nil(t, loggerSet)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	var overlap *directorylease.RequestOverlapError
	require.ErrorAs(t, err, &overlap)
	require.Equal(t, defaultLoggerSetDirectoryScopeLabel, overlap.ScopeLabel)
	require.NotZero(t, overlap.AttemptID)

	// 一括取得に失敗した要求のdefault pathがregistryへ残っていないことを確認します。
	path := filepath.Join(root, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir)
	lease, err := directorylease.Acquire("atomicity-check", path)
	require.NoError(t, err)
	lease.Release()
}

func TestLoggerSetCloseFileErrorReleasesDirectoryLease(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "lease-close-retry")
	first, err := NewLoggerSetWithDirectoryScopeLabel(
		"logger.file",
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	logger, err := first.NewFileLogger(context.Background())
	require.NoError(t, err)
	require.NoError(t, logger.Fsstore.File.Close())

	require.ErrorIs(t, closeBoundedForTest(t, boundedwait.HangTimeout, "first Close", first), os.ErrClosed)
	require.Nil(t, logger.Fsstore.File)
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.NotContains(t, protected, logger.Fsstore.ActiveFilePath)
	})

	second, err := NewLoggerSetWithDirectoryScopeLabel(
		"logger.file",
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "first Close", first))
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "second Close", second))
}

func TestStandaloneDeliveryFileLoggerOwnsDirectoryUntilClose(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "standalone-delivery-lease")
	adapter := NewDeliveryFileLoggerConfigAdapter(cfg)
	first, err := NewDeliveryFileLogger(context.Background(), adapter, nil, nil)
	require.NoError(t, err)

	second, err := NewDeliveryFileLogger(context.Background(), adapter, nil, nil)
	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "first Close", first))
	second, err = NewDeliveryFileLogger(context.Background(), adapter, nil, nil)
	require.NoError(t, err)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "second Close", second))
}

func TestLoggerSetDirectoryConflictErrorSupportsErrorsIs(t *testing.T) {
	root := t.TempDir()
	first, err := directorylease.Acquire("external-owner", root)
	require.NoError(t, err)
	defer first.Release()

	cfg := newTestFileLoggerConfig(root, "logs", "child-owner")
	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.Nil(t, loggerSet)
	require.True(t, errors.Is(err, directorylease.ErrDirectoryLeaseConflict))
}

// requireLoggerRolesShareFilesは、errがErrLoggerRolesShareFilesによる拒否であり、
// 同じfileを指したroleがfirst、secondの順であることを確認します。
func requireLoggerRolesShareFiles(t *testing.T, err error, first, second loggerType) {
	t.Helper()
	require.ErrorIs(t, err, ErrLoggerRolesShareFiles)
	var shareErr *LoggerRolesShareFilesError
	require.ErrorAs(t, err, &shareErr)
	require.Equal(t, [2]string{string(first), string(second)}, shareErr.Roles)
}

func TestLoggerSetDirectoryLeaseRejectsRolesThatResolveToSameFile(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*pkglogger.FileLoggerConfig)
		wantRoles [2]loggerType
	}{
		{
			name: "debug prefix is empty",
			configure: func(cfg *pkglogger.FileLoggerConfig) {
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
				cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = ""
			},
			wantRoles: [2]loggerType{DefaultLogger, DebugLogger},
		},
		{
			name: "error prefix is empty in default directory",
			configure: func(cfg *pkglogger.FileLoggerConfig) {
				cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
				cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
				cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = ""
			},
			wantRoles: [2]loggerType{DefaultLogger, ErrorLogger},
		},
		{
			name: "error and debug prefixes are equal in shared directory",
			configure: func(cfg *pkglogger.FileLoggerConfig) {
				cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
				cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
				cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "shared_"
				cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
				cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "shared_"
			},
			wantRoles: [2]loggerType{ErrorLogger, DebugLogger},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "same-file-role")
			tt.configure(&cfg)

			loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
			require.Nil(t, loggerSet)
			requireLoggerRolesShareFiles(t, err, tt.wantRoles[0], tt.wantRoles[1])
		})
	}
}

func TestDeliveryLoggerSetDirectoryLeaseRejectsRolesThatResolveToSameFile(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-same-file-role")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
	cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "shared_"
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true
	cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "shared_"

	loggerSet, err := NewDeliveryLoggerSet(
		NewDeliveryFileLoggerConfigAdapter(cfg),
		nil,
		nil,
	)
	require.Nil(t, loggerSet)
	requireLoggerRolesShareFiles(t, err, ErrorLogger, DebugLogger)
}
