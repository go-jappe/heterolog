package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestFileSeriesManagerUsesResolvedDirectoryAndSharedGuard(t *testing.T) {
	real, err := pathidentity.ResolveDirectory(resolvedTempDirForTest(t))
	require.NoError(t, err)
	alias := filepath.Join(resolvedTempDirForTest(t), "alias")
	testutil.Symlink(t, real, alias)
	cfg := testFileSeriesManagerConfig(real, "app", 10, 36500, 1<<20, false)
	fromAlias := NewFileSeriesManager(FileSeriesManagerOptions{
		Config: cfg, FileName: logpattern.FileNamePattern{Dir: alias, Target: "app", Extension: "log"},
	})
	fromReal := NewFileSeriesManager(FileSeriesManagerOptions{
		Config: cfg, FileName: logpattern.FileNamePattern{Dir: real, Target: "app", Extension: "log"},
	})
	require.Equal(t, fromReal.FileNamePattern(), fromAlias.FileNamePattern())
	require.Same(t, fromReal.directoryGuard, fromAlias.directoryGuard)
	_, err = NewDirectoryLifecycle(fromAlias, fromReal)
	require.ErrorIs(t, err, ErrDuplicateLifecyclePattern)

	// 構築後にリンク名が消えても、保持済みの実体を使い続けます。
	require.NoError(t, os.Remove(alias))
	_, err = fromAlias.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, real, fromAlias.FileNamePattern().Dir)
}

func TestFileSeriesManagerRejectsUnresolvableDirectoryBeforeRun(t *testing.T) {
	root := resolvedTempDirForTest(t)
	alias := filepath.Join(root, "broken")
	testutil.Symlink(t, filepath.Join(root, "absent"), alias)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		Config:   testFileSeriesManagerConfig(root, "app", 10, 36500, 1<<20, false),
		FileName: logpattern.FileNamePattern{Dir: filepath.Join(alias, "logs"), Target: "app", Extension: "log"},
	})
	require.Nil(t, manager.directoryGuard)
	_, err := NewDirectoryLifecycle(manager)
	require.ErrorIs(t, err, ErrInvalidFileName)
	_, err = manager.RunOnce(context.Background())
	require.ErrorIs(t, err, ErrInvalidFileName)
	require.NoDirExists(t, filepath.Join(root, "absent"))
}

func TestLogFileFactorySharesAliasRolesAndCheckpointIdentity(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	realBase, err := pathidentity.ResolveDirectory(cfg.FsstoreConfig.Layout.LogPath)
	require.NoError(t, err)
	alias := filepath.Join(resolvedTempDirForTest(t), "alias")
	testutil.Symlink(t, realBase, alias)
	fromReal, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	cfg.FsstoreConfig.Layout.LogPath = alias
	fromAlias, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	first := fromReal[0].fileSeriesManagers[0]
	second := fromAlias[0].fileSeriesManagers[0]
	require.Equal(t, first.FileNamePattern(), second.FileNamePattern())
	require.Same(t, first.directoryGuard, second.directoryGuard)
	require.Equal(t, checkpointStoreForFactoryTest(t, first).SourceID(), checkpointStoreForFactoryTest(t, second).SourceID())
	require.Same(t, appendlog.DirectorySizeTrackerForPath(first.FileNamePattern().Dir), appendlog.DirectorySizeTrackerForPath(second.FileNamePattern().Dir))

	// error/debugが同じaliasでも、defaultと実体が同じなら許可して一つへまとめます。
	layout := &cfg.FsstoreConfig.Layout
	roleAlias := filepath.Join(realBase, layout.Target, "alias-logs")
	testutil.Symlink(t, first.FileNamePattern().Dir, roleAlias)
	layout.IsSeparateErrorLogsEnabled = true
	layout.IsSeparateDebugLogsEnabled = true
	layout.IsSeparateDebugLogDirEnabled = true
	layout.ErrorLogDir, layout.DebugLogDir = "alias-logs", "alias-logs"
	layout.SeparateErrorLogsPrefix, layout.SeparateDebugLogsPrefix = "error_", "debug_"
	cfg.FsstoreConfig.Lifecycle.ErrorPurge = &pkgfsstore.FilePurgeConfig{
		MaxFileAge: 30, MaxFileBackups: 6, MaxDirSize: 0,
	}
	cfg.FsstoreConfig.Lifecycle.DebugPurge = &pkgfsstore.FilePurgeConfig{
		MaxFileAge: 2, MaxFileBackups: 3, MaxDirSize: 1,
	}
	shared, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.NoError(t, err)
	require.Len(t, shared, 1)
	require.Len(t, shared[0].fileSeriesManagers, 3)
	for _, manager := range shared[0].fileSeriesManagers {
		require.Equal(t, cfg.FsstoreConfig.Lifecycle.MaxDirSize, manager.Config.Purge().MaxDirSize())
	}
	require.Equal(t, 30, shared[0].fileSeriesManagers[1].Config.Purge().MaxFileAge())
	require.Equal(t, 6, shared[0].fileSeriesManagers[1].Config.Purge().MaxFileBackups())
	require.Equal(t, 2, shared[0].fileSeriesManagers[2].Config.Purge().MaxFileAge())
	require.Equal(t, 3, shared[0].fileSeriesManagers[2].Config.Purge().MaxFileBackups())
	require.Zero(t, cfg.FsstoreConfig.Lifecycle.ErrorPurge.MaxDirSize)

	// 別の実体でerror/debugだけを共有する配置は、alias経由でも拒否します。
	cfg.FsstoreConfig.Lifecycle.ErrorPurge = nil
	cfg.FsstoreConfig.Lifecycle.DebugPurge = nil
	require.NoError(t, os.Remove(roleAlias))
	separateRoleDirectory := resolvedTempDirForTest(t)
	testutil.Symlink(t, separateRoleDirectory, roleAlias)
	_, err = NewLogFileLifecycles(LogFileOptions{Config: cfg})
	var conflict *RoleDirectoryConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, [2]string{"error", "debug"}, conflict.SharedRoles)
	// aliasではなく、error/debugが実際に共有する実体directoryを示します。
	require.Equal(t, separateRoleDirectory, conflict.SharedDirectory)
	require.Equal(t, first.FileNamePattern().Dir, conflict.OtherDirectory)
}

func TestFailureArchiveFactoryKeepsCheckpointIdentityThroughAlias(t *testing.T) {
	cfg := newFailureArchiveLifecycleDurableConfig(t, validFailureArchiveUploadTargetConfig())
	real, err := pathidentity.ResolveDirectory(resolvedTempDirForTest(t))
	require.NoError(t, err)
	cfg.FailureFileArchivePath = real
	first, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: newFailureArchiveTestConfigAdapter(cfg)})
	require.NoError(t, err)
	alias := filepath.Join(resolvedTempDirForTest(t), "alias")
	testutil.Symlink(t, real, alias)
	cfg.FailureFileArchivePath = alias
	second, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: newFailureArchiveTestConfigAdapter(cfg)})
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Len(t, second, 2)
	for index := range first {
		a, b := first[index].fileSeriesManagers[0], second[index].fileSeriesManagers[0]
		require.Equal(t, a.FileNamePattern(), b.FileNamePattern())
		require.Same(t, a.directoryGuard, b.directoryGuard)
		require.Equal(t, checkpointStoreForFactoryTest(t, a).SourceID(), checkpointStoreForFactoryTest(t, b).SourceID())
	}
}

func TestLogFileFactoryRejectsBrokenAliasWithoutCreatingDirectories(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{}
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{}
	root := resolvedTempDirForTest(t)
	alias := filepath.Join(root, "broken")
	testutil.Symlink(t, filepath.Join(root, "missing"), alias)
	cfg.FsstoreConfig.Layout.LogPath = alias
	_, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
	require.Error(t, err)
	require.NoDirExists(t, filepath.Join(root, "missing"))
}
