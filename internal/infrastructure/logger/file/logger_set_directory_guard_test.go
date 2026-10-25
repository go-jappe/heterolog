package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

// LoggerSetは同じroleの通常出力とinternal logを一つのFileLoggerへ集約します。
// 一方、分離されたdefault/error/debugは同じdirectory内の別loggerなので、inline purgeは
// DirectoryGuardが公開する全roleのactive pathを削除候補から除外し、path単位registryの
// byte Trackerを共有する必要があります。
func TestLoggerSetDirectoryGuardProtectsSeparatedActiveFilesDuringInlinePurge(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const (
		logDir = "shared-logger-directory"
		target = "logger-directory-guard"
	)

	cfg := newTestFileLoggerConfig(root, logDir, target)
	cfg.FsstoreConfig.Layout.ErrorLogDir = logDir
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = fsstore.MinAllowedMaxDirSize

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	defaultLogger, err := loggerSet.NewFileLogger(ctx)
	require.NoError(t, err)
	errorLogger, err := loggerSet.NewErrorFileLogger(ctx)
	require.NoError(t, err)
	debugLogger, err := loggerSet.NewDebugFileLogger(ctx)
	require.NoError(t, err)

	require.Same(t, defaultLogger.lifecycleGuard, errorLogger.lifecycleGuard)
	require.Same(t, defaultLogger.lifecycleGuard, debugLogger.lifecycleGuard)
	require.Same(
		t,
		defaultLogger.inlinePurge.directorySizeTracker,
		errorLogger.inlinePurge.directorySizeTracker,
	)
	require.Same(
		t,
		defaultLogger.inlinePurge.directorySizeTracker,
		debugLogger.inlinePurge.directorySizeTracker,
	)

	activePaths := []string{
		defaultLogger.Fsstore.ActiveFilePath,
		errorLogger.Fsstore.ActiveFilePath,
		debugLogger.Fsstore.ActiveFilePath,
	}
	protectedPaths := make(map[string]struct{}, len(activePaths))
	defaultLogger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		for path := range protected {
			protectedPaths[path] = struct{}{}
		}
	})
	for _, path := range activePaths {
		_, protected := protectedPaths[filepath.Clean(path)]
		require.True(t, protected, "active logger is not registered: %s", path)
	}

	// active fileだけでMaxDirSizeをわずかに超えるようにし、削除順ではactive fileが
	// inactive fileより先になる日付・indexにします。guardがなければactive fileが消える条件です。
	activeSize := int64(fsstore.MinAllowedMaxDirSize/3 + 1)
	activeModTime := time.Now().Add(-2 * time.Hour)
	for _, path := range activePaths {
		require.NoError(t, os.Truncate(path, activeSize))
		require.NoError(t, os.Chtimes(path, activeModTime, activeModTime))
	}

	inactivePath := filepath.Join(filepath.Dir(activePaths[0]), "inactive_"+defaultLogger.Fsstore.FileName.Date+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, nil, 0o600))
	require.NoError(t, os.Truncate(inactivePath, 1024*1024))
	inactiveModTime := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(inactivePath, inactiveModTime, inactiveModTime))

	require.NoError(t, purgeFileLoggerForTest(ctx, defaultLogger))

	require.NoFileExists(t, inactivePath)
	for _, path := range activePaths {
		require.FileExists(t, path)
	}
}
