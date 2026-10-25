package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/stretchr/testify/require"
)

// macOSのFAT32・ExFATでは、拡張属性を保存する「._」の付随fileが各fileの隣に作られます。APFSは
// 付随fileを作らないため、同じ名前のfileを置いて再現します。付随fileもdirectoryのサイズに数えると、
// 上限を超えたままと判断して、本来残るログまで削除します。
func TestFileLoggerDirectorySizePurgeIgnoresAppleDoubleCompanions(t *testing.T) {
	ctx := context.Background()
	const (
		activeFileSize   = int64(4 * 1024 * 1024)
		inactiveFileSize = int64(2 * 1024 * 1024)
	)

	cfg := newTestFileLoggerConfig(t.TempDir(), "apple-double-purge", "apple-double-purge")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = fsstore.MinAllowedMaxDirSize
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	activePath := logger.Fsstore.ActiveFilePath
	logDir := filepath.Dir(activePath)
	require.NoError(t, os.Truncate(activePath, activeFileSize))
	baseTime := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(activePath, baseTime, baseTime))
	paths := []string{activePath}
	inactivePaths := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		path := filepath.Join(logDir, fmt.Sprintf("inactive_%s_%d.log", logger.Fsstore.FileName.Date, i+2))
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		require.NoError(t, os.Truncate(path, inactiveFileSize))
		modTime := baseTime.Add(time.Duration(4-i) * time.Minute)
		require.NoError(t, os.Chtimes(path, modTime, modTime))
		inactivePaths = append(inactivePaths, path)
		paths = append(paths, path)
	}
	for _, path := range paths {
		companion := filepath.Join(logDir, "._"+filepath.Base(path))
		require.NoError(t, os.WriteFile(companion, make([]byte, 4096), 0o600))
	}

	// 合計12MBを上限10MBへ収めるため、削除可能なfileのうち最古の2MBだけを削除します。
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))

	require.FileExists(t, activePath)
	require.NoFileExists(t, inactivePaths[0])
	for _, path := range inactivePaths[1:] {
		require.FileExists(t, path)
	}
}
