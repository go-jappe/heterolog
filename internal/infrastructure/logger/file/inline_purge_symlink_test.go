package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestInlinePurgeIgnoresNonRegularLogEntries(t *testing.T) {
	for _, test := range []struct {
		name       string
		directory  bool
		generation bool
	}{
		{name: "directory-size/symlink"},
		{name: "generation/symlink", generation: true},
		{name: "generation/directory", directory: true, generation: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "nonregular-purge")
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
			cfg.FsstoreConfig.Lifecycle.MaxFileAge = 30
			cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 10
			regularCount := 1
			if test.generation {
				cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 2
				regularCount = 2
			}
			logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
			require.NoError(t, err)
			closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
			now := time.Now().UTC().Truncate(24 * time.Hour).Add(12 * time.Hour)
			logger.Clock = pkgclock.NewFixedClock(now)
			var regularPaths []string
			for index := 1; index <= regularCount; index++ {
				modified := now.AddDate(0, 0, -2).Add(time.Duration(index) * time.Minute)
				regularPaths = append(regularPaths, writeRolePurgeFile(t, logger, modified, index, 1))
			}
			pattern := logger.Fsstore.FileName
			entryPath := filepath.Join(pattern.Dir, fmt.Sprintf("%s%s_%s_1.log",
				pattern.Prefix, pattern.Target, now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat)))
			var externalPath string
			var externalSize int64
			if test.directory {
				require.NoError(t, os.Mkdir(entryPath, 0o750))
			} else {
				externalPath = filepath.Join(t.TempDir(), "external.log")
				externalSize = 1
				if !test.generation {
					// リンク先だけで容量上限を超えると、古い正常ログを先に削除してしまう。
					externalSize = int64(cfg.FsstoreConfig.Lifecycle.MaxDirSize) + 1
				}
				require.NoError(t, os.WriteFile(externalPath, nil, 0o600))
				require.NoError(t, os.Truncate(externalPath, externalSize))
				require.NoError(t, os.Chtimes(externalPath, now, now))
				testutil.Symlink(t, externalPath, entryPath)
			}

			require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))

			for _, path := range regularPaths {
				require.FileExists(t, path, "nonregular entries must not consume capacity or generations")
			}
			require.FileExists(t, logger.Fsstore.ActiveFilePath)
			entryInfo, err := os.Lstat(entryPath)
			require.NoError(t, err, "nonregular entries must not be purge candidates")
			if test.directory {
				require.True(t, entryInfo.IsDir())
			} else {
				require.NotZero(t, entryInfo.Mode()&os.ModeSymlink)
				externalInfo, err := os.Stat(externalPath)
				require.NoError(t, err)
				require.Equal(t, externalSize, externalInfo.Size())
			}
		})
	}
}
