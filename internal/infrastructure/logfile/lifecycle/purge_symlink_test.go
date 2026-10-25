package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestLifecyclePurgeIgnoresNonRegularLogEntries(t *testing.T) {
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
			cfg := rolePurgeRunnerTestConfig(t)
			cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
			cfg.FsstoreConfig.Lifecycle.EnableFileCompress = false
			regularCount := 1
			if test.generation {
				cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 2
				regularCount = 2
			}
			now := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
			lifecycles := newRolePurgeTestLifecycles(t, cfg, now, false)
			require.Len(t, lifecycles, 1)
			managers := lifecycles[0].fileSeriesManagers
			require.Len(t, managers, 1)
			pattern := managers[0].FileNamePattern()
			activePath := writeRunnerRolePurgeFile(t, pattern, now, 1, 1)
			var regularPaths []string
			for index := 1; index <= regularCount; index++ {
				modified := now.AddDate(0, 0, -2).Add(time.Duration(index) * time.Minute)
				regularPaths = append(regularPaths, writeRunnerRolePurgeFile(t, pattern, modified, index, 1))
			}
			entryPath := filepath.Join(pattern.Dir, fmt.Sprintf("%s%s_%s_1.log",
				pattern.Prefix, pattern.Target, now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat)))
			var externalPath string
			var externalSize int64
			if test.directory {
				require.NoError(t, os.Mkdir(entryPath, 0o750))
			} else {
				externalPath = filepath.Join(resolvedTempDirForTest(t), "external.log")
				externalSize = 1
				if !test.generation {
					// 最新の外部ファイルへのリンクを数えると、古い正常ログが先にpurgeされる。
					externalSize = int64(cfg.FsstoreConfig.Lifecycle.MaxDirSize) + 1
				}
				require.NoError(t, os.WriteFile(externalPath, nil, 0o600))
				require.NoError(t, os.Truncate(externalPath, externalSize))
				require.NoError(t, os.Chtimes(externalPath, now, now))
				testutil.Symlink(t, externalPath, entryPath)
			}

			report, err := lifecycles[0].RunOnce(context.Background())
			require.NoError(t, err)

			for _, path := range regularPaths {
				require.FileExists(t, path, "nonregular entries must not consume capacity or generations")
			}
			require.FileExists(t, activePath)
			require.False(t, report.Purge.ByGeneration.Ran)
			require.False(t, report.Purge.ByDirSize.Ran)
			require.Empty(t, report.Purge.ByAge.RemovalResults)
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
