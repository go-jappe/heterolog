package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

// failure archiveの保持日数・保持世代数・ディレクトリ最大サイズの0は上限なし（その条件では削除しない）です。
// 構築時の検証で受け付け、runnerの周期処理もlog-recoveryの復旧も、最小値の上限なら削除される
// 期限切れ・超過世代・容量超過のfileを削除しません。
func TestFailureArchiveLifecycleTreatsZeroRetentionLimitsAsUnlimited(t *testing.T) {
	now := time.Date(2026, time.September, 21, 12, 0, 0, 0, time.UTC)
	const fileSize = 4 * 1024 * 1024
	// 5世代・合計20 MiBで、最古は51日前です。最小値（2世代・1日・10 MiB）の上限なら
	// いずれの条件でも削除対象を含みます。
	names := []string{
		"20260801_1.jsonl",
		"20260802_1.jsonl",
		"20260919_1.jsonl",
		"20260920_1.jsonl",
		"20260921_1.jsonl",
	}
	for _, mode := range []struct {
		name  string
		build func(FailureArchiveOptions) ([]*DirectoryLifecycle, error)
		run   func(*testing.T, *DirectoryLifecycle) RunReport
	}{
		{
			name:  "runner",
			build: NewFailureArchiveLifecycles,
			run: func(t *testing.T, lifecycle *DirectoryLifecycle) RunReport {
				report, err := lifecycle.RunOnce(context.Background())
				require.NoError(t, err)
				return report
			},
		},
		{
			name: "log recovery",
			build: func(opts FailureArchiveOptions) ([]*DirectoryLifecycle, error) {
				return NewFailureArchiveRecovery(opts)
			},
			run: func(t *testing.T, lifecycle *DirectoryLifecycle) RunReport {
				report, err := lifecycle.RecoverOnce(context.Background())
				require.NoError(t, err)
				require.NoError(t, boundedwait.Close(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(lifecycle)))
				return report
			},
		},
	} {
		t.Run(mode.name, func(t *testing.T) {
			cfg := newFailureArchiveLifecycleDurableConfig(t, objectstorage.UploadTargetConfig{})
			// 圧縮・uploadを無効にし、保持上限0によるpurgeの有無だけを検証します。
			cfg.EnableFileCompress = false
			cfg.MaxFileAge = 0
			cfg.MaxFileBackups = 0
			cfg.MaxDirSize = 0
			lifecycles, err := mode.build(FailureArchiveOptions{
				Config: newFailureArchiveTestConfigAdapter(cfg),
				Clock:  pkgclock.NewFixedClock(now),
			})
			require.NoError(t, err)
			require.Len(t, lifecycles, 2)

			for index, category := range []string{cfg.DirCaseRetryable, cfg.DirCasePermanent} {
				t.Run(category, func(t *testing.T) {
					lifecycle := lifecycles[index]
					directory, err := lifecycle.DirectoryPath()
					require.NoError(t, err)
					paths := make([]string, 0, len(names))
					for _, name := range names {
						path := filepath.Join(directory, cfg.Target+"_"+name)
						require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o640))
						require.NoError(t, os.Truncate(path, fileSize))
						paths = append(paths, path)
					}

					report := mode.run(t, lifecycle)
					require.False(t, report.Purge.ByAge.Ran, "age purge must not run with MaxFileAge=0")
					require.False(t, report.Purge.ByGeneration.Ran, "generation purge must not run with MaxFileBackups=0")
					require.False(t, report.Purge.ByDirSize.Ran, "directory size purge must not run with MaxDirSize=0")
					for _, path := range paths {
						require.FileExists(t, path)
					}
				})
			}
		})
	}
}
