package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	pkgclock "github.com/go-jappe/heterolog/pkg/clock"

	"github.com/stretchr/testify/require"
)

func TestLogFileLifecyclesPurgeUsesFileNameDateAndIndex(t *testing.T) {
	// Clockが返すJSTの21日を、そのままファイル名・保持期限の判定に使います。
	location := time.FixedZone("JST", 9*60*60)
	now := time.Date(2026, time.September, 21, 0, 30, 0, 0, location)
	type logFile struct {
		name       string
		modifiedAt time.Time
	}
	tests := []struct {
		name           string
		maxAgeDays     int
		maxBackups     int
		maxDirSize     uint64
		fileSize       int64
		files          []logFile
		removedNames   []string
		removalResults func(PurgeReport) PurgeStepReport
	}{
		{
			name:       "age_keeps_local_cutoff_day_regardless_of_mtime",
			maxAgeDays: 2,
			maxBackups: 100,
			maxDirSize: 20 << 20,
			files: []logFile{
				{"20260918_99.log", now},
				{"20260919_1.log", now.AddDate(0, 0, -30)},
				{"20260918_2.log.gz", now},
				{"20260919_10.log.gz", now.AddDate(0, 0, -30)},
				{"20260920_1.log.gz", now.AddDate(0, 0, -30)},
				{"20260921_99.log", now.AddDate(0, 0, -30)},
			},
			removedNames: []string{"20260918_2.log.gz", "20260918_99.log"},
			removalResults: func(report PurgeReport) PurgeStepReport {
				return report.ByAge
			},
		},
		{
			// 世代は未圧縮・圧縮形式を問わず日付、数値indexの順に数え、mtimeは使いません。
			name:       "generation_counts_date_then_numeric_index_across_raw_and_compressed_files",
			maxAgeDays: 36500,
			maxBackups: 2,
			maxDirSize: 20 << 20,
			files: []logFile{
				{"20260921_2.log", now.Add(time.Hour)},
				{"20260921_10.log", now.Add(-time.Hour)},
				{"20260921_10.log.gz", now.Add(-3 * time.Hour)},
				{"20260921_11.log", now.Add(-2 * time.Hour)},
				{"20260920_2.log.gz", now.Add(time.Hour)},
				{"20260920_10.log.gz", now.Add(-time.Hour)},
				{"20260920_11.log.gz", now.Add(-2 * time.Hour)},
				{"20260921_99.log", now.AddDate(0, 0, -30)},
			},
			removedNames: []string{"20260920_2.log.gz", "20260920_10.log.gz", "20260920_11.log.gz", "20260921_2.log"},
			removalResults: func(report PurgeReport) PurgeStepReport {
				return report.ByGeneration
			},
		},
		{
			name:       "capacity_uses_date_then_numeric_index_and_preserves_latest_raw",
			maxAgeDays: 36500,
			maxBackups: 100,
			maxDirSize: 10 << 20,
			fileSize:   5 << 20,
			files: []logFile{
				{"20260920_99.log", now.Add(3 * time.Hour)},
				{"20260921_2.log", now.Add(2 * time.Hour)},
				{"20260921_10.log", now.Add(time.Hour)},
				{"20260921_99.log", now.AddDate(0, 0, -30)},
			},
			removedNames: []string{"20260920_99.log", "20260921_2.log"},
			removalResults: func(report PurgeReport) PurgeStepReport {
				return report.ByDirSize
			},
		},
	}
	for _, delivery := range []bool{false, true} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("delivery=%t/%s", delivery, tt.name), func(t *testing.T) {
				cfg := rolePurgeRunnerTestConfig(t)
				cfg.FsstoreConfig.Lifecycle.MaxFileAge = tt.maxAgeDays
				cfg.FsstoreConfig.Lifecycle.MaxFileBackups = tt.maxBackups
				cfg.FsstoreConfig.Lifecycle.MaxDirSize = tt.maxDirSize
				var lifecycles []*DirectoryLifecycle
				var err error
				if delivery {
					lifecycles, err = NewDeliveryFileLifecycles(DeliveryFileOptions{
						Config: deliveryRolePurgeTestConfig(cfg),
						Clock:  pkgclock.NewFixedClock(now),
					})
				} else {
					lifecycles, err = NewLogFileLifecycles(LogFileOptions{
						Config: cfg,
						Clock:  pkgclock.NewFixedClock(now),
					})
				}
				require.NoError(t, err)
				require.Len(t, lifecycles, 3)
				for _, worker := range lifecycles {
					require.Len(t, worker.fileSeriesManagers, 1)
					pattern := worker.fileSeriesManagers[0].FileNamePattern()
					t.Run(filepath.Base(pattern.Dir), func(t *testing.T) {
						filePath := func(name string) string {
							return filepath.Join(pattern.Dir, pattern.Prefix+pattern.Target+"_"+name)
						}
						for _, file := range tt.files {
							path := filePath(file.name)
							// 圧縮・uploadは無効にし、purgeの名前とmtimeが逆になるようにします。
							require.NoError(t, os.WriteFile(path, []byte("log\n"), 0o640))
							if tt.fileSize > 0 {
								require.NoError(t, os.Truncate(path, tt.fileSize))
							}
							require.NoError(t, os.Chtimes(path, file.modifiedAt, file.modifiedAt))
						}

						report, err := worker.RunOnce(context.Background())
						require.NoError(t, err)
						step := tt.removalResults(report.Purge)
						require.True(t, step.Ran)
						removedPaths := make([]string, 0, len(step.RemovalResults))
						for _, result := range step.RemovalResults {
							require.NoError(t, result.Err)
							removedPaths = append(removedPaths, result.File.Path)
						}
						wantRemovedPaths := make([]string, 0, len(tt.removedNames))
						for _, name := range tt.removedNames {
							wantRemovedPaths = append(wantRemovedPaths, filePath(name))
						}
						require.Equal(t, wantRemovedPaths, removedPaths)
						for _, file := range tt.files {
							if slices.Contains(tt.removedNames, file.name) {
								require.NoFileExists(t, filePath(file.name))
							} else {
								require.FileExists(t, filePath(file.name))
							}
						}
					})
				}
			})
		}
	}
}
