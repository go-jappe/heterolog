package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/objectstorage"

	"github.com/stretchr/testify/require"
)

func TestFailureArchiveLifecyclePurgeUsesFileNameDateAndIndex(t *testing.T) {
	// Clockが返すJSTの21日を、そのままファイル名・保持期限の判定に使います。
	location := time.FixedZone("JST", 9*60*60)
	now := time.Date(2026, time.September, 21, 0, 30, 0, 0, location)
	type archiveFile struct {
		name       string
		modifiedAt time.Time
	}
	tests := []struct {
		name           string
		maxAgeDays     int
		maxBackups     int
		maxDirSize     uint64
		fileSize       int64
		files          []archiveFile
		removedNames   []string
		removalResults func(PurgeReport) PurgeStepReport
	}{
		{
			name:       "age_uses_local_calendar_date_and_keeps_cutoff_day",
			maxAgeDays: 2,
			maxBackups: 100,
			maxDirSize: 20 * 1024 * 1024,
			files: []archiveFile{
				{"20260918_99.jsonl", now},
				{"20260919_1.jsonl", now.AddDate(0, 0, -30)},
				{"20260918_2.jsonl.gz", now},
				{"20260919_10.jsonl.gz", now.AddDate(0, 0, -30)},
				{"20260920_1.jsonl.gz", now.AddDate(0, 0, -30)},
				{"20260921_99.jsonl", now.AddDate(0, 0, -30)},
			},
			removedNames: []string{"20260918_2.jsonl.gz", "20260918_99.jsonl"},
			removalResults: func(report PurgeReport) PurgeStepReport {
				return report.ByAge
			},
		},
		{
			// 世代は未圧縮・圧縮形式を問わず日付、数値indexの順に数え、mtimeは使いません。
			name:       "generation_counts_date_then_numeric_index_across_raw_and_compressed_files",
			maxAgeDays: 36500,
			maxBackups: 2,
			maxDirSize: 20 * 1024 * 1024,
			files: []archiveFile{
				{"20260921_2.jsonl", now.Add(time.Hour)},
				{"20260921_10.jsonl", now.Add(-time.Hour)},
				{"20260921_10.jsonl.gz", now.Add(-3 * time.Hour)},
				{"20260921_11.jsonl", now.Add(-2 * time.Hour)},
				{"20260920_2.jsonl.gz", now.Add(time.Hour)},
				{"20260920_10.jsonl.gz", now.Add(-time.Hour)},
				{"20260920_11.jsonl.gz", now.Add(-2 * time.Hour)},
				{"20260921_99.jsonl", now.Add(-2 * time.Hour)},
			},
			removedNames: []string{"20260920_2.jsonl.gz", "20260920_10.jsonl.gz", "20260920_11.jsonl.gz", "20260921_2.jsonl"},
			removalResults: func(report PurgeReport) PurgeStepReport {
				return report.ByGeneration
			},
		},
		{
			name:       "capacity_uses_date_then_numeric_index_and_preserves_latest_raw",
			maxAgeDays: 36500,
			maxBackups: 100,
			maxDirSize: 10 * 1024 * 1024,
			fileSize:   5 * 1024 * 1024,
			files: []archiveFile{
				{"20260920_99.jsonl", now.Add(3 * time.Hour)},
				{"20260921_2.jsonl", now.Add(2 * time.Hour)},
				{"20260921_10.jsonl", now.Add(time.Hour)},
				{"20260921_99.jsonl", now.AddDate(0, 0, -30)},
			},
			removedNames: []string{"20260920_99.jsonl", "20260921_2.jsonl"},
			removalResults: func(report PurgeReport) PurgeStepReport {
				return report.ByDirSize
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newFailureArchiveLifecycleDurableConfig(t, objectstorage.UploadTargetConfig{})
			cfg.EnableFileCompress = false
			cfg.MaxFileAge = tt.maxAgeDays
			cfg.MaxFileBackups = tt.maxBackups
			cfg.MaxDirSize = tt.maxDirSize
			lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{
				Config: newFailureArchiveTestConfigAdapter(cfg),
				Clock:  pkgclock.NewFixedClock(now),
			})
			require.NoError(t, err)
			require.Len(t, lifecycles, 2)
			for index, category := range []string{cfg.DirCaseRetryable, cfg.DirCasePermanent} {
				t.Run(category, func(t *testing.T) {
					worker := lifecycles[index]
					directory, err := worker.DirectoryPath()
					require.NoError(t, err)
					for _, file := range tt.files {
						path := filepath.Join(directory, cfg.Target+"_"+file.name)
						// 圧縮・uploadを無効にし、purgeのファイル名判定だけを検証します。
						require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o640))
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
						wantRemovedPaths = append(wantRemovedPaths, filepath.Join(directory, cfg.Target+"_"+name))
					}
					require.Equal(t, wantRemovedPaths, removedPaths)
					for _, file := range tt.files {
						path := filepath.Join(directory, cfg.Target+"_"+file.name)
						if slices.Contains(tt.removedNames, file.name) {
							require.NoFileExists(t, path)
						} else {
							require.FileExists(t, path)
						}
					}
				})
			}
		})
	}
}
