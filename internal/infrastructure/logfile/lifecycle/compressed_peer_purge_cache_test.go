package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestFileSeriesPurgeImmediatelyForgetsRemovedCompressedPeers(t *testing.T) {
	for _, step := range []string{"age", "generation"} {
		for _, failFirstRemoval := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/removal_failure=%t", step, failFirstRemoval), func(t *testing.T) {
				root := resolvedTempDirForTest(t)
				const target = "purge-peer-cache"
				directory := filepath.Join(root, target, "logs")
				require.NoError(t, os.MkdirAll(directory, 0o700))
				now := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
				period := now.Format("20060102")
				maxAge, maxBackups := 0, 1
				if step == "age" {
					period = now.AddDate(0, 0, -2).Format("20060102")
					maxAge, maxBackups = 1, 0
				}
				first := filepath.Join(directory, target+"_"+period+"_1.log.gz")
				second := filepath.Join(directory, target+"_"+period+"_2.log.gz")
				retained := filepath.Join(directory, target+"_20260915_3.log.gz")
				active := filepath.Join(directory, target+"_20260915_4.log")
				for _, path := range []string{first, second, retained, active} {
					require.NoError(t, os.WriteFile(path, []byte("data"), 0o600))
				}
				manager := NewFileSeriesManager(FileSeriesManagerOptions{
					Config: testFileSeriesManagerConfig(root, target, maxBackups, maxAge, 0, false),
					FileName: logpattern.FileNamePattern{
						Dir: directory, Target: target, Extension: "log",
					},
					Clock: pkgclock.NewFixedClock(now),
				})
				seedPurgePeerCache(t, manager, first, second, retained)
				if failFirstRemoval {
					// 観測後に一件だけ削除不能にし、部分成功でも成功分のcacheが
					// エラー返却より前に破棄されることを確認します。
					manager.store.Tracer = purgeConcurrencyTracer{
						spanName: purgeConcurrencySpanName(step),
						beforeStart: func() {
							require.NoError(t, os.Remove(first))
							require.NoError(t, os.Mkdir(first, 0o700))
						},
					}
				}
				var result fileSeriesPurgeResult
				err := manager.directoryGuard.RunWithLifecycleLock(func() error {
					var err error
					result, err = manager.purgeFileSeriesByAgeAndGeneration(context.Background(), nil)
					return err
				})
				if failFirstRemoval {
					// 削除前の再確認で通常fileでなくなった候補は、削除せずに失敗として返します。
					var notRegular *infrafsstore.NotRegularFileError
					require.ErrorAs(t, err, &notRegular)
					require.ErrorIs(t, err, infrafsstore.ErrNotRegularFile)
					require.Equal(t, first, notRegular.Path)
					require.Contains(t, manager.verifiedCompressedPeers, pathidentity.ComparisonKey(first))
				} else {
					require.NoError(t, err)
					require.NoFileExists(t, first)
					require.NotContains(t, manager.verifiedCompressedPeers, pathidentity.ComparisonKey(first))
				}
				purgeReport := result.report.ByAge
				if step == "generation" {
					purgeReport = result.report.ByGeneration
				}
				require.True(t, purgeReport.Ran)
				require.Len(t, purgeReport.RemovalResults, 2)
				require.NoFileExists(t, second)
				// ここまで再走査しません。次のscanによる回収で不備を隠さないためです。
				require.NotContains(t, manager.verifiedCompressedPeers, pathidentity.ComparisonKey(second))
				require.FileExists(t, retained)
				require.Contains(t, manager.verifiedCompressedPeers, pathidentity.ComparisonKey(retained))
			})
		}
	}
}

func TestDirectorySizePurgeImmediatelyForgetsRemovedPeersInEveryManager(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "directory-purge-peer-cache"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o700))
	now := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	var managers []*FileSeriesManager
	var removedPaths, retainedPaths []string
	for _, prefix := range []string{"", "error_"} {
		first := filepath.Join(directory, prefix+target+"_20260915_1.log.gz")
		second := filepath.Join(directory, prefix+target+"_20260915_2.log.gz")
		active := filepath.Join(directory, prefix+target+"_20260915_3.log")
		for _, path := range []string{first, second, active} {
			require.NoError(t, os.WriteFile(path, []byte("data"), 0o600))
		}
		manager := NewFileSeriesManager(FileSeriesManagerOptions{
			Config: testFileSeriesManagerConfig(root, target, 0, 0, 16, false),
			FileName: logpattern.FileNamePattern{
				Dir: directory, Prefix: prefix, Target: target, Extension: "log",
			},
			Clock: pkgclock.NewFixedClock(now),
		})
		seedPurgePeerCache(t, manager, first, second)
		managers = append(managers, manager)
		removedPaths = append(removedPaths, first)
		retainedPaths = append(retainedPaths, second)
	}
	lifecycle, err := NewDirectoryLifecycle(managers...)
	require.NoError(t, err)
	var report PurgeStepReport
	err = lifecycle.directoryGuard.RunWithLifecycleLock(func() error {
		var err error
		report, err = lifecycle.runDirectorySizePurge(context.Background(), nil)
		return err
	})
	require.NoError(t, err)
	require.True(t, report.Ran)
	require.Len(t, report.RemovalResults, 2)
	// directory全体のpurge結果が、各系列自身のcacheへ反映されていることを
	// 次の系列走査を呼ばずに確認します。
	for index, manager := range managers {
		require.NoFileExists(t, removedPaths[index])
		require.NotContains(t, manager.verifiedCompressedPeers, pathidentity.ComparisonKey(removedPaths[index]))
		require.FileExists(t, retainedPaths[index])
		require.Contains(t, manager.verifiedCompressedPeers, pathidentity.ComparisonKey(retainedPaths[index]))
	}
}

func seedPurgePeerCache(t *testing.T, manager *FileSeriesManager, paths ...string) {
	t.Helper()
	for _, path := range paths {
		info, err := os.Stat(path)
		require.NoError(t, err)
		manager.verifiedCompressedPeers[pathidentity.ComparisonKey(path)] = compressedPeerFingerprint{
			size:            info.Size(),
			modTimeUnixNano: info.ModTime().UnixNano(),
		}
	}
}
