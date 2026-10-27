package fsarchivehandler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

func TestInlineFilenameAgePurgeUsesCalendarDateAndIgnoresModTime(t *testing.T) {
	// Clockが返すJSTの5月6日を基準に、期限日の5月5日は残します。
	now := time.Date(2042, time.May, 6, 0, 30, 0, 0, time.FixedZone("JST", 9*60*60))
	for _, category := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		t.Run(string(category), func(t *testing.T) {
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.maxFileAge = 1
				cfg.maxFileBackups = 10
			})
			handler := pipeline.handler
			directory := failureArchiveDirectoryForFailureCategory(handler, category)
			activePath, err := pipeline.ActiveArchivePath(category)
			require.NoError(t, err)
			require.Equal(t, "test-app_20420506_1.jsonl", filepath.Base(activePath))

			var expiredPaths, retainedPaths []string
			for _, extension := range []string{"jsonl", "jsonl.gz", "jsonl.zst"} {
				expiredPath := filepath.Join(directory, "test-app_20420504_2."+extension)
				cutoffPath := filepath.Join(directory, "test-app_20420505_2."+extension)
				currentPath := filepath.Join(directory, "test-app_20420506_2."+extension)
				// 期限切れのfilenameには未来のmtimeを、保持する2つのfilenameには古いmtimeを
				// 与え、mtimeに基づく経過日数の判定ではこのtestが失敗するようにします。
				writeArchiveSizedFile(t, expiredPath, 1, now.AddDate(1, 0, 0))
				writeArchiveSizedFile(t, cutoffPath, 1, now.AddDate(-1, 0, 0))
				writeArchiveSizedFile(t, currentPath, 1, now.AddDate(-1, 0, 0))
				expiredPaths = append(expiredPaths, expiredPath)
				retainedPaths = append(retainedPaths, cutoffPath, currentPath)
			}

			require.NoError(t, handler.triggerArchiveCleanup(context.Background(), category))
			for _, path := range expiredPaths {
				require.NoFileExists(t, path)
			}
			for _, path := range append(retainedPaths, activePath) {
				require.FileExists(t, path)
			}
		})
	}
}

func TestInlineFilenameGenerationPurgeUsesNumericIndexPerSeries(t *testing.T) {
	now := time.Date(2042, time.May, 6, 12, 0, 0, 0, time.UTC)
	for _, category := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		t.Run(string(category), func(t *testing.T) {
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.maxFileAge = 30
				cfg.maxFileBackups = 2
			})
			handler := pipeline.handler
			directory := failureArchiveDirectoryForFailureCategory(handler, category)
			var expiredPaths, retainedPaths []string
			for _, extension := range []string{"jsonl", "jsonl.gz", "jsonl.zst"} {
				for _, index := range []int{2, 10, 11} {
					path := filepath.Join(directory, fmt.Sprintf("test-app_20420506_%d.%s", index, extension))
					// 10が2より前に並ぶ辞書順の罠を表面化させるとともに、
					// mtimeでもindexの順序を逆にします。
					writeArchiveSizedFile(t, path, 1, now.Add(-time.Duration(index)*time.Hour))
					if index == 2 {
						expiredPaths = append(expiredPaths, path)
					} else {
						retainedPaths = append(retainedPaths, path)
					}
				}
			}

			require.NoError(t, handler.triggerArchiveCleanup(context.Background(), category))
			for _, path := range expiredPaths {
				require.NoFileExists(t, path)
			}
			// raw、gzip、zstdは、それぞれ独立に2つのbackupを保持します。
			for _, path := range retainedPaths {
				require.FileExists(t, path)
			}
			activePath, err := pipeline.ActiveArchivePath(category)
			require.NoError(t, err)
			require.FileExists(t, activePath)
		})
	}
}

func TestInlineFilenameCapacityPurgeOrdersNamesAndKeepsProtectedBytes(t *testing.T) {
	now := time.Date(2042, time.May, 6, 12, 0, 0, 0, time.UTC)
	const fileSize int64 = 4 * 1024 * 1024
	for _, category := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		for _, extension := range []string{"jsonl", "jsonl.gz", "jsonl.zst"} {
			t.Run(string(category)+"/"+extension, func(t *testing.T) {
				pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
					cfg.maxFileAge = 30
					cfg.maxFileBackups = 10
					cfg.maxDirSize = uint64(3 * fileSize)
				})
				handler := pipeline.handler
				directory := failureArchiveDirectoryForFailureCategory(handler, category)
				activePath, err := pipeline.ActiveArchivePath(category)
				require.NoError(t, err)
				require.NoError(t, os.Truncate(activePath, fileSize))
				oldModTime := now.AddDate(-1, 0, 0)
				require.NoError(t, os.Chtimes(activePath, oldModTime, oldModTime))

				retainedPath := filepath.Join(directory, "test-app_20420503_1."+extension)
				olderDatePath := filepath.Join(directory, "test-app_20420505_20."+extension)
				lowerIndexPath := filepath.Join(directory, "test-app_20420506_2."+extension)
				newestPath := filepath.Join(directory, "test-app_20420506_10."+extension)
				writeArchiveSizedFile(t, retainedPath, fileSize, oldModTime)
				writeArchiveSizedFile(t, olderDatePath, fileSize, now.Add(-time.Hour))
				writeArchiveSizedFile(t, lowerIndexPath, fileSize, now.Add(-2*time.Hour))
				writeArchiveSizedFile(t, newestPath, fileSize, now.Add(-3*time.Hour))

				guard := handler.directoryForFailureCategory(category).guard
				owner := appendlog.NewDirectoryGuardToken()
				guard.ProtectPaths(owner, retainedPath)
				t.Cleanup(func() { guard.RemoveToken(owner) })

				require.NoError(t, handler.triggerArchiveCleanup(context.Background(), category))
				require.NoFileExists(t, olderDatePath)
				require.NoFileExists(t, lowerIndexPath)
				// activeと保持対象のbyteが、削除候補にならずに上限へ数えられる場合にだけ、
				// 20 MiBが12 MiBになります。
				var remainingBytes int64
				for _, path := range []string{activePath, retainedPath, newestPath} {
					info, err := os.Stat(path)
					require.NoError(t, err)
					require.Equal(t, fileSize, info.Size())
					remainingBytes += info.Size()
				}
				require.Equal(t, 3*fileSize, remainingBytes)
				assertArchivePathProtected(t, guard, activePath, true)
				assertArchivePathProtected(t, guard, retainedPath, true)
			})
		}
	}
}
