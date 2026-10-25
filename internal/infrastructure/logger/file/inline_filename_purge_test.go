package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestInlineFilenameAgePurgeKeepsCalendarCutoffRegardlessOfModTime(t *testing.T) {
	// 本文表示はJSTの5月6日でも、ClockはUTCの5月5日です。
	// 保持期限はClock基準の5月4日とし、5月3日以前だけを削除します。
	now := time.Date(2042, time.May, 5, 15, 30, 0, 0, time.UTC)
	for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		t.Run(string(role), func(t *testing.T) {
			logger := newInlineFilenamePurgeLogger(t, role, now, func(cfg *pkglogger.FileLoggerConfig) {
				cfg.FsstoreConfig.Lifecycle.MaxFileAge = 1
			})
			var expiredPaths, retainedPaths []string
			for _, extension := range []string{"log", "log.gz"} {
				expiredPaths = append(expiredPaths, writeInlineFilenamePurgeFile(
					t, logger, "20420503", 2, extension, 1, now.AddDate(1, 0, 0),
				))
				for _, date := range []string{"20420504", "20420505", "20420506"} {
					retainedPaths = append(retainedPaths, writeInlineFilenamePurgeFile(
						t, logger, date, 2, extension, 1, now.AddDate(-1, 0, 0),
					))
				}
			}

			require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
			for _, path := range expiredPaths {
				require.NoFileExists(t, path)
			}
			for _, path := range retainedPaths {
				require.FileExists(t, path)
			}
			require.FileExists(t, logger.Fsstore.ActiveFilePath)
			require.Equal(t, "UTC", logger.inlinePurge.lastAgePurgeDate.Location().String())
			require.Equal(t, "20420505", logger.inlinePurge.lastAgePurgeDate.Format("20060102"))
		})
	}
}

func TestInlineFilenameGenerationPurgeOrdersDateAndNumericIndexPerSeries(t *testing.T) {
	now := time.Date(2042, time.May, 6, 12, 0, 0, 0, time.UTC)
	for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		t.Run(string(role), func(t *testing.T) {
			logger := newInlineFilenamePurgeLogger(t, role, now, func(cfg *pkglogger.FileLoggerConfig) {
				cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 2
			})
			var expiredPaths, retainedPaths []string
			for _, extension := range []string{"log", "log.gz"} {
				// An earlier date outranks its larger numeric index.
				expiredPaths = append(expiredPaths, writeInlineFilenamePurgeFile(
					t, logger, "20420505", 100, extension, 1, now,
				))
				for _, index := range []int{2, 10, 11} {
					// Both mtime order and lexical index order disagree with
					// the intended retention of generations 10 and 11.
					path := writeInlineFilenamePurgeFile(
						t, logger, "20420506", index, extension, 1, now.Add(-time.Duration(index)*time.Hour),
					)
					if index == 2 {
						expiredPaths = append(expiredPaths, path)
					} else {
						retainedPaths = append(retainedPaths, path)
					}
				}
			}

			require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
			for _, path := range expiredPaths {
				require.NoFileExists(t, path)
			}
			// Raw and gzip each retain two backups, independently of each
			// other and of the protected active file.
			for _, path := range retainedPaths {
				require.FileExists(t, path)
			}
			require.FileExists(t, logger.Fsstore.ActiveFilePath)
		})
	}
}

func TestInlineFilenameCapacityPurgeOrdersNamesAndCountsProtectedFiles(t *testing.T) {
	now := time.Date(2042, time.May, 6, 12, 0, 0, 0, time.UTC)
	const fileSize int64 = 4 << 20
	for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		for _, extension := range []string{"log", "log.gz"} {
			t.Run(string(role)+"/"+extension, func(t *testing.T) {
				logger := newInlineFilenamePurgeLogger(t, role, now, func(cfg *pkglogger.FileLoggerConfig) {
					cfg.FsstoreConfig.Lifecycle.MaxDirSize = uint64(3 * fileSize)
				})
				activePath := logger.Fsstore.ActiveFilePath
				// Use the existing append-only handle rather than truncating
				// it, which is not supported by that handle on Windows.
				logger.Fsstore.Mu.Lock()
				written, err := logger.Fsstore.File.Write(make([]byte, fileSize))
				logger.Fsstore.Mu.Unlock()
				require.NoError(t, err)
				require.Equal(t, fileSize, int64(written))
				oldModTime := now.AddDate(-1, 0, 0)
				require.NoError(t, os.Chtimes(activePath, oldModTime, oldModTime))

				retainedPath := writeInlineFilenamePurgeFile(t, logger, "20420503", 1, extension, fileSize, oldModTime)
				olderDatePath := writeInlineFilenamePurgeFile(t, logger, "20420505", 20, extension, fileSize, now.Add(-time.Hour))
				lowerIndexPath := writeInlineFilenamePurgeFile(t, logger, "20420506", 2, extension, fileSize, now.Add(-2*time.Hour))
				newestPath := writeInlineFilenamePurgeFile(t, logger, "20420506", 10, extension, fileSize, now.Add(-3*time.Hour))
				owner := appendlog.NewDirectoryGuardToken()
				logger.lifecycleGuard.ProtectPaths(owner, retainedPath)
				t.Cleanup(func() { logger.lifecycleGuard.RemoveToken(owner) })

				require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
				require.NoFileExists(t, olderDatePath)
				require.NoFileExists(t, lowerIndexPath)
				// The protected 8 MiB must count toward the 12 MiB limit:
				// otherwise the three unprotected files would all survive.
				var remainingBytes int64
				for _, path := range []string{activePath, retainedPath, newestPath} {
					info, err := os.Stat(path)
					require.NoError(t, err)
					require.Equal(t, fileSize, info.Size())
					remainingBytes += info.Size()
				}
				require.Equal(t, 3*fileSize, remainingBytes)
				logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
					require.Len(t, protected, 2)
					require.Contains(t, protected, pathidentity.AbsoluteOrCleanPath(activePath))
					require.Contains(t, protected, pathidentity.AbsoluteOrCleanPath(retainedPath))
				})
			})
		}
	}
}

func newInlineFilenamePurgeLogger(
	t *testing.T,
	role loggerType,
	now time.Time,
	mutate func(*pkglogger.FileLoggerConfig),
) *FileLogger {
	t.Helper()
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "filename-purge")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.TimeConfig.IsLocalTimeEnabled = true
	cfg.TimeConfig.TimeZone = "Asia/Tokyo"
	cfg.FsstoreConfig.Layout.DebugLogDir = "logs_debug"
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogDirEnabled = true
	cfg.FsstoreConfig.Lifecycle.MaxFileAge = 30
	cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 10
	if mutate != nil {
		mutate(&cfg)
	}
	set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
	logger, err := set.fileLogger(context.Background(), role)
	require.NoError(t, err)
	require.Equal(t, role, logger.logger)
	logger.Clock = pkgclock.NewFixedClock(now)
	return logger
}

func writeInlineFilenamePurgeFile(
	t *testing.T,
	logger *FileLogger,
	date string,
	index int,
	extension string,
	size int64,
	modified time.Time,
) string {
	t.Helper()
	pattern := logger.Fsstore.FileName
	path := filepath.Join(pattern.Dir, fmt.Sprintf("%s%s_%s_%d.%s", pattern.Prefix, pattern.Target, date, index, extension))
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	require.NoError(t, os.Truncate(path, size))
	require.NoError(t, os.Chtimes(path, modified, modified))
	return path
}
