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
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFileLoggersSkipLatestRawSymlinkOnStartupAndRotation(t *testing.T) {
	for _, contract := range []string{"file", "delivery"} {
		for _, phase := range []string{"startup", "daily_rotation", "size_rotation"} {
			for _, dangling := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/dangling=%t", contract, phase, dangling), func(t *testing.T) {
					root, err := filepath.EvalSymlinks(t.TempDir())
					require.NoError(t, err)
					const target = "symlink-generation"
					directory := filepath.Join(root, target, "logs")
					require.NoError(t, os.MkdirAll(directory, 0o750))
					linkTarget := filepath.Join(t.TempDir(), "external.log")
					const targetContent = "existing external content\n"
					if !dangling {
						require.NoError(t, os.WriteFile(linkTarget, []byte(targetContent), 0o600))
					}

					seedPeriod := func(period string) {
						// The latest matching filename remains authoritative even when it
						// is a link; the writer must not fall back to the regular _7 file.
						lowerPath := filepath.Join(directory, target+"_"+period+"_7.log")
						require.NoError(t, os.WriteFile(lowerPath, []byte("earlier generation\n"), 0o600))
						testutil.Symlink(t, linkTarget, filepath.Join(directory, target+"_"+period+"_99.log"))
					}
					if phase == "startup" {
						now := time.Now().UTC()
						seedPeriod(now.Format(pkgfsstore.FileDateFormat))
						// Also seed tomorrow so crossing midnight during construction is safe.
						seedPeriod(now.AddDate(0, 0, 1).Format(pkgfsstore.FileDateFormat))
					}

					writer := newRawSymlinkTestWriter(t, contract, root, target)
					periodStart, err := time.Parse(pkgfsstore.FileDateFormat, writer.store.FileName.Date)
					require.NoError(t, err)
					writeTime := periodStart.Add(12 * time.Hour)
					oldPath := writer.store.ActiveFilePath
					if phase != "startup" {
						require.Equal(t, 1, writer.store.FileName.Index)
						if phase == "daily_rotation" {
							writeTime = writeTime.AddDate(0, 0, 1)
						} else {
							// Fill through the actual append handle to trigger size rotation
							// on Windows as well as Unix.
							padding := make([]byte, 32*1024)
							for remaining := int64(writer.maxFileSize); remaining > 0; {
								n := min(remaining, int64(len(padding)))
								written, err := writer.store.File.Write(padding[:n])
								require.NoError(t, err)
								require.Equal(t, int(n), written)
								remaining -= int64(written)
							}
						}
						seedPeriod(writeTime.Format(pkgfsstore.FileDateFormat))
					}
					writer.setTime(writeTime)
					const message = "record after skipping latest raw symlink"
					writer.write(&logging.LogEntry{
						OccurredAt: writeTime,
						Level:      pkglogger.LogLevelInfo,
						Message:    message,
					})

					period := writeTime.Format(pkgfsstore.FileDateFormat)
					wantPath := filepath.Join(directory, target+"_"+period+"_100.log")
					require.Equal(t, wantPath, writer.store.ActiveFilePath)
					require.Equal(t, wantPath, writer.store.File.Name())
					require.Equal(t, period, writer.store.FileName.Date)
					require.Equal(t, 100, writer.store.FileName.Index)
					info, err := os.Lstat(wantPath)
					require.NoError(t, err)
					require.True(t, info.Mode().IsRegular())
					assertLogFileContains(t, wantPath, message)
					require.NoError(t, writer.guard.RunWithLifecycleLock(func() error {
						writer.guard.RunPurgeWithProtection(nil, func(map[string]struct{}) map[string]struct{} {
							return nil
						}, func(purge *appendlog.Purge) {
							require.Equal(t, map[string]struct{}{wantPath: {}}, purge.ActivePaths())
							require.Contains(t, purge.ProtectedPaths(), wantPath)
						})
						return nil
					}))
					if phase != "startup" {
						require.NotEqual(t, oldPath, wantPath)
						previous, err := os.ReadFile(oldPath)
						require.NoError(t, err)
						require.NotContains(t, string(previous), message)
					}

					linkPath := filepath.Join(directory, target+"_"+period+"_99.log")
					gotTarget, err := os.Readlink(linkPath)
					require.NoError(t, err)
					require.Equal(t, linkTarget, gotTarget)
					if dangling {
						_, err := os.Lstat(linkTarget)
						require.ErrorIs(t, err, os.ErrNotExist)
					} else {
						content, err := os.ReadFile(linkTarget)
						require.NoError(t, err)
						require.Equal(t, targetContent, string(content))
					}
					lowerContent, err := os.ReadFile(filepath.Join(directory, target+"_"+period+"_7.log"))
					require.NoError(t, err)
					require.Equal(t, "earlier generation\n", string(lowerContent))
				})
			}
		}
	}
}

type rawSymlinkTestWriter struct {
	store       *appendlog.FileStore
	guard       *appendlog.DirectoryGuard
	maxFileSize uint64
	setTime     func(time.Time)
	write       func(*logging.LogEntry)
}

func newRawSymlinkTestWriter(t *testing.T, contract, root, target string) rawSymlinkTestWriter {
	t.Helper()
	ctx := context.Background()
	if contract == "delivery" {
		cfg := newTestDeliveryFileLoggerConfig(root, "logs", target)
		cfg.FsstoreConfig.Rotation.EnableRotate = true
		logger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
		return rawSymlinkTestWriter{
			store:       &logger.Fsstore,
			guard:       logger.lifecycleGuard,
			maxFileSize: cfg.FsstoreConfig.Rotation.MaxFileSize,
			setTime:     func(at time.Time) { logger.Clock = clock.NewFixedClock(at) },
			write: func(entry *logging.LogEntry) {
				t.Helper()
				report, err := logger.HandleLogEntry(ctx, entry)
				requireDeliverySucceeded(t, report, err)
			},
		}
	}

	cfg := newTestFileLoggerConfig(root, "logs", target)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	return rawSymlinkTestWriter{
		store:       &logger.Fsstore,
		guard:       logger.lifecycleGuard,
		maxFileSize: cfg.FsstoreConfig.Lifecycle.MaxFileSize,
		setTime:     func(at time.Time) { logger.Clock = clock.NewFixedClock(at) },
		write: func(entry *logging.LogEntry) {
			t.Helper()
			require.NoError(t, logger.HandleLogEntry(ctx, entry))
		},
	}
}
