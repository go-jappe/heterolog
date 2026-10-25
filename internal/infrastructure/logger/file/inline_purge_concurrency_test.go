package file

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// inlinePurgeProbeTracerは同期purgeの実削除直前に保護状態と異常終了時の後始末を確認します。
type inlinePurgeProbeTracer struct {
	observer.Tracer
	spanName string
	onPurge  func()
}

func (p *inlinePurgeProbeTracer) StartSpan(
	ctx context.Context, name string, attrs ...observer.Attr,
) (context.Context, observer.Span) {
	if name == p.spanName {
		p.onPurge()
	}
	return p.Tracer.StartSpan(ctx, name, attrs...)
}

func TestInlinePurgeDoesNotBlockFileLoggerRotation(t *testing.T) {
	for _, tt := range []struct {
		name       string
		spanName   string
		maxAge     int
		maxBackups int
		maxDirSize uint64
		oldCount   int
		oldBytes   int64
	}{
		{"age", appendlog.SpanFilePurgeByAge, 1, 10, 100 << 20, 1, 64},
		{"generation", appendlog.SpanFilePurgeByGen, 36500, 2, 100 << 20, 3, 64},
		{"directory-size", appendlog.SpanFilePurgeByDirSize, 36500, 10, infrafsstore.MinAllowedMaxDirSize, 1, int64(infrafsstore.MinAllowedMaxDirSize)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "inline-purge-concurrency")
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
			cfg.FsstoreConfig.Lifecycle.EnableRotate = true
			// 圧縮・upload checkpointの待機とは切り離し、purge自身の状態lockだけを検証します。
			cfg.FsstoreConfig.Lifecycle.EnableFileCompress = false
			cfg.FsstoreConfig.Lifecycle.MaxFileAge = tt.maxAge
			cfg.FsstoreConfig.Lifecycle.MaxFileBackups = tt.maxBackups
			cfg.FsstoreConfig.Lifecycle.MaxDirSize = tt.maxDirSize
			cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
			cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = 2 * time.Hour
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			gate := &purgeGateTracer{
				spanName: tt.spanName, entered: make(chan struct{}), release: release,
			}
			set, err := NewLoggerSet(NewConfigAdapter(cfg), gate, nil)
			require.NoError(t, err)
			var writers sync.WaitGroup
			t.Cleanup(func() {
				// 待機を検出して失敗した場合も、先にpurgeを解放してからwriterとcleanupを回収します。
				unblock()
				waitForTest(t, "inline purge writers", writers.Wait)
				closeWithinBoundForTest(t, boundedwait.HangTimeout, "set Close", set, true)
			})
			logger, err := set.NewFileLogger(ctx)
			require.NoError(t, err)
			require.NotNil(t, logger.inlinePurge)
			period, err := time.Parse(pkgfsstore.FileDateFormat, logger.Fsstore.FileName.Date)
			require.NoError(t, err)
			fileClock := clock.NewFixedClock(period.Add(12 * time.Hour))
			logger.Clock = fileClock
			entry := func(message string) *logging.LogEntry {
				return &logging.LogEntry{
					OccurredAt: fileClock.Now(), Level: pkglogger.LogLevelInfo, Message: message,
				}
			}
			oldPaths := make([]string, 0, tt.oldCount)
			for index := 1; index <= tt.oldCount; index++ {
				modified := fileClock.Now().AddDate(0, 0, -2).Add(time.Duration(index) * time.Minute)
				oldPaths = append(oldPaths, writeRolePurgeFile(t, logger, modified, index, tt.oldBytes))
			}
			activePath := logger.Fsstore.ActiveFilePath
			// 最初の実writeで通常の非同期inline cleanupを起動し、次のwriteでrotationを必要にします。
			require.NoError(t, logger.HandleLogEntry(ctx, entry(
				"active before purge "+strings.Repeat("a", int(cfg.FsstoreConfig.Lifecycle.MaxFileSize)),
			)))
			receiveForTest(t, gate.entered, "inline cleanup did not reach the selected purge step")
			activeContent, err := os.ReadFile(activePath)
			require.NoError(t, err)
			for _, path := range oldPaths {
				require.FileExists(t, path, "purge must still be stopped before removal")
			}

			rotatingEntry := entry("rotation while inline purge is blocked")
			followingEntry := entry("subsequent write before inline purge resumes")
			written := make(chan error, 1)
			writers.Add(1)
			go func() {
				defer writers.Done()
				if err := logger.HandleLogEntry(ctx, rotatingEntry); err != nil {
					written <- err
					return
				}
				written <- logger.HandleLogEntry(ctx, followingEntry)
			}()
			writeErr := receiveForTest(t, written, "writer rotation and subsequent write waited for inline purge")
			require.NoError(t, writeErr)
			newActivePath := logger.Fsstore.ActiveFilePath
			require.NotEqual(t, activePath, newActivePath)
			require.Equal(t, 2, logger.Fsstore.FileName.Index)
			wantNewContent := logger.LogFormatter.Format(rotatingEntry) + "\n" +
				logger.LogFormatter.Format(followingEntry) + "\n"
			newContent, err := os.ReadFile(newActivePath)
			require.NoError(t, err)
			require.Equal(t, wantNewContent, string(newContent))

			unblock()
			require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set), "Close must drain the accepted cleanup and rotation tasks")
			require.NoFileExists(t, oldPaths[0])
			unchangedActive, err := os.ReadFile(activePath)
			require.NoError(t, err)
			require.Equal(t, activeContent, unchangedActive, "the active file from the purge snapshot must survive rotation")
			unchangedNew, err := os.ReadFile(newActivePath)
			require.NoError(t, err)
			require.Equal(t, newContent, unchangedNew, "purge must not remove or alter the new active file")
			logger.lifecycleGuard.WithProtectedPaths(func(paths map[string]struct{}) {
				require.Empty(t, paths, "completed purge and rotation tasks must release their retained paths")
			})
		})
	}
}

func TestInlinePurgeReleasesCandidatesAfterRemoveErrorOrPanic(t *testing.T) {
	for _, operation := range []struct{ name, spanName string }{
		{"age", appendlog.SpanFilePurgeByAge},
		{"generation", appendlog.SpanFilePurgeByGen},
		{"directory-size", appendlog.SpanFilePurgeByDirSize},
	} {
		for _, outcome := range []string{"remove-error", "panic"} {
			t.Run(operation.name+"/"+outcome, func(t *testing.T) {
				ctx := context.Background()
				cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "inline-purge-release")
				cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
				cfg.FsstoreConfig.Lifecycle.EnableRotate = true
				cfg.FsstoreConfig.Lifecycle.MaxFileAge = 1
				cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 2
				set := newTestFileLoggerSet(t, NewConfigAdapter(cfg), nil, nil)
				logger, err := set.NewFileLogger(ctx)
				require.NoError(t, err)
				now := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)
				paths := []string{logger.Fsstore.ActiveFilePath}
				for index := 1; index <= 4; index++ {
					size := int64(64)
					if index == 1 {
						size = int64(cfg.FsstoreConfig.Lifecycle.MaxDirSize)
					}
					modified := now.AddDate(0, 0, -2).Add(time.Duration(index) * time.Minute)
					paths = append(paths, writeRolePurgeFile(t, logger, modified, index, size))
				}
				candidatePath, retainedPath := paths[1], paths[4]
				files := infrafsstore.GetFilesInfo(paths)
				require.Len(t, files, len(paths))
				otherOwner := appendlog.NewDirectoryGuardToken()
				logger.lifecycleGuard.ProtectPaths(otherOwner, retainedPath)
				t.Cleanup(func() { logger.lifecycleGuard.UnprotectPaths(otherOwner, retainedPath) })
				if outcome == "remove-error" {
					// OSや実行ユーザーの権限に依存せず、観測済み候補のRemoveだけを失敗させます。
					require.NoError(t, os.Remove(candidatePath))
					require.NoError(t, os.Mkdir(candidatePath, 0o700))
					require.NoError(t, os.WriteFile(filepath.Join(candidatePath, "child"), []byte("keep"), 0o600))
				}
				var protectedDuringPurge map[string]struct{}
				logger.Tracer = &inlinePurgeProbeTracer{
					Tracer: observer.NopTracer{}, spanName: operation.spanName,
					onPurge: func() {
						logger.lifecycleGuard.WithProtectedPaths(func(paths map[string]struct{}) {
							protectedDuringPurge = paths
						})
						if outcome == "panic" {
							panic("inline purge probe")
						}
					},
				}
				var reportedErrors []error
				logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					reportedErrors = append(reportedErrors, err)
				}
				notifications := &cleanupTaskNotificationQueue{}
				var remaining []infrafsstore.FileInfo
				purge := func() {
					require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
						switch operation.name {
						case "age":
							result := logger.purgeLogsByAge(ctx, logger.inlinePurge, files, newPurgeFileNames(logger.Fsstore.FileName), now, nil, notifications.enqueue)
							remaining = result.Remaining
						case "generation":
							result := logger.purgeLogsByGeneration(ctx, logger.inlinePurge, files, newPurgeFileNames(logger.Fsstore.FileName), nil, notifications.enqueue)
							remaining = result.Remaining
						case "directory-size":
							remaining = logger.purgeByDirectorySize(ctx, logger.inlinePurge, files, newPurgeFileNames(logger.Fsstore.FileName), nil, notifications.enqueue)
						}
						return nil
					}))
				}
				if outcome == "panic" {
					// 非同期cleanupをpanicさせず、同じpurge本体のdeferによる解放を同期的に確認します。
					require.PanicsWithValue(t, "inline purge probe", purge)
					require.FileExists(t, candidatePath)
				} else {
					purge()
					notifications.runDeferredNotifications()
					require.Len(t, reportedErrors, 1)
					var removalFailure *infrafsstore.FileRemovalFailure
					require.ErrorAs(t, reportedErrors[0], &removalFailure)
					require.Equal(t, candidatePath, removalFailure.File.Path)
					require.DirExists(t, candidatePath)
					require.Contains(t, remaining, files[1], "failed deletion must remain in the observed result")
				}
				for _, path := range paths {
					require.Contains(t, protectedDuringPurge, pathidentity.AbsoluteOrCleanPath(path))
				}
				logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
					require.Len(t, protected, 2, "only active and the independent owner's retention must remain")
					require.Contains(t, protected, pathidentity.AbsoluteOrCleanPath(paths[0]))
					require.Contains(t, protected, pathidentity.AbsoluteOrCleanPath(retainedPath))
					require.NotContains(t, protected, pathidentity.AbsoluteOrCleanPath(candidatePath))
				})
				require.FileExists(t, paths[0])
				require.FileExists(t, retainedPath)
			})
		}
	}
}
