package file

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// purgeGateTracerは既存のpurge spanで実削除直前を止め、writerとの重なりを確定します。
type purgeGateTracer struct {
	spanName string
	entered  chan struct{}
	release  <-chan struct{}
	once     sync.Once
}

func (g *purgeGateTracer) StartSpan(
	ctx context.Context, name string, attrs ...observer.Attr,
) (context.Context, observer.Span) {
	if name == g.spanName {
		g.once.Do(func() {
			close(g.entered)
			<-g.release
		})
	}
	return observer.NopTracer{}.StartSpan(ctx, name, attrs...)
}

func TestRunnerPurgeDoesNotBlockFileLoggerRotationOrLazyRole(t *testing.T) {
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
			cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "runner-purge-concurrency")
			cfg.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
			cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
			cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
			cfg.FsstoreConfig.Lifecycle.EnableRotate = true
			cfg.FsstoreConfig.Lifecycle.EnableFileCompress = false
			cfg.FsstoreConfig.Lifecycle.MaxFileAge = tt.maxAge
			cfg.FsstoreConfig.Lifecycle.MaxFileBackups = tt.maxBackups
			cfg.FsstoreConfig.Lifecycle.MaxDirSize = tt.maxDirSize
			cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
			cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = 2 * time.Hour
			set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
			require.NoError(t, err)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			var workers sync.WaitGroup
			t.Cleanup(func() {
				// 旧実装でwriter待ちを検出した場合も、purgeを先に解放して全workerを回収します。
				unblock()
				waitForTest(t, "runner purge workers", workers.Wait)
				closeWithinBoundForTest(t, boundedwait.HangTimeout, "set Close", set, true)
			})
			logger, err := set.NewFileLogger(ctx)
			require.NoError(t, err)
			require.Nil(t, logger.inlinePurge)
			period, err := time.Parse(pkgfsstore.FileDateFormat, logger.Fsstore.FileName.Date)
			require.NoError(t, err)
			fileClock := clock.NewFixedClock(period.Add(12 * time.Hour))
			logger.Clock = fileClock
			entry := func(message string) *logging.LogEntry {
				return &logging.LogEntry{
					OccurredAt: fileClock.Now(), Level: pkglogger.LogLevelInfo, Message: message,
				}
			}
			activePath := logger.Fsstore.ActiveFilePath
			require.NoError(t, logger.HandleLogEntry(ctx, entry(
				"active before purge "+strings.Repeat("a", int(cfg.FsstoreConfig.Lifecycle.MaxFileSize)),
			)))
			activeContent, err := os.ReadFile(activePath)
			require.NoError(t, err)
			oldPaths := make([]string, 0, tt.oldCount)
			for index := 1; index <= tt.oldCount; index++ {
				modified := fileClock.Now().AddDate(0, 0, -2).Add(time.Duration(index) * time.Minute)
				oldPaths = append(oldPaths, writeRolePurgeFile(t, logger, modified, index, tt.oldBytes))
			}
			gate := &purgeGateTracer{
				spanName: tt.spanName, entered: make(chan struct{}), release: release,
			}
			runners, err := logfilerunner.NewLogFileRunners(logfilelifecycle.LogFileOptions{
				Config: pkgfsstore.LogFileLifecycleRunnerConfig{FsstoreConfig: cfg.FsstoreConfig},
				Clock:  fileClock,
				Tracer: gate,
			}, logfilerunner.StaticRunnerConfig{IntervalValue: time.Minute})
			require.NoError(t, err)
			require.Len(t, runners, 1)
			type runResult struct {
				report logfilelifecycle.RunReport
				err    error
			}
			runDone := make(chan runResult, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				report, err := runners[0].RunOnce(ctx)
				runDone <- runResult{report: report, err: err}
			}()
			receiveForTest(t, gate.entered, "runner did not reach the selected purge step")
			for _, path := range oldPaths {
				require.FileExists(t, path, "purge must still be stopped before removal")
			}

			rotatingEntry := entry("rotation while runner purge is blocked")
			followingEntry := entry("subsequent write before runner purge resumes")
			written := make(chan error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				if err := logger.HandleLogEntry(ctx, rotatingEntry); err != nil {
					written <- err
					return
				}
				written <- logger.HandleLogEntry(ctx, followingEntry)
			}()
			type creationResult struct {
				logger *FileLogger
				err    error
			}
			created := make(chan creationResult, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				logger, err := set.NewErrorFileLogger(ctx)
				created <- creationResult{logger: logger, err: err}
			}()
			writeErr := receiveForTest(t, written, "writer rotation and subsequent write waited for runner purge")
			require.NoError(t, writeErr)
			creation := receiveForTest(t, created, "lazy role creation waited for runner purge")
			require.NoError(t, creation.err)
			require.Equal(t, ErrorLogger, creation.logger.logger)
			newActivePath := logger.Fsstore.ActiveFilePath
			require.NotEqual(t, activePath, newActivePath)
			require.Equal(t, 2, logger.Fsstore.FileName.Index)
			wantNewContent := logger.LogFormatter.Format(rotatingEntry) + "\n" +
				logger.LogFormatter.Format(followingEntry) + "\n"
			newContent, err := os.ReadFile(newActivePath)
			require.NoError(t, err)
			require.Equal(t, wantNewContent, string(newContent))

			unblock()
			run := receiveForTest(t, runDone, "runner did not finish after purge was released")
			require.NoError(t, run.err)
			step := run.report.Purge.ByAge
			switch tt.spanName {
			case appendlog.SpanFilePurgeByGen:
				step = run.report.Purge.ByGeneration
			case appendlog.SpanFilePurgeByDirSize:
				step = run.report.Purge.ByDirSize
			}
			require.True(t, step.Ran)
			require.Len(t, step.RemovalResults, 1)
			require.Equal(t, oldPaths[0], step.RemovalResults[0].File.Path)
			require.NoError(t, step.RemovalResults[0].Err)
			require.NoFileExists(t, oldPaths[0])
			for _, path := range oldPaths[1:] {
				require.FileExists(t, path, "generation purge must keep the configured newer backups")
			}
			unchangedActive, err := os.ReadFile(activePath)
			require.NoError(t, err)
			require.Equal(t, activeContent, unchangedActive, "the active file from the purge snapshot must survive rotation")
			unchangedNew, err := os.ReadFile(newActivePath)
			require.NoError(t, err)
			require.Equal(t, newContent, unchangedNew, "purge must not remove or alter the new active file")
		})
	}
}
