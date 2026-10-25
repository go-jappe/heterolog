package file

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// runnerCompressionGateCodecは実gzipの最初の圧縮だけを止め、writerとの重なりを確定します。
type runnerCompressionGateCodec struct {
	pkgfsstore.CompressionCodec
	entered chan string
	release <-chan struct{}
	once    sync.Once
}

func (c *runnerCompressionGateCodec) Compress(in, out *os.File) error {
	c.once.Do(func() {
		c.entered <- in.Name()
		<-c.release
	})
	return c.CompressionCodec.Compress(in, out)
}

func TestRunnerCompressionDoesNotBlockFileLoggerRotationOrLazyRole(t *testing.T) {
	ctx := context.Background()
	gzipCodec, err := infrafsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)
	release := make(chan struct{})
	codec := &runnerCompressionGateCodec{
		CompressionCodec: gzipCodec,
		entered:          make(chan string, 1),
		release:          release,
	}
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "runner-writer-concurrency")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.MaxFileAge = 36500
	cfg.FsstoreConfig.Lifecycle.MaxFileBackups = 10
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = 100 << 20
	cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = 2 * time.Hour
	cfg.FsstoreConfig.CompressionCodec = codec
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	var workers sync.WaitGroup
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		// 旧実装でwriter待ちのassertionが失敗しても、codecを先に解放してからCloseします。
		unblock()
		waitForTest(t, "runner compression workers", workers.Wait)
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

	// 実writeで世代1を満杯にして世代2へrotateし、世代2も次writeでrotationする状態にします。
	sourcePath := logger.Fsstore.ActiveFilePath
	require.NoError(t, logger.HandleLogEntry(ctx, entry(
		"first generation "+strings.Repeat("a", int(cfg.FsstoreConfig.Lifecycle.MaxFileSize)),
	)))
	sourceContent, err := os.ReadFile(sourcePath)
	require.NoError(t, err)
	require.NoError(t, logger.HandleLogEntry(ctx, entry("start second generation")))
	secondPath := logger.Fsstore.ActiveFilePath
	require.NotEqual(t, sourcePath, secondPath)
	require.NoError(t, logger.HandleLogEntry(ctx, entry(
		"second generation "+strings.Repeat("b", int(cfg.FsstoreConfig.Lifecycle.MaxFileSize)),
	)))
	secondContent, err := os.ReadFile(secondPath)
	require.NoError(t, err)

	runners, err := logfilerunner.NewLogFileRunners(logfilelifecycle.LogFileOptions{
		Config: pkgfsstore.LogFileLifecycleRunnerConfig{FsstoreConfig: cfg.FsstoreConfig},
		Clock:  fileClock,
	}, logfilerunner.StaticRunnerConfig{IntervalValue: time.Minute})
	require.NoError(t, err)
	require.Len(t, runners, 1, "default and lazy error role must share the directory guard")
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
	path := receiveForTest(t, codec.entered, "runner did not reach compression of the rotated first generation")
	require.Equal(t, sourcePath, path)

	rotatingEntry := entry("rotation while runner compression is blocked")
	followingEntry := entry("subsequent write before runner compression resumes")
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
	writeErr := receiveForTest(t, written, "writer rotation and subsequent write waited for runner compression")
	require.NoError(t, writeErr)
	creation := receiveForTest(t, created, "lazy role creation waited for another series' runner compression")
	require.NoError(t, creation.err)
	require.Equal(t, ErrorLogger, creation.logger.logger)
	require.NotEqual(t, secondPath, creation.logger.Fsstore.ActiveFilePath)
	thirdPath := logger.Fsstore.ActiveFilePath
	require.NotEqual(t, secondPath, thirdPath)
	require.Equal(t, 3, logger.Fsstore.FileName.Index)
	thirdContent, err := os.ReadFile(thirdPath)
	require.NoError(t, err)
	wantThirdContent := logger.LogFormatter.Format(rotatingEntry) + "\n" +
		logger.LogFormatter.Format(followingEntry) + "\n"
	require.Equal(t, wantThirdContent, string(thirdContent))
	unchangedSecond, err := os.ReadFile(secondPath)
	require.NoError(t, err)
	require.Equal(t, secondContent, unchangedSecond)

	unblock()
	run := receiveForTest(t, runDone, "runner did not finish after compression was released")
	require.NoError(t, run.err)
	require.Len(t, run.report.Compressed, 1)
	require.Equal(t, sourcePath, run.report.Compressed[0].SourcePath)
	require.NoFileExists(t, sourcePath)
	compressed, err := os.Open(sourcePath + ".gz")
	require.NoError(t, err)
	defer compressed.Close()
	decoded, err := gzip.NewReader(compressed)
	require.NoError(t, err)
	defer decoded.Close()
	archivedContent, err := io.ReadAll(decoded)
	require.NoError(t, err)
	require.Equal(t, sourceContent, archivedContent, "runner must archive only the original rotated generation")
	unchangedThird, err := os.ReadFile(thirdPath)
	require.NoError(t, err)
	require.Equal(t, thirdContent, unchangedThird)
}
