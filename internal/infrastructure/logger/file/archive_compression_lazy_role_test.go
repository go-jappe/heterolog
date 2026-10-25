package file

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type lazyRoleRecoveryBlockingCodec struct {
	inlineRecoveryCompressionCodec
	compressed chan string
	release    <-chan struct{}
	once       sync.Once
}

func (c *lazyRoleRecoveryBlockingCodec) Compress(in, out *os.File) error {
	if err := c.inlineRecoveryCompressionCodec.Compress(in, out); err != nil {
		return err
	}
	c.once.Do(func() {
		// sourceを読み切り、finalを公開する前の窓を固定します。ここで別writerが
		// sourceへ追記すると、圧縮成果物へ反映されないままsource retirementされます。
		c.compressed <- in.Name()
		<-c.release
	})
	return nil
}

func TestFileLoggerLazyRoleWaitsForForeignRawRecoveryBeforeOpeningActiveFile(t *testing.T) {
	ctx := context.Background()
	compressed := make(chan string, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	// releasedは圧縮の待機を解除した後だけtrueです。lazy roleの生成完了時に読み、解除前に完了したかを判定します。
	var released atomic.Bool
	releaseCompression := func() { releaseOnce.Do(func() { released.Store(true); close(release) }) }
	defer releaseCompression()
	codec := &lazyRoleRecoveryBlockingCodec{compressed: compressed, release: release}
	cfg := newInlineCompressionRecoveryTestConfig(t.TempDir(), "lazy-role-recovery", codec)
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
	cfg.FsstoreConfig.Duration.CompressWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.CompressErrorThresholdDuration = 2 * time.Hour

	// checkpoint未指定のprogrammatic構成では、共有directoryの別roleもrawを回復します。
	// 正常なwriterとCloseで、前回起動から残る現在期間の小さいrawを用意します。
	previous, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "previous Close", func() closerForTest { return previous }, true)
	previousError, err := previous.NewErrorFileLogger(ctx)
	require.NoError(t, err)
	require.NoError(t, previousError.HandleLogEntry(ctx, &logging.LogEntry{
		OccurredAt: time.Now().UTC(),
		Level:      pkglogger.LogLevelError,
		Message:    "error entry before restart",
	}))
	rawPath := previousError.Fsstore.ActiveFilePath
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "previous Close", previous))

	current, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		releaseCompression()
		closeWithinBoundForTest(t, boundedwait.HangTimeout, "current LoggerSet Close", current, true)
	})
	defaultLogger, err := current.NewFileLogger(ctx)
	require.NoError(t, err)
	// 再起動後はdefaultだけを生成し、通常writeが起動するfullscanからerror rawを回復します。
	require.NoError(t, defaultLogger.HandleLogEntry(ctx, &logging.LogEntry{
		OccurredAt: time.Now().UTC(),
		Level:      pkglogger.LogLevelInfo,
		Message:    "start natural raw recovery",
	}))
	path := receiveForTest(t, compressed, "raw recovery did not finish reading the source")
	require.Equal(t, rawPath, path)

	type openResult struct {
		logger       *FileLogger
		err          error
		afterRelease bool
	}
	openStarted := make(chan struct{})
	opened := make(chan openResult, 1)
	go func() {
		close(openStarted)
		logger, err := current.NewErrorFileLogger(ctx)
		opened <- openResult{logger: logger, err: err, afterRelease: released.Load()}
	}()
	receiveForTest(t, openStarted, "lazy error role open start")
	// lazy roleの生成が、raw回復の保持するlifecycle lockを待つまで進めてから、未完了であることを確認します。
	waitForGoroutineStackForTest(t, "lazy error role open",
		"(*LoggerSet).NewErrorFileLogger", "(*DirectoryGuard).RunWithLifecycleLock", "semaphore.(*Weighted).Acquire")
	require.Empty(t, opened, "lazy role opened before raw recovery completed")
	// 圧縮中に別roleのwriterへrawを渡さず、回復完了後のfilesystemからactiveを解決します。
	releaseCompression()
	var errorLogger *FileLogger
	result := receiveForTest(t, opened, "lazy role did not open after raw recovery completed")
	require.NoError(t, result.err)
	require.True(t, result.afterRelease, "lazy role opened before raw recovery completed")
	errorLogger = result.logger
	require.NotEqual(t, rawPath, errorLogger.Fsstore.ActiveFilePath)
	const message = "accepted error entry after recovery"
	require.NoError(t, errorLogger.HandleLogEntry(ctx, &logging.LogEntry{
		OccurredAt: time.Now().UTC(),
		Level:      pkglogger.LogLevelError,
		Message:    message,
	}))

	// constructorとbackground処理の排他を追加しても、通常Closeが完了し、成功した追記を保ちます。
	closed := make(chan error, 1)
	go func() { closed <- current.Close() }()
	closeErr := receiveForTest(t, closed, "Close did not finish after lazy role recovery")
	require.NoError(t, closeErr)
	content, err := os.ReadFile(errorLogger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(content), message)
	require.NoFileExists(t, rawPath)

	archive, err := os.Open(rawPath + ".gz")
	require.NoError(t, err)
	defer archive.Close()
	reader, err := gzip.NewReader(archive)
	require.NoError(t, err)
	archivedContent, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Contains(t, string(archivedContent), "error entry before restart")
}
