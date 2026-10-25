package file

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/stretchr/testify/require"
)

func TestLoggerSetFallbackOnlyOpensLazilyAndDoesNotMutateEntry(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "fallback-only-logger")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)
	require.Empty(t, loggerSet.loggers)

	entry := &logging.LogEntry{
		Level:   pkglogger.LogLevelError,
		Message: "fallback-only lazy logger",
	}
	loggerSet.newErrorFallbackHandler()(
		context.Background(),
		os.ErrInvalid,
		entry,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	// handlerがcloneへ発生時刻を補完しても、共有entryは変更しません。
	require.True(t, entry.OccurredAt.IsZero())
	require.Len(t, loggerSet.loggers, 1)
	logger := loggerSet.loggers[DefaultLogger]
	require.NotNil(t, logger)
	require.NotNil(t, logger.Fsstore.File)

	content, err := os.ReadFile(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(content), entry.Message)
}

func TestLoggerSetFallbackWriteFailureDoesNotDispatchHandlersAgain(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "fallback-no-recursion")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
	require.NoError(t, err)
	breakRegularFileStream(t, errorLogger)

	dispatchCalls := 0
	errorLogger.OnError = func(
		context.Context,
		error,
		*logging.LogEntry,
		observer.Tracer,
		observer.Meter,
	) {
		dispatchCalls++
	}

	loggerSet.newErrorFallbackHandler()(
		context.Background(),
		errors.New("origin failure"),
		&logging.LogEntry{
			Level:   pkglogger.LogLevelError,
			Message: "fallback write itself fails",
			Source:  fileLoggerInternalSource(DefaultLogger),
		},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Zero(t, dispatchCalls)
}

func TestLoggerSetBuildsInternalHandlersPerPhysicalRole(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "role-local-handlers")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	buildCount := 0
	var calledBuilds []int
	var builtRoles []InternalHandlerRole
	require.NoError(t, loggerSet.ConfigureInternalHandlers(func(role InternalHandlerRole) InternalHandlers {
		buildCount++
		builtRoles = append(builtRoles, role)
		currentBuild := buildCount
		return InternalHandlers{
			OnInfo: func(
				context.Context,
				*logging.LogEntry,
				observer.Tracer,
				observer.Meter,
			) {
				calledBuilds = append(calledBuilds, currentBuild)
			},
		}
	}))

	defaultLogger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
	require.NoError(t, err)
	debugLogger, err := loggerSet.NewDebugFileLogger(context.Background())
	require.NoError(t, err)

	require.Equal(t, 3, buildCount)
	require.Equal(t, []InternalHandlerRole{"default", "error", "debug"}, builtRoles)
	for _, logger := range []*FileLogger{defaultLogger, errorLogger, debugLogger} {
		require.NotNil(t, logger.OnInfo)
		logger.OnInfo(
			context.Background(),
			&logging.LogEntry{Level: pkglogger.LogLevelInfo},
			observer.NopTracer{},
			observer.NopMeter{},
		)
	}
	require.Equal(t, []int{1, 2, 3}, calledBuilds)
}

func TestLoggerSetDoesNotRebuildHandlersForAliasedRoles(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "aliased-role-handlers")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	buildCount := 0
	require.NoError(t, loggerSet.ConfigureInternalHandlers(func(role InternalHandlerRole) InternalHandlers {
		buildCount++
		require.Equal(t, InternalHandlerRole("default"), role)
		return InternalHandlers{}
	}))

	defaultLogger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
	require.NoError(t, err)
	debugLogger, err := loggerSet.NewDebugFileLogger(context.Background())
	require.NoError(t, err)

	require.Same(t, defaultLogger, errorLogger)
	require.Same(t, defaultLogger, debugLogger)
	require.Equal(t, 1, buildCount)
}

func TestLoggerSetLogicalLoggerCloseDoesNotCloseOwnedFileLogger(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "managed-logger-close")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	openedFile := logger.Fsstore.File
	require.NotNil(t, openedFile)

	// logical loggerは共有loggerを所有しないため、個別Closeではfileを閉じません。
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
	require.Same(t, openedFile, logger.Fsstore.File)
	require.NoError(t, logger.testInfo(context.Background(), "write after logical close"))

	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
	require.Nil(t, logger.Fsstore.File)
	// LoggerSet.Closeはidempotentで、同じ物理fileを二重closeしません。
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
}

func TestLoggerSetCloseFileErrorCompletesAllLoggers(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "logger-close-retry")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	defaultLogger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
	require.NoError(t, err)

	// os.Fileを先に閉じ、一つの物理loggerだけ最初のCloseが失敗する状態を作ります。
	require.NoError(t, errorLogger.Fsstore.File.Close())
	closeErr := closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet)
	require.ErrorIs(t, closeErr, os.ErrClosed)
	var roleErr *RoleCloseError
	require.ErrorAs(t, closeErr, &roleErr)
	require.Equal(t, string(ErrorLogger), roleErr.Role)
	require.Equal(t, errorLogger.Fsstore.ActiveFilePath, roleErr.Path)
	require.False(t, roleErr.Delivery)
	require.ErrorIs(t, roleErr, os.ErrClosed)

	loggerSet.mu.Lock()
	require.Equal(t, common.LoggerCloseStateCompleted, loggerSet.closeState)
	require.Len(t, loggerSet.closedLoggers, 2)
	_, defaultClosed := loggerSet.closedLoggers[defaultLogger]
	_, errorClosed := loggerSet.closedLoggers[errorLogger]
	loggerSet.mu.Unlock()
	require.True(t, defaultClosed)
	require.True(t, errorClosed)
	require.Nil(t, defaultLogger.Fsstore.File)
	require.Nil(t, errorLogger.Fsstore.File)
	for _, logger := range []*FileLogger{defaultLogger, errorLogger} {
		logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
			require.NotContains(t, protected, logger.Fsstore.ActiveFilePath)
		})
	}

	// Close errorを返しても全resourceは解放済みなので、再Closeは成功します。
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
}

func TestFileLoggerDispatchesWriteLatencyAfterReleasingWriterLock(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "deferred-file-handler")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	cfg.FsstoreConfig.Duration.WriteFileWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.WriteFileErrorThresholdDuration = 2 * time.Hour
	cfg.FsstoreConfig.Duration.WriteFileLatencyEnabled = true
	cfg.HandlerConfig = pkgloggerhandler.HandlerConfig{
		OnDebugConfig: []string{"file"},
	}

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	require.NoError(t, loggerSet.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
		return InternalHandlers{
			OnDebug: loggerSet.newDebugFallbackHandler(),
		}
	}))
	logger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	// 自己deadlockを検出して失敗した場合は、止まった書き込みを待たずに中止して片付けます。
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	done := make(chan error, 1)
	go func() {
		done <- logger.testInfo(context.Background(), "writer lock must be released before callback")
	}()
	// writer lockを持ったまま通知するとfallbackの書き込みが同じlockを待って止まるため、
	// 停止はhang検出の上限で失敗として報告します。
	require.NoError(t, receiveForTest(t, done, "write whose latency event is written by the file fallback"))

	content, err := os.ReadFile(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(content), "writer lock must be released before callback"))
	// fallbackが同じfileへ書いた遅延の内部ログ（書式の定数で始まる行）を確認します。
	require.Contains(t, string(content), strings.SplitN(MsgWriteLogNoDelayed, "%", 2)[0])
}

func TestArchiveUploadFailureUsesResolvedErrorStream(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "archive-upload-failure")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	cfg.HandlerConfig = pkgloggerhandler.HandlerConfig{
		OnErrorConfig: []string{"file"},
	}

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	require.NoError(t, loggerSet.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
		return InternalHandlers{
			OnError: loggerSet.newErrorFallbackHandler(),
		}
	}))
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	defaultLogger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	defaultLogger.ArchiveUploader = newSingleArchiveUploader(t, failingArchiveUploader{})
	require.False(t, defaultLogger.tryUploadRotatedFile(context.Background(), "/tmp/rotated.log"))

	errorLogger := loggerSet.loggers[ErrorLogger]
	require.NotNil(t, errorLogger)
	errorContent, err := os.ReadFile(errorLogger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(errorContent), "failed to upload rotated log file archive")

	defaultContent, err := os.ReadFile(defaultLogger.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.NotContains(t, string(defaultContent), "failed to upload rotated log file archive")
}

type failingArchiveUploader struct{}

func (failingArchiveUploader) Upload(context.Context, uploader.UploadRequest) error {
	return errors.New("archive upload failed")
}
