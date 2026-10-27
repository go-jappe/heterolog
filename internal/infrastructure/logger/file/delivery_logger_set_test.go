package file

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/onerrorhandler"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/require"
)

func TestNewDeliveryLoggerSetRejectsUnsupportedLifecycleMode(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-inline-mode")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline

	loggerSet, err := NewDeliveryLoggerSet(
		NewDeliveryFileLoggerConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.ErrorIs(t, err, pkgfsstore.ErrUnsupportedDeliveryFileLifecycleMode)
	require.Nil(t, loggerSet)
}

func TestDeliveryLoggerSetNormalAndFallbackShareFileLogger(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-shared-logger")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false

	loggerSet, err := NewDeliveryLoggerSet(
		NewDeliveryFileLoggerConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	normal, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	loggerSet.NewNamedInfoFallbackHandler().Handler(
		context.Background(),
		&logging.LogEntry{
			Level:   pkglogger.LogLevelInfo,
			Message: "delivery fallback shared write",
			Source:  "stdout",
		},
		observer.NopTracer{},
		observer.NopMeter{},
	)

	require.Same(t, normal, loggerSet.loggers[DefaultLogger])
	content, err := os.ReadFile(normal.Fsstore.ActiveFilePath)
	require.NoError(t, err)
	require.Contains(t, string(content), "delivery fallback shared write")

	// logical childのCloseは共有物理loggerを閉じず、ownerのCloseだけが閉じます。
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "normal Close", normal))
	require.NotNil(t, normal.Fsstore.File)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
	require.Nil(t, normal.Fsstore.File)
}

func TestDeliveryLoggerSetBuildsInternalHandlersPerPhysicalRole(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-role-local-handlers")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = true

	loggerSet, err := NewDeliveryLoggerSet(
		NewDeliveryFileLoggerConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
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
				pkgobserver.Tracer,
				pkgobserver.Meter,
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
	for _, logger := range []*DeliveryFileLogger{defaultLogger, errorLogger, debugLogger} {
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

func TestDeliveryLoggerSetDoesNotRebuildHandlersForAliasedRoles(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-aliased-role-handlers")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false

	loggerSet, err := NewDeliveryLoggerSet(
		NewDeliveryFileLoggerConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
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

func TestDeliveryLoggerSetCloseErrorIdentifiesRoleAndPath(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-close-identity")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true

	loggerSet, err := NewDeliveryLoggerSet(
		NewDeliveryFileLoggerConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
	require.NoError(t, err)

	// FileStoreへpointerを残したまま実fileだけを閉じ、Close errorの識別情報を検証します。
	require.NoError(t, errorLogger.Fsstore.File.Close())
	closeErr := closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet)
	require.ErrorIs(t, closeErr, os.ErrClosed)
	require.Equal(t, common.LoggerCloseStateCompleted, loggerSet.closeState)
	var roleErr *RoleCloseError
	require.ErrorAs(t, closeErr, &roleErr)
	require.Equal(t, string(ErrorLogger), roleErr.Role)
	require.Equal(t, errorLogger.Fsstore.ActiveFilePath, roleErr.Path)
	require.True(t, roleErr.Delivery)
	require.ErrorIs(t, roleErr, os.ErrClosed)
	require.Nil(t, errorLogger.Fsstore.File)
	_, closed := loggerSet.closedLoggers[errorLogger]
	require.True(t, closed)
	errorLogger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.NotContains(t, protected, errorLogger.Fsstore.ActiveFilePath)
	})

	// 最初のCloseで所有権を解放し、同じdirectoryを再取得できます。
	replacement, err := NewDeliveryLoggerSet(NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "replacement Close", replacement))
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "loggerSet Close", loggerSet))
}

func TestDeliveryLoggerSetNormalDefaultFailureRoutesToSeparateErrorStream(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-normal-dispatch")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	cfg.HandlerConfig = pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"file"}}
	adapter := NewDeliveryFileLoggerConfigAdapter(cfg)

	loggerSet, err := NewDeliveryLoggerSet(adapter, nil, nil)
	require.NoError(t, err)
	fileHandler := loggerSet.NewNamedErrorFallbackHandler()
	require.NoError(t, loggerSet.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
		return InternalHandlers{
			OnError: onerrorhandler.CombineErrorHandlers(fileHandler),
		}
	}))
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	defaultLogger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	breakDeliveryStream(t, defaultLogger)

	report, err := defaultLogger.testInfo(context.Background(), "default logger write failure")
	require.Error(t, err)
	require.Equal(t, pkglogger.DeliveryFailed, report.Targets[0].Status)

	errorLogger := loggerSet.loggers[ErrorLogger]
	require.NotNil(t, errorLogger, "default failure must be delegated to the resolved error role")
	require.NotNil(t, errorLogger.Fsstore.File)
	content, readErr := os.ReadFile(errorLogger.Fsstore.ActiveFilePath)
	require.NoError(t, readErr)
	require.Contains(t, string(content), "default logger write failure")
}

func TestDeliveryLoggerSetSuppressesErrorRoleAndUnsplitSameRoleRecursion(t *testing.T) {
	t.Run("error role wrapper suppresses file fallback", func(t *testing.T) {
		cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-error-self")
		cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
		cfg.HandlerConfig = pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"file"}}
		loggerSet, err := NewDeliveryLoggerSet(
			NewDeliveryFileLoggerConfigAdapter(cfg),
			nil,
			nil,
		)
		require.NoError(t, err)
		fileHandler := loggerSet.NewNamedErrorFallbackHandler()
		require.NoError(t, loggerSet.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
			return InternalHandlers{
				OnError: onerrorhandler.CombineErrorHandlers(fileHandler),
			}
		}))
		closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

		errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
		require.NoError(t, err)
		breakDeliveryStream(t, errorLogger)

		_, err = errorLogger.testError(context.Background(), "error role must not recurse")
		require.Error(t, err)
		require.Nil(t, errorLogger.Fsstore.File)
		require.Len(t, loggerSet.loggers, 1)
	})

	t.Run("unsplit source equals resolved role", func(t *testing.T) {
		cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-unsplit-self")
		cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = false
		cfg.HandlerConfig = pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"file"}}
		loggerSet, err := NewDeliveryLoggerSet(
			NewDeliveryFileLoggerConfigAdapter(cfg),
			nil,
			nil,
		)
		require.NoError(t, err)
		fileHandler := loggerSet.NewNamedErrorFallbackHandler()
		require.NoError(t, loggerSet.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
			return InternalHandlers{
				OnError: onerrorhandler.CombineErrorHandlers(fileHandler),
			}
		}))
		closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

		defaultLogger, err := loggerSet.NewFileLogger(context.Background())
		require.NoError(t, err)
		breakDeliveryStream(t, defaultLogger)

		_, err = defaultLogger.testInfo(context.Background(), "same role must not reopen")
		require.Error(t, err)
		require.Nil(t, defaultLogger.Fsstore.File)
		require.Len(t, loggerSet.loggers, 1)
	})
}

func TestDeliveryLoggerSetFallbackWriteFailureDoesNotDispatchHandlersAgain(t *testing.T) {
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "logs", "delivery-fallback-no-recursion")
	cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
	loggerSet, err := NewDeliveryLoggerSet(
		NewDeliveryFileLoggerConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
	require.NoError(t, err)
	breakDeliveryStream(t, errorLogger)

	dispatchCalls := 0
	errorLogger.OnError = func(
		context.Context,
		error,
		*logging.LogEntry,
		pkgobserver.Tracer,
		pkgobserver.Meter,
	) {
		dispatchCalls++
	}

	// Source=defaultなのでfallbackはerror roleへのwriteを試みます。物理writeは失敗しますが、
	// raw fallback境界はOnErrorを再実行せず、この呼び出し内で完結します。
	loggerSet.NewNamedErrorFallbackHandler().Handler(
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
	require.Nil(t, errorLogger.Fsstore.File)
}

func breakDeliveryStream(t *testing.T, logger *DeliveryFileLogger) {
	t.Helper()
	logger.Fsstore.Mu.Lock()
	defer logger.Fsstore.Mu.Unlock()
	if logger.Fsstore.File != nil {
		require.NoError(t, logger.Fsstore.File.Close())
		logger.Fsstore.File = nil
	}
}
