package file

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	appotel "github.com/go-jappe/heterolog/internal/application/otel"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	clickhouseerrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse/errors"
	cloudwatchlogslogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	cloudwatchlogserrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	lokierrors "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki/errors"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/onerrorhandler"
	loggerresource "github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/uuid"
	integrationtestutil "github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

const testTarget = "heterolog"

func TestFileLoggerAndDeliveryFileLogger_WriteToFile(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "file-logger-test-request")
	logPath := t.TempDir()

	fileLoggerConfig := newTestFileLoggerConfig(logPath, "filelogger_logs", "filelogger-test")
	fileLoggerAdapter := NewConfigAdapter(fileLoggerConfig)
	fileLogger, err := newStandaloneFileLogger(ctx, fileLoggerAdapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "fileLogger Close", func() closerForTest { return fileLogger }, true)

	fileLoggerMessage := "filelogger writes this message"
	require.NoError(t, fileLogger.testInfo(ctx, fileLoggerMessage))
	assertLogFileContains(t, fileLogger.Fsstore.ActiveFilePath, fileLoggerMessage)

	deliveryFileLoggerConfig := newTestDeliveryFileLoggerConfig(logPath, "delivery_filelogger_logs", "delivery-filelogger-test")
	deliveryFileLoggerAdapter := NewDeliveryFileLoggerConfigAdapter(deliveryFileLoggerConfig)
	deliveryFileLogger, err := NewDeliveryFileLogger(ctx, deliveryFileLoggerAdapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "deliveryFileLogger Close", func() closerForTest { return deliveryFileLogger }, true)

	deliveryFileLoggerMessage := "deliveryfilelogger writes this message"
	report, err := deliveryFileLogger.testInfo(ctx, deliveryFileLoggerMessage)
	require.NoError(t, err)
	result := requireSingleDeliveryTarget(t, report)
	require.NoError(t, result.Err)
	assert.Equal(t, BackendServices, result.Backend)
	assert.Equal(t, pkglogger.DeliverySucceeded, result.Status)
	assertLogFileContains(t, deliveryFileLogger.Fsstore.ActiveFilePath, deliveryFileLoggerMessage)
}

func TestFileLoggerInternalErrorFallsBackToLokiAndCloudWatchLogs(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "file-fallback-loki-cloudwatch-request")
	logPath := t.TempDir()
	lokiSink := newRecordingHTTPBodySink(t, http.StatusNoContent, "")
	cloudWatchSink := newRecordingHTTPBodySink(t, http.StatusOK, "{}")

	fileConfig := newTestFileLoggerConfig(logPath, "file_fallback_source_logs", "file-fallback-source")
	fileLogger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(fileConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "fileLogger Close", func() closerForTest { return fileLogger }, true)
	breakRegularFileStream(t, fileLogger)

	lokiLoggerConfig := lokilogger.NewLokiLoggerConfigAdapter(newTestLokiLoggerBaseConfig("file-fallback-loki"))
	lokiFlushConfig := lokilogger.NewFlushManagerConfigAdapter(newTestLokiFlushManagerConfig("file-fallback-loki"))
	lokiResource := newTestFallbackResource("file-fallback-loki")
	lokiClientConfig := &pkgloki.Config{
		Endpoint:   lokiSink.URL(),
		Timeout:    time.Second,
		HTTPClient: lokiSink.Client(),
	}

	cloudWatchLoggerConfig := cloudwatchlogslogger.NewCloudWatchLogsLoggerConfigAdapter(newTestCloudWatchLogsLoggerBaseConfig("file-fallback-cloudwatch"))
	cloudWatchFlushConfig := cloudwatchlogslogger.NewFlushManagerConfigAdapter(
		newTestCloudWatchLogsFlushManagerConfig("file-fallback-cloudwatch"),
		pkgcloudwatchlogs.StreamRoutingNone,
	)
	cloudWatchResource := newTestFallbackResource("file-fallback-cloudwatch")
	cloudWatchClientConfig := &pkgcloudwatchlogs.Config{
		Region:          "ap-northeast-1",
		Endpoint:        cloudWatchSink.URL(),
		LogGroupName:    "/test/file-fallback",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Timeout:         time.Second,
		HTTPClient:      cloudWatchSink.Client(),
	}

	lokiFallback := lokilogger.NewNamedErrorFallbackHandler(
		lokiLoggerConfig,
		lokiClientConfig,
		lokiFlushConfig,
		lokiResource,
		nil,
	)
	cloudWatchFallback := cloudwatchlogslogger.NewNamedErrorFallbackHandler(
		cloudWatchLoggerConfig,
		cloudWatchClientConfig,
		cloudWatchFlushConfig,
		cloudWatchResource,
		nil,
	)
	t.Cleanup(func() {
		closeWithinBoundForTest(t, boundedwait.HangTimeout, "Loki fallback Close", lokiFallback.Closer, true)
		closeWithinBoundForTest(t, boundedwait.HangTimeout, "CloudWatch Logs fallback Close", cloudWatchFallback.Closer, true)
	})
	fileLogger.OnError = onerrorhandler.CombineErrorHandlers(
		lokiFallback,
		cloudWatchFallback,
	)

	message := "file logger internal error should reach loki and cloudwatchlogs fallback"
	err = fileLogger.testInfo(ctx, message)
	require.Error(t, err)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "Loki fallback Close", lokiFallback.Closer))
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "CloudWatch Logs fallback Close", cloudWatchFallback.Closer))

	lokiSink.requireBodyContains(t, message)
	cloudWatchSink.requireBodyContains(t, message)
}

func TestFileLoggerInternalErrorFallsBackToClickHouseBatch(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "file-fallback-clickhouse-request")
	logPath := t.TempDir()
	clickHouseSink := newRecordingHTTPBodySink(t, http.StatusOK, "")

	fileConfig := newTestFileLoggerConfig(logPath, "file_fallback_clickhouse_source_logs", "file-fallback-clickhouse-source")
	fileLogger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(fileConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "fileLogger Close", func() closerForTest { return fileLogger }, true)
	breakRegularFileStream(t, fileLogger)

	// 壊れたfile streamへの1回のInfoにつき一つの内部エラーだけをfallbackへ渡す。
	// 2件でClickHouse batchをflushする境界をここで検証する。
	clickHouseLoggerConfig := clickhouselogger.NewClickHouseLoggerConfigAdapter(newTestClickHouseLoggerBaseConfig("file-fallback-clickhouse", 2))
	clickHouseFlushConfig := clickhouselogger.NewFlushManagerConfigAdapter(newTestClickHouseFlushManagerConfig("file-fallback-clickhouse"))
	clickHouseResource := newTestFallbackResource("file-fallback-clickhouse")
	clickHouseClientConfig := &pkgclickhouse.Config{
		Endpoint:   clickHouseSink.URL(),
		Database:   "default",
		Table:      "logs",
		Timeout:    time.Second,
		HTTPClient: clickHouseSink.Client(),
	}

	clickHouseFallback := clickhouselogger.NewNamedErrorFallbackHandler(
		clickHouseLoggerConfig,
		clickHouseClientConfig,
		clickHouseFlushConfig,
		clickHouseResource,
		nil,
	)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "clickHouseFallback.Closer Close", func() closerForTest { return clickHouseFallback.Closer }, true)
	fileLogger.OnError = onerrorhandler.CombineErrorHandlers(clickHouseFallback)

	firstMessage := "file logger internal error should wait in clickhouse fallback batch one"
	err = fileLogger.testInfo(ctx, firstMessage)
	require.Error(t, err)
	clickHouseSink.requireBodyCount(t, 0)

	secondMessage := "file logger internal error should flush clickhouse fallback batch two"
	err = fileLogger.testInfo(ctx, secondMessage)
	require.Error(t, err)
	// fallbackのbatchは別goroutineで送信されるため、送信の到着を通知で待ちます。
	receiveForTest(t, clickHouseSink.received, "ClickHouse fallback batch request")
	clickHouseSink.requireBodyCount(t, 1)
	clickHouseSink.requireOnlyBodyLineCount(t, 2)
	clickHouseSink.requireBodyContains(t, firstMessage)
	clickHouseSink.requireBodyContains(t, secondMessage)
}

func TestLoggerInternalErrorsNotifyEveryExplicitlyConfiguredHandlerOnce(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		source          string
		selfHandlerName string
		message         string
	}{
		{
			name:            "loki",
			err:             &lokierrors.LokiLoggerInternalError{Op: "loki test internal", Err: fmt.Errorf("loki failure")},
			source:          common.SourceNameLokiLogger,
			selfHandlerName: common.HandlerNameLokiLogger,
			message:         "loki internal error should notify fallback handlers",
		},
		{
			name:            "cloudwatchlogs",
			err:             &cloudwatchlogserrors.CloudWatchLogsLoggerInternalError{Op: "cloudwatch logs test internal", Err: fmt.Errorf("cloudwatch logs failure")},
			source:          common.SourceNameCloudWatchLogsLogger,
			selfHandlerName: common.HandlerNameCloudWatchLogsLogger,
			message:         "cloudwatchlogs internal error should notify fallback handlers",
		},
		{
			name:            "clickhouse",
			err:             &clickhouseerrors.ClickHouseLoggerInternalError{Op: "clickhouse test internal", Err: fmt.Errorf("clickhouse failure")},
			source:          common.SourceNameClickHouseLogger,
			selfHandlerName: common.HandlerNameClickHouseLogger,
			message:         "clickhouse internal error should notify fallback handlers",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := requestctx.WithRequestID(context.Background(), tt.name+"-fallback-fanout-request")
			logPath := t.TempDir()
			fileConfig := newTestFileLoggerConfig(logPath, tt.name+"_fallback_file_logs", tt.name+"-fallback-file")
			fileLoggerSet := newTestFileLoggerSet(
				t,
				NewConfigAdapter(fileConfig),
				observer.NopTracer{},
				observer.NopMeter{},
			)
			fileHandler := fileLoggerSet.NewNamedErrorFallbackHandler()
			stdoutRecorder := newRecordingNamedErrorHandler(common.HandlerNameStdoutLogger)
			slackRecorder := newRecordingNamedErrorHandler(common.HandlerNameSlackLogger)
			openSearchRecorder := newRecordingNamedErrorHandler(common.HandlerNameOpenSearchLogger)
			selfRecorder := newRecordingNamedErrorHandler(tt.selfHandlerName)

			handler := onerrorhandler.CombineErrorHandlers(
				fileHandler,
				stdoutRecorder.namedHandler(),
				slackRecorder.namedHandler(),
				openSearchRecorder.namedHandler(),
				selfRecorder.namedHandler(),
			)
			entry := &logging.LogEntry{
				Level:    pkglogger.LogLevelError,
				Message:  tt.message,
				Err:      tt.err,
				Internal: true,
				Source:   tt.source,
			}

			handler(ctx, tt.err, entry, nil, nil)

			assertAnyLogFileContains(t, logPath, tt.message)
			stdoutRecorder.requireMessage(t, tt.message)
			slackRecorder.requireMessage(t, tt.message)
			openSearchRecorder.requireMessage(t, tt.message)
			// fallback実装自身がno-fallback境界を持つため、sourceと同じbackendも
			// 明示的に選ばれていれば一度だけ実行します。
			selfRecorder.requireCallCount(t, 1)
		})
	}
}

func TestFileLoggersUseFsstoreLayoutTargetForPathAndFileName(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "file-logger-fsstore-target-request")
	logPath := resolvedTempDirForTest(t)

	fileLoggerConfig := newTestFileLoggerConfig(logPath, "filelogger_logs", "fsstore-file-target")
	fileLoggerConfig.RuntimeConfig.Target = "runtime-file-target"
	fileLogger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(fileLoggerConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "fileLogger Close", func() closerForTest { return fileLogger }, true)

	assert.Contains(t, fileLogger.Fsstore.ActiveFilePath, filepath.Join(logPath, "fsstore-file-target", "filelogger_logs"))
	assert.Contains(t, filepath.Base(fileLogger.Fsstore.ActiveFilePath), "fsstore-file-target_")
	assert.NotContains(t, fileLogger.Fsstore.ActiveFilePath, "runtime-file-target")

	deliveryFileLoggerConfig := newTestDeliveryFileLoggerConfig(logPath, "delivery_filelogger_logs", "fsstore-delivery-target")
	deliveryFileLoggerConfig.RuntimeConfig.Target = "runtime-delivery-target"
	deliveryFileLogger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(deliveryFileLoggerConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "deliveryFileLogger Close", func() closerForTest { return deliveryFileLogger }, true)

	assert.Contains(t, deliveryFileLogger.Fsstore.ActiveFilePath, filepath.Join(logPath, "fsstore-delivery-target", "delivery_filelogger_logs"))
	assert.Contains(t, filepath.Base(deliveryFileLogger.Fsstore.ActiveFilePath), "fsstore-delivery-target_")
	assert.NotContains(t, deliveryFileLogger.Fsstore.ActiveFilePath, "runtime-delivery-target")
}

func TestFileLoggersRejectOpenWhenAlreadyOpen(t *testing.T) {
	ctx := context.Background()
	logPath := t.TempDir()

	regularConfig := newTestFileLoggerConfig(logPath, "regular-already-open", "regular-already-open")
	regular, err := newStandaloneFileLogger(ctx, NewConfigAdapter(regularConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "regular Close", func() closerForTest { return regular }, true)
	regularFile := regular.Fsstore.File
	regularPath := regular.Fsstore.ActiveFilePath

	err = openFileLoggerForTest(ctx, regular)
	require.ErrorIs(t, err, ErrFileAlreadyOpen)
	require.Same(t, regularFile, regular.Fsstore.File)
	require.Equal(t, regularPath, regular.Fsstore.ActiveFilePath)
	regular.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Contains(t, protected, filepath.Clean(regularPath))
	})

	deliveryConfig := newTestDeliveryFileLoggerConfig(logPath, "delivery-already-open", "delivery-already-open")
	delivery, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(deliveryConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "delivery Close", func() closerForTest { return delivery }, true)
	deliveryFile := delivery.Fsstore.File
	deliveryPath := delivery.Fsstore.ActiveFilePath

	err = openDeliveryFileLoggerForTest(ctx, delivery)
	require.ErrorIs(t, err, ErrFileAlreadyOpen)
	require.Same(t, deliveryFile, delivery.Fsstore.File)
	require.Equal(t, deliveryPath, delivery.Fsstore.ActiveFilePath)
	delivery.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Contains(t, protected, filepath.Clean(deliveryPath))
	})
}

func TestFileLoggersShareDirectoryUsingDistinctPrefixes(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "shared-file-logger-directory-request")
	logPath := t.TempDir()
	const (
		logDir = "shared-file-logger-directory"
		target = "shared-file-logger"
	)

	cfg := newTestFileLoggerConfig(logPath, logDir, target)
	cfg.FsstoreConfig.Layout.ErrorLogDir = logDir
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeDisable
	adapter := NewConfigAdapter(cfg)

	defaultLogger, err := newStandaloneFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "defaultLogger Close", func() closerForTest { return defaultLogger }, true)

	errorLogger, err := newStandaloneErrorFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "errorLogger Close", func() closerForTest { return errorLogger }, true)

	debugLogger, err := newStandaloneDebugFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "debugLogger Close", func() closerForTest { return debugLogger }, true)

	defaultPath := defaultLogger.Fsstore.ActiveFilePath
	errorPath := errorLogger.Fsstore.ActiveFilePath
	debugPath := debugLogger.Fsstore.ActiveFilePath
	require.Equal(t, filepath.Dir(defaultPath), filepath.Dir(errorPath))
	require.Equal(t, filepath.Dir(defaultPath), filepath.Dir(debugPath))
	require.NotEqual(t, defaultPath, errorPath)
	require.NotEqual(t, defaultPath, debugPath)
	require.NotEqual(t, errorPath, debugPath)
	require.True(t, strings.HasPrefix(filepath.Base(defaultPath), target+"_"), defaultPath)
	require.True(t, strings.HasPrefix(filepath.Base(errorPath), "error_"+target+"_"), errorPath)
	require.True(t, strings.HasPrefix(filepath.Base(debugPath), "debug_"+target+"_"), debugPath)

	const (
		defaultMessage = "shared directory default file message"
		errorMessage   = "shared directory error file message"
		debugMessage   = "shared directory debug file message"
	)
	require.NoError(t, defaultLogger.testInfo(ctx, defaultMessage))
	require.NoError(t, errorLogger.testError(ctx, errorMessage))
	require.NoError(t, debugLogger.testDebug(ctx, debugMessage))

	assertLogFileContains(t, defaultPath, defaultMessage)
	assertLogFileNotContains(t, defaultPath, errorMessage)
	assertLogFileNotContains(t, defaultPath, debugMessage)
	assertLogFileContains(t, errorPath, errorMessage)
	assertLogFileNotContains(t, errorPath, defaultMessage)
	assertLogFileNotContains(t, errorPath, debugMessage)
	assertLogFileContains(t, debugPath, debugMessage)
	assertLogFileNotContains(t, debugPath, defaultMessage)
	assertLogFileNotContains(t, debugPath, errorMessage)
}

func TestFileLoggersSharedDirectoryInlineCleanupPreservesAllActiveFiles(t *testing.T) {
	ctx := context.Background()
	logPath := t.TempDir()
	const (
		logDir = "shared-inline-cleanup-directory"
		target = "shared-inline-cleanup"
	)

	cfg := newTestFileLoggerConfig(logPath, logDir, target)
	cfg.FsstoreConfig.Layout.ErrorLogDir = logDir
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = fsstore.MinAllowedMaxDirSize
	adapter := NewConfigAdapter(cfg)

	defaultLogger, err := newStandaloneFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "defaultLogger Close", func() closerForTest { return defaultLogger }, true)
	errorLogger, err := newStandaloneErrorFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "errorLogger Close", func() closerForTest { return errorLogger }, true)
	debugLogger, err := newStandaloneDebugFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "debugLogger Close", func() closerForTest { return debugLogger }, true)

	require.Same(t, defaultLogger.lifecycleGuard, errorLogger.lifecycleGuard)
	require.Same(t, defaultLogger.lifecycleGuard, debugLogger.lifecycleGuard)

	activePaths := []string{
		defaultLogger.Fsstore.ActiveFilePath,
		errorLogger.Fsstore.ActiveFilePath,
		debugLogger.Fsstore.ActiveFilePath,
	}
	// active fileだけでMaxDirSizeをわずかに超える状態を作ります。activeは削除できませんが、
	// そのsizeを上限計算から除外すると下のinactive fileが削除されず、不具合を検出できます。
	activeSize := int64(fsstore.MinAllowedMaxDirSize/3 + 1)
	oldActiveTime := time.Now().Add(-2 * time.Hour)
	for _, path := range activePaths {
		require.NoError(t, os.Truncate(path, activeSize))
		require.NoError(t, os.Chtimes(path, oldActiveTime, oldActiveTime))
	}

	inactivePath := filepath.Join(filepath.Dir(activePaths[0]), "inactive-shared-directory_"+defaultLogger.Fsstore.FileName.Date+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, nil, 0o600))
	require.NoError(t, os.Truncate(inactivePath, 1024*1024))
	newerInactiveTime := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(inactivePath, newerInactiveTime, newerInactiveTime))

	errCh := make(chan error, 3)
	for _, logger := range []*FileLogger{defaultLogger, errorLogger, debugLogger} {
		go func(logger *FileLogger) {
			errCh <- purgeFileLoggerForTest(ctx, logger)
		}(logger)
	}
	for range 3 {
		require.NoError(t, receiveForTest(t, errCh, "concurrent purge"))
	}
	require.NoFileExists(t, inactivePath)
	for _, path := range activePaths {
		require.FileExists(t, path)
	}
}

func TestSharedDirectoryFileLoggersRotateArchiveAndCleanupConcurrently(t *testing.T) {
	ctx := context.Background()
	logPath := t.TempDir()
	cfg := newTestFileLoggerConfig(logPath, "shared-rotation-cleanup", "shared-rotation-cleanup")
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	adapter := NewConfigAdapter(cfg)

	defaultLogger, err := newStandaloneFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "defaultLogger Close", func() closerForTest { return defaultLogger }, true)
	errorLogger, err := newStandaloneErrorFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "errorLogger Close", func() closerForTest { return errorLogger }, true)
	debugLogger, err := newStandaloneDebugFileLogger(ctx, adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "debugLogger Close", func() closerForTest { return debugLogger }, true)

	loggers := []*FileLogger{defaultLogger, errorLogger, debugLogger}
	oldPaths := make([]string, 0, len(loggers))
	for _, logger := range loggers {
		oldPaths = append(oldPaths, logger.Fsstore.ActiveFilePath)
		require.NoError(t, os.Truncate(logger.Fsstore.ActiveFilePath, int64(cfg.FsstoreConfig.Lifecycle.MaxFileSize)))
	}

	errCh := make(chan error, len(loggers))
	go func() { errCh <- defaultLogger.testInfo(ctx, "rotate default") }()
	go func() { errCh <- errorLogger.testError(ctx, "rotate error") }()
	go func() { errCh <- debugLogger.testDebug(ctx, "rotate debug") }()
	for range loggers {
		require.NoError(t, receiveForTest(t, errCh, "rotating write"))
	}
	// rotation後の圧縮と元logの退役は、各loggerが受理したbackground処理の完了で確定します。
	for _, logger := range loggers {
		waitBackgroundForTest(t, logger)
	}

	for _, oldPath := range oldPaths {
		compressedPath := oldPath + pkgfsstore.ExtensionCompressGzip
		require.FileExists(t, compressedPath, "compressed file was not published")
		require.NoFileExists(t, oldPath, "source was not retired")
	}
	for _, logger := range loggers {
		require.FileExists(t, logger.Fsstore.ActiveFilePath)
	}
}

func TestDeliveryFileLoggersShareDirectoryUsingDistinctPrefixes(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "shared-delivery-file-logger-directory-request")
	logPath := t.TempDir()
	const (
		logDir = "shared-delivery-file-logger-directory"
		target = "shared-delivery-file-logger"
	)

	cfg := newTestDeliveryFileLoggerConfig(logPath, logDir, target)
	cfg.FsstoreConfig.Layout.ErrorLogDir = logDir
	adapter := NewDeliveryFileLoggerConfigAdapter(cfg)

	loggerSet, err := NewDeliveryLoggerSet(adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)

	defaultLogger, err := loggerSet.NewFileLogger(ctx)
	require.NoError(t, err)
	errorLogger, err := loggerSet.NewErrorFileLogger(ctx)
	require.NoError(t, err)
	debugLogger, err := loggerSet.NewDebugFileLogger(ctx)
	require.NoError(t, err)

	defaultPath := defaultLogger.Fsstore.ActiveFilePath
	errorPath := errorLogger.Fsstore.ActiveFilePath
	debugPath := debugLogger.Fsstore.ActiveFilePath
	require.Equal(t, filepath.Dir(defaultPath), filepath.Dir(errorPath))
	require.Equal(t, filepath.Dir(defaultPath), filepath.Dir(debugPath))
	require.NotEqual(t, defaultPath, errorPath)
	require.NotEqual(t, defaultPath, debugPath)
	require.NotEqual(t, errorPath, debugPath)
	require.True(t, strings.HasPrefix(filepath.Base(defaultPath), target+"_"), defaultPath)
	require.True(t, strings.HasPrefix(filepath.Base(errorPath), "error_"+target+"_"), errorPath)
	require.True(t, strings.HasPrefix(filepath.Base(debugPath), "debug_"+target+"_"), debugPath)

	const (
		defaultMessage = "shared delivery directory default file message"
		errorMessage   = "shared delivery directory error file message"
		debugMessage   = "shared delivery directory debug file message"
	)
	result, err := defaultLogger.testInfo(ctx, defaultMessage)
	requireDeliverySucceeded(t, result, err)
	result, err = errorLogger.testError(ctx, errorMessage)
	requireDeliverySucceeded(t, result, err)
	result, err = debugLogger.testDebug(ctx, debugMessage)
	requireDeliverySucceeded(t, result, err)

	assertLogFileContains(t, defaultPath, defaultMessage)
	assertLogFileNotContains(t, defaultPath, errorMessage)
	assertLogFileNotContains(t, defaultPath, debugMessage)
	assertLogFileContains(t, errorPath, errorMessage)
	assertLogFileNotContains(t, errorPath, defaultMessage)
	assertLogFileNotContains(t, errorPath, debugMessage)
	assertLogFileContains(t, debugPath, debugMessage)
	assertLogFileNotContains(t, debugPath, defaultMessage)
	assertLogFileNotContains(t, debugPath, errorMessage)
}

func TestDeliveryFileDirectoryLifecyclePreservesAllActiveFilesInSharedDirectory(t *testing.T) {
	ctx := context.Background()
	logPath := t.TempDir()
	const (
		logDir     = "shared-delivery-manager-directory"
		target     = "shared-delivery-manager"
		maxDirSize = fsstore.MinAllowedMaxDirSize
	)

	cfg := newTestDeliveryFileLoggerConfig(logPath, logDir, target)
	cfg.FsstoreConfig.Layout.ErrorLogDir = logDir
	adapter := NewDeliveryFileLoggerConfigAdapter(cfg)

	loggerSet, err := NewDeliveryLoggerSet(adapter, nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)
	defaultLogger, err := loggerSet.NewFileLogger(ctx)
	require.NoError(t, err)
	errorLogger, err := loggerSet.NewErrorFileLogger(ctx)
	require.NoError(t, err)
	debugLogger, err := loggerSet.NewDebugFileLogger(ctx)
	require.NoError(t, err)

	activePaths := []string{
		defaultLogger.Fsstore.ActiveFilePath,
		errorLogger.Fsstore.ActiveFilePath,
		debugLogger.Fsstore.ActiveFilePath,
	}
	require.Same(t, defaultLogger.lifecycleGuard, errorLogger.lifecycleGuard)
	require.Same(t, defaultLogger.lifecycleGuard, debugLogger.lifecycleGuard)
	protectedPaths := make(map[string]struct{}, len(activePaths))
	defaultLogger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		for path := range protected {
			protectedPaths[path] = struct{}{}
		}
	})
	for _, path := range activePaths {
		_, protected := protectedPaths[filepath.Clean(path)]
		require.True(t, protected, "delivery active path is not registered: %s", path)
	}

	// 現在のdefault roleが所有権とarchive完了状態を判定できるcanonicalな旧fileを使います。
	// 未知patternのfileはMaxDirSizeの容量には含めても、安全のため削除候補にはしません。
	activeDate, err := time.ParseInLocation(pkgfsstore.FileDateFormat, defaultLogger.Fsstore.FileName.Date, defaultLogger.Clock.Now().Location())
	require.NoError(t, err)
	// 保持期限内の前日分を使い、ageではなく容量超過による削除を検証します。
	inactivePath := filepath.Join(filepath.Dir(activePaths[0]), target+"_"+activeDate.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat)+"_1.log")
	require.NoError(t, os.WriteFile(inactivePath, nil, 0o600))
	require.NoError(t, os.Truncate(inactivePath, int64(maxDirSize+1)))
	inactiveTime := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(inactivePath, inactiveTime, inactiveTime))

	lifecycles, err := logfilelifecycle.NewDeliveryFileLifecycles(logfilelifecycle.DeliveryFileOptions{
		Config: pkgfsstore.DeliveryFileLifecycleRunnerConfig{
			FsstoreConfig: pkgfsstore.DeliveryFileLifecycleStoreConfig{
				Layout: cfg.FsstoreConfig.Layout,
				Lifecycle: pkgfsstore.DeliveryFileLifecycleConfig{
					UsageCacheTTL:      time.Hour,
					EnableRotate:       cfg.FsstoreConfig.Rotation.EnableRotate,
					RotationType:       cfg.FsstoreConfig.Rotation.RotationType,
					RotationDayOfWeek:  cfg.FsstoreConfig.Rotation.RotationDayOfWeek,
					MaxFileSize:        cfg.FsstoreConfig.Rotation.MaxFileSize,
					MaxFileBackups:     fsstore.MinAllowedMaxFileBackups,
					MaxFileAge:         fsstore.MinAllowedMaxFileAge,
					MaxDirSize:         maxDirSize,
					EnableFileCompress: false,
					CompressFormat:     pkgfsstore.CompressGzip,
				},
				Duration: cfg.FsstoreConfig.Duration,
				Retry:    cfg.FsstoreConfig.Retry,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, lifecycles, 1)

	report, err := lifecycles[0].RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, report.Purge.ByDirSize.Ran)
	require.Len(t, report.Purge.ByDirSize.RemovalResults, 1)
	require.Equal(t, filepath.Clean(inactivePath), filepath.Clean(report.Purge.ByDirSize.RemovalResults[0].File.Path))
	require.NoError(t, report.Purge.ByDirSize.RemovalResults[0].Err)
	require.NoFileExists(t, inactivePath)
	for _, path := range activePaths {
		require.FileExists(t, path)
	}
}

func TestDeliveryFileLogger_DedicatedLoggersRouteByLevel(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "delivery-file-logger-routing-request")
	logPath := t.TempDir()

	defaultConfig := newTestDeliveryFileLoggerConfig(logPath, "delivery_default_logs", "delivery-default-routing")
	defaultLogger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(defaultConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "defaultLogger Close", func() closerForTest { return defaultLogger }, true)

	infoMessage := "delivery default logger writes info"
	result, err := defaultLogger.testInfo(ctx, infoMessage)
	requireDeliverySucceeded(t, result, err)
	assertLogFileContains(t, defaultLogger.Fsstore.ActiveFilePath, infoMessage)

	errorMessage := "delivery default logger skips error when separated"
	result, err = defaultLogger.testError(ctx, errorMessage)
	requireDeliverySkipped(t, result, err)
	assertLogFileNotContains(t, defaultLogger.Fsstore.ActiveFilePath, errorMessage)

	debugMessage := "delivery default logger skips debug when separated"
	result, err = defaultLogger.testDebug(ctx, debugMessage)
	requireDeliverySkipped(t, result, err)
	assertLogFileNotContains(t, defaultLogger.Fsstore.ActiveFilePath, debugMessage)

	errorConfig := newTestDeliveryFileLoggerConfig(logPath, "delivery_error_default_logs", "delivery-error-routing")
	errorLogger, err := NewDeliveryErrorFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(errorConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "errorLogger Close", func() closerForTest { return errorLogger }, true)

	infoMessageForErrorLogger := "delivery error logger skips info"
	result, err = errorLogger.testInfo(ctx, infoMessageForErrorLogger)
	requireDeliverySkipped(t, result, err)
	assertLogFileNotContains(t, errorLogger.Fsstore.ActiveFilePath, infoMessageForErrorLogger)

	errorMessageForErrorLogger := "delivery error logger writes error"
	result, err = errorLogger.testError(ctx, errorMessageForErrorLogger)
	requireDeliverySucceeded(t, result, err)
	assertLogFileContains(t, errorLogger.Fsstore.ActiveFilePath, errorMessageForErrorLogger)
	assert.Contains(t, filepath.Base(errorLogger.Fsstore.ActiveFilePath), "error_")

	debugConfig := newTestDeliveryFileLoggerConfig(logPath, "delivery_debug_logs", "delivery-debug-routing")
	debugLogger, err := NewDeliveryDebugFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(debugConfig), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "debugLogger Close", func() closerForTest { return debugLogger }, true)

	infoMessageForDebugLogger := "delivery debug logger skips info"
	result, err = debugLogger.testInfo(ctx, infoMessageForDebugLogger)
	requireDeliverySkipped(t, result, err)
	assertLogFileNotContains(t, debugLogger.Fsstore.ActiveFilePath, infoMessageForDebugLogger)

	debugMessageForDebugLogger := "delivery debug logger writes debug"
	result, err = debugLogger.testDebug(ctx, debugMessageForDebugLogger)
	requireDeliverySucceeded(t, result, err)
	assertLogFileContains(t, debugLogger.Fsstore.ActiveFilePath, debugMessageForDebugLogger)
	assert.Contains(t, filepath.Base(debugLogger.Fsstore.ActiveFilePath), "debug_")
}

func TestDeliveryFileLogger_LogFileRotation(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "delivery-file-logger-rotation-request")
	logPath := t.TempDir()

	cfg := newTestDeliveryFileLoggerConfig(logPath, "delivery_rotation_logs", "delivery-rotation")
	cfg.FsstoreConfig.Rotation.EnableRotate = true
	// 通常のfile loggerと同じ下限の1MiBを上限に使います。
	require.EqualValues(t, fsstore.MinAllowedFileSizeBytes, cfg.FsstoreConfig.Rotation.MaxFileSize)

	logger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	// 日付による切り替えと区別するため、作成したfileと同じ日の時刻へ固定します。
	period, err := time.Parse(pkgfsstore.FileDateFormat, logger.Fsstore.FileName.Date)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(period.Add(12 * time.Hour))

	// 上限に達する記録も通常の追記として同じfileへ書き、次の記録でsizeによるrotationを起こします。
	firstPath := logger.Fsstore.ActiveFilePath
	result, err := logger.testInfo(ctx, strings.Repeat("a", int(cfg.FsstoreConfig.Rotation.MaxFileSize)))
	requireDeliverySucceeded(t, result, err)
	require.Equal(t, firstPath, logger.Fsstore.ActiveFilePath)
	require.Equal(t, 1, logger.Fsstore.FileName.Index)
	firstInfo, err := os.Stat(firstPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, uint64(firstInfo.Size()), cfg.FsstoreConfig.Rotation.MaxFileSize)

	testMessage := "delivery rotation writes this line after the file rotates"
	result, err = logger.testInfo(ctx, testMessage)
	requireDeliverySucceeded(t, result, err)

	require.Equal(t, 2, logger.Fsstore.FileName.Index)
	require.NotEqual(t, firstPath, logger.Fsstore.ActiveFilePath)
	require.ElementsMatch(t, []string{firstPath, logger.Fsstore.ActiveFilePath}, deliveryLogFiles(t, logger))
	assertLogFileNotContains(t, firstPath, testMessage)
	assertLogFileContains(t, logger.Fsstore.ActiveFilePath, testMessage)
}

func TestFileLogger_UploadsRotatedFileWithoutCompression(t *testing.T) {
	ctx := context.Background()
	logPath := t.TempDir()
	cfg := newTestFileLoggerConfig(logPath, "upload_no_compress_logs", "upload-no-compress")
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = false

	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	oldPath := filepath.Join(logPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir, "rotated.log")
	require.NoError(t, os.WriteFile(oldPath, []byte("rotated log"), 0o600))

	recorder := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, recorder)

	logger.tryArchiveRotatedFile(ctx, oldPath, logger.Fsstore.FileName)

	inputs := recorder.inputsSnapshot()
	require.Len(t, inputs, 1)
	require.Equal(t, oldPath, inputs[0].FilePath())
	require.FileExists(t, oldPath)
}

func TestFileLogger_UploadsCompressedRotatedFileWhenCompressionEnabled(t *testing.T) {
	ctx := context.Background()
	logPath := t.TempDir()
	cfg := newTestFileLoggerConfig(logPath, "upload_compress_logs", "upload-compress")
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip

	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	oldPath := filepath.Join(logPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir, "rotated.log")
	require.NoError(t, os.WriteFile(oldPath, []byte("rotated log"), 0o600))

	recorder := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, recorder)

	logger.tryArchiveRotatedFile(ctx, oldPath, logger.Fsstore.FileName)

	compressedPath := oldPath + pkgfsstore.ExtensionCompressGzip
	inputs := recorder.inputsSnapshot()
	require.Len(t, inputs, 1)
	require.Equal(t, compressedPath, inputs[0].FilePath())
	require.NoFileExists(t, oldPath)
	require.NoFileExists(t, compressedPath)
}

func TestFileLogger_ManagerLifecycleModeSkipsInlineArchive(t *testing.T) {
	ctx := context.Background()
	logPath := t.TempDir()
	cfg := newTestFileLoggerConfig(logPath, "manager_lifecycle_logs", "manager-lifecycle")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeRunner
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip

	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	oldPath := filepath.Join(logPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir, "rotated.log")
	require.NoError(t, os.WriteFile(oldPath, []byte("rotated log"), 0o600))

	recorder := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, recorder)

	logger.tryArchiveRotatedFile(ctx, oldPath, logger.Fsstore.FileName)

	require.Empty(t, recorder.inputsSnapshot())
	require.FileExists(t, oldPath)
	require.NoFileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
}

func TestDeliveryFileLogger_CloseThenWriteFails(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "delivery-file-logger-close-request")
	logPath := t.TempDir()

	cfg := newTestDeliveryFileLoggerConfig(logPath, "delivery_close_logs", "delivery-close")
	logger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))

	report, err := logger.testInfo(ctx, "delivery close should fail writes")
	require.Error(t, err)
	result := requireSingleDeliveryTarget(t, report)
	require.Equal(t, err, result.Err)
	require.ErrorIs(t, result.Err, pkglogger.ErrLoggerClosed)
	assert.Equal(t, BackendServices, result.Backend)
	assert.Equal(t, pkglogger.DeliveryRejected, result.Status)
}

func TestDeliveryFileLogger_ErrorWithStack_WritesToFile(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "delivery-file-logger-error-with-stack-request")
	logPath := t.TempDir()

	cfg := newTestDeliveryFileLoggerConfig(logPath, "delivery_error_with_stack_logs", "delivery-error-with-stack")
	logger, err := NewDeliveryErrorFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	testMessage := "delivery error with stack writes this message"
	testErr := fmt.Errorf("delivery stack test error")
	report, err := logger.testErrorWithStack(ctx, testMessage, testErr)
	requireDeliverySucceeded(t, report, err)
	line := requireSingleLogLineContaining(t, logger.Fsstore.ActiveFilePath, testMessage)
	// fileへ書いた行の書式（errorの欄に文言を出すこと）がこの試験の対象です。
	assert.Contains(t, line, " - err: "+testErr.Error())
	assert.Contains(t, line, " (stacktrace):")
	assert.Contains(t, filepath.Base(logger.Fsstore.ActiveFilePath), "error_")

	// errorが渡されなくても拒否せず、errorの欄を持たないERRORとしてstackとともに記録します。
	nilErrorMessage := "delivery error with stack records a nil error"
	report, err = logger.testErrorWithStack(ctx, nilErrorMessage, nil)
	requireDeliverySucceeded(t, report, err)
	line = requireSingleLogLineContaining(t, logger.Fsstore.ActiveFilePath, nilErrorMessage)
	assert.Contains(t, line, "[ERROR]")
	assert.Contains(t, line, " (stacktrace):")
	assert.NotContains(t, line, " - err: ")
}

func TestFileLogger_ErrorWithStackRecordsNilErrorWithStack(t *testing.T) {
	ctx := requestctx.WithRequestID(context.Background(), "file-logger-error-with-stack-request")
	cfg := newTestFileLoggerConfig(t.TempDir(), "error_with_stack_logs", "error-with-stack")
	logger, err := newStandaloneErrorFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	testErr := fmt.Errorf("file stack test error")
	require.NoError(t, logger.testErrorWithStack(ctx, "file error with stack writes this message", testErr))
	line := requireSingleLogLineContaining(t, logger.Fsstore.ActiveFilePath, "file error with stack writes this message")
	// fileへ書いた行の書式（errorの欄に文言を出すこと）がこの試験の対象です。
	assert.Contains(t, line, " - err: "+testErr.Error())
	assert.Contains(t, line, " (stacktrace):")

	// errorが渡されなくても拒否せず、errorの欄を持たないERRORとしてstackとともに記録します。
	nilErrorMessage := "file error with stack records a nil error"
	require.NoError(t, logger.testErrorWithStack(ctx, nilErrorMessage, nil))
	line = requireSingleLogLineContaining(t, logger.Fsstore.ActiveFilePath, nilErrorMessage)
	assert.Contains(t, line, "[ERROR]")
	assert.Contains(t, line, " (stacktrace):")
	assert.NotContains(t, line, " - err: ")
}

// requireSingleLogLineContainingは、messageを含む行がfile内に一行だけあることを確認して返します。
func requireSingleLogLineContaining(t *testing.T, filePath string, message string) string {
	t.Helper()

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	var matched []string
	for _, line := range strings.Split(string(content), "\n") {
		if strings.Contains(line, message) {
			matched = append(matched, line)
		}
	}
	require.Len(t, matched, 1, "log lines containing %q", message)
	return matched[0]
}

func TestDeliveryFileLoggerFailedResultPreservesError(t *testing.T) {
	logger := &DeliveryFileLogger{}

	tests := []struct {
		name string
		err  error
	}{
		{name: "deadline", err: fmt.Errorf("write timed out: %w", context.DeadlineExceeded)},
		{name: "canceled", err: fmt.Errorf("write canceled: %w", context.Canceled)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := logger.deliveryFailedResult(tt.err)
			assert.Equal(t, pkglogger.DeliveryFailed, result.Status)
			assert.Equal(t, tt.err, result.Err)
		})
	}
}

func newTestFileLoggerConfig(logPath string, logDir string, target string) pkglogger.FileLoggerConfig {
	return pkglogger.FileLoggerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:                     target,
				LogPath:                    logPath,
				LogDir:                     logDir,
				ErrorLogDir:                logDir + "_error",
				IsSeparateErrorLogsEnabled: true,
				SeparateErrorLogsPrefix:    "error_",
				IsSeparateDebugLogsEnabled: true,
				SeparateDebugLogsPrefix:    "debug_",
				FileNameFormat:             pkgfsstore.FileNameFormat,
				FileDateFormat:             pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				EnableRotate:       false,
				RotationType:       pkgfsstore.FileRotationDaily,
				UsageCacheTTL:      time.Hour,
				MaxFileBackups:     fsstore.MinAllowedMaxFileBackups,
				MaxFileAge:         fsstore.MinAllowedMaxFileAge,
				MaxFileSize:        fsstore.MinAllowedFileSizeBytes,
				MaxDirSize:         fsstore.MinAllowedMaxDirSize,
				EnableFileCompress: false,
				CompressFormat:     pkgfsstore.CompressGzip,
			},
			Duration: pkgfsstore.DurationConfig{
				CompressWarnThresholdDuration:   time.Millisecond,
				CompressErrorThresholdDuration:  time.Millisecond,
				WriteFileWarnThresholdDuration:  time.Millisecond,
				WriteFileErrorThresholdDuration: time.Millisecond,
			},
			Retry: pkgfsstore.RetryConfig{
				MaxWriteRetries:            0,
				FileOperationRetryWaitTime: time.Millisecond,
			},
		},
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: false,
		},
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "logger-file-test",
			IsDebug:  false,
			Target:   target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        "2006-01-02 15:04:05",
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		SamplingConfig: pkglogger.SamplingConfig{
			IsSamplingEnabled:        false,
			EnableLoggingByRequestID: false,
			LoggingByRequestIDRate:   1,
			SamplingDebugRate:        1,
			SamplingInfoRate:         1,
			SamplingWarnRate:         1,
			SamplingErrorRate:        1,
		},
	}
}

// breakRegularFileStreamはLogger自体をClose状態へ遷移させず、file I/Oだけを失敗させます。
// public Close後の記録拒否と、記録試行中に起きた内部errorのfallbackを混同しない試験用です。
func breakRegularFileStream(t *testing.T, logger *FileLogger) {
	t.Helper()
	logger.Fsstore.Mu.Lock()
	defer logger.Fsstore.Mu.Unlock()
	require.NotNil(t, logger.Fsstore.File)
	require.NoError(t, logger.Fsstore.File.Close())
	logger.Fsstore.File = nil
}

func newTestFileLoggerSet(
	t *testing.T,
	cfg FileLoggerConfigProvider,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) *LoggerSet {
	t.Helper()

	loggerSet, err := NewLoggerSet(cfg, tracer, meter)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)
	return loggerSet
}

func newTestDeliveryFileLoggerConfig(logPath string, logDir string, target string) pkglogger.DeliveryFileLoggerConfig {
	fileLoggerConfig := newTestFileLoggerConfig(logPath, logDir, target)

	return pkglogger.DeliveryFileLoggerConfig{
		FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
			Layout: fileLoggerConfig.FsstoreConfig.Layout,
			Rotation: pkgfsstore.FileRotationConfig{
				EnableRotate:      fileLoggerConfig.FsstoreConfig.Lifecycle.EnableRotate,
				RotationType:      fileLoggerConfig.FsstoreConfig.Lifecycle.RotationType,
				RotationDayOfWeek: fileLoggerConfig.FsstoreConfig.Lifecycle.RotationDayOfWeek,
				MaxFileSize:       fileLoggerConfig.FsstoreConfig.Lifecycle.MaxFileSize,
			},
			Duration: fileLoggerConfig.FsstoreConfig.Duration,
			Retry:    fileLoggerConfig.FsstoreConfig.Retry,
		},
		TimeConfig: fileLoggerConfig.TimeConfig,
		RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
			AppName: "logger-file-test",
			IsDebug: false,
			Target:  target,
		},
		LogBodyConfig: fileLoggerConfig.LogBodyConfig,
	}
}

type recordingArchiveUploader struct {
	mu     sync.Mutex
	inputs []uploader.UploadRequest
}

func (r *recordingArchiveUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	_ = ctx
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inputs = append(r.inputs, input)
	return nil
}

func (r *recordingArchiveUploader) inputsSnapshot() []uploader.UploadRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uploader.UploadRequest(nil), r.inputs...)
}

func assertLogFileContains(t *testing.T, filePath string, message string) {
	t.Helper()

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Contains(t, string(content), message)
}

func assertLogFileNotContains(t *testing.T, filePath string, message string) {
	t.Helper()

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.NotContains(t, string(content), message)
}

func requireSingleDeliveryTarget(t *testing.T, report pkglogger.DeliveryReport) pkglogger.DeliveryResult {
	t.Helper()

	require.Len(t, report.Targets, 1)
	return report.Targets[0]
}

func requireDeliverySucceeded(t *testing.T, report pkglogger.DeliveryReport, err error) {
	t.Helper()

	require.NoError(t, err)
	result := requireSingleDeliveryTarget(t, report)
	require.NoError(t, result.Err)
	assert.Equal(t, BackendServices, result.Backend)
	assert.Equal(t, pkglogger.DeliverySucceeded, result.Status)
}

func requireDeliverySkipped(t *testing.T, report pkglogger.DeliveryReport, err error) {
	t.Helper()

	require.NoError(t, err)
	result := requireSingleDeliveryTarget(t, report)
	require.NoError(t, result.Err)
	assert.Equal(t, BackendServices, result.Backend)
	assert.Equal(t, pkglogger.DeliverySkipped, result.Status)
}

func newTestLokiLoggerBaseConfig(target string) pkglogger.LokiLoggerBaseConfig {
	return pkglogger.LokiLoggerBaseConfig{
		RuntimeConfig:           newTestFallbackRuntimeConfig(target),
		LogBodyConfig:           newTestFallbackLogBodyConfig(),
		FlushInterval:           time.Hour,
		MaxEntries:              1,
		BufferFlushBytes:        1,
		FallbackQueueMaxEntries: 1,
		MaxLineBytes:            256 * 1024,
		MaxPushRequestBytes:     4 * 1024 * 1024,
		NoBulkFlushModeEnabled:  true,
		SamplingConfig:          newTestFallbackSamplingConfig(),
	}
}

func newTestLokiFlushManagerConfig(target string) pkglogger.LokiFlushManagerConfig {
	return pkglogger.LokiFlushManagerConfig{
		Resource: newTestFallbackLoggerResource(target),
	}
}

func newTestCloudWatchLogsLoggerBaseConfig(target string) pkglogger.CloudWatchLogsLoggerBaseConfig {
	return pkglogger.CloudWatchLogsLoggerBaseConfig{
		RuntimeConfig:          newTestFallbackRuntimeConfig(target),
		LogBodyConfig:          newTestFallbackLogBodyConfig(),
		FlushInterval:          time.Hour,
		BufferFlushMaxEvents:   1,
		BufferFlushBytes:       1,
		FallbackQueueMaxEvents: 1,
		NoBulkFlushModeEnabled: true,
		SamplingConfig:         newTestFallbackSamplingConfig(),
	}
}

func newTestCloudWatchLogsFlushManagerConfig(target string) pkglogger.CloudWatchLogsFlushManagerConfig {
	return pkglogger.CloudWatchLogsFlushManagerConfig{
		Resource:      newTestFallbackLoggerResource(target),
		LogGroupName:  "/test/file-fallback",
		LogStreamName: target,
	}
}

func newTestClickHouseLoggerBaseConfig(target string, maxEntries int) pkglogger.ClickHouseLoggerBaseConfig {
	return pkglogger.ClickHouseLoggerBaseConfig{
		TimeConfig:              pkglogger.TimeConfig{},
		RuntimeConfig:           newTestFallbackRuntimeConfig(target),
		LogBodyConfig:           newTestFallbackLogBodyConfig(),
		FlushInterval:           time.Hour,
		MaxEntries:              maxEntries,
		BufferFlushBytes:        1024 * 1024,
		FallbackQueueMaxEntries: 10000,
		MaxInsertRequestBytes:   16 * 1024 * 1024,
		SamplingConfig:          newTestFallbackSamplingConfig(),
	}
}

func newTestClickHouseFlushManagerConfig(target string) pkglogger.ClickHouseFlushManagerConfig {
	return pkglogger.ClickHouseFlushManagerConfig{
		Resource: newTestFallbackLoggerResource(target),
	}
}

func newTestFallbackRuntimeConfig(target string) pkglogger.RuntimeConfig {
	return pkglogger.RuntimeConfig{
		AppName:  "logger-fallback-test",
		IsDebug:  false,
		Target:   target,
		LogLevel: pkglogger.LogLevelDebug,
	}
}

func newTestFallbackLogBodyConfig() pkglogger.LogBodyConfig {
	return pkglogger.LogBodyConfig{
		LogLineDateFormat:        time.RFC3339Nano,
		ErrorTraceDepth:          1,
		ErrorTraceDepthDebugMode: 1,
	}
}

func newTestFallbackSamplingConfig() pkglogger.SamplingConfig {
	return pkglogger.SamplingConfig{
		IsSamplingEnabled:        false,
		EnableLoggingByRequestID: false,
		LoggingByRequestIDRate:   1,
		SamplingDebugRate:        1,
		SamplingInfoRate:         1,
		SamplingWarnRate:         1,
		SamplingErrorRate:        1,
	}
}

func newTestFallbackLoggerResource(target string) pkglogger.Resource {
	return pkglogger.Resource{
		ServiceName: "logger-fallback-test",
		HostName:    "logger-fallback-host",
		Environment: target,
		Cloud:       &pkglogger.CloudResourceInfo{},
	}
}

func newTestFallbackResource(target string) loggerresource.ResourceProvider {
	return loggerresource.NewResourceAdapter(newTestFallbackLoggerResource(target))
}

type recordingHTTPBodySink struct {
	status       int
	responseBody string
	mu           sync.Mutex
	bodies       []string
	// receivedは、requestの本文を記録するたびに一件ずつ通知します。
	received chan struct{}
}

func newRecordingHTTPBodySink(t *testing.T, status int, responseBody string) *recordingHTTPBodySink {
	t.Helper()

	return &recordingHTTPBodySink{
		status:       status,
		responseBody: responseBody,
		received:     make(chan struct{}, 64),
	}
}

func (s *recordingHTTPBodySink) URL() string {
	return "http://fallback.test"
}

func (s *recordingHTTPBodySink) Client() *http.Client {
	return &http.Client{Transport: s}
}

func (s *recordingHTTPBodySink) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.bodies = append(s.bodies, string(body))
	s.mu.Unlock()
	select {
	case s.received <- struct{}{}:
	default:
	}

	return &http.Response{
		StatusCode: s.status,
		Status:     fmt.Sprintf("%d %s", s.status, http.StatusText(s.status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(s.responseBody)),
		Request:    req,
	}, nil
}

func (s *recordingHTTPBodySink) requireBodyContains(t *testing.T, want string) {
	t.Helper()

	bodies := s.bodiesSnapshot()

	require.NotEmpty(t, bodies, "fallback destination received no HTTP requests")
	for _, body := range bodies {
		if strings.Contains(body, want) {
			return
		}
	}
	t.Fatalf("fallback destination body does not contain %q; bodies=%q", want, bodies)
}

func (s *recordingHTTPBodySink) requireBodyCount(t *testing.T, want int) {
	t.Helper()

	bodies := s.bodiesSnapshot()
	require.Len(t, bodies, want, "fallback destination HTTP request count")
}

func (s *recordingHTTPBodySink) requireOnlyBodyLineCount(t *testing.T, want int) {
	t.Helper()

	bodies := s.bodiesSnapshot()
	require.Len(t, bodies, 1, "expected exactly one fallback destination HTTP request")
	body := strings.TrimSpace(bodies[0])
	require.NotEmpty(t, body)
	require.Len(t, strings.Split(body, "\n"), want, "fallback destination batch row count")
}

func (s *recordingHTTPBodySink) bodiesSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.bodies...)
}

type recordingNamedErrorHandler struct {
	name  string
	mu    sync.Mutex
	calls []recordingErrorHandlerCall
}

type recordingErrorHandlerCall struct {
	err     error
	message string
	source  string
}

func newRecordingNamedErrorHandler(name string) *recordingNamedErrorHandler {
	return &recordingNamedErrorHandler{name: name}
}

func (h *recordingNamedErrorHandler) namedHandler() logging.NamedLoggerErrorHandler {
	return logging.NamedLoggerErrorHandler{
		Name:    h.name,
		Handler: h.handle,
	}
}

func (h *recordingNamedErrorHandler) handle(
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) {
	message := ""
	source := ""
	if entry != nil {
		message = entry.Message
		source = entry.Source
	}

	h.mu.Lock()
	h.calls = append(h.calls, recordingErrorHandlerCall{
		err:     err,
		message: message,
		source:  source,
	})
	h.mu.Unlock()
}

func (h *recordingNamedErrorHandler) requireMessage(t *testing.T, want string) {
	t.Helper()

	h.mu.Lock()
	calls := append([]recordingErrorHandlerCall(nil), h.calls...)
	h.mu.Unlock()

	require.NotEmpty(t, calls, "handler %s was not called", h.name)
	for _, call := range calls {
		if strings.Contains(call.message, want) {
			return
		}
	}
	t.Fatalf("handler %s did not receive message %q; calls=%+v", h.name, want, calls)
}

func (h *recordingNamedErrorHandler) requireCallCount(t *testing.T, want int) {
	t.Helper()

	h.mu.Lock()
	got := len(h.calls)
	h.mu.Unlock()
	require.Equal(t, want, got, "handler %s call count", h.name)
}

func assertAnyLogFileContains(t *testing.T, root string, message string) {
	t.Helper()

	var found bool
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info == nil || info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), message) {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "no log file under %s contains %q", root, message)
}

func assertNoLogFileContains(t *testing.T, root string, message string) {
	t.Helper()

	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info == nil || info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		require.NotContains(t, string(content), message, "unexpected message in %s", path)
		return nil
	})
	require.NoError(t, err)
}

func deliveryLogFiles(t *testing.T, logger *DeliveryFileLogger) []string {
	t.Helper()

	logDir, err := logger.GetFileSystemStoreDirectoryPath()
	require.NoError(t, err)

	fileDate, err := fsstore.ExtractDateFromFileName(logger.Fsstore.ActiveFilePath)
	require.NoError(t, err)

	pattern := fmt.Sprintf(
		"%s_%s_*.log",
		logger.config.Fsstore().Layout().Target(),
		fileDate.Format(logger.config.Fsstore().Layout().FileDateFormat()),
	)
	matches, err := filepath.Glob(filepath.Join(logDir, pattern))
	require.NoError(t, err)
	return matches
}

// MaxFileBackupsを超えた場合、active fileを保護しながら古い世代から削除することを確認します。
// 出力先はt.TempDir配下に限定し、repositoryや実際のログ保存先へtest compressed log fileを残しません。
func TestFileLogger_MaxBackupsRotation(t *testing.T) {
	ctx := context.Background()
	const maxBackups = 5

	logPath := resolvedTempDirForTest(t)
	cfg := newTestFileLoggerConfig(logPath, "max_backups", testTarget)
	cfg.FsstoreConfig.Lifecycle.MaxFileBackups = maxBackups
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	logDir, err := logger.GetFileSystemStoreDirectoryPath()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(logPath, testTarget, "max_backups"), logDir)
	require.Equal(t, logDir, filepath.Dir(logger.Fsstore.ActiveFilePath))

	fileName := logger.Fsstore.FileName
	backupPaths := make(map[int]string, 9)
	baseModTime := time.Now().Add(-time.Hour)
	for index := 2; index <= 10; index++ {
		name := fmt.Sprintf(
			logger.config.Fsstore().Layout().FileNameFormat(),
			fileName.Prefix,
			fileName.Target,
			fileName.Date,
			index,
		)
		path := filepath.Join(logDir, name)
		require.NoError(t, os.WriteFile(path, nil, 0640))
		require.NoError(t, os.Chtimes(
			path,
			baseModTime.Add(time.Duration(index)*time.Second),
			baseModTime.Add(time.Duration(index)*time.Second),
		))
		backupPaths[index] = path
	}

	require.NoError(t, logger.testInfo(ctx, "max backups cleanup"))
	// 書き込みが受理したbackgroundのcleanupが終わってから、残った世代を確認します。
	waitBackgroundForTest(t, logger)
	require.False(t, logger.inlinePurge.cleanupRunning.Load())
	for index := 2; index <= 5; index++ {
		require.NoFileExists(t, backupPaths[index])
	}
	for index := 6; index <= 10; index++ {
		require.FileExists(t, backupPaths[index])
	}

	require.FileExists(t, logger.Fsstore.ActiveFilePath)
	entries, err := os.ReadDir(logDir)
	require.NoError(t, err)
	require.Len(t, entries, maxBackups+1)
}

// **✅ ディレクトリのサイズ管理**
func TestFileLogger_Complex_Condition_PurgeByDirSize(t *testing.T) {
	ctx := context.Background()
	const (
		activeFileSize   = int64(4 * 1024 * 1024)
		inactiveFileSize = int64(2 * 1024 * 1024)
	)

	cfg := newTestFileLoggerConfig(
		t.TempDir(),
		"complex-purge-by-dir-size",
		"complex-purge-by-dir-size",
	)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.MaxDirSize = fsstore.MinAllowedMaxDirSize

	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	activePath := logger.Fsstore.ActiveFilePath
	logDir := filepath.Dir(activePath)
	// active fileを最古かつ4MBにします。directory sizeには含めますが、
	// open中なので削除候補からは除外されなければなりません。
	require.NoError(t, os.Truncate(activePath, activeFileSize))
	baseTime := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(activePath, baseTime, baseTime))

	inactivePaths := make([]string, 0, 4)
	for i := 0; i < 4; i++ {
		// targetの命名規則には一致させず、age・generationではなくdirectory全体を扱う
		// dir-size purgeだけが削除するfileとして作成します。
		path := filepath.Join(logDir, fmt.Sprintf("inactive_%s_%d.log", logger.Fsstore.FileName.Date, i+2))
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		require.NoError(t, os.Truncate(path, inactiveFileSize))
		// mtimeを世代順とは逆にし、更新日時ではなくファイル名で削除することを検証します。
		modTime := baseTime.Add(time.Duration(4-i) * time.Minute)
		require.NoError(t, os.Chtimes(path, modTime, modTime))
		inactivePaths = append(inactivePaths, path)
	}

	// 合計12MBを上限10MBへ収めるため、削除可能なfileのうち最古の2MBだけを削除します。
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))

	require.FileExists(t, activePath)
	require.NoFileExists(t, inactivePaths[0])
	for _, path := range inactivePaths[1:] {
		require.FileExists(t, path)
	}

	files, err := filepath.Glob(filepath.Join(logDir, "*.log"))
	require.NoError(t, err)
	var totalSize uint64
	for _, path := range files {
		info, err := os.Stat(path)
		require.NoError(t, err)
		totalSize += uint64(info.Size())
	}
	require.Equal(t, cfg.FsstoreConfig.Lifecycle.MaxDirSize, totalSize)
}

// OnError時の挙動のテスト
func TestFileLogger_OnError_Debug(t *testing.T) {
	ctx := context.Background()

	sourceCfg := newTestFileLoggerConfig(
		t.TempDir(),
		"logs_test_on_error",
		"file-onerror-source-test",
	)
	logger, err := newStandaloneErrorFileLogger(ctx, NewConfigAdapter(sourceCfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	fallbackRoot := t.TempDir()
	fallbackCfg := newTestFileLoggerConfig(
		fallbackRoot,
		"logs_test_on_error_fallback",
		"file-onerror-fallback-test",
	)
	fallbackLoggerSet := newTestFileLoggerSet(
		t,
		NewConfigAdapter(fallbackCfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	slackRecorder := newRecordingNamedErrorHandler(common.HandlerNameSlackLogger)
	logger.OnError = onerrorhandler.CombineErrorHandlers(
		fallbackLoggerSet.NewNamedErrorFallbackHandler(),
		slackRecorder.namedHandler(),
	)

	// error型やsource backendを理由にhandlerを暗黙除外しません。明示的に選択した
	// fallbackはno-fallback境界で一度だけ実行されます。
	// error用fallbackはerror roleのfileへ記録するため、error未満のentryは出力しません。
	// 各messageは他のmessageの部分文字列にならないようにし、個別に出力の有無を確認します。
	const (
		debugMessage               = "onerror debug log test desu error未満なのでファイルには出力しない"
		errorLoggerMessage         = "onerror error log test desu ErrorLoggerの内部エラーもファイルに出力する"
		defaultLoggerMessage       = "onerror error log test desu DefaultLoggerの内部エラーをファイルに出力する"
		secondDefaultLoggerMessage = "onerror error log test desu DefaultLoggerの2件目の内部エラーもファイルに出力する"
	)
	logger.OnError(
		ctx,
		&errors.ErrorLoggerInternalError{Op: "test", Err: fmt.Errorf("test")},
		&logging.LogEntry{
			Level:    pkglogger.LogLevelDebug,
			Message:  debugMessage,
			Internal: true,
		},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	logger.OnError(
		ctx,
		&errors.ErrorLoggerInternalError{Op: "test", Err: fmt.Errorf("test")},
		&logging.LogEntry{
			Level:    pkglogger.LogLevelError,
			Message:  errorLoggerMessage,
			Internal: true,
		},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	logger.OnError(
		ctx,
		&errors.DefaultLoggerInternalError{Op: "test", Err: fmt.Errorf("test")},
		&logging.LogEntry{
			Level:    pkglogger.LogLevelError,
			Message:  defaultLoggerMessage,
			Internal: true,
		},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	logger.OnError(
		ctx,
		&errors.DefaultLoggerInternalError{Op: "test", Err: fmt.Errorf("test")},
		&logging.LogEntry{
			Level:    pkglogger.LogLevelError,
			Message:  secondDefaultLoggerMessage,
			Internal: true,
		},
		observer.NopTracer{},
		observer.NopMeter{},
	)

	slackRecorder.requireCallCount(t, 4)
	slackRecorder.requireMessage(t, secondDefaultLoggerMessage)
	assertAnyLogFileContains(t, fallbackRoot, errorLoggerMessage)
	assertAnyLogFileContains(t, fallbackRoot, defaultLoggerMessage)
	assertAnyLogFileContains(t, fallbackRoot, secondDefaultLoggerMessage)
	assertNoLogFileContains(t, fallbackRoot, debugMessage)
}

// spanを直接 tempoに送信
// otel collectorには経由していない
// 送信失敗もチェック
func TestFileLogger_Log_WritesToFile_ForTempo(t *testing.T) {
	settings := integrationtestutil.RequireIntegrationEnvironment(t, "TEMPO_INTEGRATION_TEST")
	// 送信先は専用の設定fileで明示した値だけを使います。fileにkeyがない場合に設定の既定の送信先
	// （localhost:4317）へ送らないよう、fileの値を必須とし、読み込んだ設定がその値であることを確認します。
	wantEndpoint := settings["HETEROLOG_OTEL_SPAN_ENDPOINT"]
	require.NotEmpty(t, strings.TrimSpace(wantEndpoint), "実Tempo integrationには専用の設定fileのHETEROLOG_OTEL_SPAN_ENDPOINTが必要です")

	cfg, err := config.LoadConfig()
	require.NoError(t, err)

	require.True(t, cfg.ObservabilityCfg.Tracing.Enabled, "実Tempo integrationにはHETEROLOG_TRACING_ENABLED=trueが必要です")
	require.True(
		t,
		strings.EqualFold(cfg.ObservabilityCfg.Tracing.Exporter.Type, "otlp"),
		"実Tempo integrationにはHETEROLOG_TRACING_EXPORTER_TYPE=otlpが必要です",
	)
	require.NotNil(t, cfg.ObservabilityCfg.Tracing.Exporter.OTLP)
	tempoEndpoint := cfg.ObservabilityCfg.Tracing.Exporter.OTLP.Endpoint
	require.Equal(t, wantEndpoint, tempoEndpoint, "HETEROLOG_OTEL_SPAN_ENDPOINTは専用の設定fileの値で読み込まれる必要があります")
	t.Log("HETEROLOG_OTEL_SPAN_ENDPOINT=", tempoEndpoint)
	// コンテキスト
	ctx := context.Background()

	// ==== exporter ====
	memExp := tracetest.NewInMemoryExporter()
	memSP := sdktrace.NewSimpleSpanProcessor(memExp)
	otlpExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(tempoEndpoint),
		//		otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		otlptracegrpc.WithInsecure(),
	)

	require.NoError(t, err)
	recOtlpExp := &recordingExporter{SpanExporter: otlpExp}

	otlpSP := sdktrace.NewSimpleSpanProcessor(recOtlpExp)
	//	otlpSP := sdktrace.NewBatchSpanProcessor(recOtlpExp)

	res, _ := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName("testservice"),
			semconv.DeploymentEnvironment("localtest"),
		),
	)
	// ==== exporter ====
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(memSP),
		sdktrace.WithSpanProcessor(otlpSP),
	)

	previousTracerProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		shutdownErr := tp.Shutdown(context.Background())
		otel.SetTracerProvider(previousTracerProvider)
		require.NoError(t, shutdownErr)
	})

	oteltracer := otel.Tracer("internal/infrastructure/logger/file/test")
	otelmeter := otel.Meter("internal/infrastructure/logger/file/test")
	tracer := appotel.NewOtelTracer(oteltracer)
	meter := appotel.NewOtelMeter(otelmeter)
	ctx, span := tracer.StartSpan(ctx, "testSpan")

	// 実Tempoとの通信試験であっても、file loggerの成果物はrepositoryや
	// /var/logへ残さず、試験専用directoryの中だけに作成します。
	fileConfig := newTestFileLoggerConfig(t.TempDir(), "logs_test", testTarget)
	adapter := NewConfigAdapter(fileConfig)
	logger, err := newStandaloneFileLogger(ctx, adapter, tracer, meter)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	// インフラ層なので直接呼び出す
	var uuidGen uuid.UUIDGenerator
	testMessage := "これはテストログです"
	ctx = requestctx.WithRequestID(ctx, uuidGen.NewID())
	err = logger.testInfo(ctx, testMessage)
	assert.NoError(t, err, "ログの書き込みに失敗")

	// ファイルの内容を確認
	filepath := logger.Fsstore.ActiveFilePath
	file, err := os.Open(filepath)

	assert.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var found bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, testMessage) {
			found = true
			break
		}
	}

	assert.True(t, found, "ログファイルに正しく記録されていない")

	// ★ ここで span を確実に終了させる
	span.End()

	// ★ flush（SimpleSpanProcessorでも基本即時だが、保険で）
	require.NoError(t, tp.ForceFlush(context.Background()))

	// ★ 追加: Tempo(OTLP) 送信エラー検証
	if errs := recOtlpExp.Errors(); len(errs) > 0 {
		for i, e := range errs {
			t.Logf("otlp export error[%d]=%v", i, e)
		}
		t.Fatalf("otlp exporter でエラーが発生しました: %d件", len(errs))
	}

	spans := memExp.GetSpans()

	// “test” span が存在すること
	var foundTest bool
	for i, s := range spans {
		t.Logf("=== span[%d] ===", i)
		// SpanContext
		sc := s.SpanContext
		t.Logf("name=%s", s.Name)
		t.Logf("trace_id=%s span_id=%s", sc.TraceID().String(), sc.SpanID().String())

		// 時刻
		t.Logf("start=%s end=%s", s.StartTime, s.EndTime)

		// ステータス
		t.Logf("status=%v", s.Status)

		if s.Name == "testSpan" {
			foundTest = true
			break
		}
	}
	assert.True(t, foundTest, "test span が exporter に出てこない")
}

type recordingExporter struct {
	sdktrace.SpanExporter
	mu   sync.Mutex
	errs []error
}

func (r *recordingExporter) ExportSpans(
	ctx context.Context,
	spans []sdktrace.ReadOnlySpan,
) error {
	err := r.SpanExporter.ExportSpans(ctx, spans)
	if err != nil {
		r.mu.Lock()
		r.errs = append(r.errs, err)
		r.mu.Unlock()
	}
	return err
}

func (r *recordingExporter) Errors() []error {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]error, len(r.errs))
	copy(out, r.errs)
	return out
}

// otel collector経由で送信
// アドレスすをかえるだけ
func TestFileLogger_Log_WritesToFile_ForTempo_Via_Collector(t *testing.T) {
	settings := integrationtestutil.RequireIntegrationEnvironment(t, "OTEL_COLLECTOR_INTEGRATION_TEST")
	// 送信先は専用の設定fileで明示した値だけを使い、processの環境から受け継いだ値は使いません。
	tempoEndpoint := settings["HETEROLOG_OTEL_COLLECTOR_ADDR"]
	require.NotEmpty(t, strings.TrimSpace(tempoEndpoint), "実OTel Collector integrationには専用の設定fileのHETEROLOG_OTEL_COLLECTOR_ADDRが必要です")

	_, err := config.LoadConfig()
	require.NoError(t, err)
	t.Log("HETEROLOG_OTEL_COLLECTOR_ADDR=", tempoEndpoint)
	// コンテキスト
	ctx := context.Background()

	// ==== exporter ====
	memExp := tracetest.NewInMemoryExporter()
	memSP := sdktrace.NewSimpleSpanProcessor(memExp)
	otlpExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(tempoEndpoint),
		//		otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		otlptracegrpc.WithInsecure(),
	)

	require.NoError(t, err)
	otlpSP := sdktrace.NewBatchSpanProcessor(otlpExp)

	res, _ := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceName("test"),
			semconv.DeploymentEnvironment("localtest"),
		),
	)
	// ==== exporter ====
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(memSP),
		sdktrace.WithSpanProcessor(otlpSP),
	)

	previousTracerProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		shutdownErr := tp.Shutdown(context.Background())
		otel.SetTracerProvider(previousTracerProvider)
		require.NoError(t, shutdownErr)
	})
	oteltracer := otel.Tracer("internal/infrastructure/logger/file/test")
	otelmeter := otel.Meter("internal/infrastructure/logger/file/test")
	tracer := appotel.NewOtelTracer(oteltracer)
	meter := appotel.NewOtelMeter(otelmeter)
	ctx, span := tracer.StartSpan(ctx, "test")

	// Collector接続の有無にかかわらず、file loggerの成果物は試験専用directoryへ隔離します。
	fileConfig := newTestFileLoggerConfig(t.TempDir(), "logs_test", testTarget)
	adapter := NewConfigAdapter(fileConfig)
	logger, err := newStandaloneFileLogger(ctx, adapter, tracer, meter)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	// インフラ層なので直接呼び出す
	var uuidGen uuid.UUIDGenerator
	testMessage := "これはテストログです"
	ctx = requestctx.WithRequestID(ctx, uuidGen.NewID())
	err = logger.testInfo(ctx, testMessage)
	assert.NoError(t, err, "ログの書き込みに失敗")

	// ファイルの内容を確認
	filepath := logger.Fsstore.ActiveFilePath
	file, err := os.Open(filepath)

	assert.NoError(t, err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var found bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, testMessage) {
			found = true
			break
		}
	}

	assert.True(t, found, "ログファイルに正しく記録されていない")

	// ★ ここで span を確実に終了させる
	span.End()

	// ★ flush（SimpleSpanProcessorでも基本即時だが、保険で）
	require.NoError(t, tp.ForceFlush(context.Background()))

	spans := memExp.GetSpans()

	// “test” span が存在すること
	var foundTest bool
	for i, s := range spans {
		t.Logf("=== span[%d] ===", i)
		// SpanContext
		sc := s.SpanContext
		t.Logf("name=%s", s.Name)
		t.Logf("trace_id=%s span_id=%s", sc.TraceID().String(), sc.SpanID().String())

		// 時刻
		t.Logf("start=%s end=%s", s.StartTime, s.EndTime)

		// ステータス
		t.Logf("status=%v", s.Status)

		if s.Name == "test" {
			foundTest = true
			break
		}
	}
	assert.True(t, foundTest, "test span が exporter に出てこない")
}
