package logger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

// 通常出力先にfileを含めず、別loggerの内部handlerだけがfileを指定する構成では、
// top-level loggerの生成時にfileをopenせず、最初のinternal logで必要なroleだけをopenする。
// error分離が無効なため、このerror eventは専用error fileではなくdefault fileへ保存する。
// internal logはstdout backendの書込み失敗から発生させ、production構築が接続した
// OnErrorを経由してfileへ届くことを確認する。
func TestFileHandlerOnlyRuntimeLazilyUsesDefaultFileForUnsplitErrorEvent(t *testing.T) {
	appCfg, _ := loadConfigFixtureForLoggerTestWithOverrides(t, map[string]string{
		"HETEROLOG_ENABLE_LOGGING":                        "true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING":      "false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING":               "false",
		"HETEROLOG_LOG_TYPE":                              "stdout",
		"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER":           "file",
		"HETEROLOG_FILE_LOGGER_ERROR_HANDLER":             "stdout",
		"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS":   "false",
		"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS":   "false",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD": "false",
	})
	requireConfigFixtureDiskSpaceForLoggerTest(t, appCfg)
	cfg := appconfig.ToLoggerConfig(appCfg)
	require.NotNil(t, cfg.File)
	require.NotNil(t, cfg.Stdout)
	require.False(t, cfg.LogOutputDestinations.Has(pkglogger.LogOutputFile))

	root := t.TempDir()
	const target = "file-handler-only-runtime"
	cfg.File.FsstoreConfig.Layout.LogPath = root
	cfg.File.FsstoreConfig.Layout.Target = target
	cfg.File.FsstoreConfig.Layout.LogDir = "logs"
	cfg.File.FsstoreConfig.Layout.ErrorLogDir = "logs_error"
	cfg.File.FsstoreConfig.Lifecycle.EnableRotate = false
	cfg.File.FsstoreConfig.Lifecycle.EnableFileCompress = false
	cfg.File.SamplingConfig.IsSamplingEnabled = false
	// stdout backendへ必ず書き込ませ、その失敗をinternal logにします。
	cfg.Stdout.RuntimeConfig.LogLevel = pkglogger.LogLevelDebug
	cfg.Stdout.SamplingConfig.IsSamplingEnabled = false
	cfg.Stdout.UseOpenTelemetryFormat = false

	ctx := context.Background()
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	cloudWatchRouting := appCfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity
	loggerInstance, err := NewLogger(ctx, cfg, cloudWatchRouting, nil, nil, tracer, meter)
	require.NoError(t, err)

	owned, ok := loggerInstance.(*topLevelLoggerRuntime)
	require.True(t, ok, "handler-only file config must be owned by the top-level logger")
	require.Same(
		t,
		owned.fileLoggerSet,
		owned.internalLogDestinations.parentOwnedFileLoggerSet,
		"handler-only File LoggerSet must remain owned by the top-level logger",
	)
	require.Nil(t, owned.internalLogDestinations.runtimeOwnedFileLoggerSet)
	require.Empty(t, logFilesUnder(t, root), "constructor must not activate normal file output")

	restoreStdout := replaceStdoutWithClosedFileForLoggerTest(t)
	err = loggerInstance.Info(ctx, "handler-only stdout write")
	restoreStdout()
	require.ErrorIs(t, err, os.ErrClosed, "the stdout backend write must fail")

	requireStdoutWriteFailureInDefaultFileOnly(t, root, target)

	defaultLogger, err := owned.fileLoggerSet.NewFileLogger(ctx)
	require.NoError(t, err)
	require.NotNil(t, defaultLogger.Fsstore.File)
	require.NotNil(t, defaultLogger.OnError, "lazy fallback logger must retain its own internal handlers")

	closeable, ok := loggerInstance.(applogging.CloseableLogger)
	require.True(t, ok)
	require.NoError(t, closeable.Close())
	require.Nil(t, defaultLogger.Fsstore.File, "top-level Close must close a lazy fallback-only logger")
}

// asyncの通常出力先にfileを含めず、async backendの内部handlerだけがfileを指定する
// 構成でも、最初のinternal logまでfileをopenせず、top-level AsyncLoggerがstreamを所有する。
// error分離が無効なため、このerror eventはdefault fileへ保存され、Closeで回収される。
// internal logはasync stdout backendの書込み失敗から発生させる。非同期処理の完了は
// synctest.Waitで待ち、固定時間のsleepやpollingに依存しない。
func TestAsyncFileHandlerOnlyRuntimeLazilyUsesDefaultFileAndClosesStream(t *testing.T) {
	appCfg, _ := loadConfigFixtureForLoggerTestWithOverrides(t, map[string]string{
		"HETEROLOG_ENABLE_LOGGING":                              "false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING":            "true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING":                     "false",
		"HETEROLOG_ASYNC_LOG_TYPE":                              "stdout",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER":           "file",
		"HETEROLOG_ASYNC_FILE_LOGGER_ERROR_HANDLER":             "stdout",
		"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_ERROR_LOGS":   "false",
		"HETEROLOG_ENABLE_ASYNC_LOG_FILE_SEPARATE_DEBUG_LOGS":   "false",
		"HETEROLOG_ENABLE_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD": "false",
	})
	requireConfigFixtureDiskSpaceForLoggerTest(t, appCfg)
	cfg := appconfig.ToAsyncBestEffortMultiLoggerConfig(appCfg)
	require.NotNil(t, cfg.File)
	require.NotNil(t, cfg.Stdout)
	require.False(t, cfg.LogOutputDestinations.Has(pkglogger.LogOutputFile))

	root := t.TempDir()
	const target = "async-file-handler-only-runtime"
	cfg.File.FsstoreConfig.Layout.LogPath = root
	cfg.File.FsstoreConfig.Layout.Target = target
	cfg.File.FsstoreConfig.Layout.LogDir = "logs"
	cfg.File.FsstoreConfig.Layout.ErrorLogDir = "logs_error"
	cfg.File.FsstoreConfig.Lifecycle.EnableRotate = false
	cfg.File.FsstoreConfig.Lifecycle.EnableFileCompress = false
	cfg.File.SamplingConfig.IsSamplingEnabled = false
	cfg.Stdout.RuntimeConfig.LogLevel = pkglogger.LogLevelDebug
	cfg.Stdout.SamplingConfig.IsSamplingEnabled = false
	cfg.Stdout.UseOpenTelemetryFormat = false
	cloudWatchRouting := appCfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamRoutingGranularity

	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		tracer := observer.NopTracer{}
		meter := observer.NopMeter{}
		loggerInstance, err := NewAsyncLogger(ctx, cfg, cloudWatchRouting, nil, nil, tracer, meter)
		require.NoError(t, err)
		closeable, ok := loggerInstance.(applogging.AsyncCloseableLogger)
		require.True(t, ok)
		closed := false
		defer func() {
			if !closed {
				_ = closeable.Close()
			}
		}()

		owned, ok := loggerInstance.(*topLevelAsyncLoggerRuntime)
		require.True(t, ok, "async handler-only file config must be owned by the top-level logger")
		require.Same(
			t,
			owned.fileLoggerSet,
			owned.internalLogDestinations.parentOwnedFileLoggerSet,
			"async handler-only File LoggerSet must remain owned by the top-level logger",
		)
		require.Nil(t, owned.internalLogDestinations.runtimeOwnedFileLoggerSet)
		require.Empty(t, logFilesUnder(t, root), "constructor must not activate normal file output")

		restoreStdout := replaceStdoutWithClosedFileForLoggerTest(t)
		loggerInstance.Info(ctx, "async handler-only stdout write")
		// 受理済みのasync処理がstdoutの失敗をfileへ記録し終えるまで待ちます。
		synctest.Wait()
		restoreStdout()

		requireStdoutWriteFailureInDefaultFileOnly(t, root, target)

		defaultLogger, err := owned.fileLoggerSet.NewFileLogger(ctx)
		require.NoError(t, err)
		require.NotNil(t, defaultLogger.Fsstore.File)

		closed = true
		require.NoError(t, closeable.Close())
		require.Nil(t, defaultLogger.Fsstore.File, "top-level async Close must close a lazy fallback-only logger")
	})
}

// replaceStdoutWithClosedFileForLoggerTestは標準出力を閉じたfileへ置き換え、stdout
// backendの書込みを失敗させます。戻り値の関数とtest終了時の後片付けで元に戻します。
// 標準出力を置き換えるため、呼び出すtestは並列実行しないでください。
func replaceStdoutWithClosedFileForLoggerTest(t *testing.T) func() {
	t.Helper()
	readFile, writeFile, err := os.Pipe()
	require.NoError(t, err)
	require.NoError(t, readFile.Close())
	require.NoError(t, writeFile.Close())
	original := os.Stdout
	os.Stdout = writeFile
	restore := sync.OnceFunc(func() { os.Stdout = original })
	t.Cleanup(restore)
	return restore
}

// requireStdoutWriteFailureInDefaultFileOnlyは、stdout backendの書込み失敗がinternal
// error logとしてdefault fileへ保存され、error分離用のfileを作っていないことを確認します。
func requireStdoutWriteFailureInDefaultFileOnly(t *testing.T, root, target string) {
	t.Helper()
	defaultPattern := filepath.Join(root, target, "logs", target+"_*.log")
	defaultFiles, err := filepath.Glob(defaultPattern)
	require.NoError(t, err)
	require.Len(t, defaultFiles, 1)
	content, err := os.ReadFile(defaultFiles[0])
	require.NoError(t, err)
	require.Contains(t, string(content), "[INTERNAL][ERROR]")
	require.Contains(t, string(content), "failed to write to stdout")
	// file出力は利用者が読むinternal logなので、原因のerror文言が記録されることを確認します。
	require.Contains(t, string(content), os.ErrClosed.Error())

	errorPattern := filepath.Join(root, target, "logs_error", "*"+target+"_*.log")
	errorFiles, err := filepath.Glob(errorPattern)
	require.NoError(t, err)
	require.Empty(t, errorFiles, "unsplit error event must not create an error stream")
}

// backendのClose中にもinternal logは発生し得るため、共有FileLoggerは子loggerより後に閉じる。
// top-level Closeを再度呼んでも、backendとunderlying file resourceのどちらも二重closeしない。
func TestLoggerWithFileLoggerSetClosesBackendBeforeSharedFileLoggers(t *testing.T) {
	appCfg, _ := loadConfigFixtureForLoggerTestWithOverrides(t, map[string]string{
		"HETEROLOG_ENABLE_LOGGING":                        "true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING":      "false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING":               "false",
		"HETEROLOG_LOG_TYPE":                              "stdout",
		"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER":           "file",
		"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS":   "false",
		"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS":   "false",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD": "false",
	})
	requireConfigFixtureDiskSpaceForLoggerTest(t, appCfg)
	cfg := appconfig.ToLoggerConfig(appCfg)
	require.NotNil(t, cfg.File)

	root := t.TempDir()
	const target = "file-logger-close-order"
	cfg.File.FsstoreConfig.Layout.LogPath = root
	cfg.File.FsstoreConfig.Layout.Target = target
	cfg.File.FsstoreConfig.Layout.LogDir = "logs"
	cfg.File.FsstoreConfig.Lifecycle.EnableRotate = false
	cfg.File.SamplingConfig.IsSamplingEnabled = false

	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	fileLoggerSet, err := newFileLoggerSet(cfg.File, tracer, meter)
	require.NoError(t, err)

	ctx := context.Background()
	const message = "backend close internal log"
	var (
		closeCalls    atomic.Int32
		defaultLogger *file.FileLogger
	)
	backend := &closeEventTestLogger{
		close: func() error {
			closeCalls.Add(1)
			fileLoggerSet.NewNamedErrorFallbackHandler().Handler(
				ctx,
				errors.New("backend close failure"),
				&logging.LogEntry{
					Level:   pkglogger.LogLevelError,
					Message: message,
					Source:  "backend",
				},
				tracer,
				meter,
			)
			var loggerErr error
			defaultLogger, loggerErr = fileLoggerSet.NewFileLogger(ctx)
			return loggerErr
		},
	}
	owned := &topLevelLoggerRuntime{
		topLevelLoggerDelegate: backend,
		fileLoggerSet:          fileLoggerSet,
	}

	require.NoError(t, owned.Close())
	require.NoError(t, owned.Close())
	require.Equal(t, int32(1), closeCalls.Load())
	require.NotNil(t, defaultLogger)
	require.Nil(t, defaultLogger.Fsstore.File)

	files := logFilesUnder(t, root)
	require.Len(t, files, 1)
	content, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(content), message)
}

type closeEventTestLogger struct {
	close func() error
}

func (*closeEventTestLogger) StopAccepting() {}

func (*closeEventTestLogger) Debug(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*closeEventTestLogger) Info(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*closeEventTestLogger) Warn(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*closeEventTestLogger) Error(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*closeEventTestLogger) ErrorWithStack(
	context.Context,
	string,
	error,
	...pkglogger.Field,
) error {
	return nil
}

func (l *closeEventTestLogger) Close() error {
	if l.close == nil {
		return nil
	}
	return l.close()
}

func logFilesUnder(t *testing.T, root string) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	return files
}
