package logger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	filelogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/require"
)

func TestContextCloseableLoggerLetsSyncAndAsyncMultiCloseEveryBackend(t *testing.T) {
	t.Run("sync", func(t *testing.T) {
		recorder := &closeOrderRecorder{}
		loggers := make([]infralogging.BackendLogEntryHandler, 0, 4)
		for i := 0; i < 4; i++ {
			child := &closeOwnershipLogger{id: i, recorder: recorder}
			loggers = append(loggers, child)
		}

		logger, err := multi.NewMultiLogger(rootMultiLoggerTestConfig{}, loggers...)
		require.NoError(t, err)
		require.NoError(t, logger.Close())
		require.Equal(t, []int{0, 1, 2, 3}, recorder.snapshot())
	})

	t.Run("async", func(t *testing.T) {
		recorder := &closeOrderRecorder{}
		loggers := make([]infralogging.BackendLogEntryHandler, 0, 4)
		for i := 0; i < 4; i++ {
			child := &closeOwnershipLogger{id: i, recorder: recorder}
			loggers = append(loggers, child)
		}

		logger, err := multi.NewAsyncBestEffortMultiLogger(
			closeOwnershipAsyncConfig{},
			loggers...,
		)
		require.NoError(t, err)
		require.NoError(t, logger.Close())
		require.Equal(t, []int{0, 1, 2, 3}, recorder.snapshot())
	})
}

func TestMultiLoggerPreservesConcreteBackendTypeInFailure(t *testing.T) {
	logErr := errors.New("backend failed")
	child := &closeOwnershipLogger{logErr: logErr}

	logger, err := multi.NewMultiLogger(rootMultiLoggerTestConfig{}, child)
	require.NoError(t, err)
	err = logger.Info(context.Background(), "test")

	var multiErr *pkglogger.MultiLoggerError
	require.ErrorAs(t, err, &multiErr)
	require.Len(t, multiErr.Failures, 1)
	require.Equal(t, fmt.Sprintf("%T", child), multiErr.Failures[0].HandlerType)
	require.ErrorIs(t, multiErr.Failures[0].Err, logErr)
}

func TestRollbackContextCloserUsesBackgroundContextAndReturnsCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	var receivedCtx context.Context
	var closer io.Closer = rollbackContextCloser{
		close: func(ctx context.Context) error {
			receivedCtx = ctx
			return closeErr
		},
	}

	err := closer.Close()

	require.ErrorIs(t, err, closeErr)
	require.NotNil(t, receivedCtx)
	require.NoError(t, receivedCtx.Err())
	_, hasDeadline := receivedCtx.Deadline()
	require.False(t, hasDeadline)
}

func TestRollbackLoggerConstructionClosesChildrenInReverseOrder(t *testing.T) {
	recorder := &closeOrderRecorder{}
	initErr := errors.New("construct failed")
	closeErr := errors.New("close failed")
	closers := []io.Closer{
		&closeOwnershipCloser{id: 0, recorder: recorder},
		&closeOwnershipCloser{id: 1, recorder: recorder, err: closeErr},
		&closeOwnershipCloser{id: 2, recorder: recorder},
	}

	err := rollbackLoggerConstruction(initErr, closers)

	require.ErrorIs(t, err, initErr)
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, []int{2, 1, 0}, recorder.snapshot())
}

func TestRollbackFailureHandlerConstructionDrainsPendingOwner(t *testing.T) {
	initErr := errors.New("construct failed")
	drainErr := errors.New("drain failed")
	handler := &closeOwnershipFailureHandlerDrainer{err: drainErr}

	err := rollbackFailureHandlerConstruction(initErr, handler)

	require.ErrorIs(t, err, initErr)
	require.ErrorIs(t, err, drainErr)
	require.Equal(t, 1, handler.calls)
	require.NotNil(t, handler.ctx)
	require.NoError(t, handler.ctx.Err())
}

func TestRollbackFailureHandlerConstructionAcceptsNoPendingOwner(t *testing.T) {
	initErr := errors.New("construct failed")

	require.Same(t, initErr, rollbackFailureHandlerConstruction(initErr, nil))
}

func TestNewDeliveryLoggerRollsBackOwnedFileLoggerOnLaterFailure(t *testing.T) {
	root := t.TempDir()
	const target = "delivery-construction-rollback"
	cfg := pkglogger.DeliveryLoggerConfig{
		Timeout:          time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		File:             newCloseOwnershipDeliveryFileConfig(root, target),
		OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchLoggerBaseConfig: pkglogger.DeliveryOpenSearchLoggerBaseConfig{},
			DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
				Index: "rollback-test",
				FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
					DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
				},
			},
		},
		// pkgの値検証を通過した後、実clientが証明書を読み込む段階で失敗させることで、
		// file logger構築後のrollback経路を検証します。
		OpenSearchClient: &pkgopensearch.Config{
			Endpoint: "https://localhost:9200",
			AuthConfig: pkgopensearch.AuthConfig{
				Type: pkgopensearch.AuthTypeCertificate,
				Config: pkgopensearch.CertificateAuthConfig{
					CertFile: filepath.Join(root, "missing-client.crt"),
					KeyFile:  filepath.Join(root, "missing-client.key"),
				},
			},
		},
	}
	loggerInstance, err := NewDeliveryLogger(
		context.Background(),
		cfg,
		pkgcloudwatchlogs.StreamRoutingNone,
		observer.NopTracer{},
		observer.NopMeter{},
	)

	// pkg/opensearchの証明書読込errorには種類を表すsentinelがないため、読み込めなかった
	// client証明書のpathとstdlibのsentinelで原因を確認します。
	require.ErrorIs(t, err, fs.ErrNotExist)
	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	require.Equal(t, filepath.Join(root, "missing-client.crt"), pathErr.Path)
	require.Nil(t, loggerInstance)

	guard := appendlog.DirectoryGuardForPath(filepath.Join(root, target, "logs"))
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Empty(t, protected, "rollback must remove the delivery file logger token")
	})
}

func TestTopLevelFileLoggerSetRetriesBackendCloseBeforeClosingFileLoggers(t *testing.T) {
	t.Run("sync", func(t *testing.T) {
		fileLoggerSet := newCloseOwnershipLoggerSet(t, "sync-close-retry")
		closeErr := errors.New("sync backend final flush failed")
		child := &retryingContextCloseLogger{firstCloseErr: closeErr}
		delegate, err := multi.NewMultiLogger(rootMultiLoggerTestConfig{}, child)
		require.NoError(t, err)
		owned := &topLevelLoggerRuntime{
			topLevelLoggerDelegate: delegate,
			fileLoggerSet:          fileLoggerSet,
		}

		require.ErrorIs(t, owned.closeAttempt(), closeErr)
		require.Equal(t, int32(1), child.closeCalls.Load())

		// backendのCloseが失敗した間は、internal log用FileLoggerを利用可能なまま維持します。
		fileLogger, err := fileLoggerSet.NewFileLogger(context.Background())
		require.NoError(t, err)
		require.NotNil(t, fileLogger.Fsstore.File)

		require.NoError(t, owned.closeAttempt())
		require.Equal(t, int32(2), child.closeCalls.Load())
		require.Nil(t, fileLogger.Fsstore.File)
		_, err = fileLoggerSet.NewFileLogger(context.Background())
		require.ErrorIs(t, err, filelogger.ErrLoggerSetClosed)

		require.NoError(t, owned.closeAttempt())
		require.Equal(t, int32(2), child.closeCalls.Load())
	})

	t.Run("async", func(t *testing.T) {
		fileLoggerSet := newCloseOwnershipLoggerSet(t, "async-close-retry")
		closeErr := errors.New("async backend final flush failed")
		child := &retryingContextCloseLogger{firstCloseErr: closeErr}
		async, err := multi.NewAsyncBestEffortMultiLogger(closeOwnershipAsyncConfig{}, child)
		require.NoError(t, err)
		owned := &topLevelAsyncLoggerRuntime{
			topLevelAsyncLoggerDelegate: async,
			fileLoggerSet:               fileLoggerSet,
		}

		require.ErrorIs(t, owned.closeAttempt(), closeErr)
		require.Equal(t, int32(1), child.closeCalls.Load())
		fileLogger, err := fileLoggerSet.NewFileLogger(context.Background())
		require.NoError(t, err)
		require.NotNil(t, fileLogger.Fsstore.File)

		require.NoError(t, owned.closeAttempt())
		require.Equal(t, int32(2), child.closeCalls.Load())
		require.Nil(t, fileLogger.Fsstore.File)
	})

	t.Run("completed file close error does not reclose backend", func(t *testing.T) {
		fileLoggerSet := newCloseOwnershipLoggerSet(t, "file-logger-close-retry")
		fileLogger, err := fileLoggerSet.NewFileLogger(context.Background())
		require.NoError(t, err)
		require.NoError(t, fileLogger.Fsstore.File.Close())

		child := &retryingContextCloseLogger{}
		delegate, err := multi.NewMultiLogger(rootMultiLoggerTestConfig{}, child)
		require.NoError(t, err)
		owned := &topLevelLoggerRuntime{
			topLevelLoggerDelegate: delegate,
			fileLoggerSet:          fileLoggerSet,
		}
		require.Error(t, owned.closeAttempt())
		require.Equal(t, int32(1), child.closeCalls.Load())

		fileLogger.Fsstore.Mu.Lock()
		fileLogger.Fsstore.File = nil
		fileLogger.Fsstore.Mu.Unlock()
		require.Error(t, owned.closeAttempt(), "completed shutdown retains its terminal file close error")
		require.Equal(t, int32(1), child.closeCalls.Load())
	})
}

type closeOwnershipAsyncConfig struct{}

type rootMultiLoggerTestConfig struct{}

func (rootMultiLoggerTestConfig) Timeout() time.Duration {
	return time.Second
}

func (rootMultiLoggerTestConfig) MaxLogEntryBytes() int {
	return pkglogger.DefaultMaxLogEntryBytes
}

func (closeOwnershipAsyncConfig) Timeout() time.Duration {
	return time.Second
}

func (closeOwnershipAsyncConfig) MaxParallel() int {
	return 1
}

func (closeOwnershipAsyncConfig) MaxLogEntryBytes() int {
	return pkglogger.DefaultMaxLogEntryBytes
}

type closeOrderRecorder struct {
	mu    sync.Mutex
	order []int
}

func (r *closeOrderRecorder) append(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, id)
}

func (r *closeOrderRecorder) snapshot() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.order...)
}

type closeOwnershipCloser struct {
	id       int
	recorder *closeOrderRecorder
	err      error
}

func (c *closeOwnershipCloser) Close() error {
	c.recorder.append(c.id)
	return c.err
}

type closeOwnershipFailureHandlerDrainer struct {
	calls int
	err   error
	ctx   context.Context
}

func (d *closeOwnershipFailureHandlerDrainer) Drain(ctx context.Context) error {
	d.calls++
	d.ctx = ctx
	return d.err
}

type closeOwnershipLogger struct {
	id       int
	recorder *closeOrderRecorder
	logErr   error
}

func (l *closeOwnershipLogger) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("close context is nil")
	}
	if l.recorder != nil {
		l.recorder.append(l.id)
	}
	return nil
}

func (l *closeOwnershipLogger) HandleLogEntry(
	context.Context,
	*infralogging.LogEntry,
) error {
	return l.logErr
}

func (*closeOwnershipLogger) Debug(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (l *closeOwnershipLogger) Info(context.Context, string, ...pkglogger.Field) error {
	return l.logErr
}

func (*closeOwnershipLogger) Warn(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*closeOwnershipLogger) Error(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*closeOwnershipLogger) ErrorWithStack(
	context.Context,
	string,
	error,
	...pkglogger.Field,
) error {
	return nil
}

type retryingContextCloseLogger struct {
	firstCloseErr error
	closeCalls    atomic.Int32
}

func (l *retryingContextCloseLogger) Close(context.Context) error {
	if l.closeCalls.Add(1) == 1 {
		return l.firstCloseErr
	}
	return nil
}

func (*retryingContextCloseLogger) HandleLogEntry(
	context.Context,
	*infralogging.LogEntry,
) error {
	return nil
}

func (*retryingContextCloseLogger) Debug(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*retryingContextCloseLogger) Info(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*retryingContextCloseLogger) Warn(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*retryingContextCloseLogger) Error(context.Context, string, ...pkglogger.Field) error {
	return nil
}

func (*retryingContextCloseLogger) ErrorWithStack(
	context.Context,
	string,
	error,
	...pkglogger.Field,
) error {
	return nil
}

func newCloseOwnershipLoggerSet(t *testing.T, target string) *filelogger.LoggerSet {
	t.Helper()
	cfg := pkglogger.FileLoggerConfig{
		FsstoreConfig: pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target:         target,
				LogPath:        t.TempDir(),
				LogDir:         "logs",
				ErrorLogDir:    "logs_error",
				FileNameFormat: pkgfsstore.FileNameFormat,
				FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{
				EnableRotate:   false,
				RotationType:   pkgfsstore.FileRotationDaily,
				MaxFileBackups: infrafsstore.MinAllowedMaxFileBackups,
				MaxFileAge:     infrafsstore.MinAllowedMaxFileAge,
				MaxFileSize:    infrafsstore.MinAllowedFileSizeBytes,
				MaxDirSize:     infrafsstore.MinAllowedMaxDirSize,
				UsageCacheTTL:  time.Hour,
				CompressFormat: pkgfsstore.CompressGzip,
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
		RuntimeConfig: pkglogger.RuntimeConfig{
			AppName:  "close-ownership-test",
			Target:   target,
			LogLevel: pkglogger.LogLevelDebug,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
		SamplingConfig: pkglogger.SamplingConfig{
			LoggingByRequestIDRate: 1,
			SamplingDebugRate:      1,
			SamplingInfoRate:       1,
			SamplingWarnRate:       1,
			SamplingErrorRate:      1,
		},
	}
	fileLoggerSet, err := filelogger.NewLoggerSet(
		filelogger.NewConfigAdapter(cfg),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	return fileLoggerSet
}

func newCloseOwnershipDeliveryFileConfig(
	root string,
	target string,
) *pkglogger.DeliveryFileLoggerConfig {
	return &pkglogger.DeliveryFileLoggerConfig{
		FsstoreConfig: pkgfsstore.DeliveryFileWriteConfig{
			Layout: pkgfsstore.FsstoreConfig{
				Target:                  target,
				LogPath:                 root,
				LogDir:                  "logs",
				ErrorLogDir:             "logs_error",
				SeparateErrorLogsPrefix: "error_",
				SeparateDebugLogsPrefix: "debug_",
				FileNameFormat:          pkgfsstore.FileNameFormat,
				FileDateFormat:          pkgfsstore.FileDateFormat,
			},
			Rotation: pkgfsstore.FileRotationConfig{
				EnableRotate: false,
				RotationType: pkgfsstore.FileRotationDaily,
				MaxFileSize:  infrafsstore.MinAllowedFileSizeBytes,
			},
			Duration: pkgfsstore.DurationConfig{
				WriteFileWarnThresholdDuration:  time.Millisecond,
				WriteFileErrorThresholdDuration: time.Millisecond,
			},
			Retry: pkgfsstore.RetryConfig{
				MaxWriteRetries:            0,
				FileOperationRetryWaitTime: time.Millisecond,
			},
		},
		TimeConfig: pkglogger.TimeConfig{},
		RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
			AppName: "close-ownership-test",
			Target:  target,
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          1,
			ErrorTraceDepthDebugMode: 1,
		},
	}
}
