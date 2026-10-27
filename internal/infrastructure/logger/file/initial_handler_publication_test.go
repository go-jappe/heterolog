package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestLoggerSetInitialHandlersPrecedeManagedMaintenanceAndCanRequestCloseOwner(t *testing.T) {
	ctx := context.Background()
	spoolBase := t.TempDir()
	durable := archiveUploaderCompressedLogFileUploadFailureStoreConfig(
		spoolBase, pkglogger.LoggerContractSync, time.Millisecond,
	)
	root, err := objectstoragespool.NewRoot(
		spoolBase, "heterolog", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync,
	)
	require.NoError(t, err)
	root, err = root.ForDestination("unregistered-destination")
	require.NoError(t, err)
	store, err := objectstoragespool.NewStore(root)
	require.NoError(t, err)
	// 未登録の系列を実spoolへ残し、起動後のroute照合でmaintenance errorを発生させます。
	// 宛先を選べないためremote uploadへは進まず、実ネットワークは使いません。
	sourcePath := filepath.Join(t.TempDir(), "unregistered_20260908_1.log.gz")
	require.NoError(t, os.WriteFile(sourcePath, []byte("unregistered archive payload"), 0o600))
	_, err = store.SaveRetryableFailure(ctx, sourcePath)
	require.NoError(t, err)

	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "initial-handler")
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled:                      true,
		CompressedLogFileUploadSpool: durable.CompressedLogFileUploadSpool,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
			S3: objectstorage.S3Config{
				Region: "ap-northeast-1", Endpoint: "http://127.0.0.1:1", ForcePathStyle: true,
				Bucket: "logs", AccessKeyID: "test", SecretAccessKey: "test",
			},
			CompressedLogFileUploadFailureStore: durable.Default.CompressedLogFileUploadFailureStore,
		},
	}
	gate := &initialHandlerPublicationTracer{
		entered: make(chan bool, 1),
		release: make(chan struct{}),
	}
	set, err := NewLoggerSet(NewConfigAdapter(cfg), gate, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	var releaseOnce sync.Once
	releaseConstructor := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(releaseConstructor)

	notified := make(chan error, 1)
	closed := make(chan error, 1)
	var notifyOnce sync.Once
	require.NoError(t, set.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
		gate.handlersReady.Store(true)
		return InternalHandlers{OnError: func(
			_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter,
		) {
			if !errors.Is(err, compressedlogworker.ErrNoMatchingRoute) {
				return
			}
			notifyOnce.Do(func() {
				notified <- err
				// 通知内では終了を要求するだけにし、通知自身の完了を待ちません。
				set.RequestClose()
				go func() { closed <- set.Close() }()
			})
		}}
	}))
	created := make(chan *FileLogger, 1)
	creationErrors := make(chan error, 1)
	go func() {
		logger, err := set.NewFileLogger(ctx)
		created <- logger
		creationErrors <- err
	}()

	handlersReady := receiveForTest(t, gate.entered, "file logger constructor did not reach its publication gate")
	// Tracerは既存のconstructor入口です。worker起動を含むconstructorへ進む前に
	// handler factoryが完了していることを、sleepやschedulerの偶然に頼らず検証します。
	require.True(t, handlersReady, "constructor started before initial handlers were prepared")
	active, err := store.ListFiles(ctx, pkgspool.StateActive)
	require.NoError(t, err)
	require.Len(t, active, 1)
	releaseConstructor()

	var logger *FileLogger
	logger = receiveForTest(t, created, "file logger constructor did not return")
	require.NoError(t, receiveForTest(t, creationErrors, "file logger constructor did not return its error"))
	notifiedErr := receiveForTest(t, notified, "initial OnError did not receive the local maintenance failure")
	require.ErrorIs(t, notifiedErr, compressedlogworker.ErrNoMatchingRoute)
	closeErr := receiveForTest(t, closed, "initial OnError could not close its LoggerSet")
	require.NoError(t, closeErr)
	require.True(t, logger.managedByLoggerSet)
	require.Nil(t, logger.ArchiveUploader)
	require.Nil(t, logger.Fsstore.File)
	active, err = store.ListFiles(ctx, pkgspool.StateActive)
	require.NoError(t, err)
	require.Len(t, active, 1)
	_, err = set.NewFileLogger(ctx)
	require.ErrorIs(t, err, ErrLoggerSetClosed)
}

type initialHandlerPublicationTracer struct {
	handlersReady atomic.Bool
	entered       chan bool
	release       chan struct{}
}

func (t *initialHandlerPublicationTracer) StartSpan(
	ctx context.Context, name string, attrs ...pkgobserver.Attr,
) (context.Context, pkgobserver.Span) {
	if name == SpanFileLoggerCreate {
		t.entered <- t.handlersReady.Load()
		<-t.release
	}
	return observer.NopTracer{}.StartSpan(ctx, name, attrs...)
}
