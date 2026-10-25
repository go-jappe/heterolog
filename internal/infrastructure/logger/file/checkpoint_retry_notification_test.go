package file

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestInlineCheckpointRetryUsesRoleFactoryErrorHandler(t *testing.T) {
	for _, role := range []loggerType{DefaultLogger, ErrorLogger, DebugLogger} {
		t.Run(string(role), func(t *testing.T) {
			synctestWithHangWatchdogForTest(t, func(t *testing.T) {
				ctx := context.Background()
				key := checkpointstore.CheckpointKey{Generation: "20260927-1", Compressed: true}
				type notification struct {
					role         InternalHandlerRole
					err, loadErr error
					entry        *logging.LogEntry
				}
				notifications := make(chan notification, 4)
				var store checkpointstore.Store
				cfg := inlineCheckpointNotificationConfig(t)
				set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
				require.NoError(t, err)
				closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "set Close", func() closerForTest { return set }, false)
				require.NoError(t, set.ConfigureInternalHandlers(func(handlerRole InternalHandlerRole) InternalHandlers {
					return InternalHandlers{OnError: func(ctx context.Context, err error, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
						_, loadErr := store.Load(ctx, key)
						notifications <- notification{role: handlerRole, err: err, loadErr: loadErr, entry: entry}
					}}
				}))
				logger, err := set.fileLogger(ctx, role)
				require.NoError(t, err)
				store = logger.archiveUploadCheckpoints
				require.NotNil(t, store)
				source := checkpointstore.ArchiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, string(role), logger.Fsstore.FileName)
				restore := blockInlineCheckpointReplacement(t, filepath.Join(cfg.ArchiveCheckpointConfig.Directory, source+".log-file-uploads.json"))
				_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"archive"})
				var pending *checkpointstore.PendingSaveError
				require.ErrorAs(t, err, &pending)
				synctest.Wait()
				require.Empty(t, notifications, "initial save error is returned, not notified again")
				for range 2 {
					time.Sleep(checkpointstore.SaveRecoveryInterval)
					synctest.Wait()
					require.Len(t, notifications, 1)
					got := receiveForTest(t, notifications, "error notification")
					require.Equal(t, InternalHandlerRole(role), got.role)
					require.NoError(t, got.loadErr, "OnError must run outside the Store lock")
					requireFileLoggerInternalError(t, got.err, role, OperationArchiveUploadCheckpoint)
					// どの系列のcheckpointの保存が失敗したかは、型付きerrorのSourceIDで判別します。
					// 内部ログの本文に表示するdirectoryは入力値の表示にすぎないため、文言では確認しません。
					var retryErr *logfile.CheckpointSaveRetryError
					require.ErrorAs(t, got.err, &retryErr)
					require.Equal(t, source, retryErr.SourceID)
				}
				latest, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"backup"})
				require.ErrorAs(t, err, &pending)
				restore()
				time.Sleep(3 * checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				require.Empty(t, notifications)
				require.NoError(t, closeBoundedForTest(t, boundedwait.BubbleTimeout, "set Close", set))
				reopened, err := checkpointstore.NewFilesystemStore(cfg.ArchiveCheckpointConfig.Directory, source)
				require.NoError(t, err)
				closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "reopened Close", func() closerForTest { return reopened }, true)
				saved, err := reopened.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, latest, saved, "recovery must save the latest accepted state")
			})
		})
	}
}

func TestInlineCheckpointRetryErrorHandlerCanRequestCloseLoggerSet(t *testing.T) {
	synctestWithHangWatchdogForTest(t, func(t *testing.T) {
		cfg := inlineCheckpointNotificationConfig(t)
		set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "set Close", func() closerForTest { return set }, false)
		closed := make(chan error, 4)
		var closeStarted atomic.Bool
		require.NoError(t, set.ConfigureInternalHandlers(func(InternalHandlerRole) InternalHandlers {
			return InternalHandlers{OnError: func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
				if closeStarted.CompareAndSwap(false, true) {
					set.RequestClose()
					go func() { closed <- set.Close() }()
				}
			}}
		}))
		logger, err := set.NewFileLogger(context.Background())
		require.NoError(t, err)
		source := checkpointstore.ArchiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, string(DefaultLogger), logger.Fsstore.FileName)
		restore := blockInlineCheckpointReplacement(t, filepath.Join(cfg.ArchiveCheckpointConfig.Directory, source+".log-file-uploads.json"))
		_, err = logger.archiveUploadCheckpoints.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{
			Generation: "20260927-1", Compressed: true,
		}, []objectstorage.DestinationID{"archive"})
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		synctest.Wait()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.True(t, closeStarted.Load())
		require.Empty(t, closed, "OnError Close waits for accepted checkpoint persistence")
		restore()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Len(t, closed, 1, "OnError must not wait for itself once recovery succeeds")
		require.NoError(t, receiveForTest(t, closed, "Close result"))
		time.Sleep(2 * checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, closed, "Close stops further save retries")
	})
}

func inlineCheckpointNotificationConfig(t *testing.T) pkglogger.FileLoggerConfig {
	t.Helper()
	cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "retry-notification")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory: t.TempDir(), SourcePrefix: "inline", AutoRecoverMissingCheckpoint: true,
	}
	require.NoError(t, checkpointstore.PrepareDirectory(cfg.ArchiveCheckpointConfig.Directory))
	target := objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{Region: "ap-northeast-1", Endpoint: "https://s3.example.com", AccessKeyID: "test", SecretAccessKey: "test", Bucket: "archive"},
	}
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{Enabled: true, Default: target, Error: target, Debug: target}
	return cfg
}

func blockInlineCheckpointReplacement(t *testing.T, path string) func() {
	t.Helper()
	previous := path + ".before-failure"
	require.NoError(t, os.Rename(path, previous))
	require.NoError(t, os.Mkdir(path, 0o750))
	child := filepath.Join(path, "block-replacement")
	require.NoError(t, os.WriteFile(child, []byte("occupied"), 0o600))
	restore := sync.OnceFunc(func() {
		require.NoError(t, os.Remove(child))
		require.NoError(t, os.Remove(path))
		require.NoError(t, os.Rename(previous, path))
	})
	t.Cleanup(restore)
	return restore
}
