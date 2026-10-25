package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFailureArchiveInlineCheckpointRetryUsesFactoryErrorHandler(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		t.Run(string(category), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				oldKey := checkpointstore.CheckpointKey{Generation: "20260920-1", Compressed: true}
				latestKey := checkpointstore.CheckpointKey{Generation: "20260921-1", Compressed: true}
				type notification struct {
					err     error
					entry   *logging.LogEntry
					loadErr error
				}
				notifications := make(chan notification, 4)
				var store checkpointstore.Store
				cfg := newInlineCheckpointNotificationConfig(t)
				pipeline, err := newInlineCheckpointNotificationPipeline(cfg, func(
					callbackCtx context.Context, err error, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter,
				) {
					_, loadErr := store.Load(callbackCtx, latestKey)
					notifications <- notification{err: err, entry: entry, loadErr: loadErr}
				})
				require.NoError(t, err)
				t.Cleanup(func() { _ = pipeline.Shutdown(ctx) })
				store = pipeline.handler.directoryForFailureCategory(category).uploadCheckpoints
				require.NotNil(t, store)
				pattern, err := pipeline.handler.archiveFileNamePattern(category)
				require.NoError(t, err)
				source := checkpointstore.ArchiveCheckpointSourceID(cfg.archiveCheckpoint.SourcePrefix, string(category), pattern)
				path := filepath.Join(cfg.archiveCheckpoint.Directory, source+".log-file-uploads.json")
				restore := blockFailureArchiveCheckpointReplacement(t, path)
				_, err = store.AdvanceCompletionCursor(ctx, oldKey, []objectstorage.DestinationID{"archive"})
				var pending *checkpointstore.PendingSaveError
				require.ErrorAs(t, err, &pending)
				synctest.Wait()
				require.Empty(t, notifications, "initial save failure is returned without a duplicate retry notification")

				for range 2 {
					time.Sleep(checkpointstore.SaveRecoveryInterval)
					synctest.Wait()
					require.Len(t, notifications, 1, "each failed background save reaches the registered factory handler once")
					notification := <-notifications
					require.NoError(t, notification.loadErr, "OnError can reenter the checkpoint store")
					var internalErr *FailureArchiveInternalError
					require.ErrorAs(t, notification.err, &internalErr)
					require.Equal(t, OperationArchiveUpload, internalErr.Op)
					var retryErr *logfile.CheckpointSaveRetryError
					require.ErrorAs(t, notification.err, &retryErr)
					require.Equal(t, source, retryErr.SourceID)
					require.Contains(t, notification.entry.Message, pattern.Dir)
				}

				latest, err := store.AdvanceCompletionCursor(ctx, latestKey, []objectstorage.DestinationID{"archive", "backup"})
				require.ErrorAs(t, err, &pending)
				synctest.Wait()
				require.Empty(t, notifications, "additional accepted updates retain their existing return-value error path")
				restore()
				time.Sleep(3 * checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				require.Empty(t, notifications, "successful recovery stops retry error notifications")
				require.NoError(t, pipeline.Shutdown(ctx))

				reopened, err := checkpointstore.NewFilesystemStore(cfg.archiveCheckpoint.Directory, source)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, reopened.Close()) })
				persisted, err := reopened.Load(ctx, latestKey)
				require.NoError(t, err)
				require.Equal(t, latest, persisted, "recovery saves the latest accepted destination set")
				removed, err := reopened.Load(ctx, oldKey)
				require.NoError(t, err)
				require.Equal(t, latest.CompletedDestinationIDs, removed.CompletedDestinationIDs, "persisted cursors keep earlier generations complete")
			})
		})
	}
}

func TestFailureArchiveInlineCheckpointRetryHandlerCanRequestShutdown(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		t.Run(string(category), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				closed := make(chan error, 4)
				var pipeline *FailureArchivePipeline
				var err error
				cfg := newInlineCheckpointNotificationConfig(t)
				pipeline, err = newInlineCheckpointNotificationPipeline(cfg, func(
					_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter,
				) {
					pipeline.RequestShutdown()
					closed <- err
				})
				require.NoError(t, err)
				t.Cleanup(func() { _ = pipeline.Shutdown(ctx) })
				store := pipeline.handler.directoryForFailureCategory(category).uploadCheckpoints
				pattern, err := pipeline.handler.archiveFileNamePattern(category)
				require.NoError(t, err)
				source := checkpointstore.ArchiveCheckpointSourceID(cfg.archiveCheckpoint.SourcePrefix, string(category), pattern)
				path := filepath.Join(cfg.archiveCheckpoint.Directory, source+".log-file-uploads.json")
				restore := blockFailureArchiveCheckpointReplacement(t, path)
				key := checkpointstore.CheckpointKey{Generation: "20260920-1", Compressed: true}
				_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"archive"})
				var pending *checkpointstore.PendingSaveError
				require.ErrorAs(t, err, &pending)
				synctest.Wait()
				require.Empty(t, closed)
				time.Sleep(checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				require.Len(t, closed, 1, "OnError requests shutdown without waiting for its own completion")
				var saveErr *os.LinkError
				require.ErrorAs(t, <-closed, &saveErr)
				shutdownDone := pipeline.ShutdownDone()
				select {
				case <-shutdownDone:
					t.Fatal("shutdown completed before the accepted checkpoint save")
				default:
				}
				restore()
				time.Sleep(checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				require.NoError(t, pipeline.Shutdown(ctx))
				time.Sleep(3 * checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				require.Empty(t, closed, "Shutdown stops further checkpoint save notifications")
				require.NoError(t, pipeline.Shutdown(ctx))
				reopened, err := checkpointstore.NewFilesystemStore(cfg.archiveCheckpoint.Directory, source)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, reopened.Close()) })
				persisted, err := reopened.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"archive"}, persisted.CompletedDestinationIDs)
			})
		})
	}
}

func TestFailureArchiveInlineCheckpointConstructorFailureClosesNotificationWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		cfg := newInlineCheckpointNotificationConfig(t)
		notifications := make(chan error, 4)
		onError := func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			notifications <- err
		}
		initial, err := newInlineCheckpointNotificationPipeline(cfg, onError)
		require.NoError(t, err)
		pattern := initial.handler.Fsstore.FileNamePermanent
		source := checkpointstore.ArchiveCheckpointSourceID(cfg.archiveCheckpoint.SourcePrefix, string(failurearchive.FailureCategoryPermanent), pattern)
		path := filepath.Join(cfg.archiveCheckpoint.Directory, source+".log-file-uploads.json")
		require.NoError(t, initial.Shutdown(ctx))
		original, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, []byte("invalid checkpoint document"), 0o600))
		failed, err := newInlineCheckpointNotificationPipeline(cfg, onError)
		require.Nil(t, failed)
		require.ErrorIs(t, err, checkpointstore.ErrCorruptCheckpoint)
		var checkpointErr *FailureArchiveCheckpointError
		require.ErrorAs(t, err, &checkpointErr)
		require.Equal(t, failurearchive.FailureCategoryPermanent, checkpointErr.Category)
		synctest.Wait()
		require.Empty(t, notifications, "constructor failure is returned, without OnError duplication")
		require.NoError(t, os.WriteFile(path, original, 0o600))
		restarted, err := newInlineCheckpointNotificationPipeline(cfg, onError)
		require.NoError(t, err, "failed construction releases the archive root lease")
		require.NoError(t, restarted.Shutdown(ctx))
		synctest.Wait()
		// The synctest bubble also requires the retryable notification worker from
		// partial construction to exit; an abandoned worker would deadlock here.
	})
}

func newInlineCheckpointNotificationConfig(t *testing.T) testArchiveConfig {
	t.Helper()
	cfg := newValidArchiveHandlerConfig(t)
	cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "archive-retry-notification"}
	cfg.objectStorageUpload = objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{Region: "us-east-1", Bucket: "failure-archive", AccessKeyID: "test", SecretAccessKey: "test"},
	}
	return cfg
}

func newInlineCheckpointNotificationPipeline(cfg testArchiveConfig, onError logging.LoggerErrorHandler) (*FailureArchivePipeline, error) {
	return NewFailureArchivePipeline(
		context.Background(),
		archiveFixedClock{now: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)},
		cfg, observer.NopTracer{}, observer.NopMeter{}, InternalLogHandlers{OnError: onError},
	)
}

func blockFailureArchiveCheckpointReplacement(t *testing.T, path string) func() {
	t.Helper()
	previous := filepath.Join(t.TempDir(), "previous.json")
	require.NoError(t, os.Rename(path, previous))
	require.NoError(t, os.Mkdir(path, 0o700))
	marker := filepath.Join(path, "prevent-replacement")
	require.NoError(t, os.WriteFile(marker, []byte("preserve"), 0o600))
	return func() {
		require.NoError(t, os.Remove(marker))
		require.NoError(t, os.Remove(path))
		require.NoError(t, os.Rename(previous, path))
	}
}
