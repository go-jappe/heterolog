package fsarchivehandler

import (
	"context"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	checkpointtest "github.com/go-jappe/heterolog/internal/testutil/checkpoint"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/require"
)

// checkpointFilesystemStoreForTestは、checkpoint設定から作ったStoreのfile保存部分を返します。
// 保存失敗の注入と保存内容の確認に使います。
func checkpointFilesystemStoreForTest(t *testing.T, store checkpointstore.Store) *checkpointstore.FilesystemStore {
	t.Helper()
	filesystem, ok := store.(*checkpointstore.FilesystemStore)
	require.True(t, ok, "checkpoint store = %T, want *checkpoint.FilesystemStore", store)
	return filesystem
}

type countingArchiveCheckpointStore struct {
	checkpointstore.Store
	records atomic.Int64
}

func (s *countingArchiveCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	s.records.Add(1)
	return s.Store.AdvanceCompletionCursor(ctx, key, destinations)
}

func TestFailureArchivePersistenceFailureDoesNotBlockCompletedFileRemoval(t *testing.T) {
	for _, method := range []string{"AdvanceCompletionCursor", "OtherFile"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			// checkpoint保存の再試行間隔は仮想時間で進めます。
			synctest.Test(t, func(t *testing.T) {
				category := failurearchive.FailureCategoryRetryable
				var filesystem *checkpointstore.FilesystemStore
				store := &countingArchiveCheckpointStore{}
				remote := newDestinationFailureArchiveUploader("destination")
				pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
					cfg.setArchiveUploader(category, remote)
					cfg.wrapArchiveCheckpointStore(category, func(production checkpointstore.Store) checkpointstore.Store {
						filesystem = checkpointFilesystemStoreForTest(t, production)
						store.Store = production
						return store
					})
				})
				t.Cleanup(func() { _ = filesystem.Close() })
				handler := pipeline.handler
				fileName := handler.Fsstore.FileNameRetryable
				path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
				require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
				key := failureArchiveCompressedLogCheckpointKey(t, handler, category, path)
				ctx := context.Background()
				checkpointPath := filesystem.DocumentPath()
				sourceID := archiveUploadCheckpointSourceIDForTest(t, handler, category)
				restore := checkpointtest.BlockPublication(t, checkpointPath)
				if method == "OtherFile" {
					_, err := filesystem.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: "20260921-98", Compressed: true}, []objectstorage.DestinationID{"destination"})
					require.Error(t, err)
				}
				var notifications atomic.Int64
				handler.OnError = func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
					notifications.Add(1)
				}
				outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, category, fileName)
				require.False(t, outcome.retryRequired)
				require.Positive(t, notifications.Load(), "persistence failure must still be reported")
				require.False(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(category, key)))
				require.NoFileExists(t, path)
				checkpoint, err := store.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"destination"}, checkpoint.CompletedDestinationIDs)
				nextPath := filepath.Join(fileName.Dir, "test-app_20260922_2.jsonl.gz")
				require.NoError(t, os.WriteFile(nextPath, []byte("next log"), 0o600))
				outcome = handler.uploadCheckpointedArchiveGroup(ctx, []string{nextPath}, category, fileName)
				require.False(t, outcome.retryRequired)
				require.NoFileExists(t, nextPath)
				require.Equal(t, 1, remote.attemptCount(nextPath, "destination"))
				require.EqualValues(t, 2, store.records.Load())
				require.Equal(t, 1, remote.attemptCount(path, "destination"))
				restore()
				expected := []checkpointstore.Checkpoint{{Key: failureArchiveCompressedLogCheckpointKey(t, handler, category, nextPath), CompletedDestinationIDs: []objectstorage.DestinationID{"destination"}}}
				require.NoError(t, handler.logFileProcessingRecovery.Close())
				restarted := checkpointtest.WaitForContentsAndRestart(t, checkpointPath, sourceID, filesystem, expected)
				checkpoint, err = restarted.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"destination"}, checkpoint.CompletedDestinationIDs)
			})
		})
	}
}

func TestFailureArchiveCheckpointSaveFailureRetainsPartialUploadAndSkipsCompletedDestination(t *testing.T) {
	t.Parallel()
	// 受理済みの再試行とcheckpoint保存の再試行間隔は仮想時間で進めます。
	synctest.Test(t, func(t *testing.T) {
		category := failurearchive.FailureCategoryRetryable
		var filesystem *checkpointstore.FilesystemStore
		remote := newDestinationFailureArchiveUploader("primary", "secondary")
		remote.failNext("secondary", 1)
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
			cfg.wrapArchiveCheckpointStore(category, func(production checkpointstore.Store) checkpointstore.Store {
				filesystem = checkpointFilesystemStoreForTest(t, production)
				return production
			})
		})
		t.Cleanup(func() { _ = filesystem.Close() })
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
		key := failureArchiveCompressedLogCheckpointKey(t, handler, category, path)
		checkpointPath := filesystem.DocumentPath()
		sourceID := archiveUploadCheckpointSourceIDForTest(t, handler, category)
		restore := checkpointtest.BlockPublication(t, checkpointPath)
		ctx := context.Background()
		outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, category, fileName)
		require.True(t, outcome.retryRequired)
		require.FileExists(t, path)
		require.True(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(category, key)))
		checkpoint, err := filesystem.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
		drainArchiveRecoveryForTest(t, handler)
		outcome = handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, category, fileName)
		require.False(t, outcome.retryRequired)
		require.NoFileExists(t, path)
		require.Equal(t, 1, remote.attemptCount(path, "primary"))
		require.Equal(t, 2, remote.attemptCount(path, "secondary"))
		restore()
		require.NoError(t, handler.logFileProcessingRecovery.Close())
		checkpointtest.WaitForContentsAndRestart(t, checkpointPath, sourceID, filesystem, []checkpointstore.Checkpoint{{Key: key, CompletedDestinationIDs: []objectstorage.DestinationID{"primary", "secondary"}}})
	})
}

func TestFailureArchiveCheckpointCloseWaitsForPersistence(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		category := failurearchive.FailureCategoryRetryable
		remote := newDestinationFailureArchiveUploader("destination")
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
		key := failureArchiveCompressedLogCheckpointKey(t, handler, category, path)
		checkpointPath := checkpointFilesystemStoreForTest(t, handler.compressedArchiveUploadCheckpointStore(category)).DocumentPath()
		sourceID := archiveUploadCheckpointSourceIDForTest(t, handler, category)
		restore := checkpointtest.BlockPublication(t, checkpointPath)
		outcome := handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
		require.False(t, outcome.retryRequired)
		require.NoFileExists(t, path)
		require.False(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(category, key)))
		closed := make(chan error, 1)
		go func() { closed <- pipeline.Shutdown(context.Background()) }()
		synctest.Wait()
		time.Sleep(2 * checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, closed, "Shutdown must retain the accepted checkpoint while persistence fails")
		require.Equal(t, 1, remote.attemptCount(path, "destination"))
		restore()
		require.NoError(t, <-closed)
		restarted, err := checkpointstore.NewFilesystemStore(filepath.Dir(checkpointPath), sourceID)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, restarted.Close()) })
		checkpoint, err := restarted.Load(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{"destination"}, checkpoint.CompletedDestinationIDs)
		paths, err := filepath.Glob(filepath.Join(fileName.Dir, "*.jsonl.gz"))
		require.NoError(t, err)
		require.Empty(t, paths, "the removed file cannot be rediscovered after restart")
	})
}
