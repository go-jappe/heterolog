package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestFailureArchiveRemovalSyncFailureRetainsCompletedCursor(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		for _, fromRecovery := range []bool{false, true} {
			name := string(category) + "/initial"
			if fromRecovery {
				name = string(category) + "/recovery"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				// 同期失敗後の受理済み再試行は、再試行間隔を仮想時間で進めて完了させます。
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					var filesystem checkpointstore.Store
					transient := &transientArchiveCheckpointStore{}
					if fromRecovery {
						transient.loadFailures = 1
					}
					store := &countingArchiveCheckpointStore{Store: transient}
					remote := newDestinationFailureArchiveUploader("destination")
					pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
						cfg.setArchiveUploader(category, remote)
						cfg.wrapArchiveCheckpointStore(category, func(production checkpointstore.Store) checkpointstore.Store {
							filesystem = production
							transient.Store = production
							return store
						})
					})
					handler := pipeline.handler
					fileName, err := handler.archiveFileNamePattern(category)
					require.NoError(t, err)
					// 削除済みのパスと利用できない親で、削除確認は成功し同期だけ失敗する状態を作る。
					fileName.Dir = filepath.Join(t.TempDir(), "unavailable-directory")
					path := filepath.Join(fileName.Dir, "test-app_20260923_1.jsonl.gz")
					generation, err := checkpointstore.ArchiveCheckpointGenerationKeyForPath(path, fileName)
					require.NoError(t, err)
					key := checkpointstore.CheckpointKey{Generation: generation, Compressed: true}
					_, err = filesystem.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"destination"})
					require.NoError(t, err)
					notified := make(chan error, 1)
					handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
						if errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
							select {
							case notified <- err:
							default:
							}
						}
					}

					outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, category, fileName)
					recoveryKey := failureArchiveRecoveryKey(category, key)
					if fromRecovery {
						require.True(t, outcome.retryRequired, "the initial Load failure still starts recovery")
						require.True(t, handler.logFileProcessingRecovery.Has(recoveryKey))
						require.Contains(t, handler.protectedArchivePaths(category), path)
					} else {
						require.True(t, outcome.retryRequired)
						require.True(t, outcome.completion.Attempted)
					}
					select {
					case err := <-notified:
						require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
					case <-time.After(8 * time.Second):
						t.Fatal("directory sync failure was not reported")
					}
					require.True(t, handler.logFileProcessingRecovery.Has(recoveryKey))
					require.Contains(t, handler.protectedArchivePaths(category), path)
					require.Contains(t, handler.protectedArchivePaths(category), path[:len(path)-len(".gz")])
					checkpoint, err := filesystem.Load(ctx, key)
					require.NoError(t, err)
					require.Equal(t, []objectstorage.DestinationID{"destination"}, checkpoint.CompletedDestinationIDs)
					require.Zero(t, remote.attemptCount(path, "destination"), "completed uploads must not be repeated")

					// 同期が回復するまで受理済みの削除を保持し、次の通常試行なしで終了させます。
					require.NoError(t, os.Mkdir(fileName.Dir, 0o700))
					require.NoError(t, handler.logFileProcessingRecovery.Close())
					require.False(t, handler.logFileProcessingRecovery.Has(recoveryKey))
					require.NotContains(t, handler.protectedArchivePaths(category), path)
					require.NotContains(t, handler.protectedArchivePaths(category), path[:len(path)-len(".gz")])
					checkpoint, err = filesystem.Load(ctx, key)
					require.NoError(t, err)
					require.True(t, checkpoint.DestinationComplete("destination"))
					require.Zero(t, remote.attemptCount(path, "destination"))
				})
			})
		}
	}
}

func TestFailureArchiveRemovalFailureStillRequiresRecovery(t *testing.T) {
	ctx := context.Background()
	category := failurearchive.FailureCategoryRetryable
	store := &countingArchiveCheckpointStore{}
	remote := newDestinationFailureArchiveUploader("destination")
	pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(category, remote)
		cfg.embedArchiveCheckpointStore(category, &store.Store, store)
	})
	handler := pipeline.handler
	fileName := handler.Fsstore.FileNameRetryable
	path := filepath.Join(fileName.Dir, "test-app_20260923_1.jsonl.gz")
	require.NoError(t, os.Mkdir(path, 0o700))
	generation, err := checkpointstore.ArchiveCheckpointGenerationKeyForPath(path, fileName)
	require.NoError(t, err)
	key := checkpointstore.CheckpointKey{Generation: generation, Compressed: true}
	_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"destination"})
	require.NoError(t, err)
	operation := &archiveFileOperation{
		store: store, key: key, path: path, category: category,
		fileName: fileName, uploader: remote,
	}
	outcome, pending, err := handler.runArchiveFileOperation(ctx, operation)
	require.ErrorIs(t, err, infrafsstore.ErrNotRegularFile)
	require.NotErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	require.True(t, pending)
	require.True(t, outcome.retryRequired)
	require.DirExists(t, path)

	// ファイル削除失敗の原因が解消すれば、同じ処理を再開して削除します。完了cursorは残します。
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
	outcome, pending, err = handler.runArchiveFileOperation(ctx, operation)
	require.NoError(t, err)
	require.False(t, pending)
	require.False(t, outcome.retryRequired)
	require.NoFileExists(t, path)
	require.Zero(t, remote.attemptCount(path, "destination"))
}

func TestFailureArchiveAcceptedSourceCleanupRetriesSyncAfterPendingPathDisappeared(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), nil)
		handler := pipeline.handler
		category := failurearchive.FailureCategoryRetryable
		directory := filepath.Join(t.TempDir(), "unavailable-directory")
		sourcePath := filepath.Join(directory, "test-app_20260923_1.jsonl")
		pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
		key := archiveLogFileProcessingRecoveryKey{Category: category, PreparationPath: pendingPath}
		// pendingの削除後に同期だけ失敗した状態でも、不在を完了証明にしません。
		handler.scheduleCompressionSourceCleanupRetry(sourcePath, category,
			errors.Join(appendlog.ErrCompressionSourceCleanup, pkgfsstore.ErrDurabilityUnconfirmed))
		require.True(t, handler.logFileProcessingRecovery.Has(key))
		require.Contains(t, handler.protectedArchivePaths(category), pendingPath)
		closed := make(chan error, 1)
		go func() { closed <- handler.logFileProcessingRecovery.Close() }()
		// ゼロ値の既定間隔を仮想時間で進め、親がない状態での再同期失敗まで確認します。
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("cleanup finished before the missing directory could be synced: %v", err)
		default:
		}
		require.NoError(t, os.Mkdir(directory, 0o700))
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		select {
		case err := <-closed:
			require.NoError(t, err)
		default:
			t.Fatal("cleanup did not finish after parent-directory sync recovered")
		}
		require.False(t, handler.logFileProcessingRecovery.Has(key))
		require.NotContains(t, handler.protectedArchivePaths(category), pendingPath)
		require.NoFileExists(t, pendingPath)
		require.NoFileExists(t, sourcePath)
	})
}
