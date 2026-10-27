package lifecycle

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
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type observedAcceptedCheckpointStore struct {
	checkpointstore.Store
	markCalls  atomic.Int64
	loadCalls  atomic.Int64
	beforeLoad func()
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *observedAcceptedCheckpointStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *observedAcceptedCheckpointStore) Load(ctx context.Context, key checkpointstore.CheckpointKey) (checkpointstore.Checkpoint, error) {
	s.loadCalls.Add(1)
	if s.beforeLoad != nil {
		s.beforeLoad()
	}
	return s.Store.Load(ctx, key)
}

func (s *observedAcceptedCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, ids []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	s.markCalls.Add(1)
	return s.Store.AdvanceCompletionCursor(ctx, key, ids)
}

// document pathにdirectoryがあると、atomicな置き換えはすべてのplatformで失敗します。
// permissionを迂回する特権でtestを実行した場合も同様です。
func blockRunnerCheckpointDocument(t *testing.T, path string) func() {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		require.NoError(t, os.Rename(path, path+".before-save-failure"))
	} else {
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	require.NoError(t, os.Mkdir(path, 0o700))
	restore := sync.OnceFunc(func() { require.NoError(t, os.Remove(path)) })
	t.Cleanup(restore)
	return restore
}

func runnerCheckpointDocumentPath(f logFileProcessingRecoveryFixture) string {
	store := f.store.Store.(*checkpointstore.FilesystemStore)
	return filepath.Join(store.Directory(), "recovery.log-file-uploads.json")
}

func TestAcceptedCheckpointSaveFailuresDoNotBlockUploadOrRemoval(t *testing.T) {
	for _, operation := range []string{"mark", "load"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				const destination = objectstorage.DestinationID("primary")
				f := newLogFileProcessingRecoveryFixture(t, destination)
				store := &observedAcceptedCheckpointStore{Store: f.store}
				f.manager.objectStorageUpload.checkpointStore = store
				var restore func()
				switch operation {
				case "mark":
					restore = blockRunnerCheckpointDocument(t, runnerCheckpointDocumentPath(f))
				case "load":
					store.beforeLoad = sync.OnceFunc(func() {
						restore = blockRunnerCheckpointDocument(t, runnerCheckpointDocumentPath(f))
						_, err := f.store.AdvanceCompletionCursor(ctx, fileCheckpointKey(f.cursor, true), []objectstorage.DestinationID{destination})
						var pending *checkpointstore.PendingSaveError
						require.ErrorAs(t, err, &pending)
					})
				}
				_, err := f.lifecycle.RunOnce(ctx)
				var pending *checkpointstore.PendingSaveError
				if operation == "load" {
					require.NoError(t, err, "reading accepted state does not repeat its original save error")
				} else {
					require.ErrorAs(t, err, &pending)
				}
				require.NoFileExists(t, f.path, "completed file removal must not wait for persistence")
				require.False(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
				require.False(t, f.manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))
				checkpoint, err := store.Load(ctx, fileCheckpointKey(f.cursor, true))
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs, "removal must retain the accepted completion cursor despite failed save")
				if operation == "load" {
					require.Empty(t, f.remote.pathsFor(destination))
				} else {
					require.Equal(t, []string{f.path}, f.remote.pathsFor(destination))
				}
				restore()
				require.Eventually(t, func() bool { return checkpointFileOnDiskHasGeneration(runnerCheckpointDocumentPath(f), f.cursor) }, 8*time.Second, 10*time.Millisecond)
			})
		})
	}
}

func TestPendingCheckpointSaveKeepsPartialFailureAndContinuesUsingMemory(t *testing.T) {
	t.Parallel()
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const primary, secondary = objectstorage.DestinationID("primary"), objectstorage.DestinationID("secondary")
		f := newLogFileProcessingRecoveryFixture(t, primary, secondary)
		restore := blockRunnerCheckpointDocument(t, runnerCheckpointDocumentPath(f))
		remoteFailure := errors.New("secondary offline")
		f.remote.failNext(secondary, remoteFailure)
		_, err := f.lifecycle.RunOnce(ctx)
		require.ErrorIs(t, err, remoteFailure)
		require.FileExists(t, f.path)
		require.True(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
		checkpoint, err := f.store.Load(ctx, fileCheckpointKey(f.cursor, true))
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{primary}, checkpoint.CompletedDestinationIDs)

		waitLogFileProcessingRecovery(t, f.manager, f.cursor)
		require.NoFileExists(t, f.path)
		require.Equal(t, []string{f.path}, f.remote.pathsFor(primary))
		require.Equal(t, []string{f.path, f.path}, f.remote.pathsFor(secondary))
		restore()
		require.Eventually(t, func() bool { return checkpointFileOnDiskHasGeneration(runnerCheckpointDocumentPath(f), f.cursor) }, 8*time.Second, 10*time.Millisecond)
	})
}

func TestPendingCheckpointSaveDoesNotDiscardBlockedDestinationContinuation(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const primary, secondary = objectstorage.DestinationID("primary"), objectstorage.DestinationID("secondary")
		f := newLogFileProcessingRecoveryFixture(t, primary, secondary)
		earlier := strings.Replace(f.path, "_2.log.gz", "_1.log.gz", 1)
		writeGzipFile(t, earlier, []byte("earlier unresolved secondary"))
		earlierGeneration := fileGeneration{Period: f.cursor.Period, Index: 1}
		_, err := f.store.AdvanceCompletionCursor(ctx, fileCheckpointKey(earlierGeneration, true), []objectstorage.DestinationID{primary})
		require.NoError(t, err)
		restore := blockRunnerCheckpointDocument(t, runnerCheckpointDocumentPath(f))
		f.remote.failNext(secondary, errors.New("earlier destination temporarily unavailable"))
		_, err = f.manager.RunOnce(ctx)
		require.Error(t, err)
		// 後続はprimaryを受理した保存errorだけを返していても、secondaryの待機が残ります。
		require.True(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
		restore()
		require.NoError(t, boundedwait.CloseInBubble(t, "f.manager.Close()", f.manager.Close, f.manager.ForceClose))
		require.Equal(t, []string{f.path}, f.remote.pathsFor(primary))
		require.Equal(t, []string{earlier, earlier, f.path}, f.remote.pathsFor(secondary))
		require.NoFileExists(t, earlier)
		require.NoFileExists(t, f.path)
	})
}

func checkpointFileOnDiskHasGeneration(path string, generation fileGeneration) bool {
	payload, err := os.ReadFile(path)
	return err == nil && strings.Contains(string(payload), checkpointstore.ArchiveCheckpointGenerationKey(generation.Period, generation.Index))
}

func TestPendingCheckpointSaveDoesNotBlockLaterFiles(t *testing.T) {
	t.Parallel()
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const destination = objectstorage.DestinationID("primary")
		f := newLogFileProcessingRecoveryFixture(t, destination)
		second := strings.Replace(f.path, "_2.log.gz", "_3.log.gz", 1)
		require.NoError(t, os.Remove(strings.TrimSuffix(second, ".gz")))
		writeGzipFile(t, second, []byte("next"))
		writeFileSeriesCheckpointLog(t, filepath.Dir(f.path), "checkpoint-recovery", f.cursor.Period, 4)
		restore := blockRunnerCheckpointDocument(t, runnerCheckpointDocumentPath(f))
		_, err := f.lifecycle.RunOnce(ctx)
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		require.NoFileExists(t, f.path)
		require.NoFileExists(t, second)
		require.Equal(t, []string{f.path, second}, f.remote.pathsFor(destination))
		restore()
		require.Eventually(t, func() bool {
			return checkpointFileOnDiskHasGeneration(runnerCheckpointDocumentPath(f), fileGeneration{Period: f.cursor.Period, Index: 3})
		}, 8*time.Second, 10*time.Millisecond)
	})
}
