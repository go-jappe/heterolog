package lifecycle

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

type closeRecordingCompressedCheckpointStore struct {
	checkpointstore.Store
	closeCalls atomic.Int32
	closeErr   error
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *closeRecordingCompressedCheckpointStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *closeRecordingCompressedCheckpointStore) Close() error {
	s.closeCalls.Add(1)
	return s.closeErr
}

func newCheckpointStoreCloseManager(t *testing.T, store checkpointstore.Store) *FileSeriesManager {
	t.Helper()
	root := resolvedTempDirForTest(t)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		Config:          testFileSeriesManagerConfig(root, "close", 10, 0, 0, false),
		FileName:        logpattern.FileNamePattern{Dir: root, Target: "close", Extension: "log"},
		CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)
	return manager
}

func TestDirectoryLifecycleFinalCloseClosesCheckpointStoreOnceAndRetainsError(t *testing.T) {
	unsaved := errors.New("latest checkpoint save is still unavailable")
	store := &closeRecordingCompressedCheckpointStore{closeErr: unsaved}
	manager := newCheckpointStoreCloseManager(t, store)
	lifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)

	// recoveryを排出しても、Storeはownerが最後にcloseするまで利用可能なまま残ります。
	require.NoError(t, boundedwait.Close(t, "lifecycle.CloseLogFileProcessingRecovery()", lifecycle.CloseLogFileProcessingRecovery, boundedwait.ForceCloserOf(lifecycle)))
	require.Zero(t, store.closeCalls.Load())
	first := lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
	require.ErrorIs(t, first, unsaved)
	require.EqualValues(t, 1, store.closeCalls.Load())
	require.Equal(t, first, lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes())
	require.ErrorIs(t, boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager)), unsaved)
	require.EqualValues(t, 1, store.closeCalls.Load())
}

func TestFileSeriesManagerFinalCloseWaitsForAcceptedLifecycleWork(t *testing.T) {
	// Closeが受理済み処理を追い越さないことは、実時間の待機ではなく
	// bubble内の全goroutineが停止した時点で確認します。
	synctest.Test(t, func(t *testing.T) {
		store := &closeRecordingCompressedCheckpointStore{}
		manager := newCheckpointStoreCloseManager(t, store)
		_, err := NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		entered := make(chan struct{})
		release := make(chan struct{})
		releaseWork := sync.OnceFunc(func() { close(release) })
		finished := make(chan struct{})
		go func() {
			_ = manager.directoryGuard.RunWithLifecycleLock(func() error {
				close(entered)
				<-release
				return nil
			})
			close(finished)
		}()
		// managerの終了処理より先に解放し、途中で失敗してもCloseの完了待ちで止まらないようにします。
		t.Cleanup(func() {
			releaseWork()
			boundedwait.ReceiveInBubble(t, finished, "lifecycle work after release")
		})
		boundedwait.ReceiveInBubble(t, entered, "receive from entered")
		closed := make(chan error, 1)
		go func() { closed <- manager.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("final Close passed accepted lifecycle work: %v", err)
		default:
		}
		require.Zero(t, store.closeCalls.Load())
		releaseWork()
		boundedwait.ReceiveInBubble(t, finished, "receive from finished")
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Minute):
			t.Fatal("final Close did not finish after accepted work completed")
		}
		require.EqualValues(t, 1, store.closeCalls.Load())
	})
}
