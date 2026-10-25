package lifecycle

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// recordBeforeLoadCheckpointStoreは、AdvanceCompletionCursorの保存失敗後、同じ操作が
// 成功するまで当該fileのLoadも失敗するStoreです。内部で保存を再試行する処理は持ちません。
// earlierGenerationの先行世代が未完了の間は、当該fileの完了cursorを先へ進められません。
// その間の復旧は記録できない成功を保持したまま当該fileのcheckpointを読むため、そのLoadも
// 失敗させますが、blockedLoadsには数えません。blockedLoadsは、記録を再試行できる状態で
// Loadを先に行った回数です。
type recordBeforeLoadCheckpointStore struct {
	checkpointstore.Store
	allowRecord       atomic.Bool
	pending           atomic.Bool
	recordCalls       atomic.Int64
	blockedLoads      atomic.Int64
	rejectedKey       checkpointstore.CheckpointKey
	earlierGeneration string
	earlierPending    atomic.Bool
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *recordBeforeLoadCheckpointStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *recordBeforeLoadCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, ids []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	s.recordCalls.Add(1)
	if !s.allowRecord.Load() {
		s.rejectedKey = key
		s.pending.Store(true)
		return checkpointstore.Checkpoint{}, errRecoveryCheckpoint
	}
	checkpoint, err := s.Store.AdvanceCompletionCursor(ctx, key, ids)
	if err == nil && key == s.rejectedKey {
		s.pending.Store(false)
	}
	if err == nil && s.earlierGeneration != "" && key.Generation == s.earlierGeneration {
		s.earlierPending.Store(false)
	}
	return checkpoint, err
}

func (s *recordBeforeLoadCheckpointStore) Load(ctx context.Context, key checkpointstore.CheckpointKey) (checkpointstore.Checkpoint, error) {
	if s.pending.Load() && key == s.rejectedKey {
		if !s.earlierPending.Load() {
			s.blockedLoads.Add(1)
		}
		return checkpointstore.Checkpoint{}, errRecoveryCheckpoint
	}
	return s.Store.Load(ctx, key)
}

func TestLogFileProcessingRecoveryRetriesUnacceptedMarkBeforeLoad(t *testing.T) {
	for _, earlierPending := range []bool{false, true} {
		name := "no earlier generation"
		if earlierPending {
			name = "earlier generation must remain pending"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			const destination = objectstorage.DestinationID("archive")
			f := newLogFileProcessingRecoveryFixture(t, destination)
			store := &recordBeforeLoadCheckpointStore{Store: f.store}
			if earlierPending {
				store.earlierGeneration = fileCheckpointKey(fileGeneration{Period: f.cursor.Period, Index: 1}, true).Generation
			}
			f.manager.objectStorageUpload.checkpointStore = store
			// fixtureの終了処理より先に実行し、途中で失敗しても保存拒否を残したまま閉じないようにします。
			t.Cleanup(func() { store.allowRecord.Store(true) })

			_, err := f.lifecycle.RunOnce(ctx)
			require.ErrorIs(t, err, errRecoveryCheckpoint)
			require.True(t, store.pending.Load())
			require.FileExists(t, f.path)
			require.Equal(t, []string{f.path}, f.remote.pathsFor(destination))

			var lowerPath string
			if earlierPending {
				lowerPath = filepath.Join(filepath.Dir(f.path), "checkpoint-recovery_"+f.cursor.Period+"_1.log.gz")
				require.NoError(t, f.manager.directoryGuard.RunWithLifecycleLock(func() error {
					// 復旧の走査も同じlockの下で行うため、file作成と同時に先行世代の未完了を記録します。
					store.earlierPending.Store(true)
					writeGzipFile(t, lowerPath, []byte("earlier unresolved generation"))
					return nil
				}))
			}
			store.allowRecord.Store(true)

			if earlierPending {
				require.FileExists(t, f.path, "unaccepted completion cannot advance past the earlier generation")
				require.True(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
				require.FileExists(t, lowerPath)
				_, err = f.lifecycle.RunOnce(ctx)
				require.NoError(t, err)
			}
			require.Eventually(t, func() bool {
				return !store.pending.Load() && store.recordCalls.Load() >= 2
			}, boundedwait.HangTimeout, time.Millisecond)
			if earlierPending {
				// 先行世代の記録を観測していなければ、blockedLoadsは記録を再試行できる状態のLoadを数えていません。
				require.False(t, store.earlierPending.Load(), "the earlier generation must be recorded through the store before the later one")
			}
			require.Zero(t, store.blockedLoads.Load(), "retry must restore the failed AdvanceCompletionCursor before loading that file")
			waitLogFileProcessingRecovery(t, f.manager, f.cursor)
			require.NoFileExists(t, f.path)
			expectedPaths := []string{f.path}
			if earlierPending {
				expectedPaths = append(expectedPaths, lowerPath)
			}
			require.Equal(t, expectedPaths, f.remote.pathsFor(destination), "checkpoint retries must not resend an already uploaded file")
		})
	}
}

func TestLogFileProcessingRecoveryRecordRetryCompletesFailedRemoteDestination(t *testing.T) {
	ctx := context.Background()
	const primary, secondary = objectstorage.DestinationID("primary"), objectstorage.DestinationID("secondary")
	f := newLogFileProcessingRecoveryFixture(t, primary, secondary)
	store := &recordBeforeLoadCheckpointStore{Store: f.store}
	f.manager.objectStorageUpload.checkpointStore = store
	remoteFailure := errors.New("secondary unavailable")
	f.remote.failNext(secondary, remoteFailure)
	_, err := f.lifecycle.RunOnce(ctx)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	require.ErrorIs(t, err, remoteFailure)
	store.allowRecord.Store(true)
	waitLogFileProcessingRecovery(t, f.manager, f.cursor)
	require.False(t, store.pending.Load())
	require.NoFileExists(t, f.path)
	require.Equal(t, []string{f.path}, f.remote.pathsFor(primary))
	require.Equal(t, []string{f.path, f.path}, f.remote.pathsFor(secondary))
	checkpoint, err := store.Load(ctx, fileCheckpointKey(f.cursor, true))
	require.NoError(t, err)
	require.ElementsMatch(t, []objectstorage.DestinationID{primary, secondary}, checkpoint.CompletedDestinationIDs)
	_, err = f.lifecycle.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{f.path}, f.remote.pathsFor(primary))
	require.Equal(t, []string{f.path, f.path}, f.remote.pathsFor(secondary))
	require.NoFileExists(t, f.path)
}

func TestCompressedUploadRetriesRememberedRecordBeforeLoad(t *testing.T) {
	ctx := context.Background()
	const destination = objectstorage.DestinationID("archive")
	f := newLogFileProcessingRecoveryFixture(t, destination)
	store := &recordBeforeLoadCheckpointStore{Store: f.store}
	upload := f.manager.objectStorageUpload
	upload.checkpointStore = store
	state, err := upload.prepareRun(ctx)
	require.NoError(t, err)
	first, err := upload.uploadIfPending(ctx, f.path, f.cursor, state, true)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	require.Equal(t, uploadAttemptResumeCheckpoint, first.state)
	require.NotEmpty(t, first.uploadedDestinationIDs)
	require.True(t, store.pending.Load())

	store.allowRecord.Store(true)
	state, err = upload.prepareRun(ctx)
	require.NoError(t, err)
	retried, err := upload.uploadIfPending(ctx, f.path, f.cursor, state, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptComplete, retried.state)
	require.Empty(t, retried.uploadedDestinationIDs)
	require.Equal(t, f.path, retried.localCompressedPathToRemove)
	require.EqualValues(t, 2, store.recordCalls.Load())
	require.Zero(t, store.blockedLoads.Load())
	require.Equal(t, []string{f.path}, f.remote.pathsFor(destination))
}
