package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFailureArchiveUploadKeepsSuccessfulDestinationsWhenConfigurationChangesAfterRestart(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		t.Run(string(failureCategory), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
			first := newDestinationFailureArchiveUploader("already-uploaded", "now-inactive")
			first.failNext("now-inactive", 1)
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(failureCategory, first)
			})
			handler := pipeline.handler
			fileName, err := handler.archiveFileNamePattern(failureCategory)
			require.NoError(t, err)
			path := filepath.Join(fileName.Dir, "test-app_20260914_1.jsonl.gz")
			require.NoError(t, os.WriteFile(path, []byte("compressed file"), 0o600))
			outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, failureCategory, fileName)
			require.True(t, outcome.retryRequired)
			require.FileExists(t, path)
			require.Equal(t, 1, first.attemptCount(path, "already-uploaded"))
			require.Equal(t, 1, first.attemptCount(path, "now-inactive"))

			// 宛先を変更して再起動し、成功と対象範囲を読み直します。追加宛先には登録前のfileを送りません。
			restarted := newDestinationFailureArchiveUploader("already-uploaded", "new-destination")
			restartedPipeline := restartTestArchiveHandler(t, pipeline, now, func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(failureCategory, restarted)
			})
			handler = restartedPipeline.handler
			outcome = handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, failureCategory, fileName)
			// checkpointには送信に失敗したnow-inactiveの登録が残りますが、設定から外した宛先へは送らず、
			// その完了も待たずに削除します。
			require.False(t, outcome.retryRequired)
			require.NoFileExists(t, path)
			require.Zero(t, restarted.attemptCount(path, "already-uploaded"))
			require.Zero(t, restarted.attemptCount(path, "now-inactive"))
			require.Zero(t, restarted.attemptCount(path, "new-destination"))
		})
	}
}

// recordErrorArchiveCheckpointStoreは成功snapshotと保存errorを同時に返します。
// 実storeの保存再試行とは別に、呼び出し側がerror時のsnapshotで削除しないことを検査します。
type recordErrorArchiveCheckpointStore struct {
	checkpointstore.Store
	err  error
	fail *atomic.Bool
}

func (s recordErrorArchiveCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	if !s.fail.Load() {
		return s.Store.AdvanceCompletionCursor(ctx, key, destinations)
	}
	return checkpointstore.Checkpoint{
		Key:                     key,
		CompletedDestinationIDs: destinations,
	}, s.err
}

func TestFailureArchiveUploadRetainsFileWhenSuccessRecordCannotBeSaved(t *testing.T) {
	ctx := context.Background()
	failureCategory := failurearchive.FailureCategoryRetryable
	var store checkpointstore.Store
	fail := &atomic.Bool{}
	fail.Store(true)
	t.Cleanup(func() { fail.Store(false) })
	remote := newDestinationFailureArchiveUploader("destination")
	pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, remote)
		cfg.wrapArchiveCheckpointStore(failureCategory, func(production checkpointstore.Store) checkpointstore.Store {
			store = production
			return recordErrorArchiveCheckpointStore{Store: production, err: errors.New("checkpoint save failed"), fail: fail}
		})
	})
	handler := pipeline.handler
	fileName := handler.Fsstore.FileNameRetryable
	path := filepath.Join(fileName.Dir, "test-app_20260914_1.jsonl.gz")
	require.NoError(t, os.WriteFile(path, []byte("compressed file"), 0o600))
	outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, failureCategory, fileName)
	require.True(t, outcome.retryRequired)
	require.False(t, outcome.completion.Attempted)
	require.FileExists(t, path)
	require.Equal(t, 1, remote.attemptCount(path, "destination"))
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, path)
	checkpoint, err := store.Load(ctx, key)
	require.NoError(t, err)
	require.Empty(t, checkpoint.CompletedDestinationIDs)

	// 通常の再試行からは未保存の成功情報を持つ再開処理を追い越しません。
	outcome = handler.uploadCheckpointedArchiveGroup(ctx, []string{path}, failureCategory, fileName)
	require.False(t, outcome.retryRequired)
	require.FileExists(t, path)
	require.Equal(t, 1, remote.attemptCount(path, "destination"))
	require.True(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(failureCategory, key)))
}
