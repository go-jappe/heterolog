package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestInlineUploadCursorPreservesSuccessAfterRemovalAndObservation(t *testing.T) {
	logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
	ctx := context.Background()
	require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
	require.NoFileExists(t, path)
	require.Empty(t, retainPendingArchiveGroupsForTest(t, logger, nil))
	checkpoint, err := store.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
	// The same generation reappearing after a stale observation is already complete.
	require.NoError(t, os.WriteFile(path, []byte("reappeared generation"), 0o600))
	require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
	require.NoFileExists(t, path)
	require.Equal(t, 1, remote.attemptCount())
}

func TestInlineUploadCursorBlocksOnlyDestinationWithOlderFailure(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger, _, _, first, _ := newInlineLogFileProcessingRecoveryFixture(t)
		second := writePendingCompressedLog(t, logger, "20260920", 2, time.Now())
		healthy := &sequenceArchiveUploader{}
		retrying := &sequenceArchiveUploader{errs: []error{errors.New("older upload failed"), errors.New("older upload still failed")}}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "healthy", Uploader: healthy},
			uploader.Destination{ID: "retrying", Uploader: retrying},
		)
		ctx := context.Background()
		require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{second, first}, logger.Fsstore.FileName), runNotificationImmediately))
		require.Equal(t, 2, healthy.attemptCount())
		require.Equal(t, 1, retrying.attemptCount(), "later generation must not skip the failed earlier generation")
		require.FileExists(t, first)
		require.FileExists(t, second)
		require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{first, second}, logger.Fsstore.FileName), runNotificationImmediately))
		require.Equal(t, 2, healthy.attemptCount())
		require.Equal(t, 1, retrying.attemptCount(), "登録済み処理は通常writeが重複送信しない")
		requireArchiveRecoveryClosesForTest(t, logger)
		require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{second, first}, logger.Fsstore.FileName), runNotificationImmediately))
		require.Equal(t, 4, retrying.attemptCount())
		require.NoFileExists(t, first)
		require.NoFileExists(t, second)
	})
}

func TestInlineUploadCursorWaitsForOlderRawAndUnacceptedSuccess(t *testing.T) {
	for _, rejectedUpdate := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw-compression", true: "checkpoint-update"}[rejectedUpdate], func(t *testing.T) {
			synctestFromWallClock(t, func(t *testing.T) {
				logger, store, remote, first, key := newInlineLogFileProcessingRecoveryFixture(t)
				// synctestの時刻ではなく、fixtureの固定時刻で後続世代を作ります。
				second := writePendingCompressedLog(t, logger, "20260920", 2, logger.Clock.Now())
				ctx := context.Background()
				if rejectedUpdate {
					store.failRecord.Store(true)
					require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{first}, logger.Fsstore.FileName), runNotificationImmediately))
				} else {
					require.NoError(t, os.Rename(first, first[:len(first)-len(filepath.Ext(first))]))
				}
				attempts := remote.attemptCount()
				require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{second}, logger.Fsstore.FileName), runNotificationImmediately))
				require.Equal(t, attempts, remote.attemptCount())
				checkpoint, err := store.Load(ctx, inlineCompressedLogCheckpointKey(t, logger, second))
				require.NoError(t, err)
				require.Empty(t, checkpoint.CompletedDestinationIDs)
				if rejectedUpdate {
					store.failRecord.Store(false)
					// 受理されなかった送信成功は、再試行間隔の経過後にcheckpointへ記録します。
					time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
					synctest.Wait()
					require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
				} else {
					require.NoError(t, os.Rename(first[:len(first)-len(filepath.Ext(first))], first))
					require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{first}, logger.Fsstore.FileName), runNotificationImmediately))
				}
				requireArchiveRecoveryClosesForTest(t, logger)
				require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{second}, logger.Fsstore.FileName), runNotificationImmediately))
				require.Equal(t, 2, remote.attemptCount())
			})
		})
	}
}

var _ checkpointstore.Store = (*recoverableInlineCheckpointStore)(nil)
