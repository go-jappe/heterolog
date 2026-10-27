package file

import (
	"context"
	"errors"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestInlineRawCursorRetriesInactiveFilesAndRetainsCompletedRaw(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		logger := newInlineUsageTestLogger(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), "raw-cursor")
		require.False(t, logger.config.Fsstore().Lifecycle().EnableFileCompress())
		directory := t.TempDir()
		store := newCompressedLogCheckpointTestStore(t, directory, "raw_cursor")
		logger.archiveUploadCheckpoints = store
		healthy := &sequenceArchiveUploader{}
		retrying := &sequenceArchiveUploader{errs: []error{errors.New("raw destination unavailable")}}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "healthy", Uploader: healthy},
			uploader.Destination{ID: "retrying", Uploader: retrying},
		)
		raw := writeCanonicalRawLog(t, logger, "20260920", 1)
		later := writePendingCompressedLog(t, logger, "20260920", 2, time.Now())
		require.True(t, logger.archiveRotatedFileWithNotifications(ctx, raw, logger.Fsstore.FileName, runNotificationImmediately))
		require.FileExists(t, raw)
		require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{later}, logger.Fsstore.FileName), runNotificationImmediately))
		require.Equal(t, 2, healthy.attemptCount())
		require.Equal(t, 1, retrying.attemptCount(), "later file waits for failed raw destination")

		active := logger.Fsstore.ActiveFilePath
		cleanupOwner := appendlog.NewDirectoryGuardToken()
		var protected []string
		var paths []archiveUploadGroup
		require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
			var err error
			paths, err = logger.retainRemainingLogFilesForOwner(ctx,
				infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{active, later, raw}), logger.Fsstore.FileName.Extension),
				logger.Fsstore.FileName, cleanupOwner, &protected)
			return err
		}))
		defer logger.lifecycleGuard.UnprotectPaths(cleanupOwner, protected...)
		require.Empty(t, paths, "active writerと受理済み復旧が所有するfileは再取得しない")
		requireArchiveRecoveryClosesForTest(t, logger)
		require.False(t, logger.processRemainingLogFiles(ctx, paths, runNotificationImmediately))
		logger.lifecycleGuard.UnprotectPaths(cleanupOwner, protected...)
		require.FileExists(t, raw, "raw upload completion preserves local retention")
		require.FileExists(t, active)
		require.NoFileExists(t, later)
		require.Equal(t, 2, healthy.attemptCount())
		require.Equal(t, 3, retrying.attemptCount())
		require.Empty(t, retainPendingArchiveGroupsForTest(t, logger, []string{raw}), "completed raw does not remain pending")

		// 再起動しても、後から現れた圧縮表現は同じ完了を共有します。
		require.NoError(t, store.Close())
		logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, directory, "raw_cursor")
		peer := raw + ".gz"
		require.NoError(t, os.WriteFile(peer, []byte("same completed generation"), 0o600))
		require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{peer}, logger.Fsstore.FileName), runNotificationImmediately))
		require.NoFileExists(t, peer)
		require.FileExists(t, raw)
		require.Equal(t, 2, healthy.attemptCount())
		require.Equal(t, 3, retrying.attemptCount())
	})
}

func TestInlineRawCursorRecoveryDoesNotRemoveRawOrRepeatSuccess(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, compressed, key := newInlineLogFileProcessingRecoveryFixture(t)
		raw := compressed[:len(compressed)-len(".gz")]
		require.NoError(t, os.Rename(compressed, raw))
		key.Compressed = false
		store.failRecord.Store(true)
		require.True(t, logger.archiveRotatedFileWithNotifications(context.Background(), raw, logger.Fsstore.FileName, runNotificationImmediately))
		require.Equal(t, 1, remote.attemptCount())
		store.failRecord.Store(false)
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		require.FileExists(t, raw)
		checkpoint, err := store.Load(context.Background(), key)
		require.NoError(t, err)
		require.True(t, checkpoint.DestinationComplete("primary"))
		require.Equal(t, 1, remote.attemptCount())
	})
}

func TestInlineRawCursorNaturalRetryProtectsExpiredRaw(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
		logger := newInlineUsageTestLogger(t, now, "raw-natural-retry")
		logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, t.TempDir(), "raw_natural_retry")
		remote := &sequenceArchiveUploader{errs: []error{errors.New("unavailable")}}
		logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
		raw := writeCanonicalRawLog(t, logger, "20200101", 1)
		old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
		require.NoError(t, os.Chtimes(raw, old, old))
		logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()
		require.NoError(t, purgeFileLoggerForTest(ctx, logger))
		require.FileExists(t, raw, "pending raw must survive ordinary age purge")
		require.Equal(t, 1, remote.attemptCount())
		logger.Clock = pkgclock.NewFixedClock(now.Add(2 * time.Hour))
		require.NoError(t, purgeFileLoggerForTest(ctx, logger))
		requireArchiveRecoveryClosesForTest(t, logger)
		require.FileExists(t, raw, "upload completion must not remove raw")
		require.Equal(t, 2, remote.attemptCount())
	})
}

func TestInlineRawCursorCompletedExpiredRawReturnsToOrdinaryPurge(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "completed-raw-purge")
	require.False(t, logger.config.Fsstore().Lifecycle().EnableFileCompress())
	store := newCompressedLogCheckpointTestStore(t, t.TempDir(), "completed_raw_purge")
	logger.archiveUploadCheckpoints = store
	remote := &sequenceArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
	raw := writeCanonicalRawLog(t, logger, "20200101", 1)
	old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
	require.NoError(t, os.Chtimes(raw, old, old))
	key := inlineCompressedLogCheckpointKey(t, logger, raw)
	key.Compressed = false
	_, err := store.AdvanceCompletionCursor(ctx, key, logger.ArchiveUploader.DestinationIDs())
	require.NoError(t, err)

	// 完了済みの未圧縮ログは送信用に保護せず、同じcleanupの通常purgeで削除できます。
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.NoFileExists(t, raw)
	require.FileExists(t, logger.Fsstore.ActiveFilePath)
	require.Zero(t, remote.attemptCount())
}
