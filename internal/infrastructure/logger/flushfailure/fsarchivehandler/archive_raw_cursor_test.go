package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

func TestFailureArchiveRawCursorRetriesInactiveFilesAndRetainsCompletedRaw(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		t.Run(string(category), func(t *testing.T) {
			// 失敗した宛先の受理済み再試行は、再試行間隔を仮想時間で進めて完了させます。
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
				remote := newDestinationFailureArchiveUploader("healthy", "retrying")
				remote.failNext("retrying", 1)
				pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
					cfg.setArchiveUploader(category, remote)
				})
				handler := pipeline.handler
				require.False(t, handler.Config.EnableFileCompress())
				pattern, err := handler.archiveFileNamePattern(category)
				require.NoError(t, err)
				raw := filepath.Join(pattern.Dir, "test-app_20260920_1.jsonl")
				later := filepath.Join(pattern.Dir, "test-app_20260920_2.jsonl.gz")
				for _, path := range []string{raw, later} {
					require.NoError(t, os.WriteFile(path, []byte("log generation"), 0o600))
				}
				active, err := pipeline.ActiveArchivePath(category)
				require.NoError(t, err)
				retry, err := handler.processRotatedArchive(ctx, active, raw, category, pattern, nil)
				require.NoError(t, err)
				require.True(t, retry)
				require.True(t, handler.uploadCheckpointedArchiveGroup(ctx, []string{later}, category, pattern).retryRequired)
				require.Zero(t, remote.attemptCount(later, "retrying"))
				observation := infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{active, later, raw}), pattern.Extension)
				protection, err := handler.retainArchivePathsForUpload(ctx, category, pattern, observation)
				require.NoError(t, err)
				require.Empty(t, protection.paths, "accepted continuations own both pending generations")
				require.False(t, handler.retryRetainedArchiveUploads(ctx, category, pattern, protection))
				drainArchiveRecoveryForTest(t, handler)
				require.FileExists(t, raw)
				require.FileExists(t, active)
				require.NoFileExists(t, later)
				require.Equal(t, 1, remote.attemptCount(raw, "healthy"))
				require.Equal(t, 2, remote.attemptCount(raw, "retrying"))
				require.Equal(t, 1, remote.attemptCount(later, "healthy"))
				require.Equal(t, 1, remote.attemptCount(later, "retrying"))
				require.Zero(t, remote.totalAttemptCount(active))
				// 再起動後も、完了cursorから同じ世代の圧縮fileを送信済みと判断します。
				restarted := restartTestArchiveHandler(t, pipeline, now, func(cfg *testArchiveConfig) {
					cfg.setArchiveUploader(category, remote)
				})
				handler = restarted.handler
				peer := raw + ".gz"
				require.NoError(t, os.WriteFile(peer, []byte("same completed generation"), 0o600))
				require.False(t, handler.uploadCheckpointedArchiveGroup(ctx, []string{peer}, category, pattern).retryRequired)
				require.NoFileExists(t, peer)
				require.FileExists(t, raw)
				require.Zero(t, remote.totalAttemptCount(peer))
			})
		})
	}
}

func TestFailureArchiveRawCursorNaturalRetryProtectsExpiredRaw(t *testing.T) {
	// 失敗した宛先の受理済み再試行は、再試行間隔を仮想時間で進めて完了させます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
		category := failurearchive.FailureCategoryRetryable
		remote := newDestinationFailureArchiveUploader("primary")
		remote.failNext("primary", 1)
		pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		state := handler.directoryForFailureCategory(category)
		pattern, err := handler.archiveFileNamePattern(category)
		require.NoError(t, err)
		raw := filepath.Join(pattern.Dir, "test-app_20200101_1.jsonl")
		require.NoError(t, os.WriteFile(raw, []byte("old raw generation"), 0o600))
		old := now.AddDate(0, 0, -handler.Config.MaxFileAge()-1)
		require.NoError(t, os.Chtimes(raw, old, old))
		// Rediscovery before a scheduled upload must protect the source during purge.
		require.True(t, state.uploadRetry.claimForAttempt(now, testUsageCacheTTL))
		handler.invalidateArchiveDirectoryEstimate(category)
		require.NoError(t, handler.runInlineArchivePurgeIfNeeded(ctx, category))
		require.FileExists(t, raw)
		require.Zero(t, remote.totalAttemptCount(raw))
		handler.Clock = archiveFixedClock{now: now.Add(testUsageCacheTTL)}
		require.NoError(t, handler.runInlineArchivePurgeIfNeeded(ctx, category))
		require.Equal(t, 1, remote.totalAttemptCount(raw))
		require.FileExists(t, raw)
		drainArchiveRecoveryForTest(t, handler)
		require.Equal(t, 2, remote.totalAttemptCount(raw))
		require.FileExists(t, raw, "failure and completion both retain raw")
	})
}
