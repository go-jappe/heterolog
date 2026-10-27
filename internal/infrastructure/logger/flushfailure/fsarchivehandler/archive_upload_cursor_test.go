package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFailureArchiveCursorBlocksOnlyDestinationWithOlderFailure(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		// 失敗した宛先の受理済み再試行は、再試行間隔を仮想時間で進めて完了させます。
		t.Run(string(category), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				remote := newDestinationFailureArchiveUploader("healthy", "retrying")
				remote.failNext("retrying", 2)
				pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
					cfg.setArchiveUploader(category, remote)
				})
				handler := pipeline.handler
				pattern, err := handler.archiveFileNamePattern(category)
				require.NoError(t, err)
				store := handler.directoryForFailureCategory(category).uploadCheckpoints
				first := filepath.Join(pattern.Dir, "test-app_20260922_2.jsonl.gz")
				second := filepath.Join(pattern.Dir, "test-app_20260922_10.jsonl.gz")
				for _, path := range []string{first, second} {
					require.NoError(t, os.WriteFile(path, []byte("compressed generation"), 0o600))
				}
				ctx := context.Background()
				require.True(t, handler.retryRetainedArchiveUploads(ctx, category, pattern, archivePathProtection{paths: []string{second, first}}))
				require.Zero(t, remote.attemptCount(second, "retrying"))
				require.FileExists(t, first)
				require.FileExists(t, second)
				require.Equal(t, 1, remote.attemptCount(first, "healthy"))
				require.Equal(t, 1, remote.attemptCount(second, "healthy"))
				drainArchiveRecoveryForTest(t, handler)
				require.False(t, handler.retryRetainedArchiveUploads(ctx, category, pattern, archivePathProtection{paths: []string{second, first}}))
				require.Equal(t, 3, remote.attemptCount(first, "retrying"))
				require.Equal(t, 1, remote.attemptCount(second, "retrying"))
				require.NoFileExists(t, first)
				require.NoFileExists(t, second)
				checkpoint, err := store.Load(ctx, failureArchiveCompressedLogCheckpointKey(t, handler, category, first))
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"healthy", "retrying"}, checkpoint.CompletedDestinationIDs)
				// 観測や削除を経ても、completion cursorは失われません。
				require.NoError(t, triggerFailureArchiveCleanupForFailureCategory(handler, category))
				require.NoError(t, os.WriteFile(first, []byte("reappeared generation"), 0o600))
				outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{first}, category, pattern)
				require.False(t, outcome.retryRequired)
				require.NoFileExists(t, first)
				require.Equal(t, 1, remote.attemptCount(first, "healthy"))
				require.Equal(t, 3, remote.attemptCount(first, "retrying"))
			})
		})
	}
}

func TestFailureArchiveCursorWaitsForOlderRawCompression(t *testing.T) {
	category := failurearchive.FailureCategoryRetryable
	remote := newDestinationFailureArchiveUploader("destination")
	pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(category, remote)
	})
	handler := pipeline.handler
	pattern := handler.Fsstore.FileNameRetryable
	raw := filepath.Join(pattern.Dir, "test-app_20260922_1.jsonl")
	first := raw + ".gz"
	second := filepath.Join(pattern.Dir, "test-app_20260922_2.jsonl.gz")
	require.NoError(t, os.WriteFile(raw, []byte("uncompressed generation"), 0o600))
	require.NoError(t, os.WriteFile(second, []byte("compressed generation"), 0o600))
	ctx := context.Background()
	outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{second}, category, pattern)
	require.True(t, outcome.retryRequired)
	require.Zero(t, remote.totalAttemptCount(second))
	require.NoError(t, os.Rename(raw, first))
	require.False(t, handler.retryRetainedArchiveUploads(ctx, category, pattern, archivePathProtection{paths: []string{second, first}}))
	drainArchiveRecoveryForTest(t, handler)
	require.Equal(t, 1, remote.totalAttemptCount(first))
	require.Equal(t, 1, remote.totalAttemptCount(second))
}
