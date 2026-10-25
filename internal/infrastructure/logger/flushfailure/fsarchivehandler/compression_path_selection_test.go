package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFindExistingCompressedArchivePathDoesNotReturnPlannedName(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260914_1.jsonl")
	require.NoError(t, os.WriteFile(source, []byte("raw"), 0o600))
	handler := &FsArchiveHandler{}
	path, found, err := handler.findExistingCompressedArchivePath(source)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, path)
	require.FileExists(t, source)
	require.NoFileExists(t, source+".gz")
}

func TestFailureArchiveMissingCompressedFileDoesNotScheduleUploadDuringPendingRecovery(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	category := failurearchive.FailureCategoryRetryable
	upload := &sequencedFailureArchiveUploader{}
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
		cfg.enableFileCompress = true
		cfg.compressFormat = codec.Format()
		cfg.compressionCodec = codec
		cfg.setArchiveUploader(category, upload)
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(category)
	// 構築時のtarget-only予約を消費し、今回はdirectory全体の観測を通します。
	require.True(t, state.uploadRetry.claimForAttempt(now, testUsageCacheTTL))

	raw := writeFailureArchiveRecoveryRaw(t, pipeline, "20260913", 1)
	pending := logpattern.CompressionSourceDeletePendingPath(raw)
	require.NoError(t, os.Rename(raw, pending))
	require.NoError(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), category))
	require.NoFileExists(t, raw)
	require.NoFileExists(t, pending)
	require.NoFileExists(t, raw+".gz")
	require.Zero(t, codec.compressCalls.Load())
	require.Empty(t, upload.attemptCounts())
	scheduled, _ := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
	require.False(t, scheduled, "missing compressed file must not create an upload reservation")
}
