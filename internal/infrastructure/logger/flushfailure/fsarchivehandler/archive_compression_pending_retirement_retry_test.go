package fsarchivehandler

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestInlineFailureArchivePendingRetirementRecoverySchedulesNextTargetOnlyUpload(
	t *testing.T,
) {
	now := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	failureCategory := failurearchive.FailureCategoryRetryable
	upload := &sequencedFailureArchiveUploader{}
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
		cfg.enableFileCompress = true
		cfg.compressFormat = codec.Format()
		cfg.compressionCodec = codec
		cfg.setArchiveUploader(failureCategory, upload)
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)

	// 構築時に予約されるprocess再起動bootstrapを消費し、pending retirementの
	// 回復だけが新しいretryを予約することを検証します。
	require.True(t, state.uploadRetry.claimForAttempt(now, testUsageCacheTTL))

	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	writeFailureArchiveRecoveryGzip(t, compressedPath, "durably compressed archive")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(rawPath)
	require.NoError(t, os.Rename(rawPath, pendingPath))

	var (
		scanMu    sync.Mutex
		scanModes []logpattern.GlobMode
	)
	handler.globArchiveFiles = func(
		pattern logpattern.FileNamePattern,
		mode logpattern.GlobMode,
	) ([]string, error) {
		scanMu.Lock()
		scanModes = append(scanModes, mode)
		scanMu.Unlock()
		return logpattern.GlobLifecycleFileNames(pattern, mode)
	}

	require.NoError(t, handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failureCategory,
	))
	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, pendingPath)
	require.FileExists(t, compressedPath)
	require.Zero(t, upload.attemptCounts()[compressedPath])

	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
	require.True(t, scheduled)
	require.Equal(t, now.Add(testUsageCacheTTL), retryAt)

	handler.Clock = archiveFixedClock{now: now.Add(testUsageCacheTTL)}
	require.NoError(t, handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failureCategory,
	))
	require.Equal(t, 1, upload.attemptCounts()[compressedPath])
	require.NoFileExists(t, compressedPath)
	require.Zero(t, codec.compressCalls.Load())
	scheduled, _ = archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
	require.False(t, scheduled)

	scanMu.Lock()
	gotModes := append([]logpattern.GlobMode(nil), scanModes...)
	scanMu.Unlock()
	require.Equal(t, []logpattern.GlobMode{
		logpattern.GlobAllFiles,
		logpattern.GlobByTarget,
	}, gotModes)
}
