package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

// 構築時に渡した容量上限を、inline purgeがそのまま判定に使うことを確認します。
// 設定は構築後に変更しないため、最小値とは異なる有効な上限を構築時に渡して区別します。
func TestFailureArchiveInlinePurgeUsesConfiguredMaxDirSize(t *testing.T) {
	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	const fileSize int64 = 4 * 1024 * 1024
	configuredMaxDirSize := uint64(3 * fileSize)
	require.Greater(t, configuredMaxDirSize, infrafsstore.MinAllowedMaxDirSize)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.maxFileAge = 30
		cfg.maxFileBackups = 10
		cfg.maxDirSize = configuredMaxDirSize
	})
	handler := pipeline.handler
	category := failurearchive.FailureCategoryRetryable
	directory := handler.Fsstore.FileNameRetryable.Dir
	activePath, err := pipeline.ActiveArchivePath(category)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(activePath, fileSize))
	oldestPath := filepath.Join(directory, "test-app_20260823_1.jsonl")
	olderPath := filepath.Join(directory, "test-app_20260824_1.jsonl")
	writeArchiveSizedFile(t, oldestPath, fileSize, now.Add(-2*time.Hour))
	writeArchiveSizedFile(t, olderPath, fileSize, now.Add(-time.Hour))

	// 合計は構築時の上限ちょうどです。最小値の上限で判定した場合は古い世代を削除します。
	require.NoError(t, handler.triggerArchiveCleanup(context.Background(), category))
	require.FileExists(t, oldestPath)
	require.FileExists(t, olderPath)

	// 上限を超えた分だけ、古い世代から削除して構築時の上限まで戻します。
	newerPath := filepath.Join(directory, "test-app_20260824_2.jsonl")
	writeArchiveSizedFile(t, newerPath, fileSize, now.Add(-30*time.Minute))
	handler.invalidateArchiveDirectoryEstimate(category)
	require.NoError(t, handler.triggerArchiveCleanup(context.Background(), category))
	require.NoFileExists(t, oldestPath)
	var remainingBytes uint64
	for _, path := range []string{activePath, olderPath, newerPath} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		remainingBytes += uint64(info.Size())
	}
	require.Equal(t, configuredMaxDirSize, remainingBytes)
}
