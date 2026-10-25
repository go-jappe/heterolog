package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// recordFailureArchiveGenerationForTestは、停止中の手動復旧が最新fileまで送信・削除した状態を、
// 登録済み宛先の完了位置と空のdirectoryで再現します。
func recordFailureArchiveGenerationForTest(t *testing.T, cfg testArchiveConfig, category failurearchive.FailureCategory, pipeline *FailureArchivePipeline, generation string) {
	t.Helper()
	ctx := context.Background()
	pattern, err := pipeline.handler.archiveFileNamePattern(category)
	require.NoError(t, err)
	sourceID := checkpointstore.ArchiveCheckpointSourceID(cfg.archiveCheckpoint.SourcePrefix, string(category), pattern)
	store, err := checkpointstore.NewFilesystemStore(cfg.archiveCheckpoint.Directory, sourceID)
	require.NoError(t, err)
	cursors, err := store.DestinationCursors(ctx)
	require.NoError(t, err)
	require.Len(t, cursors, 1)
	_, err = store.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: generation, Compressed: true}, []objectstorage.DestinationID{cursors[0].DestinationID})
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

// categoryUploadsは、bucketのfailure category別prefixへ送信したobjectのうち、file名がnameのものを数えます。
func (s *failureArchiveObjectStorageForTest) categoryUploads(bucket string, category failurearchive.FailureCategory, name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, objectPath := range s.paths {
		if objectPath == "/"+bucket+"/backend-records/"+string(category)+"/"+name {
			count++
		}
	}
	return count
}

func TestFailureArchiveRestartContinuesAfterRecordedGenerations(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 3, 14, 30, 0, 0, time.UTC)
	category := failurearchive.FailureCategoryRetryable
	storage := newFailureArchiveObjectStorageForTest(t)
	cfg := restartableFailureArchiveConfigForTest(t, "failure_archive_generation_floor")
	cfg.objectStorageUpload = storage.target("archive")
	cfg.enableRotate = true

	first := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	directory := first.handler.Fsstore.FileNameRetryable.Dir
	require.Equal(t, filepath.Join(directory, "test-app_20260903_1.jsonl"), first.handler.Fsstore.ActiveFilePathRetryable)
	shutdownFailureArchivePipelineForTest(t, first)
	recordFailureArchiveGenerationForTest(t, cfg, category, first, "20260903-1")
	require.NoError(t, os.Remove(filepath.Join(directory, "test-app_20260903_1.jsonl")))

	// 同じ期間の再起動でも、送信済みとして記録された世代番号へ新しいrecordを書きません。
	second := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	require.Equal(t, filepath.Join(directory, "test-app_20260903_2.jsonl"), second.handler.Fsstore.ActiveFilePathRetryable)
	require.NoError(t, second.HandleFailure(ctx, loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{archiveFailedRecord("after-recovery", now, 429, "retryable")},
	}))
	// 日付rotationで書き込み済みの世代を送信し、送信後に削除します。
	second.handler.Clock = archiveFixedClock{now: now.AddDate(0, 0, 1)}
	require.NoError(t, second.HandleFailure(ctx, loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{archiveFailedRecord("next-day", now.AddDate(0, 0, 1), 429, "retryable")},
	}))
	waitForFailureArchiveTasksForTest(t, second.handler)
	require.Equal(t, 1, storage.categoryUploads("archive", category, "test-app_20260903_2.jsonl.gz"))
	require.Zero(t, storage.categoryUploads("archive", category, "test-app_20260903_1.jsonl.gz"),
		"the recorded generation's object must not be overwritten")
	require.NoFileExists(t, filepath.Join(directory, "test-app_20260903_2.jsonl"))
	require.NoFileExists(t, filepath.Join(directory, "test-app_20260903_2.jsonl.gz"))
}

func TestFailureArchiveRestartWithClockBehindRecordedPeriodContinuesThatPeriod(t *testing.T) {
	now := time.Date(2026, 9, 3, 14, 30, 0, 0, time.UTC)
	category := failurearchive.FailureCategoryPermanent
	storage := newFailureArchiveObjectStorageForTest(t)
	cfg := restartableFailureArchiveConfigForTest(t, "failure_archive_clock_floor")
	cfg.objectStorageUpload = storage.target("archive")

	// 前回の起動中は時計が一日進んでいて、その期間の3世代目まで送信済みと記録された状態です。
	first := newRestartableFailureArchivePipelineForTest(t, cfg, now.AddDate(0, 0, 1))
	directory := first.handler.Fsstore.FileNamePermanent.Dir
	shutdownFailureArchivePipelineForTest(t, first)
	recordFailureArchiveGenerationForTest(t, cfg, category, first, "20260904-3")
	require.NoError(t, os.Remove(filepath.Join(directory, "test-app_20260904_1.jsonl")))

	// 時計を戻して再起動しても、記録済みの期間を続け、その世代より後から書き込みます。
	second := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	require.Equal(t, filepath.Join(directory, "test-app_20260904_4.jsonl"), second.handler.Fsstore.ActiveFilePathPermanent)
	require.NoFileExists(t, filepath.Join(directory, "test-app_20260903_1.jsonl"))
}
