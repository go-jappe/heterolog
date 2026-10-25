package fsarchivehandler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestFailureArchiveInlineCheckpointRetriesOnlyMissingDestinationsAfterRestart(
	t *testing.T,
) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		failureCategory := failureCategory
		t.Run(string(failureCategory), func(t *testing.T) {
			now := time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)
			const (
				primary   objectstorage.DestinationID = "destination-primary"
				secondary objectstorage.DestinationID = "destination-secondary"
			)
			uploadClient := newDestinationFailureArchiveUploader(primary, secondary)
			uploadClient.failNext(secondary, 1)
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(failureCategory, uploadClient)
			})
			handler := pipeline.handler

			path := filepath.Join(
				failureArchiveDirectoryForFailureCategory(handler, failureCategory),
				"test-app_20260902_1.jsonl.gz",
			)
			require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))

			require.NoError(t, triggerFailureArchiveCleanupForFailureCategory(handler, failureCategory))
			require.FileExists(t, path)
			require.Equal(t, 1, uploadClient.attemptCount(path, primary))
			require.Equal(t, 1, uploadClient.attemptCount(path, secondary))

			// 同じcheckpoint設定で構築し直し、process再起動を再現します。primaryの
			// 永続済み成功をcheckpointから読み、secondaryだけを送信対象にします。
			// 設定をzstdへ変更しても、未送信の実ファイルは既存のgzipです。
			restartedUploader := &checkpointDestinationRecordingFailureArchiveUploader{
				destinationIDs: []objectstorage.DestinationID{primary, secondary},
			}
			restarted := restartTestArchiveHandler(t, pipeline, now.Add(testUsageCacheTTL), func(cfg *testArchiveConfig) {
				cfg.compressFormat = pkgfsstore.CompressZstd
				cfg.setArchiveUploader(failureCategory, restartedUploader)
			})

			require.NoError(t, triggerFailureArchiveCleanupForFailureCategory(restarted.handler, failureCategory))
			require.NoFileExists(t, path)
			require.Equal(t, 1, uploadClient.attemptCount(path, primary))
			require.Equal(t, 1, uploadClient.attemptCount(path, secondary))
			require.Equal(t, []objectstorage.DestinationID{secondary}, restartedUploader.requested)
			require.Equal(t, path, restartedUploader.filePath)
		})
	}
}

func TestFailureArchiveInlineCheckpointSelectsFirstCompressedLogGenerationAfterRestart(
	t *testing.T,
) {
	now := time.Date(2026, 9, 3, 10, 30, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	const destination objectstorage.DestinationID = "destination-primary"
	uploadClient := newDestinationFailureArchiveUploader(destination)
	uploadClient.failNext(destination, 1)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, uploadClient)
	})
	handler := pipeline.handler

	directory := handler.Fsstore.FileNameRetryable.Dir
	gzipPath := filepath.Join(directory, "test-app_20260902_1.jsonl.gz")
	zstdPath := filepath.Join(directory, "test-app_20260902_1.jsonl.zst")
	require.NoError(t, os.WriteFile(gzipPath, []byte("gzip compressed log file"), 0o600))
	require.NoError(t, os.WriteFile(zstdPath, []byte("zstd compressed log file"), 0o600))

	require.NoError(t, handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable))
	// 先頭fileだけを選択し、そのupload失敗は別のfileへ切り替えず再試行します。
	require.FileExists(t, gzipPath)
	require.FileExists(t, zstdPath)
	require.Equal(t, 1, uploadClient.attemptCount(gzipPath, destination))
	require.Zero(t, uploadClient.attemptCount(zstdPath, destination))

	restarted := restartTestArchiveHandler(t, pipeline, now.Add(testUsageCacheTTL), func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, uploadClient)
	})
	require.NoError(t, restarted.handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable))
	require.NoFileExists(t, gzipPath)
	require.FileExists(t, zstdPath)
	require.Equal(t, 2, uploadClient.attemptCount(gzipPath, destination))
	require.Zero(t, uploadClient.attemptCount(zstdPath, destination))
}

func TestFailureArchiveInlineCheckpointDefersWhenFirstCompressedFileIsProtected(
	t *testing.T,
) {
	now := time.Date(2026, 9, 3, 10, 45, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryPermanent
	const destination objectstorage.DestinationID = "destination"
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader(destination))
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)
	store := state.uploadCheckpoints

	directory := handler.Fsstore.FileNamePermanent.Dir
	gzipPath := filepath.Join(directory, "test-app_20260902_1.jsonl.gz")
	zstdPath := filepath.Join(directory, "test-app_20260902_1.jsonl.zst")
	for _, path := range []string{gzipPath, zstdPath} {
		require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))
	}
	_, err := store.Load(context.Background(), failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, gzipPath))
	require.NoError(t, err)

	otherOwner := appendlog.NewDirectoryGuardToken()
	state.guard.ProtectPaths(otherOwner, gzipPath)
	defer state.guard.UnprotectPaths(otherOwner, gzipPath)
	retained, err := handler.retainArchivePathsForUpload(
		context.Background(),
		failureCategory,
		handler.Fsstore.FileNamePermanent,
		infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{gzipPath, zstdPath}), handler.Fsstore.FileNamePermanent.Extension))
	require.NoError(t, err)
	require.Empty(t, retained.paths)
	state.guard.WithProtectedPaths(func(protected map[string]struct{}) {
		_, siblingRetained := protected[pathidentity.AbsoluteOrCleanPath(zstdPath)]
		require.False(t, siblingRetained)
	})
}

func TestFailureArchiveCheckpointDoesNotApplySuccessToAnotherGeneration(t *testing.T) {
	now := time.Date(2026, 9, 14, 19, 0, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	const active objectstorage.DestinationID = "active"
	remote := newDestinationFailureArchiveUploader(active)
	remote.failNext(active, 1)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, remote)
	})
	handler := pipeline.handler
	store := handler.directoryForFailureCategory(failureCategory).uploadCheckpoints
	gzipPath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260913_1.jsonl.gz")
	zstdPath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260913_2.jsonl.zst")
	for _, path := range []string{gzipPath, zstdPath} {
		require.NoError(t, os.WriteFile(path, []byte("published file"), 0o600))
	}
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, gzipPath)
	_, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	_, err = store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{active})
	require.NoError(t, err)

	// 一つ前の世代の成功記録を、次の世代へ流用してはいけません。
	outcome := handler.uploadCheckpointedArchiveGroup(context.Background(), []string{zstdPath},
		failureCategory, handler.Fsstore.FileNameRetryable)
	require.True(t, outcome.retryRequired)
	require.False(t, outcome.completion.Attempted)
	require.Zero(t, remote.attemptCount(gzipPath, active))
	require.Equal(t, 1, remote.attemptCount(zstdPath, active))
	require.FileExists(t, gzipPath)
	require.FileExists(t, zstdPath)
	checkpoint, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{active}, checkpoint.CompletedDestinationIDs)
	nextCheckpoint, err := store.Load(context.Background(), failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, zstdPath))
	require.NoError(t, err)
	require.Empty(t, nextCheckpoint.CompletedDestinationIDs)
}

func TestFailureArchiveFullScanProtectsUnselectedCompressedFilesBeforeUpload(t *testing.T) {
	for _, phase := range []string{"age", "generation", "directory_size"} {
		t.Run(phase, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 19, 30, 0, 0, time.UTC)
			failureCategory := failurearchive.FailureCategoryRetryable
			const destination objectstorage.DestinationID = "destination"
			remote := newDestinationFailureArchiveUploader(destination)
			// 構築時に検証される範囲の設定で、対象の段階だけが古い世代を削除候補にします。
			var fileSize int64 = 1
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.setArchiveUploader(failureCategory, remote)
				cfg.maxFileAge, cfg.maxFileBackups, cfg.maxDirSize = 30, 100, 1<<30
				switch phase {
				case "age":
					cfg.maxFileAge = 1
				case "generation":
					// 書き込み中でない三世代は上限を超えるため、最も古い世代が削除候補です。
					cfg.maxFileBackups = infrafsstore.MinAllowedMaxFileBackups
				case "directory_size":
					cfg.maxDirSize = infrafsstore.MinAllowedMaxDirSize
					fileSize = int64(infrafsstore.MinAllowedMaxDirSize/4) + 1
				}
			})
			handler := pipeline.handler
			state := handler.directoryForFailureCategory(failureCategory)
			var paths []string
			// 送信待ちとして保護しなければ、各段階が少なくとも最も古い世代を削除する構成です。
			for _, name := range []string{"test-app_20260901_1.jsonl", "test-app_20260902_1.jsonl", "test-app_20260903_1.jsonl"} {
				for _, extension := range []string{pkgfsstore.ExtensionCompressGzip, pkgfsstore.ExtensionCompressZstd} {
					path := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, name+extension)
					writeArchiveSizedFile(t, path, fileSize, now.AddDate(0, 0, -2))
					paths = append(paths, path)
				}
			}
			// 構築時のtarget-only予約を消費し、今回はupload前のfullscan/purgeを通します。
			require.True(t, state.uploadRetry.claimForAttempt(now, testUsageCacheTTL))
			handler.invalidateArchiveDirectoryEstimate(failureCategory)
			require.NoError(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory))
			for _, path := range paths {
				require.FileExists(t, path, "unselected peers must survive the same scan's purge")
				require.Zero(t, remote.attemptCount(path, destination))
				assertArchivePathProtected(t, state.guard, path, false)
			}
		})
	}
}

func TestFailureArchiveInlineCheckpointUsesCurrentDestinationsWithoutRegistration(
	t *testing.T,
) {
	now := time.Date(2026, 9, 3, 11, 0, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	const (
		newlyAdded objectstorage.DestinationID = "destination-new"
	)
	uploadClient := newDestinationFailureArchiveUploader(newlyAdded)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, uploadClient)
	})
	handler := pipeline.handler
	store := handler.directoryForFailureCategory(failureCategory).uploadCheckpoints

	path := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260902_1.jsonl.gz",
	)
	require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, path)
	checkpoint, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	require.Empty(t, checkpoint.CompletedDestinationIDs)

	protection, err := handler.retainArchivePathsForUpload(
		context.Background(),
		failureCategory,
		handler.Fsstore.FileNameRetryable,
		infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{path}), handler.Fsstore.FileNameRetryable.Extension))
	require.NoError(t, err)
	require.Equal(t, []string{path}, protection.paths)

	checkpoint, err = store.Load(context.Background(), key)
	require.NoError(t, err)
	require.Empty(t, checkpoint.CompletedDestinationIDs)

	require.False(t, handler.retryRetainedArchiveUploads(
		context.Background(),
		failureCategory,
		handler.Fsstore.FileNameRetryable,
		protection,
	))
	require.NoFileExists(t, path)
	require.Equal(t, 1, uploadClient.attemptCount(path, newlyAdded))
}

func TestFailureArchiveInlineCheckpointDoesNotTreatNoDestinationsAsUploadSuccess(
	t *testing.T,
) {
	now := time.Date(2026, 9, 3, 11, 15, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	// 有効な宛先を持たない宛先付きuploaderで構築します。
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader())
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)
	store := state.uploadCheckpoints

	path := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260902_2.jsonl.gz",
	)
	require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, path)
	_, err := store.Load(context.Background(), key)
	require.NoError(t, err)

	protection, err := handler.retainArchivePathsForUpload(
		context.Background(),
		failureCategory,
		handler.Fsstore.FileNameRetryable,
		infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{path}), handler.Fsstore.FileNameRetryable.Extension))
	require.NoError(t, err)
	require.Empty(t, protection.paths)
	assertArchivePathProtected(t, state.guard, path, false)
	require.False(t, handler.retryRetainedArchiveUploads(
		context.Background(),
		failureCategory,
		handler.Fsstore.FileNameRetryable,
		protection,
	))
	require.FileExists(t, path)
	assertArchivePathProtected(t, state.guard, path, false)
	checkpoint, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	require.False(t, checkpoint.Complete(nil))
}

func TestFailureArchiveInlineCheckpointDoesNotDeleteForPastDestinationsWhenCurrentlyInactive(
	t *testing.T,
) {
	now := time.Date(2026, 9, 3, 11, 30, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	// 現在の有効な宛先を持たない宛先付きuploaderで構築します。
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader())
	})
	handler := pipeline.handler
	store := handler.directoryForFailureCategory(failureCategory).uploadCheckpoints

	path := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260902_1.jsonl.gz",
	)
	require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, path)
	const completedDestination objectstorage.DestinationID = "destination-completed"
	_, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	checkpoint, err := store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{completedDestination})
	require.NoError(t, err)
	require.True(t, checkpoint.Complete([]objectstorage.DestinationID{completedDestination}))

	// 過去の宛先に成功済みでも、現在の有効宛先が空ならupload成功削除しません。
	// 構築時に予約されるtarget-only再試行で確認します。
	require.NoError(t, handler.triggerArchiveCleanup(context.Background(), failurearchive.FailureCategoryRetryable))
	require.FileExists(t, path)
}

// 再起動前に登録した宛先も、設定から外した後は送信先にも削除の条件にも含めません。
func TestFailureArchiveInlineCheckpointUsesOnlyCurrentlyActiveDestinations(t *testing.T) {
	now := time.Date(2026, 9, 3, 11, 45, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryPermanent
	const (
		active   objectstorage.DestinationID = "destination-active"
		inactive objectstorage.DestinationID = "destination-inactive"
	)
	// 両方の宛先を登録して構築した後に圧縮fileを作り、どちらの宛先の対象範囲にも含めます。
	previousClient := newDestinationFailureArchiveUploader(active, inactive)
	previous := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, previousClient)
	})
	path := filepath.Join(
		previous.handler.Fsstore.FileNamePermanent.Dir,
		"test-app_20260902_1.jsonl.gz",
	)
	require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))

	// inactiveを設定から外して再起動します。
	uploadClient := newDestinationFailureArchiveUploader(active)
	pipeline := restartTestArchiveHandler(t, previous, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, uploadClient)
	})

	require.NoError(t, pipeline.handler.triggerArchiveCleanup(context.Background(), failureCategory))
	// checkpointには外した宛先の登録が残りますが、その宛先へは送らず、
	// その完了も待たずに、現在の宛先への送信の完了で削除します。
	require.NoFileExists(t, path)
	require.Equal(t, 1, uploadClient.attemptCount(path, active))
	require.Zero(t, uploadClient.attemptCount(path, inactive))
	require.Zero(t, previousClient.attemptCount(path, active))
	require.Zero(t, previousClient.attemptCount(path, inactive))
}

func TestFailureArchiveInlineCheckpointKeepsCursorForMissingCompressedLog(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryPermanent
	const oldDestination objectstorage.DestinationID = "destination-old"
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader(oldDestination))
	})
	handler := pipeline.handler
	store := handler.directoryForFailureCategory(failureCategory).uploadCheckpoints

	missingPath := filepath.Join(
		handler.Fsstore.FileNamePermanent.Dir,
		"test-app_20260902_1.jsonl.gz",
	)
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, missingPath)
	_, err := store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{oldDestination})
	require.NoError(t, err)

	_, err = handler.retainArchivePathsForUpload(
		context.Background(),
		failureCategory,
		handler.Fsstore.FileNamePermanent,
		infrafsstore.CompressionSourceGroups{})
	require.NoError(t, err)

	// local fileがなくても完了cursorを保持します。
	checkpoint, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{oldDestination}, checkpoint.CompletedDestinationIDs)
}

func TestFailureArchiveRotationAfterRestartUsesCurrentDestinationScope(
	t *testing.T,
) {
	ctx := context.Background()
	failureCategory := failurearchive.FailureCategoryRetryable
	storage := newFailureArchiveObjectStorageForTest(t)
	cfg := restartableFailureArchiveConfigForTest(t, "failure_archive_inline_rotation_snapshot")
	cfg.objectStorageUpload = storage.target("rotation")
	first := newRestartableFailureArchivePipelineForTest(t, cfg, time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC))
	rotatedPath := filepath.Join(first.handler.Fsstore.FileNameRetryable.Dir, "test-app_20260902_1.jsonl")
	require.NoError(t, os.WriteFile(rotatedPath, []byte("rotated before restart"), 0o600))
	previousActivePath := first.handler.Fsstore.ActiveFilePathRetryable
	shutdownFailureArchivePipelineForTest(t, first)
	require.NoError(t, os.WriteFile(previousActivePath, []byte("written before restart"), 0o600))

	// 宛先は構築時に固定されるため、停止中に変更して翌日に再起動します。送信はupload時点の
	// 有効宛先だけへ行い、rotation時の宛先へは送りません。新しい宛先の登録時には、
	// 前日の期間の元ファイルはwriterが戻って追記しないため、停止前に書き込み中だった
	// 最終世代も登録前の世代として扱い、送信も削除もしません。
	cfg.objectStorageUpload = storage.target("current")
	restarted := newRestartableFailureArchivePipelineForTest(t, cfg, time.Date(2026, 9, 4, 13, 0, 0, 0, time.UTC))
	require.NotEqual(t, previousActivePath, restarted.handler.Fsstore.ActiveFilePathRetryable)
	for _, path := range []string{rotatedPath, previousActivePath} {
		restarted.handler.dispatchArchiveRotatedFile(ctx, restarted.handler.Fsstore.ActiveFilePathRetryable, path, failureCategory)
		waitForFailureArchiveTasksForTest(t, restarted.handler)
		require.NoFileExists(t, path)
	}
	for _, path := range []string{rotatedPath, previousActivePath} {
		require.Zero(t, storage.uploads("rotation", path+".gz"))
		require.Zero(t, storage.uploads("current", path+".gz"))
		require.FileExists(t, path+".gz", "通常の保持設定で削除されるまで残します")
	}
}

func TestFailureArchiveRestartInSamePeriodKeepsActiveFileInNewDestinationScope(t *testing.T) {
	ctx := context.Background()
	storage := newFailureArchiveObjectStorageForTest(t)
	cfg := restartableFailureArchiveConfigForTest(t, "failure_archive_inline_same_period_restart")
	cfg.objectStorageUpload = storage.target("rotation")
	cfg.enableRotate = true
	now := time.Date(2026, 9, 3, 13, 0, 0, 0, time.UTC)
	first := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	activePath := first.handler.Fsstore.ActiveFilePathRetryable
	require.NoError(t, first.HandleFailure(ctx, loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{archiveFailedRecord("before-restart", now, 429, "retryable")},
	}))
	shutdownFailureArchivePipelineForTest(t, first)

	// 同じ期間に新しい宛先で再起動します。停止前の最終世代はまだ追記され得るため、
	// 新しい宛先の対象に残し、日付rotation後に新しい宛先へ送信してから削除します。
	cfg.objectStorageUpload = storage.target("current")
	restartedAt := now.Add(2 * time.Hour)
	restarted := newRestartableFailureArchivePipelineForTest(t, cfg, restartedAt)
	require.Equal(t, activePath, restarted.handler.Fsstore.ActiveFilePathRetryable)
	require.NoError(t, restarted.HandleFailure(ctx, loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{archiveFailedRecord("after-restart", restartedAt, 429, "retryable")},
	}))
	nextDay := restartedAt.AddDate(0, 0, 1)
	restarted.handler.Clock = archiveFixedClock{now: nextDay}
	require.NoError(t, restarted.HandleFailure(ctx, loggerfailure.SendResult{
		RetryableRecords: []loggerfailure.FailedRecord{archiveFailedRecord("next-day", nextDay, 429, "retryable")},
	}))
	waitForFailureArchiveTasksForTest(t, restarted.handler)

	category := failurearchive.FailureCategoryRetryable
	name := filepath.Base(activePath) + ".gz"
	require.Zero(t, storage.categoryUploads("rotation", category, name))
	require.Equal(t, 1, storage.categoryUploads("current", category, name))
	require.NoFileExists(t, activePath)
	require.NoFileExists(t, activePath+".gz")
}

func TestFailureArchiveRawOnlyRestartDoesNotSendPreRegistrationGeneration(
	t *testing.T,
) {
	ctx := context.Background()
	failureCategory := failurearchive.FailureCategoryPermanent
	storage := newFailureArchiveObjectStorageForTest(t)
	cfg := restartableFailureArchiveConfigForTest(t, "failure_archive_inline_raw_restart")
	cfg.objectStorageUpload = storage.target("rotation")
	now := time.Date(2026, 9, 3, 14, 0, 0, 0, time.UTC)
	first := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	pattern := first.handler.Fsstore.FileNamePermanent
	rawPath := filepath.Join(pattern.Dir, "test-app_20260902_1.jsonl")
	require.NoError(t, os.WriteFile(rawPath, []byte("rotated failure"), 0o600))
	shutdownFailureArchivePipelineForTest(t, first)

	// 圧縮も送信もしないまま停止したrawは、再起動時に登録した宛先の対象外です。
	// 再起動後に圧縮しても、旧宛先と新しい宛先のどちらにも送信せず、upload完了を
	// 理由に削除もしません。
	cfg.objectStorageUpload = storage.target("restarted")
	restarted := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	handler := restarted.handler
	handler.dispatchArchiveRotatedFile(ctx, handler.Fsstore.ActiveFilePathPermanent, rawPath, failureCategory)
	waitForFailureArchiveTasksForTest(t, handler)
	compressedPath := rawPath + ".gz"
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.Zero(t, storage.total())
	state := handler.directoryForFailureCategory(failureCategory)
	checkpoint, err := state.uploadCheckpoints.Load(ctx, failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, compressedPath))
	require.NoError(t, err)
	require.Len(t, checkpoint.CompletedDestinationIDs, 1)
	require.Equal(t, checkpoint.CompletedDestinationIDs, checkpoint.OutOfScopeDestinationIDs)
	// 送信待ちとしてretainせず、通常の保持期間・世代数・容量によるpurgeへ任せます。
	protection, err := handler.retainArchivePathsForUpload(ctx, failureCategory, pattern,
		infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{compressedPath}), pattern.Extension))
	require.NoError(t, err)
	require.Empty(t, protection.paths)
}

func TestFailureArchiveActivationKeepsArchiveCompressedWhileUploadWasDisabled(
	t *testing.T,
) {
	ctx := context.Background()
	now := time.Date(2026, 9, 3, 14, 30, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	storage := newFailureArchiveObjectStorageForTest(t)
	cfg := restartableFailureArchiveConfigForTest(t, "failure_archive_inline_empty_required")

	// upload無効の間に圧縮したfileは、成功記録を作らずlocalに保持します。
	disabled := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	directory := disabled.handler.Fsstore.FileNameRetryable.Dir
	rawPath := filepath.Join(directory, "test-app_20260902_1.jsonl")
	require.NoError(t, os.WriteFile(rawPath, []byte("rotated failure"), 0o600))
	disabled.handler.dispatchArchiveRotatedFile(ctx, disabled.handler.Fsstore.ActiveFilePathRetryable, rawPath, failureCategory)
	waitForFailureArchiveTasksForTest(t, disabled.handler)
	compressedPath := rawPath + ".gz"
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	shutdownFailureArchivePipelineForTest(t, disabled)

	// 再起動時のconstructorが追加した宛先を登録します。登録前から存在したfileは
	// その宛先へ遡って送信せず、upload完了を理由に削除もしません。
	cfg.objectStorageUpload = storage.target("archive")
	enabled := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	for range 2 {
		// 起動直後のtarget-only再試行と、続く通常の全体観測を通します。
		require.NoError(t, enabled.handler.triggerArchiveCleanup(ctx, failureCategory))
	}
	require.Zero(t, storage.total())
	require.FileExists(t, compressedPath)

	// 登録後にrotationした世代は、追加した宛先へ送信してから削除します。
	laterRawPath := filepath.Join(directory, "test-app_20260902_2.jsonl")
	require.NoError(t, os.WriteFile(laterRawPath, []byte("rotated after activation"), 0o600))
	enabled.handler.dispatchArchiveRotatedFile(ctx, enabled.handler.Fsstore.ActiveFilePathRetryable, laterRawPath, failureCategory)
	waitForFailureArchiveTasksForTest(t, enabled.handler)
	require.Equal(t, 1, storage.uploads("archive", laterRawPath+".gz"))
	require.NoFileExists(t, laterRawPath)
	require.NoFileExists(t, laterRawPath+".gz")
	require.Zero(t, storage.uploads("archive", compressedPath))
	require.FileExists(t, compressedPath)
}

func TestFailureArchiveRotationCompletesCompressionWhenCheckpointLoadFails(t *testing.T) {
	now := time.Date(2026, 9, 3, 15, 0, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	store := &transientArchiveCheckpointStore{
		loadFailures: 100,
	}
	t.Cleanup(func() {
		store.mu.Lock()
		store.loadFailures = 0
		store.mu.Unlock()
	})
	uploadClient := newDestinationFailureArchiveUploader("destination")
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.setArchiveUploader(failureCategory, uploadClient)
		cfg.embedArchiveCheckpointStore(failureCategory, &store.Store, store)
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)

	rawPath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260902_1.jsonl",
	)
	require.NoError(t, os.WriteFile(rawPath, []byte("rotated failure"), 0o600))
	protectedPaths := handler.protectRotatedFileBeforeTransition(failureCategory, rawPath)
	handler.finishRotatedFileTransition(
		context.Background(),
		handler.Fsstore.ActiveFilePathRetryable,
		rawPath,
		failureCategory,
		protectedPaths,
		nil,
	)

	waitForFailureArchiveTasksForTest(t, handler)
	require.NoFileExists(t, rawPath)
	require.FileExists(t, rawPath+".gz")
	require.Zero(t, uploadClient.totalAttemptCount(rawPath+".gz"))
	state.guard.WithProtectedPaths(func(paths map[string]struct{}) {
		for _, protectedPath := range protectedPaths.paths {
			_, protected := paths[pathidentity.AbsoluteOrCleanPath(protectedPath)]
			require.True(t, protected, "log file processing recovery retains the generation until completion or shutdown")
		}
	})
	handler.archiveTaskMu.Lock()
	require.Zero(t, handler.archiveTaskCount)
	handler.archiveTaskMu.Unlock()
}

func TestFailureArchiveInlineCheckpointCursorRemembersGenerationAcrossCompressedPeers(t *testing.T) {
	now := time.Date(2026, 9, 3, 15, 30, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	const destination objectstorage.DestinationID = "destination"
	remote := newDestinationFailureArchiveUploader(destination)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, remote)
	})
	handler := pipeline.handler
	store := handler.directoryForFailureCategory(failureCategory).uploadCheckpoints
	directory := handler.Fsstore.FileNameRetryable.Dir
	gzipPath := filepath.Join(directory, "test-app_20260902_1.jsonl.gz")
	zstdPath := filepath.Join(directory, "test-app_20260902_1.jsonl.zst")
	for _, path := range []string{gzipPath, zstdPath} {
		require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))
	}
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, gzipPath)
	_, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	checkpoint, err := store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{destination})
	require.NoError(t, err)
	require.True(t, checkpoint.Complete([]objectstorage.DestinationID{destination}))

	// upload完了を記録したfileだけを削除します。未選択の別fileは変更しません。
	require.False(t, handler.retryRetainedArchiveUploads(
		context.Background(), failureCategory, handler.Fsstore.FileNameRetryable,
		archivePathProtection{paths: []string{gzipPath, zstdPath}},
	))
	require.NoFileExists(t, gzipPath)
	require.FileExists(t, zstdPath)

	// 再起動後も、同世代の別圧縮形式を永続cursorから送信済みと判断します。
	restarted := restartTestArchiveHandler(t, pipeline, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, remote)
	})
	restartedHandler := restarted.handler
	checkpoint, err = restartedHandler.directoryForFailureCategory(failureCategory).uploadCheckpoints.Load(context.Background(), key)
	require.NoError(t, err)
	require.True(t, checkpoint.Complete([]objectstorage.DestinationID{destination}))
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)

	remote.failNext(destination, 1)
	require.False(t, restartedHandler.retryRetainedArchiveUploads(
		context.Background(), failureCategory, restartedHandler.Fsstore.FileNameRetryable,
		archivePathProtection{paths: []string{zstdPath}},
	))
	require.NoFileExists(t, zstdPath)
	require.Zero(t, remote.attemptCount(zstdPath, destination))
}

func TestFailureArchiveRecoveryDoesNotDependOnUploadCheckpoint(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name       string
		createPeer bool
	}{
		{name: "missing_peer"},
		{name: "existing_invalid_peer", createPeer: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			now := time.Date(2026, 9, 3, 16, 0, 0, 0, time.UTC)
			failureCategory := failurearchive.FailureCategoryRetryable
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.enableFileCompress = true
				cfg.compressFormat = pkgfsstore.CompressGzip
				cfg.wrapArchiveCheckpointStore(failureCategory, func(checkpointstore.Store) checkpointstore.Store {
					return failingCompressedArchiveCheckpointStore{err: errors.New("injected checkpoint prepare failure")}
				})
			})
			handler := pipeline.handler
			state := handler.directoryForFailureCategory(failureCategory)

			rawPath := filepath.Join(
				handler.Fsstore.FileNameRetryable.Dir,
				"test-app_20260902_1.jsonl",
			)
			rawContents := []byte("raw archive must remain unchanged")
			require.NoError(t, os.WriteFile(rawPath, rawContents, 0o600))
			compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
			peerContents := []byte("invalid compressed peer must remain unchanged")
			if testCase.createPeer {
				require.NoError(t, os.WriteFile(compressedPath, peerContents, 0o600))
			}

			observation, err := handler.observeArchiveDirectoryForPurge(failureCategory)
			require.NoError(t, err)
			var retained []string
			var mutated bool
			var failures []archiveCompressionRecoveryFailure
			require.NoError(t, state.guard.RunWithLifecycleLock(func() error {
				mutated = handler.recoverRemainingUncompressedArchives(
					context.Background(),
					failureCategory,
					observation,
					&retained,
					"",
					&failures,
				)
				return nil
			}))
			handler.unprotectArchivePaths(failureCategory, retained)

			require.True(t, mutated)
			if testCase.createPeer {
				// 内容破損したpeerを除去し、rawは次回の再圧縮に残します。
				require.FileExists(t, rawPath)
				require.NoFileExists(t, compressedPath)
				actualRaw, err := os.ReadFile(rawPath)
				require.NoError(t, err)
				require.Equal(t, rawContents, actualRaw)
			} else {
				require.NoFileExists(t, rawPath)
				require.FileExists(t, compressedPath)
				require.NoError(t, handler.Fsstore.VerifyExistingCompressedFileIntegrity(compressedPath))
			}

		})
	}
}

func TestFailureArchiveRecoveryUsesCurrentCodecAndKeepsPreRegistrationGeneration(
	t *testing.T,
) {
	ctx := context.Background()
	failureCategory := failurearchive.FailureCategoryPermanent
	storage := newFailureArchiveObjectStorageForTest(t)
	cfg := restartableFailureArchiveConfigForTest(t, "failure_archive_inline_codec_change")
	cfg.compressFormat = pkgfsstore.CompressGzip
	cfg.objectStorageUpload = storage.target("rotation")
	now := time.Date(2026, 9, 3, 16, 30, 0, 0, time.UTC)
	first := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	rawPath := filepath.Join(first.handler.Fsstore.FileNamePermanent.Dir, "test-app_20260902_1.jsonl")
	require.NoError(t, os.WriteFile(rawPath, []byte("rotated failure"), 0o600))
	shutdownFailureArchivePipelineForTest(t, first)

	// process再起動前にformat設定をgzipからzstdへ、upload宛先も変更した状態を再現します。
	cfg.compressFormat = pkgfsstore.CompressZstd
	cfg.objectStorageUpload = storage.target("current")
	restarted := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	handler := restarted.handler
	state := handler.directoryForFailureCategory(failureCategory)
	observation, err := handler.observeArchiveDirectoryForPurge(failureCategory)
	require.NoError(t, err)
	var retained []string
	var mutated bool
	var failures []archiveCompressionRecoveryFailure
	require.NoError(t, state.guard.RunWithLifecycleLock(func() error {
		mutated = handler.recoverRemainingUncompressedArchives(
			ctx,
			failureCategory,
			observation,
			&retained,
			"",
			&failures,
		)
		return nil
	}))
	handler.unprotectArchivePaths(failureCategory, retained)
	require.True(t, mutated)

	gzipPath := rawPath + pkgfsstore.ExtensionCompressGzip
	zstdPath := rawPath + pkgfsstore.ExtensionCompressZstd
	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, gzipPath)
	require.FileExists(t, zstdPath)

	// 再起動時に登録した宛先にとって登録前の世代です。現在のcodecで圧縮しても
	// どの宛先にも送信せず、upload完了を理由に削除しません。
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, zstdPath)
	checkpoint, err := state.uploadCheckpoints.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, key, checkpoint.Key)
	require.Len(t, checkpoint.CompletedDestinationIDs, 1)
	require.Equal(t, checkpoint.CompletedDestinationIDs, checkpoint.OutOfScopeDestinationIDs)
	require.False(t, handler.retryRetainedArchiveUploads(ctx, failureCategory,
		handler.Fsstore.FileNamePermanent, archivePathProtection{paths: []string{zstdPath}}))
	require.Zero(t, storage.total())
	require.FileExists(t, zstdPath)
}

func TestFailureArchiveRotationReusesExistingCompressedLogAfterCodecChange(t *testing.T) {
	now := time.Date(2026, 9, 3, 17, 0, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	const destination objectstorage.DestinationID = "destination"
	uploadClient := newDestinationFailureArchiveUploader(destination)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
		cfg.setArchiveUploader(failureCategory, uploadClient)
	})
	handler := pipeline.handler

	rawPath := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260902_1.jsonl",
	)
	gzipPath := rawPath + pkgfsstore.ExtensionCompressGzip
	zstdPath := rawPath + pkgfsstore.ExtensionCompressZstd
	require.NoError(t, os.WriteFile(rawPath, []byte("rotated failure"), 0o600))
	codec, err := infrafsstore.ResolveCompressionCodec(pkgfsstore.CompressZstd, nil)
	require.NoError(t, err)
	source, err := os.Open(rawPath)
	require.NoError(t, err)
	compressed, err := os.Create(zstdPath)
	require.NoError(t, err)
	require.NoError(t, codec.Compress(source, compressed))
	require.NoError(t, source.Close())
	require.NoError(t, compressed.Close())

	handler.dispatchArchiveRotatedFile(
		context.Background(),
		handler.Fsstore.ActiveFilePathRetryable,
		rawPath,
		failureCategory,
	)
	waitForFailureArchiveTasksForTest(t, handler)

	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, gzipPath)
	require.NoFileExists(t, zstdPath)
	require.Zero(t, uploadClient.attemptCount(gzipPath, destination))
	require.Equal(t, 1, uploadClient.attemptCount(zstdPath, destination))
}

func TestFailureArchiveFullScanDoesNotRecordCompletionBeforeCompression(t *testing.T) {
	now := time.Date(2026, 9, 3, 17, 30, 0, 0, time.UTC)
	const panicValue = "stop during compression"
	failureCategory := failurearchive.FailureCategoryPermanent
	const firstDestination objectstorage.DestinationID = "destination-at-scan"
	uploadClient := newDestinationFailureArchiveUploader(firstDestination)
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
		cfg.compressionCodec = panicFailureArchiveCompressionCodec{
			format:     pkgfsstore.CompressGzip,
			panicValue: panicValue,
		}
		cfg.setArchiveUploader(failureCategory, uploadClient)
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)
	// 構築時のtarget-only予約を消費し、今回は圧縮を含むfull scanを通します。
	require.True(t, state.uploadRetry.claimForAttempt(now, testUsageCacheTTL))

	directory := handler.Fsstore.FileNamePermanent.Dir
	firstRaw := filepath.Join(directory, "test-app_20260901_1.jsonl")
	secondRaw := filepath.Join(directory, "test-app_20260902_2.jsonl")
	for _, path := range []string{firstRaw, secondRaw} {
		require.NoError(t, os.WriteFile(path, []byte("failure archive"), 0o600))
	}
	recovered := captureFailureArchivePanic(func() {
		_ = handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory)
	})
	require.Equal(t, panicValue, recovered)

	// 圧縮が中断されても、送信成功前の宛先完了は記録しません。
	for _, path := range []string{firstRaw, secondRaw} {
		key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, path+".gz")
		checkpoint, err := state.uploadCheckpoints.Load(context.Background(), key)
		require.NoError(t, err)
		require.Empty(t, checkpoint.CompletedDestinationIDs)
	}
	require.FileExists(t, firstRaw)
	require.FileExists(t, secondRaw)
	require.Zero(t, uploadClient.totalAttemptCount(firstRaw+".gz"))
	require.Zero(t, uploadClient.totalAttemptCount(secondRaw+".gz"))

}

func TestFailureArchiveFullScanKeepsUploadCheckpointWhenCompressedLogStatTemporarilyFails(t *testing.T) {
	now := time.Date(2026, 9, 3, 18, 0, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryRetryable
	const originalDestination objectstorage.DestinationID = "destination-before-stat-failure"
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader(originalDestination))
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)
	store := state.uploadCheckpoints
	// 構築時のtarget-only予約を消費し、今回はfull scanを通します。
	require.True(t, state.uploadRetry.claimForAttempt(now, testUsageCacheTTL))

	path := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260902_1.jsonl.gz",
	)
	require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, path)
	_, err := store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{originalDestination})
	require.NoError(t, err)

	// Globにはpathが現れる一方、この一回だけStat結果から落ちる状態を再現します。
	// upload/purge/bytesには使いませんが、Pruneの存在証明にはGlob結果を残します。
	handler.inspectArchiveFiles = func(paths []string) infrafsstore.FileInspection {
		visible := make([]string, 0, len(paths))
		for _, candidate := range paths {
			if pathidentity.AbsoluteOrCleanPath(candidate) !=
				pathidentity.AbsoluteOrCleanPath(path) {
				visible = append(visible, candidate)
			}
		}
		return infrafsstore.FileInspection{Paths: paths, RegularFiles: infrafsstore.GetFilesInfo(visible), PathErrors: map[string]error{path: os.ErrPermission}}
	}
	require.ErrorIs(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory), os.ErrPermission)
	require.FileExists(t, path)

	checkpoint, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{originalDestination}, checkpoint.CompletedDestinationIDs)

}

func TestFailureArchiveFullScanDoesNotUploadUninspectableFilesAndKeepsThemForNewDestinationAfterRestart(
	t *testing.T,
) {
	ctx := context.Background()
	failureCategory := failurearchive.FailureCategoryRetryable
	storage := newFailureArchiveObjectStorageForTest(t)
	cfg := restartableFailureArchiveConfigForTest(t, "failure_archive_initial_stat_failure")
	cfg.enableFileCompress = false
	cfg.objectStorageUpload = storage.target("first")
	now := time.Date(2026, 9, 3, 18, 15, 0, 0, time.UTC)
	first := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	handler := first.handler
	path := filepath.Join(
		handler.Fsstore.FileNameRetryable.Dir,
		"test-app_20260902_1.jsonl.gz",
	)
	require.NoError(t, os.WriteFile(path, []byte("compressed"), 0o600))

	// Globでは初めてcompressed log fileを発見する一方、このscanだけStat結果から落ちる状態です。
	// 通常fileと確認できない間は、送信せず成功記録も作りません。
	handler.inspectArchiveFiles = func(paths []string) infrafsstore.FileInspection {
		visible := make([]string, 0, len(paths))
		for _, candidate := range paths {
			if pathidentity.AbsoluteOrCleanPath(candidate) !=
				pathidentity.AbsoluteOrCleanPath(path) {
				visible = append(visible, candidate)
			}
		}
		return infrafsstore.FileInspection{Paths: paths, RegularFiles: infrafsstore.GetFilesInfo(visible), PathErrors: map[string]error{path: os.ErrPermission}}
	}
	require.ErrorIs(t, handler.runInlineArchivePurgeWithOptions(
		ctx,
		failureCategory,
		false,
		"",
	), os.ErrPermission)
	require.FileExists(t, path)
	require.Zero(t, storage.total())
	shutdownFailureArchivePipelineForTest(t, first)

	// 送信できないまま停止し、宛先を変更して再起動します。新しい宛先の登録前から
	// 存在したfileなので、通常fileと確認できた後も送信せず、upload完了を理由に削除もしません。
	cfg.objectStorageUpload = storage.target("restarted")
	restarted := newRestartableFailureArchivePipelineForTest(t, cfg, now)
	for range 2 {
		require.NoError(t, restarted.handler.triggerArchiveCleanup(ctx, failureCategory))
	}
	require.Zero(t, storage.total())
	require.FileExists(t, path)
}

func TestFailureArchiveRotationDefersWhenPendingUploadOwnsSameGeneration(t *testing.T) {
	now := time.Date(2026, 9, 3, 18, 30, 0, 0, time.UTC)
	failureCategory := failurearchive.FailureCategoryPermanent
	const destination objectstorage.DestinationID = "destination"
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressFormat = pkgfsstore.CompressGzip
		cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader(destination))
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)

	rawPath := filepath.Join(
		handler.Fsstore.FileNamePermanent.Dir,
		"test-app_20260902_1.jsonl",
	)
	gzipPath := rawPath + pkgfsstore.ExtensionCompressGzip
	zstdPath := rawPath + pkgfsstore.ExtensionCompressZstd
	require.NoError(t, os.WriteFile(rawPath, []byte("rotated failure"), 0o600))
	require.NoError(t, os.WriteFile(zstdPath, []byte("pending upload"), 0o600))
	uploadProtection, err := handler.retainArchivePathsForUpload(
		context.Background(),
		failureCategory,
		handler.Fsstore.FileNamePermanent,
		infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{zstdPath}), handler.Fsstore.FileNamePermanent.Extension))
	require.NoError(t, err)
	require.Equal(t, []string{zstdPath}, uploadProtection.paths)
	require.NotNil(t, uploadProtection.token)

	rotationProtection := handler.protectRotatedFileBeforeTransition(failureCategory, rawPath)
	handler.finishRotatedFileTransition(
		context.Background(),
		handler.Fsstore.ActiveFilePathPermanent,
		rawPath,
		failureCategory,
		rotationProtection,
		nil,
	)

	// rotation側はfilesystemを変更せず延期し、自分のtokenだけを解除します。
	// 先行upload ownerのtoken/pathは早期解除されません。
	require.FileExists(t, rawPath)
	require.NoFileExists(t, gzipPath)
	require.FileExists(t, zstdPath)
	assertArchivePathProtected(t, state.guard, zstdPath, true)
	assertArchivePathProtected(t, state.guard, rawPath, true)
	handler.archiveTaskMu.Lock()
	require.Zero(t, handler.archiveTaskCount)
	handler.archiveTaskMu.Unlock()

	handler.releaseArchivePathProtection(failureCategory, uploadProtection)
	assertArchivePathProtected(t, state.guard, zstdPath, false)
	assertArchivePathProtected(t, state.guard, rawPath, false)
}

// failureArchiveObjectStorageForTestは、実際のconstructorが作るS3 uploaderの送信先です。
// bucketごとに別の宛先IDになり、受け取ったobjectをbucketとfile名ごとに数えます。
type failureArchiveObjectStorageForTest struct {
	server *httptest.Server
	mu     sync.Mutex
	paths  []string
}

func newFailureArchiveObjectStorageForTest(t *testing.T) *failureArchiveObjectStorageForTest {
	t.Helper()
	storage := &failureArchiveObjectStorageForTest{}
	storage.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		storage.mu.Lock()
		storage.paths = append(storage.paths, r.URL.Path)
		storage.mu.Unlock()
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(storage.server.Close)
	return storage
}

func (s *failureArchiveObjectStorageForTest) target(bucket string) objectstorage.UploadTargetConfig {
	return objectstorage.UploadTargetConfig{
		Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
		S3: objectstorage.S3Config{
			Region: "us-east-1", Bucket: bucket, Prefix: "backend-records", Endpoint: s.server.URL,
			ForcePathStyle: true, AccessKeyID: "test", SecretAccessKey: "test", Timeout: time.Second,
		},
	}
}

// uploadsは、bucketへ送信したobjectのうち、file名がpathと同じものを数えます。
func (s *failureArchiveObjectStorageForTest) uploads(bucket, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, objectPath := range s.paths {
		if strings.HasPrefix(objectPath, "/"+bucket+"/") && strings.HasSuffix(objectPath, "/"+filepath.Base(path)) {
			count++
		}
	}
	return count
}

func (s *failureArchiveObjectStorageForTest) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.paths)
}

// restartableFailureArchiveConfigForTestは、process起動と同じconstructorで宛先を登録する
// 再起動試験の設定です。保持期限は、試験中の日付で期限切れにならない長さにします。
func restartableFailureArchiveConfigForTest(t *testing.T, sourcePrefix string) testArchiveConfig {
	t.Helper()
	cfg := newValidArchiveHandlerConfig(t)
	cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.enableFileCompress = true
	cfg.maxFileAge = 30
	cfg.archiveCheckpoint = pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: sourcePrefix}
	require.NoError(t, checkpointstore.PrepareDirectory(cfg.archiveCheckpoint.Directory))
	return cfg
}

// newRestartableFailureArchivePipelineForTestは、process起動と同じconstructorでPipelineを作ります。
// checkpoint Storeの構築と宛先の登録も差し替えず、起動時の経路で行います。
func newRestartableFailureArchivePipelineForTest(t *testing.T, cfg testArchiveConfig, now time.Time) *FailureArchivePipeline {
	t.Helper()
	pipeline, err := NewFailureArchivePipeline(
		context.Background(), archiveFixedClock{now: now}, cfg,
		observer.NopTracer{}, observer.NopMeter{}, InternalLogHandlers{},
	)
	require.NoError(t, err)
	// 実行環境の空き容量によって記録が過負荷として退避されないよう、固定値を使います。
	useTestArchiveAvailableDiskSpace(pipeline)
	t.Cleanup(func() {
		_ = pipeline.ForceClose()
		waitFailureArchiveClosedForTest(t, pipeline)
	})
	return pipeline
}

// shutdownFailureArchivePipelineForTestは、受理済みの処理を完了させてprocessの停止を再現します。
func shutdownFailureArchivePipelineForTest(t *testing.T, pipeline *FailureArchivePipeline) {
	t.Helper()
	require.NoError(t, shutdownFailureArchiveForTest(pipeline))
	waitFailureArchiveClosedForTest(t, pipeline)
}

func newFailureArchiveCompressedLogCheckpointStoreForTest(
	t *testing.T,
	directory string,
	sourceID string,
) *checkpointstore.FilesystemStore {
	t.Helper()
	require.NoError(t, checkpointstore.PrepareDirectory(directory))
	store, err := checkpointstore.NewFilesystemStore(directory, sourceID)
	require.NoError(t, err)
	return store
}

func failureArchiveCompressedLogCheckpointKey(
	t *testing.T,
	handler *FsArchiveHandler,
	failureCategory failurearchive.FailureCategory,
	path string,
) checkpointstore.CheckpointKey {
	t.Helper()
	fileName, err := handler.archiveFileNamePattern(failureCategory)
	require.NoError(t, err)
	key, err := checkpointstore.ArchiveCheckpointGenerationKeyForPath(path, fileName)
	require.NoError(t, err)
	return checkpointstore.CheckpointKey{Generation: key, Compressed: true}
}

func failureArchiveDirectoryForFailureCategory(
	handler *FsArchiveHandler,
	failureCategory failurearchive.FailureCategory,
) string {
	if failureCategory == failurearchive.FailureCategoryPermanent {
		return handler.Fsstore.FileNamePermanent.Dir
	}
	return handler.Fsstore.FileNameRetryable.Dir
}

func triggerFailureArchiveCleanupForFailureCategory(
	handler *FsArchiveHandler,
	failureCategory failurearchive.FailureCategory,
) error {
	return handler.triggerArchiveCleanup(context.Background(), failureCategory)
}

func waitForFailureArchiveTasksForTest(t *testing.T, handler *FsArchiveHandler) {
	t.Helper()
	handler.archiveTaskMu.Lock()
	taskDone := handler.archiveTaskGeneration
	taskCount := handler.archiveTaskCount
	handler.archiveTaskMu.Unlock()
	if taskCount == 0 {
		return
	}
	select {
	case <-taskDone:
	case <-time.After(failureArchiveHangTimeout):
		t.Fatal("timed out waiting for failure archive task")
	}
}

type failingCompressedArchiveCheckpointStore struct {
	err error
}

func (s failingCompressedArchiveCheckpointStore) RegisterDestinations(context.Context, []objectstorage.DestinationID, []checkpointstore.CheckpointKey) error {
	return s.err
}

func (s failingCompressedArchiveCheckpointStore) Load(
	context.Context,
	checkpointstore.CheckpointKey,
) (checkpointstore.Checkpoint, error) {
	return checkpointstore.Checkpoint{}, s.err
}

func (s failingCompressedArchiveCheckpointStore) AdvanceCompletionCursor(
	context.Context,
	checkpointstore.CheckpointKey,
	[]objectstorage.DestinationID,
) (checkpointstore.Checkpoint, error) {
	return checkpointstore.Checkpoint{}, s.err
}

func (s failingCompressedArchiveCheckpointStore) LoadUploadProgress(context.Context, checkpointstore.CheckpointKey, objectstorage.DestinationID) (checkpointstore.UploadProgress, bool, error) {
	return checkpointstore.UploadProgress{}, false, s.err
}

func (s failingCompressedArchiveCheckpointStore) SaveUploadProgress(context.Context, checkpointstore.UploadProgress) error {
	return s.err
}

func (s failingCompressedArchiveCheckpointStore) DestinationCursors(context.Context) ([]checkpointstore.DestinationCursor, error) {
	return nil, s.err
}

func (s failingCompressedArchiveCheckpointStore) Close() error {
	return nil
}

type destinationFailureArchiveUploader struct {
	mu                sync.Mutex
	destinationIDs    []objectstorage.DestinationID
	failuresRemaining map[objectstorage.DestinationID]int
	attempts          map[string]map[objectstorage.DestinationID]int
}

type checkpointDestinationRecordingFailureArchiveUploader struct {
	destinationIDs []objectstorage.DestinationID
	requested      []objectstorage.DestinationID
	filePath       string
}

func (u *checkpointDestinationRecordingFailureArchiveUploader) DestinationIDs() []objectstorage.DestinationID {
	return append([]objectstorage.DestinationID(nil), u.destinationIDs...)
}

func (u *checkpointDestinationRecordingFailureArchiveUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}

func (u *checkpointDestinationRecordingFailureArchiveUploader) UploadDestinations(
	_ context.Context,
	input uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	u.filePath = input.FilePath()
	u.requested = append([]objectstorage.DestinationID(nil), destinationIDs...)
	results := make([]uploader.DestinationResult, 0, len(destinationIDs))
	for _, destinationID := range destinationIDs {
		results = append(results, uploader.DestinationResult{DestinationID: destinationID})
	}
	return uploader.UploadReport{Results: results}
}

func newDestinationFailureArchiveUploader(
	destinationIDs ...objectstorage.DestinationID,
) *destinationFailureArchiveUploader {
	return &destinationFailureArchiveUploader{
		destinationIDs:    append([]objectstorage.DestinationID(nil), destinationIDs...),
		failuresRemaining: make(map[objectstorage.DestinationID]int),
		attempts:          make(map[string]map[objectstorage.DestinationID]int),
	}
}

func (u *destinationFailureArchiveUploader) DestinationIDs() []objectstorage.DestinationID {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]objectstorage.DestinationID(nil), u.destinationIDs...)
}

func (u *destinationFailureArchiveUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}

func (u *destinationFailureArchiveUploader) UploadDestinations(
	_ context.Context,
	input uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	u.mu.Lock()
	defer u.mu.Unlock()
	report := uploader.UploadReport{
		Results: make([]uploader.DestinationResult, len(destinationIDs)),
	}
	for index, destinationID := range destinationIDs {
		attempts := u.attempts[input.FilePath()]
		if attempts == nil {
			attempts = make(map[objectstorage.DestinationID]int)
			u.attempts[input.FilePath()] = attempts
		}
		attempts[destinationID]++
		report.Results[index].DestinationID = destinationID
		if u.failuresRemaining[destinationID] > 0 {
			u.failuresRemaining[destinationID]--
			report.Results[index].Err = errors.New("injected destination upload failure")
		}
	}
	return report
}

func (u *destinationFailureArchiveUploader) failNext(
	destinationID objectstorage.DestinationID,
	count int,
) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.failuresRemaining[destinationID] = count
}

func (u *destinationFailureArchiveUploader) attemptCount(
	path string,
	destinationID objectstorage.DestinationID,
) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attempts[path][destinationID]
}

func (u *destinationFailureArchiveUploader) totalAttemptCount(path string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	total := 0
	for _, count := range u.attempts[path] {
		total += count
	}
	return total
}
