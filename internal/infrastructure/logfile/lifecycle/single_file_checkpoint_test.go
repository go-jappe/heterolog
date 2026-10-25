package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestObjectStorageUploadManagerDoesNotReuseAnotherGenerationSuccess(t *testing.T) {
	ctx := context.Background()
	const destination = objectstorage.DestinationID("archive")
	path := filepath.Join(resolvedTempDirForTest(t), "app_20260915_2.log.gz")
	cursor := fileGeneration{Period: "20260915", Index: 2}
	remote := newDestinationUploaderRecorder(destination)
	compressedStore, err := checkpointstore.NewFilesystemStore(resolvedTempDirForTest(t), "single-file")
	require.NoError(t, err)
	_, err = compressedStore.AdvanceCompletionCursor(ctx,
		fileCheckpointKey(fileGeneration{Period: cursor.Period, Index: 1}, true),
		[]objectstorage.DestinationID{destination})
	require.NoError(t, err)
	manager := newObjectStorageUploadManager(remote, compressedStore)
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)

	result, err := manager.uploadIfPending(ctx, path, cursor, state, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptComplete, result.state)
	require.Equal(t, path, result.localCompressedPathToRemove)
	require.Equal(t, []string{path}, remote.pathsFor(destination))
}

func TestObjectStorageUploadManagerUsesGenerationSuccessForObservedPath(t *testing.T) {
	ctx := context.Background()
	const destination = objectstorage.DestinationID("archive")
	path := filepath.Join(resolvedTempDirForTest(t), "App_20260915_1.log.gz")
	alias := filepath.Dir(path) + string(os.PathSeparator) + "." + string(os.PathSeparator) + filepath.Base(path)
	if runtime.GOOS == "windows" {
		alias = strings.ToUpper(alias)
	}
	cursor := fileGeneration{Period: "20260915", Index: 1}
	remote := newDestinationUploaderRecorder(destination)
	compressedStore, err := checkpointstore.NewFilesystemStore(resolvedTempDirForTest(t), "single-file")
	require.NoError(t, err)
	_, err = compressedStore.AdvanceCompletionCursor(ctx, fileCheckpointKey(cursor, true),
		[]objectstorage.DestinationID{destination})
	require.NoError(t, err)
	manager := newObjectStorageUploadManager(remote, compressedStore)
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)

	result, err := manager.uploadIfPending(ctx, alias, cursor, state, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptComplete, result.state)
	require.Equal(t, filepath.Clean(alias), result.localCompressedPathToRemove)
	require.Empty(t, remote.pathsFor(destination))
}

func TestObjectStorageUploadManagerDoesNotResendCompletedDestination(t *testing.T) {
	ctx := context.Background()
	const previous = objectstorage.DestinationID("previous")
	const active = objectstorage.DestinationID("active")
	cursor := fileGeneration{Period: "20260915", Index: 1}
	key := fileCheckpointKey(cursor, true)
	compressedStore, err := checkpointstore.NewFilesystemStore(resolvedTempDirForTest(t), "copy-successes")
	require.NoError(t, err)
	_, err = compressedStore.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{previous})
	require.NoError(t, err)
	uploadErr := errors.New("current destination failed")
	remote := newDestinationUploaderRecorder(previous, active)
	remote.failNext(active, uploadErr)
	manager := newObjectStorageUploadManager(remote, compressedStore)
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)
	path := filepath.Join(resolvedTempDirForTest(t), "app_20260915_1.log.gz")

	result, err := manager.uploadIfPending(ctx, path, cursor, state, true)
	require.ErrorIs(t, err, uploadErr)
	require.Equal(t, uploadAttemptResumeUpload, result.state)
	require.Empty(t, result.localCompressedPathToRemove)
	require.Empty(t, remote.pathsFor(previous))
	require.Equal(t, []string{path}, remote.pathsFor(active))
	require.Equal(t, []objectstorage.DestinationID{previous}, manager.checkpointsByKey[key].CompletedDestinationIDs)
	require.True(t, manager.uploadPending(cursor, true, true))
	checkpoint, err := compressedStore.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{previous}, checkpoint.CompletedDestinationIDs)
}

func TestObjectStorageUploadManagerGenerationCompletionCoversRawAndCompressedPaths(t *testing.T) {
	ctx := context.Background()
	const destination = objectstorage.DestinationID("archive")
	cursor := fileGeneration{Period: "20260915", Index: 1}
	compressedStore, err := checkpointstore.NewFilesystemStore(resolvedTempDirForTest(t), "raw-cursor")
	require.NoError(t, err)
	_, err = compressedStore.AdvanceCompletionCursor(ctx, fileCheckpointKey(cursor, true), []objectstorage.DestinationID{destination})
	require.NoError(t, err)
	manager := newObjectStorageUploadManager(newDestinationUploaderRecorder(destination), compressedStore)
	_, err = manager.prepareRun(ctx)
	require.NoError(t, err)
	raw := filepath.Join(resolvedTempDirForTest(t), "app_20260915_1.log")
	_, err = manager.loadFileCheckpoint(ctx, raw+".gz", cursor, true)
	require.NoError(t, err)

	require.False(t, manager.uploadPending(cursor, true, false))
	require.False(t, manager.uploadPending(cursor, true, true))
}

func TestFileSeriesManagerKeepsCompletionCursorForUnobservedAndUnprotectedGenerations(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "generation-prune"
	const destination = objectstorage.DestinationID("archive")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	writeFileSeriesCheckpointLog(t, directory, target, period, 1)
	writeGzipFile(t, filepath.Join(directory, target+"_"+period+"_2.log.gz"), []byte("compressed"))
	latest := writeFileSeriesCheckpointLog(t, directory, target, period, 4)
	require.NoError(t, os.WriteFile(filepath.Join(directory, target+"_"+period+"_3.log.gz.tmp"), []byte("temporary"), 0o600))
	foreign := writeFileSeriesCheckpointLog(t, directory, "foreign", period, 7)
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	compressedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "prune-generations")
	require.NoError(t, err)
	keyFor := func(index uint64) checkpointstore.CheckpointKey {
		return fileCheckpointKey(fileGeneration{Period: period, Index: index}, true)
	}
	for _, index := range []uint64{1, 2, 3, 4, 5, 7, 9} {
		_, err := compressedStore.AdvanceCompletionCursor(ctx, keyFor(index), []objectstorage.DestinationID{destination})
		require.NoError(t, err)
	}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		CheckpointStore: compressedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)
	lifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	token := &appendlog.DirectoryGuardToken{}
	protectedPath := filepath.Join(directory, target+"_"+period+"_5.log")
	manager.directoryGuard.ProtectPaths(token, protectedPath, foreign)
	defer manager.directoryGuard.UnprotectPaths(token, protectedPath, foreign)

	_, err = lifecycle.ReconcileObjectStorageUploadDestinations(ctx)
	require.NoError(t, err)
	for _, index := range []uint64{1, 2, 3, 4, 5, 7, 9} {
		checkpoint, err := compressedStore.Load(ctx, keyFor(index))
		require.NoError(t, err)
		require.True(t, checkpoint.DestinationComplete(destination), "generation %d", index)
	}
	require.FileExists(t, latest)
	manager.directoryGuard.UnprotectPaths(token, protectedPath)
	_, err = lifecycle.ReconcileObjectStorageUploadDestinations(ctx)
	require.NoError(t, err)
	checkpoint, err := compressedStore.Load(ctx, keyFor(5))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)
}

func TestFileSeriesManagerKeepsUnselectedPeerAfterGenerationSuccess(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "generation-checkpoint-selected-peer"
	const destination = objectstorage.DestinationID("archive")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	gzipPath, zstdPath := raw+".gz", raw+".zst"
	payload, err := os.ReadFile(raw)
	require.NoError(t, err)
	writeGzipFile(t, gzipPath, payload)
	require.NoError(t, os.WriteFile(zstdPath, []byte("previously published archive"), 0o600))
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	compressedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "path-conflict")
	require.NoError(t, err)
	cursor := fileGeneration{Period: currentLogDate(), Index: 1}
	key := fileCheckpointKey(cursor, true)
	_, err = compressedStore.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{destination})
	require.NoError(t, err)
	remote := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, true),
		Uploader: remote, CheckpointStore: compressedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.Compressed)
	require.Empty(t, report.ObjectStorageUploads)
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, gzipPath, report.UploadedFileRemovals[0].File.Path)
	require.Empty(t, report.Purge.ByAge.RemovalResults)
	require.Empty(t, report.Purge.ByGeneration.RemovalResults)
	require.Empty(t, report.Purge.ByDirSize.RemovalResults)
	require.NoFileExists(t, gzipPath)
	require.NoFileExists(t, raw)
	require.FileExists(t, zstdPath)
	require.FileExists(t, latest)
	checkpoint, err := compressedStore.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)
	require.Empty(t, remote.pathsFor(destination))
}

func TestFileSeriesManagerRestartUsesGenerationSuccessWithPreviousCompressionFormat(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "generation-codec-restart"
	const primary = objectstorage.DestinationID("primary")
	const secondary = objectstorage.DestinationID("secondary")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	raw := writeFileSeriesCheckpointLog(t, directory, target, period, 1)
	latest := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	generationStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "codec-restart")
	require.NoError(t, err)
	firstRemote := newDestinationUploaderRecorder(primary, secondary)
	uploadErr := errors.New("secondary temporarily unavailable")
	firstRemote.failNext(secondary, uploadErr)
	first := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: firstRemote, CheckpointStore: generationStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "first.Close()", first.Close, boundedwait.ForceCloserOf(first), nil)
	_, err = first.RunOnce(ctx)
	require.ErrorIs(t, err, uploadErr)
	gzipPath := raw + ".gz"
	require.FileExists(t, gzipPath)
	require.NoFileExists(t, raw)

	require.ErrorIs(t, first.ForceClose(), shutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "first.Close()", first.Close, boundedwait.ForceCloserOf(first)), shutdown.ErrForcedClose)
	restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "codec-restart")
	require.NoError(t, err)
	restartedRemote := newDestinationUploaderRecorder(primary, secondary)
	restarted := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root, target, pkgfsstore.FileRotationDaily, 10, 0, 0, true, pkgfsstore.CompressZstd,
		),
		Uploader: restartedRemote, CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted), nil)
	restarted.compressFile = func(context.Context, string) (string, error) {
		t.Error("existing gzip must not be recompressed with the new format")
		return "", errors.New("unexpected recompression")
	}
	report, err := restarted.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.Compressed)
	require.Empty(t, restartedRemote.pathsFor(primary))
	require.Equal(t, []string{gzipPath}, restartedRemote.pathsFor(secondary))
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, gzipPath, report.UploadedFileRemovals[0].File.Path)
	require.NoFileExists(t, gzipPath)
	require.NoFileExists(t, raw+".zst")
	require.FileExists(t, latest)
}

func TestFileSeriesManagerDoesNotPersistCompressionPlanBeforeUploadSuccess(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "unpublished-checkpoint-restart"
		const destination = objectstorage.DestinationID("archive")
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
		latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
		checkpointDirectory := newCheckpointDirectoryForTest(t, root)
		generationStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "plan-restart")
		require.NoError(t, err)
		first := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config: testFileSeriesManagerConfigWithCompressionFormat(
				root, target, pkgfsstore.FileRotationDaily, 10, 0, 0, true, pkgfsstore.CompressZstd,
			),
			Uploader: newDestinationUploaderRecorder(destination), CheckpointStore: generationStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "first.Close()", first.Close, first.ForceClose, nil)
		lifecycle, err := NewDirectoryLifecycle(first)
		require.NoError(t, err)
		_, err = lifecycle.ReconcileObjectStorageUploadDestinations(ctx)
		require.NoError(t, err)
		cursor := fileGeneration{Period: currentLogDate(), Index: 1}
		key := fileCheckpointKey(cursor, true)
		beforeUpload, err := first.objectStorageUpload.checkpointStore.Load(
			ctx, key,
		)
		require.NoError(t, err)
		require.Empty(t, beforeUpload.CompletedDestinationIDs)
		checkpointPath := filepath.Join(checkpointDirectory, "plan-restart.log-file-uploads.json")
		require.FileExists(t, checkpointPath)
		require.NoFileExists(t, raw+".zst")

		// 圧縮着手前に停止し、gzip設定で再起動します。旧形式の予定は記録されていません。
		restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "plan-restart")
		require.NoError(t, err)
		remote := newDestinationUploaderRecorder(destination)
		uploadErr := errors.New("retain new gzip for checkpoint inspection")
		remote.failNext(destination, uploadErr)
		restarted := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
			Uploader: remote, CheckpointStore: restartedStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "restarted.Close()", restarted.Close, restarted.ForceClose, nil)
		report, err := restarted.RunOnce(ctx)
		require.ErrorIs(t, err, uploadErr)
		require.Len(t, report.Compressed, 1)
		require.Equal(t, raw+".gz", report.Compressed[0].CompressedPath)
		require.NoFileExists(t, raw)
		require.NoFileExists(t, raw+".zst")
		require.FileExists(t, latest)
		checkpoint, err := restarted.objectStorageUpload.checkpointStore.Load(
			ctx, key,
		)
		require.NoError(t, err)
		require.Empty(t, checkpoint.CompletedDestinationIDs)
		require.False(t, checkpoint.Complete([]objectstorage.DestinationID{destination}))
		require.FileExists(t, checkpointPath)
	})
}

func TestFileSeriesManagerCheckpointFailureKeepsLaterGenerationPending(t *testing.T) {
	for _, operation := range []string{"load", "record"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			root := resolvedTempDirForTest(t)
			const target = "checkpoint-continuity"
			const destination = objectstorage.DestinationID("archive")
			directory := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(directory, 0o750))
			period := currentLogDate()
			firstPath := filepath.Join(directory, target+"_"+period+"_1.log.gz")
			secondPath := filepath.Join(directory, target+"_"+period+"_2.log.gz")
			writeGzipFile(t, firstPath, []byte("first generation"))
			writeGzipFile(t, secondPath, []byte("second generation"))
			activePath := writeFileSeriesCheckpointLog(t, directory, target, period, 3)
			checkpointDirectory := newCheckpointDirectoryForTest(t, root)
			realCompressedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "continuity")
			require.NoError(t, err)
			checkpointErr := errors.New("compressed success checkpoint unavailable")
			compressedStore := &faultedCompressedLogCheckpointStore{
				Store:     realCompressedStore,
				key:       fileCheckpointKey(fileGeneration{Period: period, Index: 1}, true),
				operation: operation, failure: checkpointErr,
			}
			remote := newDestinationUploaderRecorder(destination)
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, false),
				Uploader: remote, CheckpointStore: compressedStore,
			})
			useShortLogFileProcessingRecovery(t, manager)

			report, err := manager.RunOnce(ctx)
			require.ErrorIs(t, err, checkpointErr)
			if operation == "record" {
				require.Equal(t, []string{firstPath}, remote.pathsFor(destination))
			} else {
				require.Empty(t, remote.pathsFor(destination))
			}
			require.Empty(t, report.UploadedFileRemovals)
			require.Empty(t, report.Purge.ByAge.RemovalResults)
			require.Empty(t, report.Purge.ByGeneration.RemovalResults)
			require.Empty(t, report.Purge.ByDirSize.RemovalResults)
			for _, path := range []string{firstPath, activePath} {
				require.FileExists(t, path)
			}
			require.FileExists(t, secondPath)

			// 記録結果に成功snapshotが含まれていてもerror中は採用しません。
			// 復旧後は先行fileの完了位置を受理してから後続fileへ進みます。
			require.NoError(t, manager.directoryGuard.RunWithLifecycleLock(func() error {
				compressedStore.failure = nil
				return nil
			}))
			waitLogFileProcessingRecovery(t, manager, fileGeneration{Period: period, Index: 1})
			// 後続世代は、先行世代の完了を記録した再開処理か、次の周期のどちらかが送信します。
			// 再開処理が後続世代を保護している間、周期はその世代を処理しないため、完了まで待ちます。
			_, err = manager.RunOnce(ctx)
			require.NoError(t, err)
			waitLogFileProcessingRecovery(t, manager, fileGeneration{Period: period, Index: 2})
			require.Equal(t, []string{firstPath, secondPath}, remote.pathsFor(destination))
			require.NoFileExists(t, firstPath)
			require.NoFileExists(t, secondPath)
			require.FileExists(t, activePath)
		})
	}
}

func TestFileSeriesManagerAllInactiveStillAppliesLocalCapacityRetention(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "inactive-local-retention"
	const destination = objectstorage.DestinationID("previous-archive")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	compressedPath := filepath.Join(directory, target+"_"+period+"_1.log.gz")
	writeGzipFile(t, compressedPath, []byte("local-only retention"))
	activePath := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	compressedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "local-retention")
	require.NoError(t, err)
	cursor := fileGeneration{Period: period, Index: 1}
	_, err = compressedStore.AdvanceCompletionCursor(ctx, fileCheckpointKey(cursor, true), []objectstorage.DestinationID{destination})
	require.NoError(t, err)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 1, false),
		CheckpointStore: compressedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.True(t, report.Purge.ByDirSize.Ran)
	require.Len(t, report.Purge.ByDirSize.RemovalResults, 1)
	require.Equal(t, compressedPath, report.Purge.ByDirSize.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByDirSize.RemovalResults[0].Err)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, activePath)
}

type faultedCompressedLogCheckpointStore struct {
	checkpointstore.Store
	key       checkpointstore.CheckpointKey
	operation string
	failure   error
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *faultedCompressedLogCheckpointStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *faultedCompressedLogCheckpointStore) Load(
	ctx context.Context, key checkpointstore.CheckpointKey,
) (checkpointstore.Checkpoint, error) {
	if key == s.key && s.operation == "load" && s.failure != nil {
		return checkpointstore.Checkpoint{}, s.failure
	}
	return s.Store.Load(ctx, key)
}

func (s *faultedCompressedLogCheckpointStore) AdvanceCompletionCursor(
	ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID,
) (checkpointstore.Checkpoint, error) {
	if key == s.key && s.operation == "record" && s.failure != nil {
		return checkpointstore.Checkpoint{Key: key, CompletedDestinationIDs: destinations}, s.failure
	}
	return s.Store.AdvanceCompletionCursor(ctx, key, destinations)
}
