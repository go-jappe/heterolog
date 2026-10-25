package lifecycle

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestObjectStorageUploadManagerAdvancesSuccessfulDestinationAndStopsFailedDestinationAcrossRestart(t *testing.T) {
	const (
		primary   = objectstorage.DestinationID("primary")
		secondary = objectstorage.DestinationID("secondary")
		sourceID  = "continue-after-gap"
	)
	ctx := context.Background()
	directory := resolvedTempDirForTest(t)
	firstCursor := fileGeneration{Period: "20260829", Index: 1}
	secondCursor := fileGeneration{Period: firstCursor.Period, Index: 2}
	firstPath := filepath.Join(directory, "app_20260829_1.log.gz")
	secondPath := filepath.Join(directory, "app_20260829_2.log.gz")
	firstFileStore, err := checkpointstore.NewFilesystemStore(directory, sourceID)
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstFileStore.Close()", firstFileStore.Close, boundedwait.ForceCloserOf(firstFileStore), boundedwait.NoError)
	firstUploader := newDestinationUploaderRecorder(primary, secondary)
	uploadErr := errors.New("secondary unavailable for first generation")
	firstUploader.failNext(secondary, uploadErr)
	manager := newObjectStorageUploadManager(firstUploader, firstFileStore)
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)

	first, err := manager.uploadIfPending(ctx, firstPath, firstCursor, state, true)
	require.ErrorIs(t, err, uploadErr)
	require.Equal(t, uploadAttemptResumeUpload, first.state)
	require.Equal(t, []objectstorage.DestinationID{primary}, first.uploadedDestinationIDs)
	second, err := manager.uploadIfPending(ctx, secondPath, secondCursor, state, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptResumeUpload, second.state)
	require.Empty(t, second.localCompressedPathToRemove)
	require.Equal(t, []string{firstPath, secondPath}, firstUploader.pathsFor(primary))
	require.Equal(t, []string{firstPath}, firstUploader.pathsFor(secondary))

	// Storeを再作成しても宛先ごとの完了位置が残り、後続から先に再開しません。
	require.NoError(t, boundedwait.Close(t, "firstFileStore.Close()", firstFileStore.Close, boundedwait.ForceCloserOf(firstFileStore)))
	restartedFileStore, err := checkpointstore.NewFilesystemStore(directory, sourceID)
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedFileStore.Close()", restartedFileStore.Close, boundedwait.ForceCloserOf(restartedFileStore), boundedwait.NoError)
	restartedUploader := newDestinationUploaderRecorder(primary, secondary)
	restarted := newObjectStorageUploadManager(restartedUploader, restartedFileStore)
	restartedState, err := restarted.prepareRun(ctx)
	require.NoError(t, err)
	restartedState.observedKeys = []checkpointstore.CheckpointKey{fileCheckpointKey(firstCursor, true), fileCheckpointKey(secondCursor, true)}
	blocked, err := restarted.uploadIfPending(ctx, secondPath, secondCursor, restartedState, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptResumeUpload, blocked.state)
	require.Empty(t, restartedUploader.pathsFor(secondary))
	for _, file := range []struct {
		path   string
		cursor fileGeneration
	}{{firstPath, firstCursor}, {secondPath, secondCursor}} {
		result, err := restarted.uploadIfPending(ctx, file.path, file.cursor, restartedState, true)
		require.NoError(t, err)
		require.Equal(t, uploadAttemptComplete, result.state)
	}
	require.Empty(t, restartedUploader.pathsFor(primary))
	require.Equal(t, []string{firstPath, secondPath}, restartedUploader.pathsFor(secondary))
}

func TestObjectStorageUploadManagerWaitsForEarlierCompressedCheckpointRejection(t *testing.T) {
	for _, method := range []string{"Load", "AdvanceCompletionCursor"} {
		t.Run(method, func(t *testing.T) {
			const destination = objectstorage.DestinationID("primary")
			ctx := context.Background()
			directory := resolvedTempDirForTest(t)
			fileStore, err := checkpointstore.NewFilesystemStore(directory, "file-series")
			require.NoError(t, err)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "fileStore.Close()", fileStore.Close, boundedwait.ForceCloserOf(fileStore), boundedwait.NoError)
			faulted := &recoveryFaultCheckpointStore{Store: fileStore}
			faulted.failLoad.Store(method == "Load")
			faulted.failRecord.Store(method == "AdvanceCompletionCursor")
			remote := newDestinationUploaderRecorder(destination)
			manager := newObjectStorageUploadManager(remote, faulted)
			state, err := manager.prepareRun(ctx)
			require.NoError(t, err)
			firstCursor := fileGeneration{Period: "20260829", Index: 1}
			secondCursor := fileGeneration{Period: firstCursor.Period, Index: 2}
			firstPath := filepath.Join(directory, "app_20260829_1.log.gz")
			secondPath := filepath.Join(directory, "app_20260829_2.log.gz")

			first, err := manager.uploadIfPending(ctx, firstPath, firstCursor, state, true)
			require.ErrorIs(t, err, errRecoveryCheckpoint)
			expectedState := uploadAttemptResumeUpload
			if method == "AdvanceCompletionCursor" {
				expectedState = uploadAttemptResumeCheckpoint
			}
			require.Equal(t, expectedState, first.state)
			faulted.failLoad.Store(false)
			faulted.failRecord.Store(false)
			second, err := manager.uploadIfPending(ctx, secondPath, secondCursor, state, true)
			require.NoError(t, err)
			require.Equal(t, uploadAttemptResumeUpload, second.state)
			require.Empty(t, second.localCompressedPathToRemove)
			checkpoint, err := fileStore.Load(ctx, fileCheckpointKey(secondCursor, true))
			require.NoError(t, err)
			require.False(t, checkpoint.DestinationComplete(destination))
			if method == "Load" {
				require.Empty(t, remote.pathsFor(destination))
			} else {
				require.Equal(t, []string{firstPath}, remote.pathsFor(destination))
			}
			first, err = manager.uploadIfPending(ctx, firstPath, firstCursor, state, true)
			require.NoError(t, err)
			require.Equal(t, uploadAttemptComplete, first.state)
			second, err = manager.uploadIfPending(ctx, secondPath, secondCursor, state, true)
			require.NoError(t, err)
			require.Equal(t, uploadAttemptComplete, second.state)
			require.Equal(t, []string{firstPath, secondPath}, remote.pathsFor(destination))
		})
	}
}

func TestObjectStorageUploadManagerCheckpointCursorKeepsFailedDestinationBehindEarlierGeneration(t *testing.T) {
	const (
		primary   = objectstorage.DestinationID("primary")
		secondary = objectstorage.DestinationID("secondary")
	)
	ctx := context.Background()
	directory := resolvedTempDirForTest(t)
	firstGeneration := fileGeneration{Period: "20260829", Index: 1}
	secondGeneration := fileGeneration{Period: firstGeneration.Period, Index: 2}
	firstRaw := filepath.Join(directory, "app_20260829_1.log")
	firstCompressed := firstRaw + ".gz"
	secondCompressed := filepath.Join(directory, "app_20260829_2.log.gz")
	remote := newDestinationUploaderRecorder(primary, secondary)
	uploadErr := errors.New("secondary unavailable for first generation")
	remote.failNext(secondary, uploadErr)
	store := newTestFileCheckpointStore(t)
	manager := newObjectStorageUploadManager(remote, store)
	state, err := manager.prepareRun(ctx)
	require.NoError(t, err)

	first, err := manager.uploadIfPending(ctx, firstRaw, firstGeneration, state, false)
	require.ErrorIs(t, err, uploadErr)
	require.Equal(t, uploadAttemptResumeUpload, first.state)
	second, err := manager.uploadIfPending(ctx, secondCompressed, secondGeneration, state, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptResumeUpload, second.state)
	require.Equal(t, []string{firstRaw, secondCompressed}, remote.pathsFor(primary))
	require.Equal(t, []string{firstRaw}, remote.pathsFor(secondary))

	// The failed raw generation is compressed before the next run. Neither the
	// path change nor a new scan permits secondary to jump over that failure.
	manager.pruneUploadedPaths(map[string]struct{}{
		pathidentity.ComparisonKey(firstCompressed): {}, pathidentity.ComparisonKey(secondCompressed): {},
	})
	state, err = manager.prepareRun(ctx)
	require.NoError(t, err)
	state.observedKeys = []checkpointstore.CheckpointKey{
		fileCheckpointKey(firstGeneration, true), fileCheckpointKey(secondGeneration, true),
	}
	blocked, err := manager.uploadIfPending(ctx, secondCompressed, secondGeneration, state, true)
	require.NoError(t, err)
	require.Equal(t, uploadAttemptResumeUpload, blocked.state)
	require.Empty(t, blocked.uploadedDestinationIDs)
	for _, file := range []struct {
		path       string
		generation fileGeneration
	}{{firstCompressed, firstGeneration}, {secondCompressed, secondGeneration}} {
		result, err := manager.uploadIfPending(ctx, file.path, file.generation, state, true)
		require.NoError(t, err)
		require.Equal(t, uploadAttemptComplete, result.state)
	}
	require.Equal(t, []string{firstRaw, secondCompressed}, remote.pathsFor(primary))
	require.Equal(t, []string{firstRaw, firstCompressed, secondCompressed}, remote.pathsFor(secondary))
	record, err := store.Load(ctx, fileCheckpointKey(secondGeneration, true))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{primary, secondary}, record.CompletedDestinationIDs)
	require.Empty(t, manager.uploadedPathDestinations)
}
