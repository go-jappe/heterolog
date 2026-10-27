package lifecycle

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

func TestLifecycleHandoffResumesOnlyUnfinishedStoreAfterRestart(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		name := "retryable"
		category := uploader.FailureCategoryRetryable
		var remoteErr error = errors.New("temporary upload failure")
		if permanent {
			name = "permanent"
			category = uploader.FailureCategoryPermanent
			remoteErr = &pkgs3.HTTPStatusError{Code: http.StatusForbidden}
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			const destination = objectstorage.DestinationID("s3-current")
			const sourceID = "runner-handoff-progress"
			base := resolvedTempDirForTest(t)
			fileName := logpattern.FileNamePattern{Dir: resolvedTempDirForTest(t), Target: "app", Extension: "log"}
			generation := fileGeneration{Period: "20260927", Index: 1}
			laterGeneration := fileGeneration{Period: generation.Period, Index: 2}
			key := fileCheckpointKey(generation, true)
			path := filepath.Join(fileName.Dir, "app_20260927_1.log.gz")
			payload := []byte("retained compressed log")
			require.NoError(t, os.WriteFile(path, payload, 0o600))
			checkpointDirectory := resolvedTempDirForTest(t)
			store, err := checkpointstore.NewFilesystemStore(checkpointDirectory, sourceID)
			require.NoError(t, err)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
			require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{destination}, nil))
			target := objectstorage.UploadTargetConfig{
				Enabled: true,
				CompressedLogFileUploadFailureStore: objectstorage.CompressedLogFileUploadFailureStoreConfig{
					DurableType: []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool},
					DurableStoreConfig: objectstorage.CompressedLogFileUploadFailureDurableStoreConfig{
						FileArchiveConfig: objectstorage.CompressedLogFileUploadFailureArchiveConfig{
							RootPath:       base,
							LoggerContract: pkglogger.LoggerContractSync.String(), LifecycleInterval: time.Hour,
						},
					},
				},
			}
			spoolConfig := runnerCompressedLogFileUploadSpoolTestConfig(base, "heterolog", pkglogger.LoggerContractSync)
			runtime := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolConfig)
			remote := newDestinationUploaderRecorder(destination)
			remote.failureCategory = category
			remote.failNext(destination, remoteErr)
			wrapped, owner, err := WrapObjectStorageUploader(target, runtime, remote, fileName, clock.NewUTCClock(), nil, store, false)
			require.NoError(t, err)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "owner.Close()", owner.Close, boundedwait.ForceCloserOf(owner), boundedwait.NoError)
			manager := newObjectStorageUploadManager(wrapped, store)
			state, err := manager.prepareRun(ctx)
			require.NoError(t, err)
			state.observedKeys = []checkpointstore.CheckpointKey{key, fileCheckpointKey(laterGeneration, true)}

			spoolRoot, err := objectstoragespool.NewRoot(base, "heterolog", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			spoolRoot, err = spoolRoot.ForDestination(destination)
			require.NoError(t, err)
			staging := filepath.Join(spoolRoot.Path(), ".staging")
			require.NoError(t, os.Remove(staging))
			require.NoError(t, os.WriteFile(staging, []byte("blocks spool saves"), 0o600))
			result, err := manager.uploadIfPending(ctx, path, generation, state, true)
			require.Error(t, err)
			require.Equal(t, uploadAttemptResumeUpload, result.state)
			require.Empty(t, result.localCompressedPathToRemove)
			current, err := store.Load(ctx, key)
			require.NoError(t, err)
			require.False(t, current.DestinationComplete(destination), "archive alone must not advance the cursor")

			archiveRoot, err := compressedlogarchive.NewRoot(base, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			archiveRoot, err = archiveRoot.ForDestination(destination)
			require.NoError(t, err)
			archiveDirectory, spoolDirectory := archiveRoot.RetryableDir(), spoolRoot.ActiveDir()
			if permanent {
				archiveDirectory, spoolDirectory = archiveRoot.PermanentDir(), spoolRoot.DeadDir()
			}
			archivedPath := filepath.Join(archiveDirectory, filepath.Base(path))
			archived, err := os.ReadFile(archivedPath)
			require.NoError(t, err)
			require.Equal(t, payload, archived)
			unchangedTime := time.Date(2025, time.September, 27, 0, 0, 0, 0, time.UTC)
			require.NoError(t, os.Chtimes(archivedPath, unchangedTime, unchangedTime))
			result, err = manager.uploadIfPending(ctx, path, generation, state, true)
			require.Error(t, err)
			require.Equal(t, uploadAttemptResumeUpload, result.state)
			require.Equal(t, []string{path}, remote.pathsFor(destination))
			info, err := os.Stat(archivedPath)
			require.NoError(t, err)
			require.True(t, info.ModTime().Equal(unchangedTime))
			// 先行する保存が未完了なら、同じdestinationの後続generationをその後ろに留めなければなりません。
			laterPath := filepath.Join(fileName.Dir, "app_20260927_2.log.gz")
			laterResult, err := manager.uploadIfPending(ctx, laterPath, laterGeneration, state, true)
			require.NoError(t, err)
			require.Equal(t, uploadAttemptResumeUpload, laterResult.state)
			require.Equal(t, []string{path}, remote.pathsFor(destination))

			require.NoError(t, boundedwait.Close(t, "owner.Close()", owner.Close, boundedwait.ForceCloserOf(owner)))
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			require.NoError(t, os.Remove(staging))
			require.NoError(t, os.Mkdir(staging, 0o700))
			reopened, err := checkpointstore.NewFilesystemStore(checkpointDirectory, sourceID)
			require.NoError(t, err)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "reopened.Close()", reopened.Close, boundedwait.ForceCloserOf(reopened), boundedwait.NoError)
			restartedRemote := newDestinationUploaderRecorder(destination)
			wrapped, restartedOwner, err := WrapObjectStorageUploader(target, runtime, restartedRemote, fileName, clock.NewUTCClock(), nil, reopened, false)
			require.NoError(t, err)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedOwner.Close()", restartedOwner.Close, boundedwait.ForceCloserOf(restartedOwner), boundedwait.NoError)
			restarted := newObjectStorageUploadManager(wrapped, reopened)
			restartedState, err := restarted.prepareRun(ctx)
			require.NoError(t, err)
			result, err = restarted.uploadIfPending(ctx, path, generation, restartedState, true)
			require.NoError(t, err)
			require.Equal(t, uploadAttemptComplete, result.state)
			require.Equal(t, path, result.localCompressedPathToRemove)
			require.Empty(t, restartedRemote.pathsFor(destination))
			spooled, err := os.ReadFile(filepath.Join(spoolDirectory, filepath.Base(path)))
			require.NoError(t, err)
			require.Equal(t, payload, spooled)
			info, err = os.Stat(archivedPath)
			require.NoError(t, err)
			require.True(t, info.ModTime().Equal(unchangedTime), "restart must retain the completed archive step")
			current, err = reopened.Load(ctx, key)
			require.NoError(t, err)
			require.True(t, current.DestinationComplete(destination))
			_, found, err := reopened.LoadUploadProgress(ctx, key, destination)
			require.NoError(t, err)
			require.False(t, found, "completed cursor replaces the intermediate progress")
		})
	}
}

func TestLifecycleCloseFinishesEveryDurableHandoffWithoutDeliveringSpoolBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const destination = objectstorage.DestinationID("s3-current")
		base := resolvedTempDirForTest(t)
		fileName := logpattern.FileNamePattern{Dir: resolvedTempDirForTest(t), Target: "app", Extension: "log"}
		path := filepath.Join(fileName.Dir, "app_20260927_1.log.gz")
		writeGzipFile(t, path, []byte("accepted durable handoff"))
		writeFileSeriesCheckpointLog(t, fileName.Dir, fileName.Target, "20260927", 2)
		store := newTestFileCheckpointStore(t)
		require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{destination}, nil))
		target := objectstorage.UploadTargetConfig{
			Enabled: true,
			CompressedLogFileUploadFailureStore: objectstorage.CompressedLogFileUploadFailureStoreConfig{
				DurableType: []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool},
				DurableStoreConfig: objectstorage.CompressedLogFileUploadFailureDurableStoreConfig{
					FileArchiveConfig: objectstorage.CompressedLogFileUploadFailureArchiveConfig{
						RootPath: base, LoggerContract: pkglogger.LoggerContractSync.String(), LifecycleInterval: time.Hour,
					},
				},
			},
		}
		spoolConfig := runnerCompressedLogFileUploadSpoolTestConfig(base, "heterolog", pkglogger.LoggerContractSync)
		runtime := runnerCompressedLogFileUploadSpoolTestRuntimes(t, spoolConfig)
		remote := newDestinationUploaderRecorder(destination)
		remote.failNext(destination, errors.New("temporary remote failure"))
		wrapped, owner, err := WrapObjectStorageUploader(target, runtime, remote, fileName, clock.NewUTCClock(), nil, store, false)
		require.NoError(t, err)
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "owner.Close()", owner.Close, boundedwait.ForceCloserOf(owner), boundedwait.NoError)
		manager := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: fileName, Config: testFileSeriesManagerConfig(base, "app", 0, 0, 0, false),
			Uploader: wrapped, CheckpointStore: store,
		})
		// Closeを始める前に失敗しても、受理済みの再試行を中止してから終了します。
		forceCloseManagerOnFailure(t, manager)
		spoolRoot, err := objectstoragespool.NewRoot(base, "heterolog", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
		require.NoError(t, err)
		spoolRoot, err = spoolRoot.ForDestination(destination)
		require.NoError(t, err)
		staging := filepath.Join(spoolRoot.Path(), ".staging")
		require.NoError(t, os.Remove(staging))
		require.NoError(t, os.WriteFile(staging, []byte("temporarily blocks spool"), 0o600))
		_, err = manager.RunOnce(ctx)
		require.Error(t, err)
		archiveRoot, err := compressedlogarchive.NewRoot(base, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
		require.NoError(t, err)
		archiveRoot, err = archiveRoot.ForDestination(destination)
		require.NoError(t, err)
		archived := filepath.Join(archiveRoot.RetryableDir(), filepath.Base(path))
		unchanged := time.Date(2025, time.September, 27, 0, 0, 0, 0, time.UTC)
		require.NoError(t, os.Chtimes(archived, unchanged, unchanged))
		closed := closeInBackgroundForTest(t, "manager.Close()", manager.Close, manager.ForceClose)
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close discarded the unfinished spool handoff: %v", err)
		default:
		}
		require.NoError(t, os.Remove(staging))
		require.NoError(t, os.Mkdir(staging, 0o700))
		require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "receive from closed"))
		require.NoFileExists(t, path)
		require.FileExists(t, filepath.Join(spoolRoot.ActiveDir(), filepath.Base(path)))
		require.Equal(t, []string{path}, remote.pathsFor(destination), "shutdown completes the handoff without delivering the stored spool backlog")
		info, err := os.Stat(archived)
		require.NoError(t, err)
		require.True(t, info.ModTime().Equal(unchanged), "the completed archive handoff is not repeated")
	})
}
