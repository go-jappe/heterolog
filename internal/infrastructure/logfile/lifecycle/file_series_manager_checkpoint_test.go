package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestFileSeriesManagerNewDestinationSkipsExistingRawFilesAndReenabledDestinationResumes(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "per-file-destinations"
	const primary = objectstorage.DestinationID("primary")
	const secondary = objectstorage.DestinationID("secondary")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	first := writeFileSeriesCheckpointLog(t, directory, target, period, 1)
	second := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
	third := writeFileSeriesCheckpointLog(t, directory, target, period, 3)
	store, err := checkpointstore.NewFilesystemStore(newCheckpointDirectoryForTest(t, root), "series")
	require.NoError(t, err)
	remote := newDestinationUploaderRecorder(primary)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: remote, CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	_, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{first, second}, remote.pathsFor(primary))

	// 新宛先は登録前の旧世代を完了済みとします。既存宛先の完了は取り消しません。
	remote = newDestinationUploaderRecorder(primary, secondary)
	manager.objectStorageUpload.stateMu.Lock()
	manager.objectStorageUpload.uploader = remote
	manager.objectStorageUpload.stateMu.Unlock()
	_, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, remote.pathsFor(primary))
	require.Empty(t, remote.pathsFor(secondary))
	fourth := writeFileSeriesCheckpointLog(t, directory, target, period, 4)
	_, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{third}, remote.pathsFor(primary))
	require.Equal(t, []string{third}, remote.pathsFor(secondary))

	// 無効中は送信しません。再有効化ではその間のファイルを飛ばしません。
	manager.objectStorageUpload.stateMu.Lock()
	manager.objectStorageUpload.uploader = nil
	manager.objectStorageUpload.stateMu.Unlock()
	writeFileSeriesCheckpointLog(t, directory, target, period, 5)
	_, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{third}, remote.pathsFor(primary))
	manager.objectStorageUpload.stateMu.Lock()
	manager.objectStorageUpload.uploader = remote
	manager.objectStorageUpload.stateMu.Unlock()
	_, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{third, fourth}, remote.pathsFor(primary))
	require.Equal(t, []string{third, fourth}, remote.pathsFor(secondary))
	for index := uint64(1); index <= 4; index++ {
		record, err := store.Load(ctx, fileCheckpointKey(fileGeneration{Period: period, Index: index}, false))
		require.NoError(t, err)
		require.True(t, record.Complete([]objectstorage.DestinationID{primary, secondary}))
	}
}

func TestLifecyclePreparationDoesNotRecordUnsentFiles(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "prepare-without-success"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	first := writeFileSeriesCheckpointLog(t, directory, target, period, 1)
	writeFileSeriesCheckpointLog(t, directory, target, period, 2)
	store, err := checkpointstore.NewFilesystemStore(newCheckpointDirectoryForTest(t, root), "prepare")
	require.NoError(t, err)
	remote := newDestinationUploaderRecorder("primary")
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: remote, CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	lifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	_, err = lifecycle.ReconcileObjectStorageUploadDestinations(ctx)
	require.NoError(t, err)
	record, err := store.Load(ctx, fileCheckpointKey(fileGeneration{Period: period, Index: 1}, false))
	require.NoError(t, err)
	require.Empty(t, record.CompletedDestinationIDs)
	require.Empty(t, remote.pathsFor("primary"))
	_, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{first}, remote.pathsFor("primary"))
}

func TestFileSeriesManagerCheckpointRestartDoesNotReuploadCompletedFiles(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-restart"
		checkpointSource = "default-log-series"
		destination      = objectstorage.DestinationID("primary-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	first := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
	second := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
	_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 3)
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	store, err := checkpointstore.NewFilesystemStore(
		checkpointDirectory,
		checkpointSource,
	)
	require.NoError(t, err)

	firstUploader := newDestinationUploaderRecorder(destination)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        firstUploader,
		CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	report, err := firstManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.ObjectStorageUploads, 2)
	require.Equal(t, []string{first, second}, firstUploader.pathsFor(destination))

	// process再起動を模してFileSeriesManagerとCheckpointStore instanceを作り直しても、filesystem上の
	// checkpointから完了済みfileを判定し、remoteへ再送しません。
	restartedStore, err := checkpointstore.NewFilesystemStore(
		checkpointDirectory,
		checkpointSource,
	)
	require.NoError(t, err)
	restartedUploader := newDestinationUploaderRecorder(destination)
	restartedManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        restartedUploader,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)
	report, err = restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, restartedUploader.pathsFor(destination))
}

func TestFileSeriesManagerCheckpointDestinationsAdvanceIndependently(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const (
			target           = "checkpoint-independent"
			checkpointSource = "default-log-series"
			successful       = objectstorage.DestinationID("successful-archive")
			retrying         = objectstorage.DestinationID("retrying-archive")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))
		period := currentLogDate()
		_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
		second := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
		third := writeFileSeriesCheckpointLog(t, logDir, target, period, 3)
		_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 4)

		store, err := checkpointstore.NewFilesystemStore(
			newCheckpointDirectoryForTest(t, root),
			checkpointSource,
		)
		require.NoError(t, err)
		initialGeneration := fileGeneration{Period: period, Index: 1}
		destinations := []objectstorage.DestinationID{successful, retrying}
		_, err = store.AdvanceCompletionCursor(ctx, fileCheckpointKey(initialGeneration, false), destinations)
		require.NoError(t, err)
		uploadErr := errors.New("retrying destination is unavailable")
		recorder := newDestinationUploaderRecorder(successful, retrying)
		recorder.failNext(retrying, uploadErr)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
			Uploader:        recorder,
			CheckpointStore: store,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)

		report, err := manager.RunOnce(ctx)
		require.ErrorIs(t, err, uploadErr)
		require.Len(t, report.ObjectStorageUploads, 2)
		require.Equal(t, []string{second, third}, recorder.pathsFor(successful))
		require.Equal(t, []string{second}, recorder.pathsFor(retrying))

		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		// 成功済み宛先は再送せず、失敗した宛先だけを先行世代から順に送ります。
		require.Equal(t, []string{second, third}, recorder.pathsFor(successful))
		require.Equal(t, []string{second, second, third}, recorder.pathsFor(retrying))
	})
}

func TestFileSeriesManagerCompressionSwitchSharesGenerationCompletionWithoutSkippingFailedRaw(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "generation-cursor-compression-switch"
	const primary, retrying = objectstorage.DestinationID("primary"), objectstorage.DestinationID("retrying")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	raw := writeFileSeriesCheckpointLog(t, directory, target, period, 1)
	next := filepath.Join(directory, target+"_"+period+"_2.log.gz")
	writeGzipFile(t, next, []byte("following generation"))
	latest := writeFileSeriesCheckpointLog(t, directory, target, period, 3)
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	store, err := checkpointstore.NewFilesystemStore(checkpointDirectory, target)
	require.NoError(t, err)
	remote := newDestinationUploaderRecorder(primary, retrying)
	failure := errors.New("raw upload temporarily fails")
	remote.failNext(retrying, failure)
	pattern := logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"}
	first := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: pattern, Config: testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: remote, CheckpointStore: store,
	})
	_, err = first.RunOnce(ctx)
	require.ErrorIs(t, err, failure)
	require.Equal(t, []string{raw, next}, remote.pathsFor(primary))
	require.Equal(t, []string{raw}, remote.pathsFor(retrying))
	require.FileExists(t, raw)
	require.FileExists(t, next)
	require.ErrorIs(t, first.ForceClose(), pkgshutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "first.Close()", first.Close, boundedwait.ForceCloserOf(first)), pkgshutdown.ErrForcedClose)

	restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, target)
	require.NoError(t, err)
	restartedRemote := newDestinationUploaderRecorder(primary, retrying)
	restarted := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: pattern, Config: testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: restartedRemote, CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted), boundedwait.NoError)
	report, err := restarted.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, restartedRemote.pathsFor(primary), "raw completion already covers the newly compressed representation")
	require.Equal(t, []string{raw + ".gz", next}, restartedRemote.pathsFor(retrying), "the failed raw generation must complete before the following generation")
	require.Len(t, report.UploadedFileRemovals, 2)
	require.NoFileExists(t, raw)
	require.NoFileExists(t, raw+".gz")
	require.NoFileExists(t, next)
	require.FileExists(t, latest)
}

func TestFileSeriesManagerCheckpointRestartResumesFailedDestinationWithoutReuploadingCompletedDestination(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-restart-after-upload-failure"
		checkpointSource = "default-log-series"
		successful       = objectstorage.DestinationID("successful-archive")
		retrying         = objectstorage.DestinationID("retrying-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
	failedPath := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
	followingPath := writeFileSeriesCheckpointLog(t, logDir, target, period, 3)
	_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 4)

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	store, err := checkpointstore.NewFilesystemStore(
		checkpointDirectory,
		checkpointSource,
	)
	require.NoError(t, err)
	initialGeneration := fileGeneration{Period: period, Index: 1}
	destinations := []objectstorage.DestinationID{successful, retrying}
	_, err = store.AdvanceCompletionCursor(ctx, fileCheckpointKey(initialGeneration, false), destinations)
	require.NoError(t, err)

	uploadErr := errors.New("retrying destination is unavailable")
	firstUploader := newDestinationUploaderRecorder(destinations...)
	firstUploader.failNext(retrying, uploadErr)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        firstUploader,
		CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	report, err := firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, uploadErr)
	require.Len(t, report.ObjectStorageUploads, 2)
	require.Equal(t, []string{failedPath, followingPath}, firstUploader.pathsFor(successful))
	require.Equal(t, []string{failedPath}, firstUploader.pathsFor(retrying))

	// process再起動を模してmanager・uploader・checkpoint storeをすべて作り直します。
	// 成功済み宛先は永続checkpointから世代3までの完了を復元して再送せず、失敗した
	// 宛先だけがlocalに残る世代2から世代順に再送してcheckpointを前進させます。
	require.ErrorIs(t, firstManager.ForceClose(), pkgshutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)), pkgshutdown.ErrForcedClose)
	restartedStore, err := checkpointstore.NewFilesystemStore(
		checkpointDirectory,
		checkpointSource,
	)
	require.NoError(t, err)
	restartedUploader := newDestinationUploaderRecorder(destinations...)
	restartedManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        restartedUploader,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)
	report, err = restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.ObjectStorageUploads, 2)
	require.Empty(t, restartedUploader.pathsFor(successful))
	require.Equal(t, []string{failedPath, followingPath}, restartedUploader.pathsFor(retrying))
}

func TestFileSeriesManagerCheckpointResumesInOrderAfterCompressionFailure(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-compression-failure"
		checkpointSource = "default-log-series"
		primary          = objectstorage.DestinationID("primary-archive")
		secondary        = objectstorage.DestinationID("secondary-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	first := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
	failed := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
	followingCompressed := filepath.Join(
		logDir,
		target+"_"+period+"_3.log"+pkgfsstore.ExtensionCompressGzip,
	)
	writeGzipFile(t, followingCompressed, []byte("already compressed following file"))
	_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 4)

	store, err := checkpointstore.NewFilesystemStore(
		newCheckpointDirectoryForTest(t, root),
		checkpointSource,
	)
	require.NoError(t, err)
	destinations := []objectstorage.DestinationID{primary, secondary}

	recorder := newDestinationUploaderRecorder(destinations...)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader:        recorder,
		CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)
	realCompress := manager.compressFile
	compressionErr := errors.New("compression temporarily unavailable")
	manager.compressFile = func(ctx context.Context, path string) (string, error) {
		if path == failed {
			return "", compressionErr
		}
		return realCompress(ctx, path)
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, compressionErr)
	var deferred *deferredFileProcessingError
	require.ErrorAs(t, err, &deferred)
	require.Equal(t, fileGeneration{Period: period, Index: 2}, deferred.generation)
	require.Equal(t, failed, deferred.path)
	require.Len(t, report.Compressed, 1)
	firstCompressed := first + pkgfsstore.ExtensionCompressGzip
	require.Equal(t, firstCompressed, report.Compressed[0].CompressedPath)
	require.Len(t, report.ObjectStorageUploads, 1)
	for _, destination := range destinations {
		require.Equal(t, []string{firstCompressed}, recorder.pathsFor(destination))
	}
	require.FileExists(t, failed)
	require.FileExists(t, followingCompressed)

	// process再起動を模してManagerを作り直してもsourceとcheckpointから失敗位置を復元します。
	// 失敗世代を完了してから、後続fileを送信します。
	require.ErrorIs(t, manager.ForceClose(), pkgshutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager)), pkgshutdown.ErrForcedClose)
	restartedStore, err := checkpointstore.NewFilesystemStore(store.Directory(), store.SourceID())
	require.NoError(t, err)
	restartedManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader:        recorder,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)
	report, err = restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Len(t, report.Compressed, 1)
	failedCompressed := failed + pkgfsstore.ExtensionCompressGzip
	require.Equal(t, failedCompressed, report.Compressed[0].CompressedPath)
	require.Len(t, report.ObjectStorageUploads, 2)
	for _, destination := range destinations {
		require.Equal(
			t,
			[]string{firstCompressed, failedCompressed, followingCompressed},
			recorder.pathsFor(destination),
		)
	}
}

func TestFileSeriesManagerCheckpointCommitFailureRecoversWithoutReuploadAndProtectsFileFromPurge(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-commit-failure"
		checkpointSource = "default-log-series"
		destination      = objectstorage.DestinationID("primary-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	// upload成功後の保持期限もファイル名の日付で判定されます。
	period := time.Now().UTC().AddDate(0, 0, -3).Format("20060102")
	retryPath := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
	_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 3)
	old := time.Now().Add(-72 * time.Hour)
	require.NoError(t, os.Chtimes(retryPath, old, old))

	realStore, err := checkpointstore.NewFilesystemStore(
		newCheckpointDirectoryForTest(t, root),
		checkpointSource,
	)
	require.NoError(t, err)
	commitErr := errors.New("checkpoint storage is temporarily unavailable")
	testClock := &managerCheckpointClock{now: time.Now()}
	store := &failOnceCheckpointStore{
		Store:        realStore,
		destination:  destination,
		generation:   fileGeneration{Period: period, Index: 2},
		failure:      commitErr,
		failuresLeft: 1,
	}
	recorder := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 1, 0, false),
		Uploader:        recorder,
		CheckpointStore: store,
		Clock:           testClock,
	})
	useShortLogFileProcessingRecovery(t, manager)

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, []string{retryPath}, recorder.pathsFor(destination))
	require.FileExists(t, retryPath)
	require.Empty(t, report.Purge.ByAge.RemovalResults)

	// checkpointだけを独立して再試行し、remote成功を繰り返しません。
	require.Eventually(t, func() bool {
		return !manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(fileGeneration{Period: period, Index: 2}, false))
	}, boundedwait.HangTimeout, 5*time.Millisecond)
	testClock.now = testClock.now.Add(24 * time.Hour)
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{retryPath}, recorder.pathsFor(destination))
	require.True(t, report.Purge.ByAge.Ran)
	require.Len(t, report.Purge.ByAge.RemovalResults, 1)
	require.Equal(t, retryPath, report.Purge.ByAge.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByAge.RemovalResults[0].Err)
	require.NoFileExists(t, retryPath)
}

func TestFileSeriesManagerDurableSaveDeadlineKeepsCompressedFileAndCheckpointsPending(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "durable-save-deadline"
		const sourceID = "durable-save-deadline-series"
		const destination = objectstorage.DestinationID("primary-archive")
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))
		// 保持期間1日を過ぎた期間の世代にし、保護がなければ同じ周期のage purgeで削除される状態にします。
		period := logDateDaysBefore(10)
		compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
		writeGzipFile(t, compressedPath, []byte("retain until durable save succeeds"))
		activePath := writeFileSeriesCheckpointLog(t, logDir, target, currentLogDate(), 1)
		checkpointDir := newCheckpointDirectoryForTest(t, root)

		compressedStore, err := checkpointstore.NewFilesystemStore(checkpointDir, sourceID)
		require.NoError(t, err)
		generation := fileGeneration{Period: period, Index: 1}
		key := fileCheckpointKey(generation, true)
		remoteErr := errors.New("remote unavailable")
		uploadRecorder := newDestinationUploaderRecorder(destination)
		uploadRecorder.failNext(destination, errors.Join(remoteErr, context.DeadlineExceeded))
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, false),
			Uploader: uploadRecorder, CheckpointStore: compressedStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)

		report, err := manager.RunOnce(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, err, remoteErr)
		require.NoError(t, ctx.Err(), "the durable save deadline is not the runner's parent deadline")
		require.Empty(t, report.ObjectStorageUploads)
		require.Empty(t, report.UploadedFileRemovals)
		// 送信待ちの圧縮fileは、同じ周期のageとdirectory-size purgeでも削除しません。
		require.True(t, report.Purge.ByAge.Ran)
		require.Empty(t, report.Purge.ByAge.RemovalResults)
		require.True(t, report.Purge.ByDirSize.Ran)
		require.Empty(t, report.Purge.ByDirSize.RemovalResults)
		require.FileExists(t, compressedPath)
		require.FileExists(t, activePath)
		checkpoint, err := compressedStore.Load(ctx, key)
		require.NoError(t, err)
		require.Empty(t, checkpoint.CompletedDestinationIDs)

		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.Equal(t, []string{compressedPath, compressedPath}, uploadRecorder.pathsFor(destination))
		require.NoFileExists(t, compressedPath)
		require.FileExists(t, activePath)
	})
}

func TestFileSeriesManagerCheckpointUpdateFailureRetriesWithoutRemoteReupload(
	t *testing.T,
) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "uploaded-generation-commit-failure"
		checkpointSource = "uploaded-generation-commit-failure-series"
		destination      = objectstorage.DestinationID("primary-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	generation := fileGeneration{Period: period, Index: 1}
	compressedPath := filepath.Join(
		logDir,
		target+"_"+period+"_1.log"+pkgfsstore.ExtensionCompressGzip,
	)
	writeGzipFile(t, compressedPath, []byte("archive"))
	active := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)

	realStore, err := checkpointstore.NewFilesystemStore(
		newCheckpointDirectoryForTest(t, root),
		checkpointSource,
	)
	require.NoError(t, err)
	commitErr := errors.New("uploaded generation storage is temporarily unavailable")
	store := &failOnceCheckpointStore{
		Store:        realStore,
		destination:  destination,
		generation:   generation,
		failure:      commitErr,
		compressed:   true,
		failuresLeft: 1,
	}
	uploadRecorder := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        uploadRecorder,
		CheckpointStore: store,
	})
	useShortLogFileProcessingRecovery(t, manager)

	firstReport, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, []string{compressedPath}, uploadRecorder.pathsFor(destination))
	require.Len(t, firstReport.ObjectStorageUploads, 1)
	require.Empty(t, firstReport.UploadedFileRemovals)
	require.FileExists(t, compressedPath)

	waitLogFileProcessingRecovery(t, manager, generation)
	// remote成功のprocess内証明を保持しているため、失敗したcheckpoint commitだけを再試行します。
	require.Equal(t, []string{compressedPath}, uploadRecorder.pathsFor(destination))
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, active)
}

type managerCheckpointClock struct {
	now time.Time
}

func (c *managerCheckpointClock) Now() time.Time {
	return c.now
}

func TestFileSeriesManagerCheckpointFileOrderIsNumericAndExcludesUnrelatedTargetPrefix(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "checkpoint-order"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	second := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
	tenth := writeFileSeriesCheckpointLog(t, logDir, target, period, 10)
	_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 11)

	// GlobByTargetには一致するもののcanonicalなperiod/indexを持たないfileは対象外です。
	for _, name := range []string{
		target + "_memo_1.log",
		target + "-other_" + period + "_1.log",
		target + "_" + period + "_2.backup.log",
		target + "_" + period + "_0.log",
		target + "_" + period + "_02.log",
		target + "_" + period + "_+2.log",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(logDir, name), []byte("unrelated"), 0o600))
	}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)

	files, err := manager.listLifecycleTargetFiles()
	require.NoError(t, err)
	require.Equal(t, []string{second, tenth}, files)
}

func TestFileSeriesManagerCheckpointNonCanonicalHigherIndexDoesNotHideActiveFile(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "checkpoint-active-alias"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	first := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
	active := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
	require.NoError(t, os.WriteFile(
		filepath.Join(logDir, target+"_"+period+"_010.log"),
		[]byte("non-canonical"),
		0o600,
	))

	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)
	files, err := manager.listLifecycleTargetFiles()
	require.NoError(t, err)
	require.Equal(t, []string{first}, files)
	require.FileExists(t, active)
}

func TestFileSeriesManagerCheckpointContinuesAcrossCompressionFormatChange(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-compression-transition"
		checkpointSource = "default-log-series"
		destination      = objectstorage.DestinationID("primary-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()

	// user定義形式の利用時に圧縮された未upload fileと、zstdへ変更後のfileを同じ系列へ置きます。
	legacyCustom := filepath.Join(logDir, target+"_"+period+"_1.log.br")
	currentZstd := filepath.Join(logDir, target+"_"+period+"_2.log"+pkgfsstore.ExtensionCompressZstd)
	for _, path := range []string{legacyCustom, currentZstd} {
		require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	}
	// 最新の未圧縮fileはactiveの可能性があるため、従来どおり今回の対象外です。
	active := writeFileSeriesCheckpointLog(t, logDir, target, period, 3)

	store, err := checkpointstore.NewFilesystemStore(
		newCheckpointDirectoryForTest(t, root),
		checkpointSource,
	)
	require.NoError(t, err)

	uploaderRecorder := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root,
			target,
			pkgfsstore.FileRotationDaily,
			10,
			0,
			0,
			true,
			pkgfsstore.CompressZstd,
		),
		Uploader:        uploaderRecorder,
		CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{legacyCustom, currentZstd}, uploaderRecorder.pathsFor(destination))
	require.Len(t, report.ObjectStorageUploads, 2)
	require.Equal(t, []string{legacyCustom, currentZstd}, []string{
		report.ObjectStorageUploads[0].Path,
		report.ObjectStorageUploads[1].Path,
	})
	require.FileExists(t, active)
}

func TestFileSeriesManagerCheckpointCompletesOnlyFirstCompressedFileForSameGeneration(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-compression-peer"
		checkpointSource = "default-log-series"
		destination      = objectstorage.DestinationID("primary-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	legacyCustom := filepath.Join(logDir, target+"_"+period+"_1.log.br")
	currentZstd := filepath.Join(logDir, target+"_"+period+"_1.log"+pkgfsstore.ExtensionCompressZstd)
	for _, path := range []string{legacyCustom, currentZstd} {
		require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	}
	_ = writeFileSeriesCheckpointLog(t, logDir, target, period, 2)

	store, err := checkpointstore.NewFilesystemStore(
		newCheckpointDirectoryForTest(t, root),
		checkpointSource,
	)
	require.NoError(t, err)

	uploaderRecorder := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root,
			target,
			pkgfsstore.FileRotationDaily,
			10,
			0,
			0,
			true,
			pkgfsstore.CompressZstd,
		),
		Uploader:        uploaderRecorder,
		CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{legacyCustom}, uploaderRecorder.pathsFor(destination))
	require.Len(t, report.ObjectStorageUploads, 1)
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, legacyCustom, report.UploadedFileRemovals[0].File.Path)
	require.NoFileExists(t, legacyCustom)
	require.FileExists(t, currentZstd)

	// 同じ世代の圧縮方式が違っても、永続化した完了位置により再送しません。
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{legacyCustom}, uploaderRecorder.pathsFor(destination))
	require.Empty(t, report.ObjectStorageUploads)
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, currentZstd, report.UploadedFileRemovals[0].File.Path)
	require.NoFileExists(t, currentZstd)
}

func TestFileSeriesManagerCheckpointRestartRetriesOnlyFailedDestinationForSingleCompressedFile(
	t *testing.T,
) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-compression-peer-restart"
		checkpointSource = "compression-peer-restart-series"
		primary          = objectstorage.DestinationID("primary-archive")
		secondary        = objectstorage.DestinationID("secondary-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	generation := fileGeneration{Period: period, Index: 1}
	legacyCustom := filepath.Join(logDir, target+"_"+period+"_1.log.br")
	require.NoError(t, os.WriteFile(legacyCustom, []byte(filepath.Base(legacyCustom)), 0o600))
	active := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	store, err := checkpointstore.NewFilesystemStore(
		checkpointDirectory,
		checkpointSource,
	)
	require.NoError(t, err)
	destinations := []objectstorage.DestinationID{primary, secondary}

	uploadErr := errors.New("secondary destination unavailable")
	firstUploader := newDestinationUploaderRecorder(destinations...)
	firstUploader.failNext(secondary, uploadErr)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root,
			target,
			pkgfsstore.FileRotationDaily,
			10,
			0,
			0,
			true,
			pkgfsstore.CompressZstd,
		),
		Uploader:        firstUploader,
		CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)

	firstReport, err := firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, uploadErr)
	require.Empty(t, firstReport.UploadedFileRemovals)
	require.Equal(t, []string{legacyCustom}, firstUploader.pathsFor(primary))
	require.Equal(t, []string{legacyCustom}, firstUploader.pathsFor(secondary))
	require.FileExists(t, legacyCustom)

	require.ErrorIs(t, firstManager.ForceClose(), pkgshutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)), pkgshutdown.ErrForcedClose)
	restartedStore, err := checkpointstore.NewFilesystemStore(
		checkpointDirectory,
		checkpointSource,
	)
	require.NoError(t, err)
	persisted, err := restartedStore.Load(ctx, fileCheckpointKey(generation, true))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{primary}, persisted.CompletedDestinationIDs)

	restartedUploader := newDestinationUploaderRecorder(destinations...)
	restartedManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root,
			target,
			pkgfsstore.FileRotationDaily,
			10,
			0,
			0,
			true,
			pkgfsstore.CompressZstd,
		),
		Uploader:        restartedUploader,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)

	restartedReport, err := restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, restartedUploader.pathsFor(primary))
	require.Equal(
		t,
		[]string{legacyCustom},
		restartedUploader.pathsFor(secondary),
	)
	require.Len(t, restartedReport.UploadedFileRemovals, 1)
	require.NoFileExists(t, legacyCustom)
	require.FileExists(t, active)
}

func TestFileSeriesManagerCheckpointProcessesOtherFilesWhilePeerIsProtected(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const (
			target           = "checkpoint-protected-compression-peer"
			checkpointSource = "protected-compression-peer-series"
			destination      = objectstorage.DestinationID("primary-archive")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))
		period := currentLogDate()
		firstCompressedLog := filepath.Join(logDir, target+"_"+period+"_1.log.br")
		protectedCompressedLog := filepath.Join(
			logDir,
			target+"_"+period+"_2.log"+pkgfsstore.ExtensionCompressZstd,
		)
		secondGeneration := filepath.Join(
			logDir,
			target+"_"+period+"_3.log"+pkgfsstore.ExtensionCompressZstd,
		)
		for _, path := range []string{firstCompressedLog, protectedCompressedLog, secondGeneration} {
			require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
		}
		active := writeFileSeriesCheckpointLog(t, logDir, target, period, 4)

		store, err := checkpointstore.NewFilesystemStore(
			newCheckpointDirectoryForTest(t, root),
			checkpointSource,
		)
		require.NoError(t, err)
		uploadRecorder := newDestinationUploaderRecorder(destination)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config: testFileSeriesManagerConfigWithCompressionFormat(
				root,
				target,
				pkgfsstore.FileRotationDaily,
				10,
				0,
				0,
				true,
				pkgfsstore.CompressZstd,
			),
			Uploader:        uploadRecorder,
			CheckpointStore: store,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)
		require.NoError(t, manager.validate())
		blocker := appendlog.NewDirectoryGuardToken()
		manager.directoryGuard.ProtectPaths(blocker, protectedCompressedLog)

		firstReport, err := manager.RunOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{firstCompressedLog}, uploadRecorder.pathsFor(destination))
		require.Len(t, firstReport.UploadedFileRemovals, 1)
		require.NoFileExists(t, firstCompressedLog)
		require.FileExists(t, protectedCompressedLog)
		require.FileExists(t, secondGeneration)
		// 保護中の二件目を飛ばして三件目の完了位置を記録しません。

		manager.directoryGuard.UnprotectPaths(blocker, protectedCompressedLog)
		secondReport, err := manager.RunOnce(ctx)
		require.NoError(t, err)
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.Equal(
			t,
			[]string{firstCompressedLog, protectedCompressedLog, secondGeneration},
			uploadRecorder.pathsFor(destination),
		)
		require.Len(t, secondReport.UploadedFileRemovals, 1)
		require.NoFileExists(t, firstCompressedLog)
		require.NoFileExists(t, protectedCompressedLog)
		require.NoFileExists(t, secondGeneration)
		require.FileExists(t, active)
	})
}

func TestFileSeriesManagerKeepsSingleArchiveClaimThroughUploadCompletion(
	t *testing.T,
) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-group-protected-after-upload"
		checkpointSource = "group-protected-after-upload-series"
		destination      = objectstorage.DestinationID("primary-archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	firstCompressedLog := filepath.Join(logDir, target+"_"+period+"_1.log.br")
	rawPath := strings.TrimSuffix(firstCompressedLog, ".br")
	require.NoError(t, os.WriteFile(firstCompressedLog, []byte(filepath.Base(firstCompressedLog)), 0o600))
	secondGeneration := filepath.Join(
		logDir,
		target+"_"+period+"_2.log"+pkgfsstore.ExtensionCompressZstd,
	)
	require.NoError(t, os.WriteFile(secondGeneration, []byte("later generation"), 0o600))
	active := writeFileSeriesCheckpointLog(t, logDir, target, period, 3)

	store, err := checkpointstore.NewFilesystemStore(
		newCheckpointDirectoryForTest(t, root),
		checkpointSource,
	)
	require.NoError(t, err)

	recorder := newDestinationUploaderRecorder(destination)
	blocker := appendlog.NewDirectoryGuardToken()
	var manager *FileSeriesManager
	uploadClient := &destinationUploaderWithFirstSuccessHook{
		destinationUploaderRecorder: recorder,
		afterFirstSuccess: func() {
			// remote成功後・checkpoint/local削除の前にも、圧縮fileと
			// 不存在のraw共通キーは入口から引き続き同じownerが保持します。
			for _, path := range []string{rawPath, firstCompressedLog} {
				require.False(t, manager.directoryGuard.ProtectPathsIfNoOtherOwnerConflict(
					blocker, nil, []string{path},
				))
			}
		},
	}
	manager = newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root,
			target,
			pkgfsstore.FileRotationDaily,
			10,
			0,
			0,
			true,
			pkgfsstore.CompressZstd,
		),
		Uploader:        uploadClient,
		CheckpointStore: store,
	})
	require.NoError(t, manager.validate())

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(
		t,
		[]string{firstCompressedLog, secondGeneration},
		recorder.pathsFor(destination),
	)
	require.Len(t, report.UploadedFileRemovals, 2)
	require.NoFileExists(t, firstCompressedLog)
	require.NoFileExists(t, secondGeneration)
	require.FileExists(t, active)
	require.True(t, manager.directoryGuard.ProtectPathsIfNoOtherOwnerConflict(
		blocker, nil, []string{rawPath, firstCompressedLog},
	))
	manager.directoryGuard.UnprotectPaths(blocker, rawPath, firstCompressedLog)
}

func TestFileSeriesManagerUsesCurrentDestinationsForUnprocessedGenerationsAfterRestart(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target           = "checkpoint-batch-first-discovery"
		checkpointSource = "batch-first-discovery-series"
		firstDestination = objectstorage.DestinationID("destination-at-first-scan")
		laterDestination = objectstorage.DestinationID("destination-after-restart")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	firstRaw := writeFileSeriesCheckpointLog(t, logDir, target, period, 1)
	secondRaw := writeFileSeriesCheckpointLog(t, logDir, target, period, 2)
	active := writeFileSeriesCheckpointLog(t, logDir, target, period, 3)
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)

	firstStore, err := checkpointstore.NewFilesystemStore(
		checkpointDirectory,
		checkpointSource,
	)
	require.NoError(t, err)
	firstUploader := newDestinationUploaderRecorder(firstDestination)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root,
			target,
			pkgfsstore.FileRotationDaily,
			10,
			0,
			0,
			true,
			pkgfsstore.CompressZstd,
		),
		Uploader:        firstUploader,
		CheckpointStore: firstStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	compressionErr := errors.New("first generation compression failed")
	firstManager.compressFile = func(context.Context, string) (string, error) {
		return "", compressionErr
	}

	_, err = firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, compressionErr)
	require.Empty(t, firstUploader.pathsFor(firstDestination))
	require.FileExists(t, firstRaw)
	require.FileExists(t, secondRaw)

	// process再起動と同時にactive destinationを入れ替えます。両generationとも
	// 前RunOnceでは圧縮に失敗しましたが、過去の宛先は固定せず、
	// 現在のlaterDestinationへlocalに残る両generationを送信します。
	require.ErrorIs(t, firstManager.ForceClose(), pkgshutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)), pkgshutdown.ErrForcedClose)
	restartedStore, err := checkpointstore.NewFilesystemStore(
		checkpointDirectory,
		checkpointSource,
	)
	require.NoError(t, err)
	restartedUploader := newDestinationUploaderRecorder(laterDestination)
	restartedManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config: testFileSeriesManagerConfigWithCompressionFormat(
			root,
			target,
			pkgfsstore.FileRotationDaily,
			10,
			0,
			0,
			true,
			pkgfsstore.CompressZstd,
		),
		Uploader:        restartedUploader,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)

	restartedReport, err := restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{
		firstRaw + pkgfsstore.ExtensionCompressZstd,
		secondRaw + pkgfsstore.ExtensionCompressZstd,
	}, restartedUploader.pathsFor(laterDestination))
	require.Len(t, restartedReport.ObjectStorageUploads, 2)
	require.NoFileExists(t, firstRaw)
	require.NoFileExists(t, secondRaw)
	require.NoFileExists(t, firstRaw+pkgfsstore.ExtensionCompressZstd)
	require.NoFileExists(t, secondRaw+pkgfsstore.ExtensionCompressZstd)
	require.FileExists(t, active)
}

func writeFileSeriesCheckpointLog(
	t *testing.T,
	directory string,
	target string,
	period string,
	index uint64,
) string {
	t.Helper()
	path := filepath.Join(directory, target+"_"+period+"_"+strconv.FormatUint(index, 10)+".log")
	require.NoError(t, os.WriteFile(path, []byte(filepath.Base(path)), 0o600))
	return path
}

type destinationUploaderRecorder struct {
	destinations []objectstorage.DestinationID

	mu              sync.Mutex
	paths           map[objectstorage.DestinationID][]string
	nextFailure     map[objectstorage.DestinationID]error
	nextPanic       map[objectstorage.DestinationID]any
	failureCategory uploader.FailureCategory
}

type destinationUploaderWithFirstSuccessHook struct {
	*destinationUploaderRecorder
	once              sync.Once
	afterFirstSuccess func()
}

func (u *destinationUploaderWithFirstSuccessHook) UploadDestinations(
	ctx context.Context,
	input uploader.UploadRequest,
	destinations []objectstorage.DestinationID,
) uploader.UploadReport {
	report := u.destinationUploaderRecorder.UploadDestinations(ctx, input, destinations)
	u.once.Do(func() {
		if report.Err() == nil && u.afterFirstSuccess != nil {
			u.afterFirstSuccess()
		}
	})
	return report
}

func newDestinationUploaderRecorder(destinations ...objectstorage.DestinationID) *destinationUploaderRecorder {
	return &destinationUploaderRecorder{
		destinations: append([]objectstorage.DestinationID(nil), destinations...),
		paths:        make(map[objectstorage.DestinationID][]string),
		nextFailure:  make(map[objectstorage.DestinationID]error),
		nextPanic:    make(map[objectstorage.DestinationID]any),
	}
}

func (u *destinationUploaderRecorder) DestinationIDs() []objectstorage.DestinationID {
	return append([]objectstorage.DestinationID(nil), u.destinations...)
}

// このfixtureはS3宛先を模します。IDの表記からbackendを判定しません。
func (u *destinationUploaderRecorder) DestinationService(id objectstorage.DestinationID) (objectstorage.UploadTarget, bool) {
	for _, destination := range u.destinations {
		if destination == id {
			return objectstorage.UploadTargetS3, true
		}
	}
	return "", false
}

func (u *destinationUploaderRecorder) Upload(ctx context.Context, input uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}

func (u *destinationUploaderRecorder) UploadDestinations(
	_ context.Context,
	input uploader.UploadRequest,
	destinations []objectstorage.DestinationID,
) uploader.UploadReport {
	u.mu.Lock()
	defer u.mu.Unlock()

	report := uploader.UploadReport{Results: make([]uploader.DestinationResult, 0, len(destinations))}
	for _, destination := range destinations {
		u.paths[destination] = append(u.paths[destination], input.FilePath())
		if panicValue, shouldPanic := u.nextPanic[destination]; shouldPanic {
			delete(u.nextPanic, destination)
			panic(panicValue)
		}
		failure := u.nextFailure[destination]
		delete(u.nextFailure, destination)
		var category uploader.FailureCategory
		if failure != nil {
			category = u.failureCategory
		}
		report.Results = append(report.Results, uploader.DestinationResult{
			DestinationID:   destination,
			Err:             failure,
			FailureCategory: category,
		})
	}
	return report
}

func (u *destinationUploaderRecorder) failNext(destination objectstorage.DestinationID, err error) {
	u.mu.Lock()
	u.nextFailure[destination] = err
	u.mu.Unlock()
}

func (u *destinationUploaderRecorder) panicNext(
	destination objectstorage.DestinationID,
	panicValue any,
) {
	u.mu.Lock()
	u.nextPanic[destination] = panicValue
	u.mu.Unlock()
}

func (u *destinationUploaderRecorder) pathsFor(destination objectstorage.DestinationID) []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.paths[destination]...)
}

// failOnceCheckpointStoreは、選択した更新を受理する前に失敗します。
type failOnceCheckpointStore struct {
	checkpointstore.Store
	destination  objectstorage.DestinationID
	generation   fileGeneration
	compressed   bool
	failure      error
	mu           sync.Mutex
	failuresLeft int
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *failOnceCheckpointStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *failOnceCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	s.mu.Lock()
	fail := false
	for _, destination := range destinations {
		if s.failuresLeft > 0 && destination == s.destination && key == fileCheckpointKey(s.generation, s.compressed) {
			s.failuresLeft--
			fail = true
			break
		}
	}
	s.mu.Unlock()
	if fail {
		return checkpointstore.Checkpoint{}, s.failure
	}
	return s.Store.AdvanceCompletionCursor(ctx, key, destinations)
}
