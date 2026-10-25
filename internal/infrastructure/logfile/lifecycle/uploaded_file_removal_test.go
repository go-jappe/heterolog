package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestFileSeriesManagerKeepsCompressedFileWhenObjectStorageUploadIsDisabled(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "upload-disabled-retains-compressed"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	period := currentLogDate()
	compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	latestPath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, compressedPath, []byte("retained locally"))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)
	report, err := manager.RunOnce(context.Background())

	require.NoError(t, err)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, compressedPath)
	require.FileExists(t, latestPath)
}

func TestFileSeriesManagerKeepsUncompressedFileAfterObjectStorageUpload(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "upload-retains-uncompressed"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	period := currentLogDate()
	rawPath := filepath.Join(logDir, target+"_"+period+"_1.log")
	latestPath := filepath.Join(logDir, target+"_"+period+"_2.log")
	require.NoError(t, os.WriteFile(rawPath, []byte("upload but retain"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))
	uploadRecorder := &recordingUploader{}
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: uploadRecorder,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)

	report, err := manager.RunOnce(context.Background())

	require.NoError(t, err)
	require.Equal(t, []string{rawPath}, uploadRecorder.pathsSnapshot())
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, rawPath)
	require.FileExists(t, latestPath)
}

func TestFileSeriesManagerRemovesCompressedFileOnlyAfterEveryDestinationCompletes(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		root := resolvedTempDirForTest(t)
		const (
			target    = "all-destinations-before-removal"
			source    = "all-destinations-series"
			primary   = objectstorage.DestinationID("primary")
			secondary = objectstorage.DestinationID("secondary")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))

		// 保持期間1日を過ぎた期間の世代にし、保護がなければ同じ周期のage purgeで削除される状態にします。
		period := logDateDaysBefore(10)
		cursor := fileGeneration{Period: period, Index: 1}
		compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
		latestPath := filepath.Join(logDir, target+"_"+currentLogDate()+"_1.log")
		writeGzipFile(t, compressedPath, []byte("archive"))
		require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

		checkpointStore, err := checkpointstore.NewFilesystemStore(
			newCheckpointDirectoryForTest(t, root),
			string(source),
		)
		require.NoError(t, err)
		destinations := []objectstorage.DestinationID{primary, secondary}
		uploadErr := errors.New("secondary destination unavailable")
		uploadRecorder := newDestinationUploaderRecorder(destinations...)
		uploadRecorder.failNext(secondary, uploadErr)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:          testFileSeriesManagerConfig(root, target, 1, 1, 1, false),
			Uploader:        uploadRecorder,
			CheckpointStore: checkpointStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)

		report, err := manager.RunOnce(context.Background())
		require.ErrorIs(t, err, uploadErr)
		require.Empty(t, report.UploadedFileRemovals)
		require.FileExists(t, compressedPath)
		// 一部の宛先だけが完了した圧縮fileは、同じ周期のageとdirectory-size purgeでも削除しません。
		require.True(t, report.Purge.ByAge.Ran)
		require.Empty(t, report.Purge.ByAge.RemovalResults)
		require.True(t, report.Purge.ByDirSize.Ran)
		require.Empty(t, report.Purge.ByDirSize.RemovalResults)
		require.Equal(t, []string{compressedPath}, uploadRecorder.pathsFor(primary))
		require.Equal(t, []string{compressedPath}, uploadRecorder.pathsFor(secondary))
		checkpointState, err := checkpointStore.Load(context.Background(), fileCheckpointKey(cursor, true))
		require.NoError(t, err)
		require.True(t, checkpointState.DestinationComplete(primary))
		require.False(t, checkpointState.DestinationComplete(secondary))

		uploadRecorder.failNext(secondary, uploadErr)
		// 受理済みの再送と削除は、次周期の走査なしで終了時に完了させます。
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.CloseLogFileProcessingRecovery()", manager.CloseLogFileProcessingRecovery, manager.ForceClose))
		require.Equal(t, []string{compressedPath}, uploadRecorder.pathsFor(primary))
		require.Equal(
			t,
			[]string{compressedPath, compressedPath, compressedPath},
			uploadRecorder.pathsFor(secondary),
		)
		require.NoFileExists(t, compressedPath)
		require.FileExists(t, latestPath)
		// ファイルを削除した後も、宛先ごとの完了位置を保持します。
		checkpointState, err = checkpointStore.Load(context.Background(), fileCheckpointKey(cursor, true))
		require.NoError(t, err)
		require.ElementsMatch(t, []objectstorage.DestinationID{primary, secondary}, checkpointState.CompletedDestinationIDs)
	})
}

func TestFileSeriesManagerRetriesOnlyFailedDestinationAfterPartialSuccess(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		root := resolvedTempDirForTest(t)
		const (
			target    = "partial-success-retry"
			source    = string("partial-success-retry-series")
			primary   = objectstorage.DestinationID("primary")
			secondary = objectstorage.DestinationID("secondary")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))

		period := currentLogDate()
		cursor := fileGeneration{Period: period, Index: 1}
		compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
		latestPath := filepath.Join(logDir, target+"_"+period+"_2.log")
		writeGzipFile(t, compressedPath, []byte("archive content"))
		require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

		store, err := checkpointstore.NewFilesystemStore(newCheckpointDirectoryForTest(t, root), source)
		require.NoError(t, err)
		uploadErr := errors.New("secondary destination unavailable")
		uploadRecorder := newDestinationUploaderRecorder(primary, secondary)
		uploadRecorder.failNext(secondary, uploadErr)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
			Uploader:        uploadRecorder,
			CheckpointStore: store,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)

		firstReport, err := manager.RunOnce(context.Background())
		require.ErrorIs(t, err, uploadErr)
		require.Empty(t, firstReport.UploadedFileRemovals)
		require.FileExists(t, compressedPath)
		checkpoint, err := store.Load(context.Background(), fileCheckpointKey(cursor, true))
		require.NoError(t, err)
		require.True(t, checkpoint.DestinationComplete(primary))
		require.False(t, checkpoint.DestinationComplete(secondary))

		require.NoError(t, boundedwait.CloseInBubble(t, "manager.CloseLogFileProcessingRecovery()", manager.CloseLogFileProcessingRecovery, manager.ForceClose))
		require.Equal(t, []string{compressedPath}, uploadRecorder.pathsFor(primary))
		require.Equal(
			t,
			[]string{compressedPath, compressedPath},
			uploadRecorder.pathsFor(secondary),
		)
		require.NoFileExists(t, compressedPath)
		require.FileExists(t, latestPath)
	})
}

func TestFileSeriesManagerForgetsUploadProofWhenUploadedFileIsAlreadyMissing(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "uploaded-file-already-missing"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	period := currentLogDate()
	compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	latestPath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, compressedPath, []byte("uploaded before local removal"))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: removeUploadedPathUploader{},
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)
	report, err := manager.RunOnce(context.Background())

	require.NoError(t, err)
	require.Len(t, report.UploadedFileRemovals, 1)

	require.NoError(t, report.UploadedFileRemovals[0].Err)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, latestPath)
	require.Empty(t, manager.objectStorageUpload.uploadedPathDestinationIDs(compressedPath))
	record, err := manager.objectStorageUpload.checkpointStore.Load(context.Background(), fileCheckpointKey(fileGeneration{Period: period, Index: 1}, true))
	require.NoError(t, err)
	require.True(t, record.Complete([]objectstorage.DestinationID{"primary"}), "local disappearance must preserve the generation completion")
}

func TestFileSeriesManagerRestartRetriesOnlyFailedDestination(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		root := resolvedTempDirForTest(t)
		const (
			target    = "partial-restart-compressed"
			source    = "partial-restart-series"
			primary   = objectstorage.DestinationID("primary")
			secondary = objectstorage.DestinationID("secondary")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))

		period := currentLogDate()
		cursor := fileGeneration{Period: period, Index: 1}
		compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
		activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
		writeGzipFile(t, compressedPath, []byte("archive"))
		require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

		checkpointStore, err := checkpointstore.NewFilesystemStore(
			newCheckpointDirectoryForTest(t, root),
			string(source),
		)
		require.NoError(t, err)
		destinations := []objectstorage.DestinationID{primary, secondary}

		firstUploader := newDestinationUploaderRecorder(destinations...)
		uploadErr := errors.New("secondary unavailable")
		firstUploader.failNext(secondary, uploadErr)
		firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:          testFileSeriesManagerConfig(root, target, 1, 1, 1, false),
			Uploader:        firstUploader,
			CheckpointStore: checkpointStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "firstManager.Close()", firstManager.Close, firstManager.ForceClose, nil)
		firstReport, err := firstManager.RunOnce(context.Background())
		require.ErrorIs(t, err, uploadErr)
		require.Empty(t, firstReport.UploadedFileRemovals)
		require.FileExists(t, compressedPath)

		// process再起動後もprimaryの成功記録は同じファイルのcheckpointから復元されます。
		// path単位のmemory状態に依存せず、未完了のsecondaryだけを再試行します。
		require.ErrorIs(t, firstManager.ForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "firstManager.Close()", firstManager.Close, firstManager.ForceClose), shutdown.ErrForcedClose)
		restartedCheckpointStore, err := checkpointstore.NewFilesystemStore(
			checkpointStore.Directory(),
			checkpointStore.SourceID(),
		)
		require.NoError(t, err)
		persistedState, err := restartedCheckpointStore.Load(context.Background(), fileCheckpointKey(cursor, true))
		require.NoError(t, err)
		require.True(t, persistedState.DestinationComplete(primary))
		require.False(t, persistedState.DestinationComplete(secondary))
		restartedUploader := newDestinationUploaderRecorder(destinations...)
		restartedUploader.failNext(secondary, uploadErr)
		restartedManager := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:          testFileSeriesManagerConfig(root, target, 1, 1, 1, false),
			Uploader:        restartedUploader,
			CheckpointStore: restartedCheckpointStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "restartedManager.Close()", restartedManager.Close, restartedManager.ForceClose, nil)
		restartedReport, err := restartedManager.RunOnce(context.Background())
		require.ErrorIs(t, err, uploadErr)
		require.Empty(t, restartedReport.UploadedFileRemovals)
		require.FileExists(t, compressedPath)
		require.Empty(t, restartedUploader.pathsFor(primary))
		require.Equal(t, []string{compressedPath}, restartedUploader.pathsFor(secondary))

		require.NoError(t, boundedwait.CloseInBubble(t, "restartedManager.CloseLogFileProcessingRecovery()", restartedManager.CloseLogFileProcessingRecovery, restartedManager.ForceClose))
		require.Empty(t, restartedUploader.pathsFor(primary))
		require.Equal(
			t,
			[]string{compressedPath, compressedPath},
			restartedUploader.pathsFor(secondary),
		)
		require.NoFileExists(t, compressedPath)
		require.FileExists(t, activePath)
	})
}

func TestFileSeriesManagerRawCompletionAllowsCompressedFileRemovalWithoutResending(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const (
		target      = "checkpoint-complete-removal"
		source      = "checkpoint-complete-series"
		destination = objectstorage.DestinationID("archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	period := currentLogDate()
	cursor := fileGeneration{Period: period, Index: 1}
	compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	latestPath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, compressedPath, []byte("already uploaded"))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	checkpointStore, err := checkpointstore.NewFilesystemStore(
		newCheckpointDirectoryForTest(t, root),
		string(source),
	)
	require.NoError(t, err)
	_, err = checkpointStore.AdvanceCompletionCursor(context.Background(), fileCheckpointKey(cursor, false),
		[]objectstorage.DestinationID{destination})
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "checkpointStore.Close()", checkpointStore.Close, boundedwait.ForceCloserOf(checkpointStore)))
	// 元ログで完了した世代は、再起動後の圧縮fileでも完了済みです。
	restartedCheckpointStore, err := checkpointstore.NewFilesystemStore(
		checkpointStore.Directory(),
		checkpointStore.SourceID(),
	)
	require.NoError(t, err)
	checkpoint, err := restartedCheckpointStore.Load(context.Background(), fileCheckpointKey(cursor, true))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)
	uploadRecorder := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        uploadRecorder,
		CheckpointStore: restartedCheckpointStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)

	report, err := manager.RunOnce(context.Background())

	require.NoError(t, err)
	require.Empty(t, uploadRecorder.pathsFor(destination))
	require.Empty(t, report.ObjectStorageUploads)
	require.Len(t, report.UploadedFileRemovals, 1)
	require.NoError(t, report.UploadedFileRemovals[0].Err)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, latestPath)
	checkpoint, err = restartedCheckpointStore.Load(context.Background(), fileCheckpointKey(cursor, true))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)
}

func TestFileSeriesManagerLeavesInitiallyOutOfScopeCompressedFileToNormalRetention(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const (
		target      = "checkpoint-seed-retains-compressed"
		source      = "checkpoint-seed-retains-series"
		destination = objectstorage.DestinationID("archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	period := currentLogDate()
	compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	// 保持期間1日を過ぎた期間の世代です。
	expiredPath := filepath.Join(logDir, target+"_"+logDateDaysBefore(10)+"_1.log.gz")
	latestPath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, compressedPath, []byte("initially out-of-scope historical archive"))
	writeGzipFile(t, expiredPath, []byte("expired historical archive"))
	require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

	checkpointStore, err := checkpointstore.NewFilesystemStore(
		newCheckpointDirectoryForTest(t, root),
		string(source),
	)
	require.NoError(t, err)
	uploadRecorder := newDestinationUploaderRecorder(destination)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 1, 0, false),
		Uploader:        uploadRecorder,
		CheckpointStore: checkpointStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)

	report, err := manager.RunOnce(context.Background())
	require.NoError(t, err)
	// 登録前の圧縮fileは新しい宛先へ送らず、upload完了を理由に削除しません。
	// 保持期限を過ぎたfileだけを、通常のage purgeで削除します。
	require.Empty(t, uploadRecorder.pathsFor(destination))
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.Len(t, report.Purge.ByAge.RemovalResults, 1)
	require.Equal(t, expiredPath, report.Purge.ByAge.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByAge.RemovalResults[0].Err)
	require.NoFileExists(t, expiredPath)
	require.FileExists(t, compressedPath)
	require.FileExists(t, latestPath)
	cursor := fileGeneration{Period: period, Index: 1}
	checkpoint, err := checkpointStore.Load(context.Background(), fileCheckpointKey(cursor, true))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.OutOfScopeDestinationIDs)
}

func TestFileSeriesManagerKeepsGenerationCompletionForReplacedCompressedPathAfterRemovalFailure(t *testing.T) {
	// 削除の再試行間隔は実時間で待たず、bubble内の時刻で進めます。fileの更新時刻と
	// managerの時計を揃えるため、bubbleの時計を実時刻から始めます。
	synctestFromWallClock(t, func(t *testing.T) {
		root := resolvedTempDirForTest(t)
		const target = "uploaded-removal-retry"
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))

		// 保持期間1日を過ぎた期間の世代にし、保護がなければ同じ周期のage purgeで削除を試みる状態にします。
		period := logDateDaysBefore(10)
		compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
		latestPath := filepath.Join(logDir, target+"_"+currentLogDate()+"_1.log")
		writeGzipFile(t, compressedPath, []byte("uploaded before replacement"))
		require.NoError(t, os.WriteFile(latestPath, []byte("active"), 0o600))

		uploadRecorder := &recordingUploader{}
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 1, 1, 1, false),
			Uploader: uploadRecorder,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)
		restoreRemoval := testutil.DenyFileRemoval(t, compressedPath)
		defer restoreRemoval()
		report, err := manager.RunOnce(context.Background())
		require.ErrorIs(t, err, os.ErrPermission)
		require.Equal(t, []string{compressedPath}, uploadRecorder.pathsSnapshot())
		require.Len(t, report.UploadedFileRemovals, 1)
		require.ErrorIs(t, report.UploadedFileRemovals[0].Err, os.ErrPermission)
		require.FileExists(t, compressedPath)
		// 削除を再試行する圧縮fileは、同じ周期のageとdirectory-size purgeの対象にしません。
		require.True(t, report.Purge.ByAge.Ran)
		require.Empty(t, report.Purge.ByAge.RemovalResults)
		require.True(t, report.Purge.ByDirSize.Ran)
		require.Empty(t, report.Purge.ByDirSize.RemovalResults)

		restoreRemoval()
		require.NoError(t, os.Remove(compressedPath))
		writeGzipFile(t, compressedPath, []byte("remove after upload"))
		requireRecoveryClosesForTest(t, manager)
		require.Equal(t, []string{compressedPath}, uploadRecorder.pathsSnapshot())
		require.NoFileExists(t, compressedPath)
		require.FileExists(t, latestPath)
	})
}

func TestFileSeriesManagerDoesNotReuploadAfterCheckpointedLocalRemovalFailure(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		root := resolvedTempDirForTest(t)
		const (
			target    = "checkpoint-removal-retry"
			source    = "checkpoint-removal-series"
			primary   = objectstorage.DestinationID("primary")
			secondary = objectstorage.DestinationID("secondary")
		)
		logDir := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(logDir, 0o750))

		period := currentLogDate()
		compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
		activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
		writeGzipFile(t, compressedPath, []byte("checkpointed before removal"))
		require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

		checkpointStore, err := checkpointstore.NewFilesystemStore(
			newCheckpointDirectoryForTest(t, root),
			string(source),
		)
		require.NoError(t, err)
		destinations := []objectstorage.DestinationID{primary, secondary}
		uploadRecorder := newDestinationUploaderRecorder(destinations...)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
			Config:          testFileSeriesManagerConfig(root, target, 1, 1, 1, false),
			Uploader:        uploadRecorder,
			CheckpointStore: checkpointStore,
		})
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)
		restoreRemoval := testutil.DenyFileRemoval(t, compressedPath)
		defer restoreRemoval()

		firstReport, err := manager.RunOnce(context.Background())
		require.ErrorIs(t, err, os.ErrPermission)
		require.Len(t, firstReport.UploadedFileRemovals, 1)
		require.ErrorIs(t, firstReport.UploadedFileRemovals[0].Err, os.ErrPermission)

		secondReport, err := manager.RunOnce(context.Background())
		require.NoError(t, err)
		for _, destination := range destinations {
			require.Equal(
				t,
				[]string{compressedPath},
				uploadRecorder.pathsFor(destination),
			)
		}
		require.Empty(t, secondReport.UploadedFileRemovals, "the independent recovery owns the pending removal")
		require.FileExists(t, compressedPath)
		require.FileExists(t, activePath)
	})
}

func TestFileSeriesManagerKeepsUndeliveredBacklogWhenOnlyActiveDestinationHasInitialScope(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target    = "compressed-required-snapshot"
		source    = string("compressed-required-snapshot-series")
		primary   = objectstorage.DestinationID("primary")
		secondary = objectstorage.DestinationID("secondary")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, compressedPath, []byte("snapshot once"))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	firstStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	uploadErr := errors.New("primary unavailable")
	firstUploader := newDestinationUploaderRecorder(primary)
	firstUploader.failNext(primary, uploadErr)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        firstUploader,
		CheckpointStore: firstStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)

	_, err = firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, uploadErr)
	require.FileExists(t, compressedPath)

	// primaryを無効にし、secondaryだけを有効にしても、secondaryの登録前から存在した
	// fileはどこにも送信していません。送信せず、upload完了を理由に削除もしません。
	require.ErrorIs(t, firstManager.ForceClose(), shutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)), shutdown.ErrForcedClose)
	restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	restartedUploader := newDestinationUploaderRecorder(secondary)
	restartedManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        restartedUploader,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)
	report, err := restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, restartedUploader.pathsFor(primary))
	require.Empty(t, restartedUploader.pathsFor(secondary))
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, compressedPath)
	require.FileExists(t, activePath)
	// 送信待ちではないため、通常の保持・purgeからは保護しません。
	require.False(t, restartedManager.shouldProtectFromLifecyclePurge(compressedPath))
	checkpoint, err := restartedStore.Load(ctx, fileCheckpointKey(fileGeneration{Period: period, Index: 1}, true))
	require.NoError(t, err)
	require.True(t, checkpoint.Complete([]objectstorage.DestinationID{secondary}))
	require.False(t, checkpoint.DeliveredToAny([]objectstorage.DestinationID{secondary}))
}

func TestFileSeriesManagerDefersSecondCompressionSuffixAfterRestart(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target  = "later-compression-suffix"
		source  = string("later-compression-suffix-series")
		primary = objectstorage.DestinationID("primary")
		removed = objectstorage.DestinationID("removed-after-first-scan")
		added   = objectstorage.DestinationID("added-after-first-scan")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	gzipPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	zstdPath := filepath.Join(logDir, target+"_"+period+"_1.log.zst")
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, gzipPath, []byte("first suffix"))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	firstStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	uploadErr := errors.New("removed destination unavailable")
	firstUploader := newDestinationUploaderRecorder(primary, removed)
	firstUploader.failNext(removed, uploadErr)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        firstUploader,
		CheckpointStore: firstStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	_, err = firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, uploadErr)
	require.Equal(t, []string{gzipPath}, firstUploader.pathsFor(primary))
	require.FileExists(t, gzipPath)

	// 同じrawの別encodingが後から現れても、最初のfileだけを既存checkpointで完了させます。
	// 未選択の別encodingは次周期まで保全します。
	require.ErrorIs(t, firstManager.ForceClose(), shutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)), shutdown.ErrForcedClose)
	require.NoError(t, os.WriteFile(zstdPath, []byte("later suffix"), 0o600))
	restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	restartedUploader := newDestinationUploaderRecorder(primary, added)
	restartedManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        restartedUploader,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)
	report, err := restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, restartedUploader.pathsFor(primary))
	require.Empty(t, restartedUploader.pathsFor(added))
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, gzipPath, report.UploadedFileRemovals[0].File.Path)
	require.NoFileExists(t, gzipPath)
	require.FileExists(t, zstdPath)
	require.FileExists(t, activePath)

	report, err = restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, restartedUploader.pathsFor(primary))
	// 同世代の別encodingにも完了位置を適用し、追加時の初期完了位置も維持します。
	require.Empty(t, restartedUploader.pathsFor(added))
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, zstdPath, report.UploadedFileRemovals[0].File.Path)
	require.NoFileExists(t, zstdPath)
}

func TestFileSeriesManagerRetriesRejectedMarkWithoutUploadingAgain(
	t *testing.T,
) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target      = "multi-compressed-log-commit-failure"
		source      = string("multi-compressed-log-commit-failure-series")
		destination = objectstorage.DestinationID("primary")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	cursor := fileGeneration{Period: period, Index: 1}
	gzipPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, gzipPath, []byte("gzip compressed log file"))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	realStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)

	commitErr := errors.New("mark upload is temporarily unavailable")
	store := &failOnceCheckpointStore{
		Store:        realStore,
		destination:  destination,
		generation:   cursor,
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
	require.Equal(t, []string{gzipPath}, uploadRecorder.pathsFor(destination))
	require.Empty(t, firstReport.UploadedFileRemovals)
	require.FileExists(t, gzipPath)

	waitLogFileProcessingRecovery(t, manager, cursor)
	// 送信の成功をメモリから記録し直すだけで、remoteへの送信は繰り返しません。
	require.Equal(t, []string{gzipPath}, uploadRecorder.pathsFor(destination))
	require.NoFileExists(t, gzipPath)
	require.FileExists(t, activePath)
}

func TestFileSeriesManagerIgnoresRemovedDestinationWhenCurrentDestinationsSucceeded(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target    = "compressed-inactive-waiver"
		source    = string("compressed-inactive-waiver-series")
		primary   = objectstorage.DestinationID("primary")
		secondary = objectstorage.DestinationID("secondary")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, compressedPath, []byte("waive inactive destination"))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	firstStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	uploadErr := errors.New("secondary unavailable")
	firstUploader := newDestinationUploaderRecorder(primary, secondary)
	firstUploader.failNext(secondary, uploadErr)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        firstUploader,
		CheckpointStore: firstStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	_, err = firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, uploadErr)
	require.FileExists(t, compressedPath)

	// secondaryを設定から外した後は、その未成功を要求しません。
	// 現在activeなprimaryは既に成功済みなので再送せずlocal削除できます。
	require.ErrorIs(t, firstManager.ForceClose(), shutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)), shutdown.ErrForcedClose)
	restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	restartedUploader := newDestinationUploaderRecorder(primary)
	restartedManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        restartedUploader,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)
	report, err := restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, restartedUploader.pathsFor(primary))
	require.Len(t, report.UploadedFileRemovals, 1)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, activePath)
}

func TestFileSeriesManagerKeepsLocalOnlyCompressedLogsWhenDestinationAdded(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target      = "compressed-empty-required-snapshot"
		source      = string("compressed-empty-required-snapshot-series")
		destination = objectstorage.DestinationID("added-later")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	oldestPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	compressedPath := filepath.Join(logDir, target+"_"+period+"_2.log.gz")
	previousActivePath := filepath.Join(logDir, target+"_"+period+"_3.log")
	writeGzipFile(t, oldestPath, []byte("oldest local only"))
	writeGzipFile(t, compressedPath, []byte("local only"))
	require.NoError(t, os.WriteFile(previousActivePath, []byte("active before restart"), 0o600))
	config := testFileSeriesManagerConfig(root, target, 2, 0, 0, true)

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	firstStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	firstManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          config,
		CheckpointStore: firstStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	firstReport, err := firstManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, firstReport.UploadedFileRemovals)
	require.Empty(t, firstReport.Purge.ByGeneration.RemovalResults)
	require.FileExists(t, oldestPath)
	require.FileExists(t, compressedPath)

	// 宛先なしで保存していた過去ファイルと、停止中にrotateされた旧active fileは、
	// 追加した宛先へ遡って送信せず、upload完了を理由に削除もしません。
	require.NoError(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)))
	activePath := filepath.Join(logDir, target+"_"+period+"_4.log")
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))
	restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	uploadRecorder := newDestinationUploaderRecorder(destination)
	restartedManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          config,
		Uploader:        uploadRecorder,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)
	restartedReport, err := restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, uploadRecorder.pathsFor(destination))
	require.Empty(t, restartedReport.ObjectStorageUploads)
	require.Empty(t, restartedReport.UploadedFileRemovals)
	require.NoFileExists(t, previousActivePath)
	require.FileExists(t, previousActivePath+".gz")
	// 世代数の上限は通常どおり適用し、上限を超えた最も古い世代だけを削除します。
	require.Len(t, restartedReport.Purge.ByGeneration.RemovalResults, 1)
	require.Equal(t, oldestPath, restartedReport.Purge.ByGeneration.RemovalResults[0].File.Path)
	require.NoError(t, restartedReport.Purge.ByGeneration.RemovalResults[0].Err)
	require.NoFileExists(t, oldestPath)
	require.FileExists(t, compressedPath)
	require.FileExists(t, activePath)
}

func TestFileSeriesManagerUsesCurrentDestinationsAfterCompressionFailureAndRestart(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target    = "snapshot-before-compression"
		source    = string("snapshot-before-compression-series")
		primary   = objectstorage.DestinationID("primary")
		secondary = objectstorage.DestinationID("secondary-added-later")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	rawPath := filepath.Join(logDir, target+"_"+period+"_1.log")
	compressedPath := rawPath + ".gz"
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	require.NoError(t, os.WriteFile(rawPath, []byte("snapshot before compression"), 0o600))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	firstStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	compressionErr := errors.New("compression interrupted before upload")
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader:        newDestinationUploaderRecorder(primary),
		CheckpointStore: firstStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	firstManager.compressFile = func(context.Context, string) (string, error) {
		return "", compressionErr
	}
	_, err = firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, compressionErr)
	require.FileExists(t, rawPath)
	require.NoFileExists(t, compressedPath)
	checkpoint, err := firstStore.Load(ctx, fileCheckpointKey(fileGeneration{Period: period, Index: 1}, true))
	require.NoError(t, err)
	require.Empty(t, checkpoint.CompletedDestinationIDs)

	// secondaryの追加前の旧世代は、後から圧縮しても初期完了位置に含まれます。
	require.ErrorIs(t, firstManager.ForceClose(), shutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)), shutdown.ErrForcedClose)
	restartedStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	restartedUploader := newDestinationUploaderRecorder(primary, secondary)
	restartedManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader:        restartedUploader,
		CheckpointStore: restartedStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restartedManager.Close()", restartedManager.Close, boundedwait.ForceCloserOf(restartedManager), nil)
	report, err := restartedManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{compressedPath}, restartedUploader.pathsFor(primary))
	require.Empty(t, restartedUploader.pathsFor(secondary))
	require.Len(t, report.UploadedFileRemovals, 1)
	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, activePath)
}

func TestFileSeriesManagerPreservesCompressedPayloadWhenCheckpointLoadFails(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target      = "snapshot-failure-before-compression"
		destination = objectstorage.DestinationID("primary")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	rawPath := filepath.Join(logDir, target+"_"+period+"_1.log")
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	const payload = "preserve after compression"
	require.NoError(t, os.WriteFile(rawPath, []byte(payload), 0o600))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	compressedLogStore := &recoveryFaultCheckpointStore{Store: newTestFileCheckpointStore(t)}
	compressedLogStore.failLoad.Store(true)
	uploadRecorder := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader:        uploadRecorder,
		CheckpointStore: compressedLogStore,
	})
	useShortLogFileProcessingRecovery(t, manager)
	t.Cleanup(func() { compressedLogStore.failLoad.Store(false) })
	compressionCalled := false
	realCompress := manager.compressFile
	manager.compressFile = func(ctx context.Context, path string) (string, error) {
		compressionCalled = true
		return realCompress(ctx, path)
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	require.True(t, compressionCalled)
	require.Empty(t, uploadRecorder.pathsFor(destination))
	require.Len(t, report.Compressed, 1)
	require.NoFileExists(t, rawPath)
	require.Equal(t, payload, readGzipFile(t, rawPath+".gz"))
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, activePath)
}

func TestFileSeriesManagerPreservesValidPeerAfterRetirementWhenCheckpointLoadFails(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target      = "snapshot-failure-before-source-retirement"
		destination = objectstorage.DestinationID("primary")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	rawPath := filepath.Join(logDir, target+"_"+period+"_1.log")
	compressedPath := rawPath + ".gz"
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	const payload = "already compressed source"
	require.NoError(t, os.WriteFile(rawPath, []byte(payload), 0o600))
	writeGzipFile(t, compressedPath, []byte(payload))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	compressedLogStore := &recoveryFaultCheckpointStore{Store: newTestFileCheckpointStore(t)}
	compressedLogStore.failLoad.Store(true)
	uploadRecorder := newDestinationUploaderRecorder(destination)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader:        uploadRecorder,
		CheckpointStore: compressedLogStore,
	})
	useShortLogFileProcessingRecovery(t, manager)
	t.Cleanup(func() { compressedLogStore.failLoad.Store(false) })
	manager.compressFile = func(context.Context, string) (string, error) {
		t.Fatal("a valid compressed peer must not be recompressed")
		return "", nil
	}

	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	require.Empty(t, uploadRecorder.pathsFor(destination))
	require.Empty(t, report.Compressed)
	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(rawPath))
	require.FileExists(t, compressedPath)
	require.Equal(t, payload, readGzipFile(t, compressedPath))
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, activePath)
}

func TestFileSeriesManagerAllInactiveUsesLocalRetentionUntilDestinationReactivated(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target      = "compressed-all-required-inactive"
		source      = string("compressed-all-required-inactive-series")
		destination = objectstorage.DestinationID("temporarily-disabled")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, compressedPath, []byte("retained after disable"))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	firstStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	uploadErr := errors.New("destination unavailable")
	firstUploader := newDestinationUploaderRecorder(destination)
	firstUploader.failNext(destination, uploadErr)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        firstUploader,
		CheckpointStore: firstStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	_, err = firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, uploadErr)
	require.FileExists(t, compressedPath)

	// 全destinationを無効にした周期はupload成功削除せず、通常retentionへ戻します。
	require.ErrorIs(t, firstManager.ForceClose(), shutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)), shutdown.ErrForcedClose)
	disabledStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	disabledManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		CheckpointStore: disabledStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "disabledManager.Close()", disabledManager.Close, boundedwait.ForceCloserOf(disabledManager), nil)
	disabledReport, err := disabledManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, disabledReport.UploadedFileRemovals)
	require.FileExists(t, compressedPath)
	require.False(t, disabledManager.objectStorageUpload.uploadPending(
		fileGeneration{Period: period, Index: 1},
		true,
		true,
	))

	// 再有効化時にlocalに残っていれば、現在のその宛先へ再試行します。
	reenabledStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	reenabledUploader := newDestinationUploaderRecorder(destination)
	reenabledManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        reenabledUploader,
		CheckpointStore: reenabledStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "reenabledManager.Close()", reenabledManager.Close, boundedwait.ForceCloserOf(reenabledManager), nil)
	reenabledReport, err := reenabledManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{compressedPath}, reenabledUploader.pathsFor(destination))
	require.Len(t, reenabledReport.UploadedFileRemovals, 1)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, activePath)
}

func TestFileSeriesManagerDoesNotSuccessDeleteAfterAllDestinationsBecomeInactive(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const (
		target      = "completed-removal-after-disable"
		source      = string("completed-removal-after-disable-series")
		destination = objectstorage.DestinationID("archive")
	)
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	period := currentLogDate()
	compressedPath := filepath.Join(logDir, target+"_"+period+"_1.log.gz")
	activePath := filepath.Join(logDir, target+"_"+period+"_2.log")
	writeGzipFile(t, compressedPath, []byte("remove on retry"))
	require.NoError(t, os.WriteFile(activePath, []byte("active"), 0o600))

	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	firstStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	firstManager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader:        newDestinationUploaderRecorder(destination),
		CheckpointStore: firstStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager), nil)
	useShortLogFileProcessingRecovery(t, firstManager)
	restoreRemoval := testutil.DenyFileRemoval(t, compressedPath)
	defer restoreRemoval()
	firstReport, err := firstManager.RunOnce(ctx)
	require.ErrorIs(t, err, os.ErrPermission)
	require.Len(t, firstReport.UploadedFileRemovals, 1)
	require.ErrorIs(t, firstReport.UploadedFileRemovals[0].Err, os.ErrPermission)
	require.FileExists(t, compressedPath)

	restoreRemoval()
	require.NoError(t, boundedwait.Close(t, "firstManager.Close()", firstManager.Close, boundedwait.ForceCloserOf(firstManager)))
	require.NoFileExists(t, compressedPath, "graceful shutdown drains the accepted removal")
	// Simulate the completed file reappearing after restart. A currently empty
	// destination set must retain it despite the persisted success cursor.
	writeGzipFile(t, compressedPath, []byte("remove on retry"))
	disabledStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	disabledManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName:        logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:          testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		CheckpointStore: disabledStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "disabledManager.Close()", disabledManager.Close, boundedwait.ForceCloserOf(disabledManager), nil)
	disabledReport, err := disabledManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, disabledReport.UploadedFileRemovals)
	require.FileExists(t, compressedPath)
	require.FileExists(t, activePath)

	reenabledStore, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	reenabledUploader := newDestinationUploaderRecorder(destination)
	reenabledManager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: reenabledUploader, CheckpointStore: reenabledStore,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "reenabledManager.Close()", reenabledManager.Close, boundedwait.ForceCloserOf(reenabledManager), nil)
	reenabledReport, err := reenabledManager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, reenabledUploader.pathsFor(destination))
	require.Len(t, reenabledReport.UploadedFileRemovals, 1)
	require.NoFileExists(t, compressedPath)
}

type removeUploadedPathUploader struct{}

func (removeUploadedPathUploader) Upload(
	_ context.Context,
	input uploader.UploadRequest,
) error {
	return os.Remove(input.FilePath())
}
