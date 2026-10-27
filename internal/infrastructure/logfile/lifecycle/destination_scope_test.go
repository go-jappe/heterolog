package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

func TestDestinationRegistrationKeepsPreRegistrationFilesAndLatestRawPending(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "destination-scope"
	const destination = objectstorage.DestinationID("archive")
	active := []objectstorage.DestinationID{destination}
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	oldCompressed := filepath.Join(directory, target+"_"+period+"_1.log.gz")
	writeGzipFile(t, oldCompressed, []byte("already compressed"))
	oldRaw := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
	latest := writeFileSeriesCheckpointLog(t, directory, target, period, 9)
	store := newTestFileCheckpointStore(t)
	remote := newDestinationUploaderRecorder(destination)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: remote, CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)

	// 登録前から存在した世代は新しい宛先へ送らず、upload完了を理由に削除もしません。
	// 書き込み中の可能性がある最新rawの世代だけを、rotation後の送信対象に残します。
	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, remote.pathsFor(destination))
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, oldCompressed)
	require.NoFileExists(t, oldRaw)
	require.FileExists(t, oldRaw+".gz", "登録後に圧縮した登録前の世代も対象外として保持します")
	require.FileExists(t, latest)
	for _, index := range []uint64{1, 2} {
		record, err := store.Load(ctx, fileCheckpointKey(fileGeneration{Period: period, Index: index}, true))
		require.NoError(t, err)
		require.True(t, record.Complete(active))
		require.Equal(t, active, record.OutOfScopeDestinationIDs)
		require.False(t, record.DeliveredToAny(active))
	}
	pending, err := store.Load(ctx, checkpointstore.CheckpointKey{Generation: period + "-9"})
	require.NoError(t, err)
	require.False(t, pending.Complete(active))

	// 登録後に完了位置より前の世代番号で現れたfileも、送信せず対象外として保持します。
	// writerは記録済みの世代を再利用しないため、このようなfileは外部から置かれた場合だけです。
	newCompressed := filepath.Join(directory, target+"_"+logDateDaysBefore(1)+"_1.log.gz")
	writeGzipFile(t, newCompressed, []byte("created after registration"))
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, remote.pathsFor(destination))
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, oldCompressed)
	require.FileExists(t, newCompressed)
	writeFileSeriesCheckpointLog(t, directory, target, period, 10)
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{latest + ".gz"}, remote.pathsFor(destination))
	require.Len(t, report.UploadedFileRemovals, 1)
	require.NoFileExists(t, latest+".gz")
	require.FileExists(t, oldCompressed)
	require.FileExists(t, oldRaw+".gz")
	require.FileExists(t, newCompressed)
}

func TestDestinationRegistrationInitialCompletionKeepsRawUnderNormalRetention(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "initial-raw-completion"
	const destination = objectstorage.DestinationID("archive")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	remote := newDestinationUploaderRecorder(destination)
	store := newTestFileCheckpointStore(t)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: remote, CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.FileExists(t, raw)
	require.FileExists(t, latest)
	state, err := store.Load(ctx, fileCheckpointKey(fileGeneration{Period: currentLogDate(), Index: 1}, false))
	require.NoError(t, err)
	require.True(t, state.Complete([]objectstorage.DestinationID{destination}))
}

func TestDestinationRegistrationAcceptedSaveFailureKeepsInitialCompletionAndPendingUploads(t *testing.T) {
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "initial-cursor-save-failure"
		const destination = objectstorage.DestinationID("archive")
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		period := currentLogDate()
		oldCompressed := filepath.Join(directory, target+"_"+period+"_1.log.gz")
		writeGzipFile(t, oldCompressed, []byte("initially complete"))
		latest := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
		store := newTestFileCheckpointStore(t)
		documentPath := filepath.Join(store.Directory(), store.SourceID()+".log-file-uploads.json")
		restore := blockRunnerCheckpointDocument(t, documentPath)
		remote := newDestinationUploaderRecorder(destination)
		manager := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
			Uploader: remote, CheckpointStore: store,
		})
		// Closeは保存の完了を待つため、途中で失敗した場合も保存先の障害を先に解除します。
		t.Cleanup(func() {
			restore()
			_ = boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose)
		})

		report, err := manager.RunOnce(ctx)
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		require.Empty(t, report.ObjectStorageUploads)
		require.Empty(t, remote.pathsFor(destination), "accepted initial completion must not fall back to aggregate upload")
		// 受理済みの初期位置は送信不要を表すだけで、登録前のfileを削除する根拠にはしません。
		require.FileExists(t, oldCompressed)
		require.FileExists(t, latest)
		require.Empty(t, report.UploadedFileRemovals)
		require.False(t, manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))
		activeKey := checkpointstore.CheckpointKey{Generation: period + "-2"}
		active, err := store.Load(ctx, activeKey)
		require.NoError(t, err)
		require.False(t, active.Complete([]objectstorage.DestinationID{destination}))

		// checkpointの永続化がまだblockされている間に、それまでactiveだったraw generationを
		// rotateします。uploadするのは、実際に保留中のこのgenerationだけでなければなりません。
		next := writeFileSeriesCheckpointLog(t, directory, target, period, 3)
		report, err = manager.RunOnce(ctx)
		require.ErrorAs(t, err, &pending)
		require.Equal(t, []string{latest + ".gz"}, remote.pathsFor(destination))
		require.Len(t, report.ObjectStorageUploads, 1)
		require.Len(t, report.UploadedFileRemovals, 1)
		require.NoFileExists(t, latest+".gz")
		require.FileExists(t, oldCompressed)
		require.FileExists(t, next)
		require.False(t, manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))
		active, err = store.Load(ctx, activeKey)
		require.NoError(t, err)
		require.True(t, active.Complete([]objectstorage.DestinationID{destination}))
		restore()
		require.Eventually(t, func() bool {
			return checkpointFileOnDiskHasGeneration(documentPath, fileGeneration{Period: period, Index: 2})
		}, 8*time.Second, 10*time.Millisecond)
	})
}

func TestDestinationRegistrationUploadsOnlyPendingDestinations(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "destination-pending"
	const primary = objectstorage.DestinationID("primary")
	const retrying = objectstorage.DestinationID("retrying")
	const added = objectstorage.DestinationID("added")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	compressed := filepath.Join(directory, target+"_"+period+"_1.log.gz")
	writeGzipFile(t, compressed, []byte("pending retry"))
	writeFileSeriesCheckpointLog(t, directory, target, period, 2)
	store := newTestFileCheckpointStore(t)
	require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{primary, retrying}, nil))
	_, err := store.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: period + "-1", Compressed: true}, []objectstorage.DestinationID{primary})
	require.NoError(t, err)
	remote := newDestinationUploaderRecorder(primary, retrying, added)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: remote, CheckpointStore: store,
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, remote.pathsFor(primary))
	require.Contains(t, remote.pathsFor(retrying), compressed)
	require.Empty(t, remote.pathsFor(added))
	require.Len(t, report.UploadedFileRemovals, 1)
	require.NoFileExists(t, compressed)
}

func TestDestinationRegistrationLeavesPreRegistrationCompressedFilesToGenerationRetention(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "destination-retention"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	first := filepath.Join(directory, target+"_"+period+"_1.log.gz")
	second := filepath.Join(directory, target+"_"+period+"_2.log.gz")
	writeGzipFile(t, first, []byte("oldest"))
	writeGzipFile(t, second, []byte("retained"))
	writeFileSeriesCheckpointLog(t, directory, target, period, 3)
	remote := newDestinationUploaderRecorder("new")
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 1, 0, 0, true),
		Uploader: remote, CheckpointStore: newTestFileCheckpointStore(t),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	// 登録前の圧縮fileは送信せず、upload完了を理由に削除しません。
	// 世代数の上限は通常どおり適用し、上限を超えた古い世代だけを削除します。
	require.Empty(t, remote.pathsFor("new"))
	require.Empty(t, report.UploadedFileRemovals)
	require.Len(t, report.Purge.ByGeneration.RemovalResults, 1)
	require.Equal(t, first, report.Purge.ByGeneration.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByGeneration.RemovalResults[0].Err)
	require.NoFileExists(t, first)
	require.FileExists(t, second)
}

func TestDestinationRegistrationLeavesPreRegistrationCompressionFormatsToAgeRetention(t *testing.T) {
	root := resolvedTempDirForTest(t)
	const target = "destination-retention-formats"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	// 保持期間1日を過ぎた期間の世代です。
	old := filepath.Join(directory, target+"_"+logDateDaysBefore(10)+"_1.log")
	writeGzipFile(t, old+".gz", []byte("older format"))
	require.NoError(t, os.WriteFile(old+".zst", []byte("unselected format"), 0o600))
	writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	remote := newDestinationUploaderRecorder("new")
	// managerの既定の時計と同じUTCで、注入する時計とfile名の期間を揃えます。
	now := time.Now().UTC()
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 1, 0, true),
		Uploader: remote, CheckpointStore: newTestFileCheckpointStore(t),
		Clock: pkgclock.NewFixedClock(now),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	// 同じ世代の圧縮形式は一周期に一つずつ処理します。どちらも送信せず、upload完了を
	// 理由に削除しないため、保持期限を過ぎたfileは通常のage purgeで削除します。
	report, err := manager.RunOnce(context.Background())
	require.NoError(t, err)
	require.Empty(t, remote.pathsFor("new"))
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.Len(t, report.Purge.ByAge.RemovalResults, 1)
	require.Equal(t, old+".gz", report.Purge.ByAge.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByAge.RemovalResults[0].Err)
	require.NoFileExists(t, old+".gz")
	require.FileExists(t, old+".zst")
	// age purgeは一日一回です。翌日の周期で、残った別形式も同じ規則で削除します。
	manager.Clock = pkgclock.NewFixedClock(now.Add(24 * time.Hour))
	report, err = manager.RunOnce(context.Background())
	require.NoError(t, err)
	require.Empty(t, remote.pathsFor("new"))
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.Len(t, report.Purge.ByAge.RemovalResults, 1)
	require.Equal(t, old+".zst", report.Purge.ByAge.RemovalResults[0].File.Path)
	require.NoError(t, report.Purge.ByAge.RemovalResults[0].Err)
	require.NoFileExists(t, old+".zst")
}

func TestDestinationRegistrationFactoryScopeSurvivesMissingSuccessDocument(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "destination-recovery"
	const primary = objectstorage.DestinationID("primary")
	const added = objectstorage.DestinationID("added")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	old := filepath.Join(directory, target+"_"+period+"_1.log.gz")
	writeGzipFile(t, old, []byte("before initial registration"))
	latest := writeFileSeriesCheckpointLog(t, directory, target, period, 9)
	pattern := logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"}
	config := testFileSeriesManagerConfig(root, target, 10, 0, 0, true)
	checkpointConfig := pkgfsstore.ArchiveCheckpointConfig{
		Directory: newCheckpointDirectoryForTest(t, root), SourcePrefix: "scope", AutoRecoverMissingCheckpoint: true,
	}
	remote := newDestinationUploaderRecorder(primary)
	store, err := newArchiveCheckpointStore(checkpointConfig, "default", pattern, config.Layout(), remote, nil,
		archiveCheckpointFactoryOptions{activePeriod: period})
	require.NoError(t, err)
	// このarchiveは、factoryへの登録後、最初のrunより前に現れます。
	pending := filepath.Join(directory, target+"_"+period+"_2.log.gz")
	writeGzipFile(t, pending, []byte("after primary registration"))
	failure := errors.New("temporary destination failure")
	remote.failNext(primary, failure)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{FileName: pattern, Config: config, Uploader: remote, CheckpointStore: store})
	_, err = manager.RunOnce(ctx)
	require.ErrorIs(t, err, failure)
	require.Equal(t, []string{pending}, remote.pathsFor(primary))
	require.FileExists(t, old, "登録前から存在したfileは送信も削除もしません")
	require.ErrorIs(t, manager.ForceClose(), pkgshutdown.ErrForcedClose)
	require.ErrorIs(t, boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager)), pkgshutdown.ErrForcedClose)
	sourceID := archiveCheckpointSourceID(checkpointConfig.SourcePrefix, "default", pattern)
	require.NoError(t, os.Remove(filepath.Join(checkpointConfig.Directory, sourceID+".log-file-uploads.json")))

	restartedRemote := newDestinationUploaderRecorder(primary, added)
	restartedStore, err := newArchiveCheckpointStore(checkpointConfig, "default", pattern, config.Layout(), restartedRemote, nil,
		archiveCheckpointFactoryOptions{activePeriod: period})
	require.NoError(t, err)
	restarted := NewFileSeriesManager(FileSeriesManagerOptions{FileName: pattern, Config: config, Uploader: restartedRemote, CheckpointStore: restartedStore})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted), boundedwait.NoError)
	report, err := restarted.RunOnce(ctx)
	require.NoError(t, err)
	// 完了記録を失ったprimaryは、保持されていた登録前のfileも含めて最初から再送します。
	// 追加した宛先の対象外の範囲は維持し、primaryへの送信後にだけ削除します。
	require.Equal(t, []string{old, pending}, restartedRemote.pathsFor(primary))
	require.Empty(t, restartedRemote.pathsFor(added))
	require.Len(t, report.UploadedFileRemovals, 2)
	require.NoFileExists(t, old)
	require.FileExists(t, latest)
	require.NoFileExists(t, pending)
}

func TestDestinationRegistrationPrecedesIndependentLogFileProcessingRecoveryUpload(t *testing.T) {
	ctx := context.Background()
	const existing = objectstorage.DestinationID("existing")
	const added = objectstorage.DestinationID("added")
	fixture := newLogFileProcessingRecoveryFixture(t, existing)
	fixture.store.failLoad.Store(true)
	_, err := fixture.lifecycle.RunOnce(ctx)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	remote := newDestinationUploaderRecorder(existing, added)
	require.NoError(t, fixture.manager.directoryGuard.RunWithLifecycleLock(func() error {
		fixture.manager.objectStorageUpload.stateMu.Lock()
		fixture.manager.objectStorageUpload.uploader = remote
		fixture.manager.objectStorageUpload.stateMu.Unlock()
		fixture.store.failLoad.Store(false)
		return nil
	}))
	waitLogFileProcessingRecovery(t, fixture.manager, fixture.cursor)
	require.Equal(t, []string{fixture.path}, remote.pathsFor(existing))
	require.Empty(t, remote.pathsFor(added))
	require.NoFileExists(t, fixture.path)
}

func TestDestinationRegistrationFailurePreventsCompressionAndUpload(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "destination-registration-failure"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	raw := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
	writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
	store := &recoveryFaultCheckpointStore{Store: newTestFileCheckpointStore(t)}
	store.failRegister.Store(true)
	remote := newDestinationUploaderRecorder("new")
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: remote, CheckpointStore: store,
	})
	useShortLogFileProcessingRecovery(t, manager)
	t.Cleanup(func() { store.failRegister.Store(false) })
	report, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	require.Empty(t, report.Compressed)
	require.Empty(t, remote.pathsFor("new"))
	require.FileExists(t, raw)
	require.NoFileExists(t, raw+".gz")
}

func TestDestinationRegistrationRecoveryKeepsPreRegistrationFileAfterCheckpointReadFailure(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "destination-scope-recovery"
	const destination = objectstorage.DestinationID("archive")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	generation := fileGeneration{Period: period, Index: 1}
	compressed := filepath.Join(directory, target+"_"+period+"_1.log.gz")
	writeGzipFile(t, compressed, []byte("existed before registration"))
	writeFileSeriesCheckpointLog(t, directory, target, period, 2)
	store := &recoveryFaultCheckpointStore{Store: newTestFileCheckpointStore(t)}
	remote := newDestinationUploaderRecorder(destination)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: remote, CheckpointStore: store,
	})
	useShortLogFileProcessingRecovery(t, manager)

	// 完了情報を読めない間は、受理済みの処理として保持して再開を待ちます。
	store.failLoad.Store(true)
	_, err := manager.RunOnce(ctx)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	require.True(t, manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(generation, true)))
	require.FileExists(t, compressed)

	// 再開後に登録前の対象外と確認できたfileは、送信せず、upload完了を理由に削除もしません。
	store.failLoad.Store(false)
	waitLogFileProcessingRecovery(t, manager, generation)
	require.Empty(t, remote.pathsFor(destination))
	require.FileExists(t, compressed)
	require.False(t, manager.shouldProtectFromLifecyclePurge(compressed))
}

// checkpointの欠落を自動復旧した後は、宛先の登録時に対象外とした世代も再送対象に戻ります。
// 対象外の範囲も空に戻るため、再送した世代は送信後に削除します。
func TestMissingCheckpointRecoveryReturnsPreRegistrationFilesToDelivery(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "missing-checkpoint-scope"
	const source = "missing-checkpoint-scope"
	const destination = objectstorage.DestinationID("archive")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	preRegistration := filepath.Join(directory, target+"_"+period+"_1.log.gz")
	writeGzipFile(t, preRegistration, []byte("existed before destination registration"))
	latest := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	key := fileCheckpointKey(fileGeneration{Period: period, Index: 1}, true)
	newManager := func(store checkpointstore.Store, remote *destinationUploaderRecorder) *FileSeriesManager {
		return NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
			Uploader: remote, CheckpointStore: store,
		})
	}

	store, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source)
	require.NoError(t, err)
	remote := newDestinationUploaderRecorder(destination)
	manager := newManager(store, remote)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), nil)
	_, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, remote.pathsFor(destination))
	require.FileExists(t, preRegistration)
	scoped, err := store.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, scoped.OutOfScopeDestinationIDs)
	documentPath := store.DocumentPath()
	require.NoError(t, boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager)))
	require.NoError(t, os.Remove(documentPath))

	// 宛先登録manifestが残るため欠落と判断し、対象外の範囲を含めて空のcursorから復旧します。
	recovered, err := checkpointstore.NewFilesystemStore(checkpointDirectory, source, checkpointstore.WithMissingCheckpointRecovery(true))
	require.NoError(t, err)
	remote = newDestinationUploaderRecorder(destination)
	manager = newManager(recovered, remote)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)
	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{preRegistration}, remote.pathsFor(destination))
	require.Len(t, report.UploadedFileRemovals, 1)
	require.Equal(t, preRegistration, report.UploadedFileRemovals[0].File.Path)
	require.NoError(t, report.UploadedFileRemovals[0].Err)
	require.NoFileExists(t, preRegistration)
	require.FileExists(t, latest)
	delivered, err := recovered.Load(ctx, key)
	require.NoError(t, err)
	require.Empty(t, delivered.OutOfScopeDestinationIDs)
	require.True(t, delivered.DestinationDelivered(destination))
}

// 宛先の登録時点の最新rawが過去のrotation期間に属する場合、writerはその期間へ戻って追記しないため、
// 登録前に完結した世代として対象外にします。rotation後に圧縮しても送信・削除しません。
func TestDestinationRegistrationLeavesPastPeriodLatestRawOutOfScope(t *testing.T) {
	ctx := context.Background()
	root := resolvedTempDirForTest(t)
	const target = "past-period-latest-raw"
	const destination = objectstorage.DestinationID("archive")
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	// managerの既定の時計と同じUTCで、注入する時計とfile名の期間を揃えます。
	now := time.Now().UTC()
	previous := now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat)
	current := now.Format(pkgfsstore.FileDateFormat)
	pastRaw := writeFileSeriesCheckpointLog(t, directory, target, previous, 1)
	remote := newDestinationUploaderRecorder(destination)
	store := newTestFileCheckpointStore(t)
	manager := NewFileSeriesManager(FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: remote, CheckpointStore: store, Clock: pkgclock.NewFixedClock(now),
	})
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager), boundedwait.NoError)

	report, err := manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.ObjectStorageUploads)
	require.FileExists(t, pastRaw, "the newest file stays uncompressed until the writer rotates")
	scope, err := store.Load(ctx, fileCheckpointKey(fileGeneration{Period: previous, Index: 1}, false))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, scope.OutOfScopeDestinationIDs)

	// writerが現在の期間へrotationした後、過去の期間のrawを圧縮しても送信・削除しません。
	latest := writeFileSeriesCheckpointLog(t, directory, target, current, 1)
	report, err = manager.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []CompressionReport{{SourcePath: pastRaw, CompressedPath: pastRaw + pkgfsstore.ExtensionCompressGzip}}, report.Compressed)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	require.Empty(t, remote.pathsFor(destination))
	require.NoFileExists(t, pastRaw)
	require.FileExists(t, pastRaw+pkgfsstore.ExtensionCompressGzip)
	require.FileExists(t, latest)
}
