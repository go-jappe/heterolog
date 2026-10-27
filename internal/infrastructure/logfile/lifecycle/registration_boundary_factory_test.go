package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// replaceDestinationUploaderForTestは、factoryが作った宛先別uploaderを、同じ宛先IDの記録用uploaderへ
// 置き換えます。登録済みの宛先と完了位置はそのまま使い、外部へ送信しません。
func replaceDestinationUploaderForTest(t *testing.T, manager *FileSeriesManager) *destinationUploaderRecorder {
	t.Helper()
	client, ok := manager.objectStorageUpload.uploader.(uploader.DestinationUploader)
	require.True(t, ok)
	destinations := client.DestinationIDs()
	require.NotEmpty(t, destinations)
	remote := newDestinationUploaderRecorder(destinations...)
	manager.objectStorageUpload.stateMu.Lock()
	manager.objectStorageUpload.uploader = remote
	manager.objectStorageUpload.stateMu.Unlock()
	return remote
}

// requirePastPeriodRawOutOfScopeは、登録時に過去の期間の最新rawを、全宛先で登録前に完結した
// 対象外の世代として記録したことを確認します。対象外の世代は送信済みとしても扱いません。
func requirePastPeriodRawOutOfScope(t *testing.T, store checkpointstore.Store, generation fileGeneration, destinations []objectstorage.DestinationID) {
	t.Helper()
	scope, err := store.Load(context.Background(), fileCheckpointKey(generation, false))
	require.NoError(t, err)
	require.ElementsMatch(t, destinations, scope.OutOfScopeDestinationIDs)
	require.Empty(t, scope.PendingDestinationIDs(destinations))
	require.False(t, scope.DeliveredToAny(destinations))
}

// requireCurrentPeriodRawPendingは、登録時に現在のrotation期間に属する最新rawを、writerがまだ
// 追記し得る世代として全宛先の送信対象に残したことを確認します。登録前に完結した対象外の世代とも、
// 送信済みの世代とも記録しません。
func requireCurrentPeriodRawPending(t *testing.T, store checkpointstore.Store, generation fileGeneration, destinations []objectstorage.DestinationID) {
	t.Helper()
	scope, err := store.Load(context.Background(), fileCheckpointKey(generation, false))
	require.NoError(t, err)
	for _, destination := range destinations {
		require.NotContains(t, scope.OutOfScopeDestinationIDs, destination, "the current-period latest raw may still be appended")
	}
	require.ElementsMatch(t, destinations, scope.PendingDestinationIDs(destinations))
}

// failureArchiveLifecycleForDirectoryは、factoryが作ったfailure archiveのlifecycleから、
// directoryを管理するものを返します。
func failureArchiveLifecycleForDirectory(t *testing.T, lifecycles []*DirectoryLifecycle, directory string) *DirectoryLifecycle {
	t.Helper()
	for _, lifecycle := range lifecycles {
		if path, err := lifecycle.DirectoryPath(); err == nil && path == directory {
			return lifecycle
		}
	}
	require.FailNow(t, "no failure archive lifecycle manages the directory", directory)
	return nil
}

// 新しい宛先の初期cursorは構築時に確定します（docs/design/archive-checkpoint-repair.md）。
// 本番のfactoryも、構築時刻が属するrotation期間より前の最新rawを、writerが戻って追記しない
// 登録前の世代として対象外にし、rotation後にも送信・削除しません。
func TestLogFileFactoryRegistrationLeavesPastPeriodLatestRawOutOfScope(t *testing.T) {
	ctx := context.Background()
	cfg := configuredCheckpointFactoryTestConfig(t)
	layout := cfg.FsstoreConfig.Layout
	directory := filepath.Join(layout.LogPath, layout.Target, layout.LogDir)
	require.NoError(t, os.MkdirAll(directory, 0o750))
	now := time.Now().UTC()
	previous := fileGeneration{Period: now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat), Index: 1}
	pastRaw := writeFileSeriesCheckpointLog(t, directory, layout.Target, previous.Period, previous.Index)

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg, Clock: pkgclock.NewFixedClock(now)})
	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	lifecycle := lifecycles[0]
	closeDirectoryLifecycleOnCleanup(t, lifecycle)
	manager := lifecycle.fileSeriesManagers[0]
	remote := replaceDestinationUploaderForTest(t, manager)
	requirePastPeriodRawOutOfScope(t, checkpointStoreForFactoryTest(t, manager), previous, remote.destinations)

	// writerが現在の期間へrotationした後、非activeになった過去の期間のrawも送信・削除しません。
	latest := writeFileSeriesCheckpointLog(t, directory, layout.Target, now.Format(pkgfsstore.FileDateFormat), 1)
	report, err := lifecycle.RunOnce(ctx)
	require.NoError(t, err)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	for _, destination := range remote.destinations {
		require.Empty(t, remote.pathsFor(destination))
	}
	require.FileExists(t, pastRaw)
	require.FileExists(t, latest)
}

// 構築時刻が属するrotation期間の最新rawは、writerがまだ追記し得るため新しい宛先の送信対象に
// 残します（docs/reference.ja.md「object storageへのアップロード」）。本番のfactoryが登録境界をこの期間より後ろへずらすと、
// 追加した宛先へこの世代が届かなくなります。rotation後に全宛先へ送信することも確認します。
func TestLogFileFactoryRegistrationKeepsCurrentPeriodLatestRawPending(t *testing.T) {
	ctx := context.Background()
	cfg := configuredCheckpointFactoryTestConfig(t)
	layout := cfg.FsstoreConfig.Layout
	directory := filepath.Join(layout.LogPath, layout.Target, layout.LogDir)
	require.NoError(t, os.MkdirAll(directory, 0o750))
	now := time.Now().UTC()
	current := fileGeneration{Period: now.Format(pkgfsstore.FileDateFormat), Index: 1}
	currentRaw := writeFileSeriesCheckpointLog(t, directory, layout.Target, current.Period, current.Index)

	lifecycles, err := NewLogFileLifecycles(LogFileOptions{Config: cfg, Clock: pkgclock.NewFixedClock(now)})
	require.NoError(t, err)
	require.Len(t, lifecycles, 1)
	lifecycle := lifecycles[0]
	closeDirectoryLifecycleOnCleanup(t, lifecycle)
	manager := lifecycle.fileSeriesManagers[0]
	remote := replaceDestinationUploaderForTest(t, manager)
	requireCurrentPeriodRawPending(t, checkpointStoreForFactoryTest(t, manager), current, remote.destinations)

	// writerが次の世代へ移った後、登録時の最新rawを新しい宛先へ送信します。
	writeFileSeriesCheckpointLog(t, directory, layout.Target, current.Period, current.Index+1)
	_, err = lifecycle.RunOnce(ctx)
	require.NoError(t, err)
	for _, destination := range remote.destinations {
		require.Equal(t, []string{currentRaw}, remote.pathsFor(destination))
	}
}

// backend送信失敗のfailure archiveも、構築時の宛先登録で同じ境界を使います。
// 過去の期間の最新JSONLは対象外とし、rotation後に圧縮しても送信・削除しません。
func TestFailureArchiveFactoryRegistrationLeavesPastPeriodLatestRawOutOfScope(t *testing.T) {
	ctx := context.Background()
	cfg := newFailureArchiveLifecycleTestConfig(t, validFailureArchiveUploadTargetConfig())
	directory, err := failurearchive.GetFileArchiveDirectoryPath(cfg, cfg.DirCaseRetryable())
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(directory, 0o750))
	now := time.Now().UTC()
	previous := fileGeneration{Period: now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat), Index: 1}
	pastRaw := filepath.Join(directory, cfg.Target()+"_"+previous.Period+"_1.jsonl")
	require.NoError(t, os.WriteFile(pastRaw, []byte("{\"status\":\"retryable\"}\n"), 0o640))

	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg, Clock: pkgclock.NewFixedClock(now)})
	require.NoError(t, err)
	for _, lifecycle := range lifecycles {
		closeDirectoryLifecycleOnCleanup(t, lifecycle)
	}
	retryable := failureArchiveLifecycleForDirectory(t, lifecycles, directory)
	manager := retryable.fileSeriesManagers[0]
	remote := replaceDestinationUploaderForTest(t, manager)
	requirePastPeriodRawOutOfScope(t, checkpointStoreForFactoryTest(t, manager), previous, remote.destinations)

	latest := filepath.Join(directory, cfg.Target()+"_"+now.Format(pkgfsstore.FileDateFormat)+"_1.jsonl")
	require.NoError(t, os.WriteFile(latest, []byte("{\"status\":\"retryable\"}\n"), 0o640))
	report, err := retryable.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, []CompressionReport{{SourcePath: pastRaw, CompressedPath: pastRaw + pkgfsstore.ExtensionCompressZstd}}, report.Compressed)
	require.Empty(t, report.ObjectStorageUploads)
	require.Empty(t, report.UploadedFileRemovals)
	for _, destination := range remote.destinations {
		require.Empty(t, remote.pathsFor(destination))
	}
	require.FileExists(t, pastRaw+pkgfsstore.ExtensionCompressZstd)
	require.FileExists(t, latest)
}

// failure archiveでも、構築時刻が属する期間の最新JSONLはまだ追記され得るため新しい宛先の
// 対象に残し、rotation後に圧縮して全宛先へ送信します。
func TestFailureArchiveFactoryRegistrationKeepsCurrentPeriodLatestRawPending(t *testing.T) {
	ctx := context.Background()
	cfg := newFailureArchiveLifecycleTestConfig(t, validFailureArchiveUploadTargetConfig())
	directory, err := failurearchive.GetFileArchiveDirectoryPath(cfg, cfg.DirCaseRetryable())
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(directory, 0o750))
	now := time.Now().UTC()
	current := fileGeneration{Period: now.Format(pkgfsstore.FileDateFormat), Index: 1}
	currentRaw := filepath.Join(directory, cfg.Target()+"_"+current.Period+"_1.jsonl")
	require.NoError(t, os.WriteFile(currentRaw, []byte("{\"status\":\"retryable\"}\n"), 0o640))

	lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg, Clock: pkgclock.NewFixedClock(now)})
	require.NoError(t, err)
	for _, lifecycle := range lifecycles {
		closeDirectoryLifecycleOnCleanup(t, lifecycle)
	}
	retryable := failureArchiveLifecycleForDirectory(t, lifecycles, directory)
	manager := retryable.fileSeriesManagers[0]
	remote := replaceDestinationUploaderForTest(t, manager)
	requireCurrentPeriodRawPending(t, checkpointStoreForFactoryTest(t, manager), current, remote.destinations)

	next := filepath.Join(directory, cfg.Target()+"_"+current.Period+"_2.jsonl")
	require.NoError(t, os.WriteFile(next, []byte("{\"status\":\"retryable\"}\n"), 0o640))
	report, err := retryable.RunOnce(ctx)
	require.NoError(t, err)
	compressed := currentRaw + pkgfsstore.ExtensionCompressZstd
	require.Equal(t, []CompressionReport{{SourcePath: currentRaw, CompressedPath: compressed}}, report.Compressed)
	for _, destination := range remote.destinations {
		require.Equal(t, []string{compressed}, remote.pathsFor(destination))
	}
}

// 受理済みのfile処理の再開（recoverLogFileProcessing）も、送信を続ける前に宛先を登録し直します。
// 再開の時点で初めて登録される宛先にも、注入した時計の現在の期間より前の最新rawを対象外とする
// 同じ境界を使います。宛先の追加は、先行周期の後に有効な宛先が増えた状態を再現するため、
// managerのuploaderを同じStoreのまま差し替えます。復旧の再試行は仮想時間で進めます。
func TestLogFileProcessingRecoveryRegistrationLeavesPastPeriodLatestRawOutOfScope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "file-recovery-registration-boundary"
		const existing, added = objectstorage.DestinationID("existing"), objectstorage.DestinationID("added")
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		now := time.Now().UTC()
		period := now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat)
		accepted := fileGeneration{Period: period, Index: 1}
		pastRaw := fileGeneration{Period: period, Index: 2}
		compressed := filepath.Join(directory, target+"_"+period+"_1.log.gz")
		writeGzipFile(t, compressed, []byte("accepted upload"))
		raw := writeFileSeriesCheckpointLog(t, directory, target, pastRaw.Period, pastRaw.Index)
		base, err := checkpointstore.NewFilesystemStore(newCheckpointDirectoryForTest(t, root), "file-recovery-registration-boundary")
		require.NoError(t, err)
		require.NoError(t, base.RegisterDestinations(ctx, []objectstorage.DestinationID{existing}, nil))
		store := &recoveryFaultCheckpointStore{Store: base}
		manager := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
			Uploader: newDestinationUploaderRecorder(existing), CheckpointStore: store, Clock: pkgclock.NewFixedClock(now),
		})
		useShortLogFileProcessingRecovery(t, manager)
		lifecycle, err := NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		closeDirectoryLifecycleOnCleanup(t, lifecycle)
		store.failRecord.Store(true)
		// fixtureの終了処理より先に障害を解除します。
		t.Cleanup(func() { store.failRecord.Store(false) })

		// 送信済みの記録に失敗した世代は、受理済みの再開処理が引き受けます。
		_, err = lifecycle.RunOnce(ctx)
		require.ErrorIs(t, err, errRecoveryCheckpoint)
		require.True(t, manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(accepted, true)))
		remote := newDestinationUploaderRecorder(existing, added)
		manager.objectStorageUpload.stateMu.Lock()
		manager.objectStorageUpload.uploader = remote
		manager.objectStorageUpload.stateMu.Unlock()
		store.failRecord.Store(false)
		time.Sleep(time.Second)
		synctest.Wait()
		require.False(t, manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(accepted, true)))

		// 再開処理で登録したaddedでは、過去の期間の最新rawも登録前に完結した世代です。
		scope, err := base.Load(ctx, fileCheckpointKey(pastRaw, false))
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{added}, scope.OutOfScopeDestinationIDs)
		require.Equal(t, []objectstorage.DestinationID{existing}, scope.PendingDestinationIDs([]objectstorage.DestinationID{existing, added}))
		require.Empty(t, remote.pathsFor(added))
		require.FileExists(t, raw)
	})
}

// 受理済みのfile処理の再開で初めて登録される宛先でも、注入した時計の現在の期間に属する最新rawは
// まだ追記され得るため対象に残し、rotation後にaddedへも送信します。再開処理が登録境界を
// 後ろの期間へずらすと、addedにこの世代が届かなくなります。復旧の再試行は仮想時間で進めます。
func TestLogFileProcessingRecoveryRegistrationKeepsCurrentPeriodLatestRawPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "file-recovery-registration-current"
		const existing, added = objectstorage.DestinationID("existing"), objectstorage.DestinationID("added")
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		now := time.Now().UTC()
		previous := now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat)
		accepted := fileGeneration{Period: previous, Index: 1}
		writeGzipFile(t, filepath.Join(directory, target+"_"+previous+"_1.log.gz"), []byte("accepted upload"))
		current := fileGeneration{Period: now.Format(pkgfsstore.FileDateFormat), Index: 1}
		currentRaw := writeFileSeriesCheckpointLog(t, directory, target, current.Period, current.Index)
		base, err := checkpointstore.NewFilesystemStore(newCheckpointDirectoryForTest(t, root), target)
		require.NoError(t, err)
		require.NoError(t, base.RegisterDestinations(ctx, []objectstorage.DestinationID{existing}, nil))
		store := &recoveryFaultCheckpointStore{Store: base}
		manager := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
			Uploader: newDestinationUploaderRecorder(existing), CheckpointStore: store, Clock: pkgclock.NewFixedClock(now),
		})
		useShortLogFileProcessingRecovery(t, manager)
		lifecycle, err := NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		closeDirectoryLifecycleOnCleanup(t, lifecycle)
		store.failRecord.Store(true)
		// fixtureの終了処理より先に障害を解除します。
		t.Cleanup(func() { store.failRecord.Store(false) })

		_, err = lifecycle.RunOnce(ctx)
		require.ErrorIs(t, err, errRecoveryCheckpoint)
		require.True(t, manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(accepted, true)))
		remote := newDestinationUploaderRecorder(existing, added)
		manager.objectStorageUpload.stateMu.Lock()
		manager.objectStorageUpload.uploader = remote
		manager.objectStorageUpload.stateMu.Unlock()
		store.failRecord.Store(false)
		time.Sleep(time.Second)
		synctest.Wait()
		require.False(t, manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(accepted, true)))
		requireCurrentPeriodRawPending(t, base, current, []objectstorage.DestinationID{existing, added})

		// writerが次の世代へ移った後、再開処理で登録したaddedにも登録時の最新rawを送信します。
		writeFileSeriesCheckpointLog(t, directory, target, current.Period, current.Index+1)
		_, err = lifecycle.RunOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{currentRaw}, remote.pathsFor(existing))
		require.Equal(t, []string{currentRaw}, remote.pathsFor(added))
	})
}

// 初回の宛先登録に失敗した場合は、受理済みの準備の復旧が登録をやり直します。その経路でも、
// 注入した時計の現在の期間より前の最新rawは対象外です。復旧の再試行は仮想時間で進めます。
func TestCheckpointPreparationRecoveryRegistrationLeavesPastPeriodLatestRawOutOfScope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		const target = "recovery-registration-boundary"
		const destination = objectstorage.DestinationID("archive")
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		now := time.Now().UTC()
		previous := fileGeneration{Period: now.AddDate(0, 0, -1).Format(pkgfsstore.FileDateFormat), Index: 1}
		pastRaw := writeFileSeriesCheckpointLog(t, directory, target, previous.Period, previous.Index)
		base, err := checkpointstore.NewFilesystemStore(newCheckpointDirectoryForTest(t, root), "recovery-registration-boundary")
		require.NoError(t, err)
		store := &recoveryFaultCheckpointStore{Store: base}
		store.failRegister.Store(true)
		remote := newDestinationUploaderRecorder(destination)
		manager := NewFileSeriesManager(FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
			Uploader: remote, CheckpointStore: store, Clock: pkgclock.NewFixedClock(now),
		})
		useShortLogFileProcessingRecovery(t, manager)
		lifecycle, err := NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		closeDirectoryLifecycleOnCleanup(t, lifecycle)
		// fixtureの終了処理より先に障害を解除します。
		t.Cleanup(func() { store.failRegister.Store(false) })

		_, err = lifecycle.RunOnce(ctx)
		require.ErrorIs(t, err, errRecoveryCheckpoint)
		require.True(t, manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))
		store.failRegister.Store(false)
		time.Sleep(time.Second)
		synctest.Wait()
		require.False(t, manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey), "the accepted preparation retried the registration")
		requirePastPeriodRawOutOfScope(t, base, previous, []objectstorage.DestinationID{destination})

		latest := writeFileSeriesCheckpointLog(t, directory, target, now.Format(pkgfsstore.FileDateFormat), 1)
		report, err := lifecycle.RunOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, []CompressionReport{{SourcePath: pastRaw, CompressedPath: pastRaw + pkgfsstore.ExtensionCompressGzip}}, report.Compressed)
		require.Empty(t, report.ObjectStorageUploads)
		require.Empty(t, report.UploadedFileRemovals)
		require.Empty(t, remote.pathsFor(destination))
		require.FileExists(t, pastRaw+pkgfsstore.ExtensionCompressGzip)
		require.FileExists(t, latest)
	})
}
