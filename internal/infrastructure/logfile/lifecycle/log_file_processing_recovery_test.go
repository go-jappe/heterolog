package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

var errRecoveryCheckpoint = errors.New("checkpoint temporarily unavailable")

type recoveryFaultCheckpointStore struct {
	checkpointstore.Store
	failRecord    atomic.Bool
	failLoad      atomic.Bool
	blockLoad     atomic.Bool
	loadRelease   chan struct{}
	blockedLoads  atomic.Int64
	failRegister  atomic.Bool
	registerCalls atomic.Int64
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *recoveryFaultCheckpointStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *recoveryFaultCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, ids []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	if s.failRecord.Load() {
		// 下位のstoreが成功を受理する前に拒否します。
		return checkpointstore.Checkpoint{}, errRecoveryCheckpoint
	}
	return s.Store.AdvanceCompletionCursor(ctx, key, ids)
}

func (s *recoveryFaultCheckpointStore) Load(ctx context.Context, key checkpointstore.CheckpointKey) (checkpointstore.Checkpoint, error) {
	if s.blockLoad.Load() {
		s.blockedLoads.Add(1)
		select {
		case <-ctx.Done():
			return checkpointstore.Checkpoint{}, ctx.Err()
		case <-s.loadRelease:
		}
	}
	if s.failLoad.Load() {
		return checkpointstore.Checkpoint{}, errRecoveryCheckpoint
	}
	return s.Store.Load(ctx, key)
}

func (s *recoveryFaultCheckpointStore) RegisterDestinations(ctx context.Context, ids []objectstorage.DestinationID, keys []checkpointstore.CheckpointKey) error {
	s.registerCalls.Add(1)
	if s.failRegister.Load() {
		return errRecoveryCheckpoint
	}
	return s.Store.RegisterDestinations(ctx, ids, keys)
}

type logFileProcessingRecoveryFixture struct {
	manager   *FileSeriesManager
	lifecycle *DirectoryLifecycle
	store     *recoveryFaultCheckpointStore
	remote    *destinationUploaderRecorder
	path      string
	raw       string
	cursor    fileGeneration
}

func newLogFileProcessingRecoveryFixture(t *testing.T, destinations ...objectstorage.DestinationID) logFileProcessingRecoveryFixture {
	return newLogFileProcessingRecoveryFixtureWithReporter(t, nil, destinations...)
}

func newLogFileProcessingRecoveryFixtureWithReporter(t *testing.T, reporter RunReporter, destinations ...objectstorage.DestinationID) logFileProcessingRecoveryFixture {
	t.Helper()
	root := resolvedTempDirForTest(t)
	const target = "checkpoint-recovery"
	directory := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(directory, 0o750))
	period := currentLogDate()
	raw := filepath.Join(directory, target+"_"+period+"_2.log")
	path := raw + ".gz"
	writeGzipFile(t, path, []byte("upload once"))
	writeFileSeriesCheckpointLog(t, directory, target, period, 3)
	checkpointDirectory := newCheckpointDirectoryForTest(t, root)
	compressed, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "recovery")
	require.NoError(t, err)
	store := &recoveryFaultCheckpointStore{Store: compressed}
	remote := newDestinationUploaderRecorder(destinations...)
	manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: remote, CheckpointStore: store, Reporter: reporter,
	})
	useShortLogFileProcessingRecovery(t, manager)
	lifecycle, err := NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	closeDirectoryLifecycleOnCleanup(t, lifecycle)
	return logFileProcessingRecoveryFixture{manager: manager, lifecycle: lifecycle, store: store, remote: remote, path: path, raw: raw, cursor: fileGeneration{Period: period, Index: 2}}
}

// useShortLogFileProcessingRecoveryは、受理済み復旧の再試行間隔を20msにし、試験終了時にmanagerを
// 閉じます。成功した試験では受理済み復旧の完了を待つ通常の終了がerrorなく終わることを確認します。
// 失敗・skipした試験では障害注入が解除されないまま残る場合があるため、中止を通知してから閉じ、
// 終了処理のerrorは失敗理由に重ねません。共有fixtureのため、bubble外の実時間でもbubble内の
// 仮想時間でもboundedwait.HangTimeoutで待ち、期限を過ぎたらForceCloseを通知して失敗させます。
// bubble内でより長い終了待ちを意図する試験は、本文でboundedwait.CloseWithinを使って先に閉じます。
func useShortLogFileProcessingRecovery(t *testing.T, manager *FileSeriesManager) {
	t.Helper()
	setLogFileProcessingRecoveryInterval(t, manager, 20*time.Millisecond)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, manager.ForceClose, boundedwait.NoError)
}

// closeDirectoryLifecycleOnCleanupは、試験終了時にlifecycleの資源を解放します。成功した試験では
// 受理済み処理の完了を待つ通常の終了がerrorなく終わることを確認します。失敗・skipした試験では
// 障害注入が残っていても終了処理で停止しないよう、中止を通知してから解放します。
// 例えばWindowsでsymlinkの作成権限がなくskipした試験は、障害注入を解除する前に終了します。
// 待機の上限はuseShortLogFileProcessingRecoveryと同じです。
func closeDirectoryLifecycleOnCleanup(t *testing.T, lifecycle *DirectoryLifecycle) {
	t.Helper()
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()",
		lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose, boundedwait.NoError)
}

func setLogFileProcessingRecoveryInterval(t *testing.T, manager *FileSeriesManager, interval time.Duration) {
	t.Helper()
	if manager.logFileProcessingRecovery != nil {
		require.NoError(t, boundedwait.Close(t, "manager.logFileProcessingRecovery.Close()", manager.logFileProcessingRecovery.Close, boundedwait.ForceCloserOf(manager.logFileProcessingRecovery)))
	}
	queue, err := logfile.NewLogFileProcessingRecovery[logFileProcessingRecoveryKey](interval)
	require.NoError(t, err)
	manager.logFileProcessingRecovery = queue
}

// waitLogFileProcessingRecoveryは、cursorの世代の受理済み復旧が完了して登録が外れるまで待ちます。
// 上限は停止の検出だけに使うboundedwait.HangTimeoutです。
func waitLogFileProcessingRecovery(t *testing.T, manager *FileSeriesManager, cursor fileGeneration) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(cursor, true))
	}, boundedwait.HangTimeout, 5*time.Millisecond, "accepted recovery of generation %s-%d did not complete", cursor.Period, cursor.Index)
}

func TestLogFileProcessingRecoveryContinuesWithoutNormalRunnerInterval(t *testing.T) {
	for _, operation := range []string{"record", "load", "register"} {
		t.Run(operation, func(t *testing.T) {
			const destination = objectstorage.DestinationID("archive")
			f := newLogFileProcessingRecoveryFixture(t, destination)
			var failure *atomic.Bool
			switch operation {
			case "record":
				failure = &f.store.failRecord
			case "load":
				failure = &f.store.failLoad
			case "register":
				failure = &f.store.failRegister
			}
			failure.Store(true)
			_, err := f.lifecycle.RunOnce(context.Background())
			require.ErrorIs(t, err, errRecoveryCheckpoint)
			failure.Store(false)
			// 2回目のRunOnce、書き込み、purge、15分のintervalのいずれも必要ありません。
			require.Eventually(t, func() bool {
				_, err := os.Stat(f.path)
				return errors.Is(err, os.ErrNotExist) &&
					!f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)) &&
					!f.manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey)
			}, boundedwait.HangTimeout, 5*time.Millisecond)
			require.Equal(t, []string{f.path}, f.remote.pathsFor(destination))
			checkpoint, err := f.store.Load(context.Background(), fileCheckpointKey(f.cursor, true))
			require.NoError(t, err)
			require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)
			require.GreaterOrEqual(t, f.store.registerCalls.Load(), int64(3), "recovery refreshes destination registration")
		})
	}
}

type freshRegistrationRecoveryStore struct {
	checkpointstore.Store
	registeredKeys chan<- []checkpointstore.CheckpointKey
	resume         <-chan struct{}
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *freshRegistrationRecoveryStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *freshRegistrationRecoveryStore) RegisterDestinations(ctx context.Context, ids []objectstorage.DestinationID, keys []checkpointstore.CheckpointKey) error {
	if err := s.Store.RegisterDestinations(ctx, ids, keys); err != nil {
		return err
	}
	select {
	case s.registeredKeys <- append([]checkpointstore.CheckpointKey(nil), keys...):
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestCheckpointPreparationRecoveryRegistersFreshObservedGenerations(t *testing.T) {
	ctx := context.Background()
	const destination = objectstorage.DestinationID("archive")
	f := newLogFileProcessingRecoveryFixture(t, destination)
	registeredKeys := make(chan []checkpointstore.CheckpointKey, 10)
	resume := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	t.Cleanup(release)
	f.manager.objectStorageUpload.checkpointStore = &freshRegistrationRecoveryStore{
		Store: f.store, registeredKeys: registeredKeys, resume: resume,
	}
	f.store.failRegister.Store(true)
	_, err := f.lifecycle.RunOnce(ctx)
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	require.True(t, f.manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))

	owner := appendlog.NewDirectoryGuardToken()
	protectedPath := filepath.Join(filepath.Dir(f.path), "checkpoint-recovery_"+f.cursor.Period+"_6.log.gz")
	defer f.manager.directoryGuard.UnprotectPaths(owner, protectedPath)
	key := func(index uint64) checkpointstore.CheckpointKey {
		return fileCheckpointKey(fileGeneration{Period: f.cursor.Period, Index: index}, true)
	}
	require.NoError(t, f.manager.directoryGuard.RunWithLifecycleLock(func() error {
		// これらのgenerationは、失敗したpreparationのscanには存在しませんでした。
		writeFileSeriesCheckpointLog(t, filepath.Dir(f.path), "checkpoint-recovery", f.cursor.Period, 4)
		writeGzipFile(t, filepath.Join(filepath.Dir(f.path), "checkpoint-recovery_"+f.cursor.Period+"_5.log.gz"), []byte("new compressed generation"))
		f.manager.directoryGuard.ProtectPaths(owner, protectedPath)
		for _, index := range []uint64{4, 5, 6, 7} {
			if _, err := f.store.AdvanceCompletionCursor(ctx, key(index), []objectstorage.DestinationID{destination}); err != nil {
				return err
			}
		}
		f.store.failRegister.Store(false)
		return nil
	}))

	select {
	case keys := <-registeredKeys:
		require.Contains(t, keys, fileCheckpointKey(fileGeneration{Period: f.cursor.Period, Index: 4}, false))
		require.Contains(t, keys, key(5))
		for _, index := range []uint64{4, 5, 6} {
			checkpoint, err := f.store.Load(ctx, key(index))
			require.NoError(t, err)
			require.True(t, checkpoint.DestinationComplete(destination), "generation %d must survive the retry", index)
		}
		require.NotContains(t, keys, key(7))
		orphan, err := f.store.Load(ctx, key(7))
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{destination}, orphan.CompletedDestinationIDs, "filesystem absence does not rewind a completion cursor")
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("checkpoint preparation did not retry registration")
	}
	release()
}

func TestLogFileProcessingRecoveryCompletesFailedRemoteDestinationWithoutNormalRun(t *testing.T) {
	const primary, secondary = objectstorage.DestinationID("primary"), objectstorage.DestinationID("secondary")
	f := newLogFileProcessingRecoveryFixture(t, primary, secondary)
	f.store.failRecord.Store(true)
	remoteFailure := errors.New("secondary unavailable")
	f.remote.failNext(secondary, remoteFailure)
	_, err := f.lifecycle.RunOnce(context.Background())
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	require.ErrorIs(t, err, remoteFailure)
	f.store.failRecord.Store(false)
	waitLogFileProcessingRecovery(t, f.manager, f.cursor)
	require.NoFileExists(t, f.path)
	require.Equal(t, []string{f.path}, f.remote.pathsFor(primary))
	require.Equal(t, []string{f.path, f.path}, f.remote.pathsFor(secondary))
	checkpoint, err := f.store.Load(context.Background(), fileCheckpointKey(f.cursor, true))
	require.NoError(t, err)
	require.ElementsMatch(t, []objectstorage.DestinationID{primary, secondary}, checkpoint.CompletedDestinationIDs)
	_, err = f.lifecycle.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{f.path}, f.remote.pathsFor(primary))
	require.Equal(t, []string{f.path, f.path}, f.remote.pathsFor(secondary))
	require.NoFileExists(t, f.path)
}

func TestLogFileProcessingRecoveryRetainsCompletionCursorAfterRemovalAndClose(t *testing.T) {
	f := newLogFileProcessingRecoveryFixture(t, "archive")
	_, err := f.lifecycle.RunOnce(context.Background())
	require.NoError(t, err)
	require.NoFileExists(t, f.path)
	token := appendlog.NewDirectoryGuardToken()
	paths := []string{f.raw, f.path}
	require.True(t, f.manager.directoryGuard.ProtectPathsIfNoOtherOwnerConflict(token, nil, paths))
	f.manager.directoryGuard.UnprotectPaths(token, paths...)
	// 通常のscanが、durableな完了位置を削除することはありません。
	_, err = f.lifecycle.RunOnce(context.Background())
	require.NoError(t, err)
	checkpoint, err := f.store.Load(context.Background(), fileCheckpointKey(f.cursor, true))
	require.NoError(t, err)
	require.NotEmpty(t, checkpoint.CompletedDestinationIDs)
	require.NoError(t, boundedwait.Close(t, "f.lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", f.lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(f.lifecycle)))
	require.False(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
	require.True(t, f.manager.directoryGuard.ProtectPathsIfNoOtherOwnerConflict(token, nil, paths))
	f.manager.directoryGuard.UnprotectPaths(token, paths...)
}

func TestLogFileProcessingRecoveryWaitsForEarlierGenerationBeforeAdvancingCursor(t *testing.T) {
	// 記録しないことは、再試行が何度も実行されるまでbubble内の時刻を進めてから確認します。
	synctest.Test(t, func(t *testing.T) {
		const destination = objectstorage.DestinationID("archive")
		f := newLogFileProcessingRecoveryFixture(t, destination)
		f.store.failRecord.Store(true)
		// fixtureの終了処理より先に障害を解除します。
		t.Cleanup(func() { f.store.failRecord.Store(false) })
		_, err := f.lifecycle.RunOnce(context.Background())
		require.ErrorIs(t, err, errRecoveryCheckpoint)
		lowerPath := filepath.Join(filepath.Dir(f.path), "checkpoint-recovery_"+f.cursor.Period+"_1.log.gz")
		require.NoError(t, f.manager.directoryGuard.RunWithLifecycleLock(func() error {
			writeGzipFile(t, lowerPath, []byte("earlier unresolved generation"))
			return nil
		}))
		f.store.failRecord.Store(false)
		// 後から発見した先行世代も未完了なら、実送信済みの世代2の記録を待ちます。
		// 再試行間隔(20ms)の50倍の時刻を進め、その間の再試行でも記録しないことを確認します。
		time.Sleep(time.Second)
		synctest.Wait()
		checkpoint, err := f.store.Load(context.Background(), fileCheckpointKey(f.cursor, true))
		require.NoError(t, err)
		require.Empty(t, checkpoint.CompletedDestinationIDs)
		require.FileExists(t, f.path)
		require.FileExists(t, lowerPath)
		_, err = f.lifecycle.RunOnce(context.Background())
		require.NoError(t, err)
		waitLogFileProcessingRecovery(t, f.manager, f.cursor)
		require.Equal(t, []string{f.path, lowerPath}, f.remote.pathsFor(destination))
		require.NoFileExists(t, f.path)
		require.NoFileExists(t, lowerPath)
	})
}

func TestLogFileProcessingRecoveryCloseWaitsForStoreRecovery(t *testing.T) {
	// 復旧中のCloseが戻らないことは、実時間の待機ではなくbubble内の全goroutineが
	// 停止した時点で確認します。再試行間隔もbubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		f := newLogFileProcessingRecoveryFixture(t, "archive")
		f.store.failLoad.Store(true)
		_, err := f.lifecycle.RunOnce(context.Background())
		require.ErrorIs(t, err, errRecoveryCheckpoint)
		f.store.loadRelease = make(chan struct{})
		release := sync.OnceFunc(func() { close(f.store.loadRelease) })
		// fixtureの終了処理より先に解放し、途中で失敗しても復旧の完了待ちで止まらないようにします。
		t.Cleanup(release)
		f.store.blockLoad.Store(true)
		f.store.failLoad.Store(false)
		require.Eventually(t, func() bool { return f.store.blockedLoads.Load() > 0 }, time.Minute, time.Millisecond)
		closed := make(chan error, 1)
		go func() { closed <- f.lifecycle.CloseLogFileProcessingRecovery() }()
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("checkpoint shutdown discarded the blocked recovery: %v", err)
		default:
		}
		require.FileExists(t, f.path)
		release()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Minute):
			t.Fatal("recovery Close did not finish after the blocked load was released")
		}
		require.False(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
		require.NoFileExists(t, f.path)
	})
}

func TestLogFileProcessingRecoveryCloseDrainsAcceptedPreparationChildren(t *testing.T) {
	// Closeが受理済み処理を待っていることは、実時間の待機ではなく、bubble内の全goroutineが
	// 停止した時点で確認します。再試行間隔もbubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		f := newLogFileProcessingRecoveryFixture(t, "archive")
		f.store.failRegister.Store(true)
		f.store.failRecord.Store(true)
		// fixtureの終了処理より先に障害を解除し、途中で失敗しても完了待ちで止まらないようにします。
		t.Cleanup(func() {
			f.store.failRegister.Store(false)
			f.store.failRecord.Store(false)
		})
		_, err := f.lifecycle.RunOnce(context.Background())
		require.ErrorIs(t, err, errRecoveryCheckpoint)
		require.True(t, f.manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))
		closed := make(chan error, 1)
		go func() { closed <- f.lifecycle.CloseLogFileProcessingRecovery() }()
		// Closeの開始後は新しい処理を受け付けず、受理済みの準備処理だけが残ります。
		synctest.Wait()
		probe := logFileProcessingRecoveryKey{File: checkpointstore.CheckpointKey{Generation: "shutdown-probe"}}
		require.False(t, f.manager.logFileProcessingRecovery.Schedule(probe, nil, func(context.Context) (bool, error) { return true, nil }, nil, nil),
			"Close must stop admitting new recovery operations")
		f.store.failRegister.Store(false)
		require.Eventually(t, func() bool { return len(f.remote.pathsFor("archive")) == 1 }, time.Minute, time.Millisecond)
		// 送信後のcheckpoint記録が失敗している間は、Closeは受理済みの更新を捨てずに待ちます。
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close discarded a prepared file's pending checkpoint update: %v", err)
		default:
		}
		f.store.failRecord.Store(false)
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(time.Minute):
			t.Fatal("Close did not drain the accepted preparation's file continuation")
		}
		require.NoFileExists(t, f.path)
		require.Equal(t, []string{f.path}, f.remote.pathsFor("archive"))
	})
}

func TestDirectoryLifecycleCancellationDoesNotWaitForRecoveryLifecycleLock(t *testing.T) {
	f := newLogFileProcessingRecoveryFixture(t, "archive")
	entered, release := make(chan struct{}), make(chan struct{})
	held := make(chan struct{})
	go func() {
		_ = f.manager.directoryGuard.RunWithLifecycleLock(func() error {
			close(entered)
			<-release
			return nil
		})
		close(held)
	}()
	// fixtureの終了処理より先にlockを解放し、途中で失敗しても解放待ちで止まらないようにします。
	t.Cleanup(func() {
		close(release)
		boundedwait.Receive(t, held, "lifecycle lock holder")
	})
	boundedwait.Receive(t, entered, "lifecycle lock holder")
	for _, run := range []func(context.Context) (RunReport, error){f.lifecycle.RunOnce, f.lifecycle.ReconcileObjectStorageUploadDestinations} {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := run(ctx); done <- err }()
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(boundedwait.HangTimeout):
			t.Fatal("normal runner ignored cancellation while waiting for directory lifecycle")
		}
	}
}

func TestLogFileProcessingRecoveryReportsActualResumedUploadAndRemoval(t *testing.T) {
	reports := make(chan RunReport, 16)
	f := newLogFileProcessingRecoveryFixtureWithReporter(t,
		lifecycleRunReporterFunc(func(_ context.Context, report RunReport, err error) {
			if err == nil {
				reports <- report
			}
		}), "archive",
	)
	f.store.failLoad.Store(true)
	_, err := f.lifecycle.RunOnce(context.Background())
	require.ErrorIs(t, err, errRecoveryCheckpoint)
	f.store.failLoad.Store(false)
	select {
	case report := <-reports:
		require.Equal(t, filepath.Dir(f.path), report.DirectoryPath)
		require.Equal(t, []ObjectStorageUploadReport{{Path: f.path, DestinationIDs: []objectstorage.DestinationID{"archive"}}}, report.ObjectStorageUploads)
		require.Len(t, report.UploadedFileRemovals, 1)
		require.Equal(t, f.path, report.UploadedFileRemovals[0].File.Path)
		require.NoError(t, report.UploadedFileRemovals[0].Err)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("resumed upload/removal was not reported")
	}
}
