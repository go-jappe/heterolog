package file

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

	"github.com/go-jappe/heterolog/internal/application/observer"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type recoverableInlineCheckpointStore struct {
	checkpointstore.Store
	failLoad         atomic.Bool
	failRecord       atomic.Bool
	recordFailure    error
	loads            atomic.Int64
	records          atomic.Int64
	completedRecords atomic.Int64
}

func (s *recoverableInlineCheckpointStore) Load(ctx context.Context, key checkpointstore.CheckpointKey) (checkpointstore.Checkpoint, error) {
	s.loads.Add(1)
	if s.failLoad.Load() {
		return checkpointstore.Checkpoint{}, errors.New("temporary checkpoint load failure")
	}
	return s.Store.Load(ctx, key)
}

func (s *recoverableInlineCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, ids []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	s.records.Add(1)
	if s.failRecord.Load() {
		if s.recordFailure != nil {
			return checkpointstore.Checkpoint{}, s.recordFailure
		}
		return checkpointstore.Checkpoint{}, errors.New("temporary checkpoint record failure")
	}
	checkpoint, err := s.Store.AdvanceCompletionCursor(ctx, key, ids)
	if err == nil {
		s.completedRecords.Add(1)
	}
	return checkpoint, err
}

func newInlineLogFileProcessingRecoveryFixture(t *testing.T) (*FileLogger, *recoverableInlineCheckpointStore, *sequenceArchiveUploader, string, checkpointstore.CheckpointKey) {
	t.Helper()
	now := time.Date(2026, time.September, 23, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "checkpoint-independent-recovery")
	store := &recoverableInlineCheckpointStore{Store: newCompressedLogCheckpointTestStore(t, t.TempDir(), "inline_recovery")}
	t.Cleanup(func() {
		store.failLoad.Store(false)
		store.failRecord.Store(false)
	})
	logger.archiveUploadCheckpoints = store
	remote := &sequenceArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
	path := writePendingCompressedLog(t, logger, "20260920", 1, now)
	key := inlineCompressedLogCheckpointKey(t, logger, path)
	return logger, store, remote, path, key
}

func TestInlineLogFileProcessingRecoveryDoesNotAcceptMixedPendingSaveError(t *testing.T) {
	for _, stage := range []string{"resume cursor update", "resume upload"} {
		t.Run(stage, func(t *testing.T) {
			logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
			failure := errors.New("checkpoint update unavailable")
			store.recordFailure = errors.Join(&checkpointstore.PendingSaveError{Err: errors.New("earlier save pending")}, failure)
			store.failRecord.Store(true)
			next := archiveFileOperationContinuation{
				group:        archiveUploadGroup{key: key, path: path, fileName: logger.Fsstore.FileName},
				destinations: logger.archiveUploadDestinationsSnapshot(),
			}
			if stage == "resume cursor update" {
				next.uploadState().UploadedDestinationIDs = []objectstorage.DestinationID{"primary"}
			}
			done, err := next.resume(context.Background(), logger, store, runNotificationImmediately)
			require.False(t, done)
			require.ErrorIs(t, err, failure)
			require.Equal(t, []objectstorage.DestinationID{"primary"}, next.uploadState().UploadedDestinationIDs)
			require.FileExists(t, path)
			attempts := remote.attemptCount()
			store.failRecord.Store(false)
			done, err = next.resume(context.Background(), logger, store, runNotificationImmediately)
			require.NoError(t, err)
			require.True(t, done)
			require.NoFileExists(t, path)
			require.Equal(t, attempts, remote.attemptCount(), "送信済みの宛先はcheckpoint更新だけを再試行する")
		})
	}
}

func TestInlineLogFileProcessingRecoveryRetainsRemovalSyncFailure(t *testing.T) {
	logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
	ctx := context.Background()
	_, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	require.NoError(t, os.Remove(path))
	// ファイルが既に消え、親を同期できない場合を実ファイル操作で再現します。
	missingPath := filepath.Join(t.TempDir(), "missing-parent", filepath.Base(path))
	next := archiveFileOperationContinuation{
		group:        archiveUploadGroup{key: key, path: missingPath},
		destinations: logger.archiveUploadDestinationsSnapshot(),
	}
	done, err := next.resume(ctx, logger, store, runNotificationImmediately)
	require.False(t, done, "同期だけの失敗も完了まで復旧処理を保持する")
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	checkpoint, err := store.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
	require.Zero(t, remote.attemptCount())
	require.NoError(t, os.Mkdir(filepath.Dir(missingPath), 0o700))
	done, err = next.resume(ctx, logger, store, runNotificationImmediately)
	require.NoError(t, err)
	require.True(t, done)
	require.Zero(t, remote.attemptCount(), "uploadを繰り返さず親directoryの同期だけを再開する")
}

func TestInlineCheckpointLoadRecoveryContinuesUploadAndRemovalWithoutWrite(t *testing.T) {
	t.Parallel()
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
		store.failLoad.Store(true)
		require.True(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		require.Zero(t, remote.attemptCount())
		require.True(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		store.failLoad.Store(false)
		// 新しいwriteを起こさなくても、再試行間隔の経過後に送信と削除まで進みます。
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		require.Equal(t, 1, remote.attemptCount())
		require.NoFileExists(t, path)
	})
}

func TestInlineLogFileProcessingRecoveryRetriesOnlyFailedDestination(t *testing.T) {
	t.Parallel()
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, primary, path, key := newInlineLogFileProcessingRecoveryFixture(t)
		secondary := &sequenceArchiveUploader{errs: []error{errors.New("S3 unavailable")}}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "primary", Uploader: primary},
			uploader.Destination{ID: "secondary", Uploader: secondary},
		)
		store.failRecord.Store(true)
		require.True(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		store.failRecord.Store(false)
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		require.Equal(t, 1, primary.attemptCount())
		require.Equal(t, 2, secondary.attemptCount())
		require.NoFileExists(t, path)
		checkpoint, err := store.Load(context.Background(), key)
		require.NoError(t, err)
		require.ElementsMatch(t, []objectstorage.DestinationID{"primary", "secondary"}, checkpoint.CompletedDestinationIDs)
	})
}

func TestInlineCloseDrainsOrdinaryUploadFailureWithoutAnotherWrite(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger, _, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
		remote.errs = []error{errors.New("S3 unavailable"), errors.New("S3 still unavailable")}
		require.True(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		require.True(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		closed := make(chan error, 1)
		go func() { closed <- logger.Close() }()
		synctest.Wait()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, closed)
		require.FileExists(t, path)
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		requireStartedCloseCompletesForTest(t, logger, closed)
		require.Equal(t, 3, remote.attemptCount())
		require.NoFileExists(t, path)
	})
}

func TestInlineLogFileProcessingRecoveryDeduplicatesNormalAttemptAndDrainsOnClose(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
		store.failRecord.Store(true)
		require.True(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		for range 3 {
			require.True(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		}
		require.Equal(t, 1, remote.attemptCount())
		require.EqualValues(t, 1, store.records.Load())
		closed := make(chan error, 1)
		go func() { closed <- logger.Close() }()
		synctest.Wait()
		time.Sleep(2 * checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, closed, "Close must retain unfinished checkpoint recording")
		require.True(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		require.FileExists(t, path)
		logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) { require.Contains(t, protected, path) })
		store.failRecord.Store(false)
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		requireStartedCloseCompletesForTest(t, logger, closed)
		require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		require.Equal(t, 1, remote.attemptCount(), "checkpoint recovery must not repeat upload")
		require.NoFileExists(t, path)
		logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) { require.NotContains(t, protected, path) })
		// Close以降の再登録は拒否され、取得した新しい保護も残しません。
		logger.queueArchiveLogFileProcessingRecovery(archiveFileOperationContinuation{group: archiveUploadGroup{key: key, path: path}}, nil)
		require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) { require.NotContains(t, protected, path) })
	})
}

func TestInlineLogFileProcessingRecoveryErrorHandlerCanRequestClose(t *testing.T) {
	t.Parallel()
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, path, _ := newInlineLogFileProcessingRecoveryFixture(t)
		store.failRecord.Store(true)
		closed := make(chan error, 1)
		var closeOnRecovery atomic.Bool
		var closeStarted atomic.Bool
		// callbackは再試行の開始前に設定し、実行中のloggerのフィールドを書き換えません。
		logger.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
			if closeOnRecovery.Load() && closeStarted.CompareAndSwap(false, true) {
				logger.RequestClose()
				go func() { closed <- logger.Close() }()
			}
		}
		require.True(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		closeOnRecovery.Store(true)
		synctest.Wait()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.True(t, closeStarted.Load())
		require.Empty(t, closed, "error handler Close waits for the failed recording")
		store.failRecord.Store(false)
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		requireStartedCloseCompletesForTest(t, logger, closed)
		require.Equal(t, 1, remote.attemptCount())
		require.NoFileExists(t, path)
	})
}

func TestInlineLogFileProcessingRecoveryCloseWaitsForLifecycleLock(t *testing.T) {
	t.Parallel()
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
		store.failRecord.Store(true)
		require.True(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		lockHeld := make(chan struct{})
		unlock := make(chan struct{})
		lockReleased := make(chan struct{})
		release := sync.OnceFunc(func() { close(unlock); <-lockReleased })
		go func() {
			defer close(lockReleased)
			_ = logger.lifecycleGuard.RunWithLifecycleLock(func() error {
				close(lockHeld)
				<-unlock
				return nil
			})
		}()
		receiveForTest(t, lockHeld, "lifecycle lock acquisition")
		defer release()
		store.failRecord.Store(false)
		synctest.Wait()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Zero(t, store.completedRecords.Load(), "ordering check waits for the lifecycle lock before advancing the cursor")
		closed := make(chan error, 1)
		go func() { closed <- logger.Close() }()
		synctest.Wait()
		time.Sleep(2 * checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, closed, "Close must retain the recovery waiting for the lifecycle lock")
		require.True(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		release()
		requireStartedCloseCompletesForTest(t, logger, closed)
		require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		require.Equal(t, 1, remote.attemptCount())
		require.NoFileExists(t, path)
	})
}

func TestInlineAcceptedTaskRegistersRecoveryAfterCloseStarts(t *testing.T) {
	t.Parallel()
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
		observed := &closeObservingInlineCheckpointStore{Store: store}
		logger.archiveUploadCheckpoints = observed
		store.failRecord.Store(true)
		require.True(t, logger.beginBackgroundOperation())
		releaseTask := make(chan struct{})
		finishedTask := make(chan struct{})
		release := sync.OnceFunc(func() { close(releaseTask) })
		defer release()
		go func() {
			defer close(finishedTask)
			defer logger.endBackgroundOperation()
			<-releaseTask
			logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), (&backgroundTaskNotificationQueue{}).enqueue)
		}()
		closed := make(chan error, 1)
		go func() { closed <- logger.Close() }()
		synctest.Wait()
		logger.backgroundMu.Lock()
		closeState := logger.closeState
		logger.backgroundMu.Unlock()
		require.Equal(t, loggercommon.LoggerCloseStateStarted, closeState)
		require.False(t, logger.beginBackgroundOperation(), "new work is refused after Close starts")
		require.Zero(t, remote.attemptCount())
		require.Zero(t, observed.closeCalls.Load())
		release()
		receiveForTest(t, finishedTask, "accepted task completion")
		synctest.Wait()
		require.True(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}), "accepted work must retain recovery admission until it finishes")
		for range 2 {
			time.Sleep(checkpointstore.SaveRecoveryInterval)
			synctest.Wait()
			require.Empty(t, closed)
			require.Zero(t, observed.closeCalls.Load(), "checkpoint Store remains open throughout file recovery")
			require.Equal(t, 1, remote.attemptCount())
			logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) { require.Contains(t, protected, path) })
		}
		store.failRecord.Store(false)
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		requireStartedCloseCompletesForTest(t, logger, closed)
		require.EqualValues(t, 1, observed.closeCalls.Load())
		require.Equal(t, 1, remote.attemptCount(), "draining records the already completed upload")
		require.NoFileExists(t, path)
		filesystem := store.Store.(*checkpointstore.FilesystemStore)
		restarted, err := checkpointstore.NewFilesystemStore(filesystem.Directory(), filesystem.SourceID())
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "restarted Close", func() closerForTest { return restarted }, true)
		checkpoint, err := restarted.Load(context.Background(), key)
		require.NoError(t, err)
		require.True(t, checkpoint.DestinationComplete("primary"))
	})
}

// 宛先の登録前から存在したため対象外になった圧縮fileは、最初のcheckpoint読込に失敗して
// 復旧処理へ移っても送信せずに残します。対象外の宛先だけで完了した世代は送達済みではないため、
// 復旧処理が完了と判定しても削除しません。
func TestInlineRecoveryKeepsPreRegistrationArchiveAfterCheckpointLoadFailure(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
		logger := newInlineUsageTestLogger(t, now, "pre-registration-recovery")
		path := writePendingCompressedLog(t, logger, "20260920", 1, now)
		cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "pre_registration"}
		remote := &checkpointDestinationRecordingArchiveUploader{destinationIDs: []objectstorage.DestinationID{"archive"}}
		// 既存fileがある状態で宛先を登録し、そのfileを新しい宛先の対象外にします。
		store, err := newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, logger.Fsstore.FileName, true, remote.DestinationIDs(), nil)
		require.NoError(t, err)
		failing := &recoverableInlineCheckpointStore{Store: store}
		failing.failLoad.Store(true)
		logger.archiveUploadCheckpoints, logger.ArchiveUploader = failing, remote
		groups := archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName)
		key := archiveLogFileProcessingRecoveryKey{File: groups[0].key}

		require.True(t, logger.processRemainingLogFiles(ctx, groups, runNotificationImmediately))
		require.True(t, logger.archiveLogFileProcessingRecovery.Has(key), "the failed checkpoint read must hand the file to recovery")
		failing.failLoad.Store(false)
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()

		require.False(t, logger.archiveLogFileProcessingRecovery.Has(key), "recovery completes without delivering the out-of-scope file")
		require.Empty(t, remote.requested)
		require.FileExists(t, path, "a file that existed before destination registration must not be deleted")
	})
}
