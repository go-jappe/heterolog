package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type transientArchiveCheckpointStore struct {
	checkpointstore.Store
	mu                           sync.Mutex
	loadFailures, recordFailures int
	// loadFailuresUntilが閉じるまで、Loadは回数に関係なく失敗します。upload対象の保護の確認も
	// Loadを読み、失敗を通知せず保護を続けるため、通知を確かめる試験は回数でなく通知まで失敗させます。
	loadFailuresUntil <-chan struct{}
	recordSucceeded   chan struct{}
	failure           error
}

func (s *transientArchiveCheckpointStore) fail(remaining *int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if *remaining == 0 {
		return nil
	}
	*remaining--
	if s.failure != nil {
		return s.failure
	}
	return errors.New("temporary checkpoint persistence failure")
}

func (s *transientArchiveCheckpointStore) Load(ctx context.Context, key checkpointstore.CheckpointKey) (checkpointstore.Checkpoint, error) {
	if s.loadFailuresUntil != nil {
		select {
		case <-s.loadFailuresUntil:
		default:
			return checkpointstore.Checkpoint{}, s.failure
		}
	}
	if err := s.fail(&s.loadFailures); err != nil {
		return checkpointstore.Checkpoint{}, err
	}
	return s.Store.Load(ctx, key)
}

func (s *transientArchiveCheckpointStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	if err := s.fail(&s.recordFailures); err != nil {
		return checkpointstore.Checkpoint{}, err
	}
	checkpoint, err := s.Store.AdvanceCompletionCursor(ctx, key, destinations)
	if err == nil && s.recordSucceeded != nil {
		select {
		case s.recordSucceeded <- struct{}{}:
		default:
		}
	}
	return checkpoint, err
}

func TestFailureArchiveLogFileProcessingRecoveryDoesNotAcceptMixedPendingSaveError(t *testing.T) {
	for _, stage := range []string{"resume cursor update", "resume upload"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("checkpoint update unavailable")
			pipeline, _, remote, operation := newArchiveConflictRecoveryFixture(t, []objectstorage.DestinationID{"primary"},
				func(filesystem checkpointstore.Store) checkpointstore.Store {
					return &transientArchiveCheckpointStore{
						Store: filesystem, recordFailures: 1,
						failure: errors.Join(&checkpointstore.PendingSaveError{Err: errors.New("earlier save pending")}, failure),
					}
				})
			if stage == "resume cursor update" {
				operation.uploadedDestinationIDs = []objectstorage.DestinationID{"primary"}
			}
			_, pending, err := pipeline.handler.runArchiveFileOperation(context.Background(), operation)
			require.True(t, pending)
			require.ErrorIs(t, err, failure)
			require.Equal(t, []objectstorage.DestinationID{"primary"}, operation.uploadedDestinationIDs)
			require.FileExists(t, operation.path)
			attempts := remote.attempts["primary"]
			_, pending, err = pipeline.handler.runArchiveFileOperation(context.Background(), operation)
			require.NoError(t, err)
			require.False(t, pending)
			require.NoFileExists(t, operation.path)
			require.Equal(t, attempts, remote.attempts["primary"], "送信済みの宛先はcheckpoint更新だけを再試行する")
		})
	}
}

func TestFailureArchiveLogFileProcessingRecoveryWithoutWriteOrPurge(t *testing.T) {
	for _, stage := range []string{"load", "record"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				category := failurearchive.FailureCategoryRetryable
				store := &transientArchiveCheckpointStore{}
				switch stage {
				case "load":
					store.loadFailures = 1
				case "record":
					store.recordFailures = 1
				}
				remote := newDestinationFailureArchiveUploader("destination")
				pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
					cfg.setArchiveUploader(category, remote)
					cfg.embedArchiveCheckpointStore(category, &store.Store, store)
				})
				handler := pipeline.handler
				fileName := handler.Fsstore.FileNameRetryable
				path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
				require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
				outcome := handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
				require.True(t, outcome.retryRequired)
				key := failureArchiveCompressedLogCheckpointKey(t, handler, category, path)
				recoveryKey := failureArchiveRecoveryKey(category, key)
				require.True(t, handler.logFileProcessingRecovery.Has(recoveryKey))
				protected := handler.protectedArchivePaths(category)
				require.Contains(t, protected, path)
				require.Contains(t, protected, path[:len(path)-len(".gz")])
				require.FileExists(t, path)
				// 通常経路と後続世代は、保留中の完了更新を追い越しません。
				attempts := remote.attemptCount(path, "destination")
				handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
				require.Equal(t, attempts, remote.attemptCount(path, "destination"))
				other := filepath.Join(fileName.Dir, "test-app_20260922_2.jsonl.gz")
				require.NoError(t, os.WriteFile(other, []byte("another compressed log"), 0o600))
				otherOutcome := handler.uploadCheckpointedArchiveGroup(context.Background(), []string{other}, category, fileName)
				require.True(t, otherOutcome.retryRequired)
				require.FileExists(t, other)
				require.Zero(t, remote.attemptCount(other, "destination"))
				// Clockは固定、通常writeもcleanupも呼ばず、再試行間隔の経過だけで後続の削除まで進みます。
				// 間隔はtimerの仮想時間で進め、経過前には再試行しないことも確認します。
				time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval - time.Nanosecond)
				synctest.Wait()
				require.True(t, handler.logFileProcessingRecovery.Has(recoveryKey), "recovery must wait for the retry interval")
				require.FileExists(t, path)
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				require.False(t, handler.logFileProcessingRecovery.Has(recoveryKey))
				require.NoFileExists(t, path)
				require.Equal(t, 1, remote.attemptCount(path, "destination"))
				checkpoint, err := store.Store.Load(context.Background(), key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"destination"}, checkpoint.CompletedDestinationIDs)
				require.NotContains(t, handler.protectedArchivePaths(category), path)
				require.NoError(t, handler.logFileProcessingRecovery.Close(), "先行世代を待っていた受理済み後続処理も完了する")
				otherOutcome = handler.uploadCheckpointedArchiveGroup(context.Background(), []string{other}, category, fileName)
				require.False(t, otherOutcome.retryRequired)
				require.NoFileExists(t, other)
				require.Equal(t, 1, remote.attemptCount(other, "destination"))
			})
		})
	}
}

func TestFailureArchiveLogFileProcessingRecoveryRetriesOnlyFailedRemoteDestination(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		category := failurearchive.FailureCategoryRetryable
		store := &transientArchiveCheckpointStore{recordFailures: 1}
		remote := newDestinationFailureArchiveUploader("success", "failure")
		remote.failNext("failure", 1)
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
			cfg.embedArchiveCheckpointStore(category, &store.Store, store)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
		outcome := handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
		require.True(t, outcome.retryRequired)
		key := failureArchiveCompressedLogCheckpointKey(t, handler, category, path)
		recoveryKey := failureArchiveRecoveryKey(category, key)
		// 再試行間隔はtimerの仮想時間で進め、経過前には失敗した宛先へ再送しないことも確認します。
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval - time.Nanosecond)
		synctest.Wait()
		require.True(t, handler.logFileProcessingRecovery.Has(recoveryKey), "recovery must wait for the retry interval")
		require.Equal(t, 1, remote.attemptCount(path, "failure"))
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		require.False(t, handler.logFileProcessingRecovery.Has(recoveryKey))
		require.NoFileExists(t, path)
		require.Equal(t, 1, remote.attemptCount(path, "success"))
		require.Equal(t, 2, remote.attemptCount(path, "failure"))
		checkpoint, err := store.Load(context.Background(), key)
		require.NoError(t, err)
		require.ElementsMatch(t, []objectstorage.DestinationID{"success", "failure"}, checkpoint.CompletedDestinationIDs)
		outcome = handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
		require.False(t, outcome.retryRequired)
		require.NoFileExists(t, path)
		require.Equal(t, 1, remote.attemptCount(path, "success"))
		require.Equal(t, 2, remote.attemptCount(path, "failure"))
	})
}

func TestFailureArchiveShutdownDrainsPendingLogFileProcessingRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		category := failurearchive.FailureCategoryRetryable
		store := &transientArchiveCheckpointStore{recordFailures: 1}
		remote := newDestinationFailureArchiveUploader("destination")
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
			cfg.embedArchiveCheckpointStore(category, &store.Store, store)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
		handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
		key := failureArchiveCompressedLogCheckpointKey(t, handler, category, path)
		require.True(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(category, key)))
		require.NoError(t, pipeline.Shutdown(context.Background()))
		require.False(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(category, key)))
		require.NoFileExists(t, path)
		require.NotContains(t, handler.protectedArchivePaths(category), path)
		require.Equal(t, 1, remote.attemptCount(path, "destination"))
	})
}

func TestFailureArchiveAcceptedTaskRegistersRecoveryAfterShutdownStarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		category := failurearchive.FailureCategoryRetryable
		store := &transientArchiveCheckpointStore{
			recordFailures: 1,
		}
		remote := newDestinationFailureArchiveUploader("destination")
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
			cfg.embedArchiveCheckpointStore(category, &store.Store, store)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("accepted archive"), 0o600))
		require.True(t, handler.beginArchiveTask())
		finishTask := sync.OnceFunc(handler.endArchiveTask)
		defer finishTask()
		closed := make(chan error, 1)
		go func() { closed <- pipeline.Shutdown(context.Background()) }()
		require.Eventually(t, func() bool {
			handler.archiveTaskMu.Lock()
			defer handler.archiveTaskMu.Unlock()
			return handler.archiveTaskClosing
		}, time.Second, time.Millisecond)

		outcome := handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
		require.True(t, outcome.retryRequired)
		key := failureArchiveCompressedLogCheckpointKey(t, handler, category, path)
		require.True(t, handler.logFileProcessingRecovery.Has(failureArchiveRecoveryKey(category, key)))
		require.Contains(t, handler.protectedArchivePaths(category), path)
		finishTask()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(8 * time.Second):
			t.Fatal("Shutdown did not drain recovery registered by the accepted task")
		}
		require.NoFileExists(t, path)
		require.Equal(t, 1, remote.attemptCount(path, "destination"))
	})
}

func TestFailureArchiveLogFileProcessingRecoveryNotifiesEachFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		category := failurearchive.FailureCategoryRetryable
		store := &transientArchiveCheckpointStore{recordFailures: 2}
		remote := newDestinationFailureArchiveUploader("destination")
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, remote)
			cfg.embedArchiveCheckpointStore(category, &store.Store, store)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
		// 初回と再試行の失敗をその場で通知し、終了待ちは呼び出し側から行います。
		notified := make(chan struct{}, 4)
		handler.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
			notified <- struct{}{}
		}
		handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
		for range 2 {
			select {
			case <-notified:
			case <-time.After(12 * time.Second):
				t.Fatal("log file processing recovery failure was not reported")
			}
		}
		require.NoError(t, pipeline.Shutdown(context.Background()))
		require.Empty(t, notified, "each failed attempt must be reported once")
		require.NoFileExists(t, path)
		require.Equal(t, 1, remote.attemptCount(path, "destination"), "checkpoint retry must not upload again")
	})
}

func TestFailureArchiveShutdownWaitsForRecoveryDirectoryLock(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		category := failurearchive.FailureCategoryRetryable
		store := &transientArchiveCheckpointStore{
			recordFailures: 1, recordSucceeded: make(chan struct{}, 1),
		}
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.setArchiveUploader(category, newDestinationFailureArchiveUploader("destination"))
			cfg.embedArchiveCheckpointStore(category, &store.Store, store)
		})
		handler := pipeline.handler
		fileName := handler.Fsstore.FileNameRetryable
		path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
		require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
		handler.uploadCheckpointedArchiveGroup(context.Background(), []string{path}, category, fileName)
		locked, unlock, lockStopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
		go func() {
			defer close(lockStopped)
			_ = handler.retryableDirectory.guard.RunWithLifecycleLock(func() error { close(locked); <-unlock; return nil })
		}()
		release := sync.OnceFunc(func() { close(unlock) })
		defer func() { release(); <-lockStopped }()
		<-locked
		time.Sleep(2 * checkpointstore.SaveRecoveryInterval)
		require.Empty(t, store.recordSucceeded, "ordering check waits for the directory lock before recording completion")
		closed := make(chan error, 1)
		go func() { closed <- pipeline.Shutdown(context.Background()) }()
		synctest.Wait()
		require.Empty(t, closed, "Shutdown must retain recovery waiting for the lifecycle lock")
		require.FileExists(t, path)
		require.Contains(t, handler.protectedArchivePaths(category), path)
		release()
		require.NoError(t, <-closed)
		require.NoFileExists(t, path)
		require.NotContains(t, handler.protectedArchivePaths(category), path)
	})
}
