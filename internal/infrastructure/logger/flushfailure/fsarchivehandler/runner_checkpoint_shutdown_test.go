package fsarchivehandler

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
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFailureArchiveRunnerShutdownClosesFactoryCheckpointWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newRunnerCheckpointShutdownConfig(t)
		pipeline, err := newInlineCheckpointNotificationPipeline(cfg, nil)
		require.NoError(t, err)
		require.Len(t, pipeline.runnerRuntime.runners, 2)
		for _, runner := range pipeline.runnerRuntime.runners {
			_, err := runner.ReconcileObjectStorageUploadDestinations(context.Background())
			require.NoError(t, err)
		}
		require.NoError(t, pipeline.Shutdown(context.Background()))
		require.NoError(t, pipeline.Shutdown(context.Background()))
		synctest.Wait()
		waitFailureArchiveRunnerNotificationsDone(t, pipeline)
		require.Nil(t, pipeline.archiveRootLease)

		restarted, err := newInlineCheckpointNotificationPipeline(cfg, nil)
		require.NoError(t, err, "Shutdown releases the archive root after closing both runner stores")
		require.NoError(t, restarted.Shutdown(context.Background()))
		synctest.Wait()
		// The bubble cannot finish if either factory checkpoint notification worker
		// outlives the pipeline, even when the store never needed a save retry.
	})
}

func TestFailureArchiveRunnerCheckpointNotificationCanRequestShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		directory := t.TempDir()
		const source = "shutdown-notification"
		var pipeline *FailureArchivePipeline
		reported := make(chan error, 4)
		releaseCallback := make(chan struct{})
		var releaseOnce sync.Once
		resumeCallback := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
		closed := make(chan error, 1)
		store, err := logfile.NewCheckpointStore(directory, source, func(reportedErr error) {
			reported <- reportedErr
			<-releaseCallback
			pipeline.RequestShutdown()
			closed <- nil
		})
		require.NoError(t, err)
		runner := newCheckpointShutdownRunner(t, store)
		pipeline = &FailureArchivePipeline{runnerRuntime: newFailureArchiveRunnerRuntime([]*logfilerunner.Runner{runner})}
		pipeline.runnerRuntime.start(ctx)
		path := filepath.Join(directory, source+".log-file-uploads.json")
		restore := blockFailureArchiveCheckpointReplacement(t, path)
		// 試験が途中で失敗しても、止めた通知を再開してから片付けます。
		t.Cleanup(resumeCallback)
		_, err = store.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: "20260926-1"}, []objectstorage.DestinationID{"archive"})
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Len(t, reported, 1, "the runner store reports its failed save retry")
		var retryErr *logfile.CheckpointSaveRetryError
		require.ErrorAs(t, <-reported, &retryErr)

		// Complete the pending save before the callback invokes Shutdown so the
		// ordinary final scan succeeds, then require all retry workers to stop.
		restore()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		resumeCallback()
		synctest.Wait()
		require.Len(t, closed, 1, "checkpoint retry notification can request shutdown without waiting for itself")
		require.NoError(t, <-closed)
		require.NoError(t, pipeline.Shutdown(ctx))
		require.Nil(t, pipeline.archiveRootLease)
		time.Sleep(3 * checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, reported)
		require.NoError(t, pipeline.Shutdown(ctx))
	})
}

func TestFailureArchiveRunnerShutdownPreservesQueuedCheckpointNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		directory := t.TempDir()
		const source = "queued-shutdown-notifications"
		releaseCallback := make(chan struct{})
		var releaseOnce sync.Once
		resumeCallback := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
		var pipeline *FailureArchivePipeline
		var notifications atomic.Int32
		dispatcher := notification.NewDispatcher()
		store, err := logfile.NewCheckpointStore(directory, source, func(error) {
			<-releaseCallback
			dispatcher.Enqueue(func() {
				notifications.Add(1)
				pipeline.RequestShutdown()
			})
		})
		require.NoError(t, err)
		runner := newCheckpointShutdownRunner(t, store)
		pipeline = &FailureArchivePipeline{
			runnerRuntime:       newFailureArchiveRunnerRuntime([]*logfilerunner.Runner{runner}),
			runnerNotifications: dispatcher,
		}
		pipeline.runnerRuntime.start(ctx)
		restore := blockFailureArchiveCheckpointReplacement(t, filepath.Join(directory, source+".log-file-uploads.json"))
		// 試験が途中で失敗しても、止めた通知を再開してから片付けます。
		t.Cleanup(resumeCallback)
		_, err = store.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: "20260926-1"}, []objectstorage.DestinationID{"archive"})
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		for range 2 {
			time.Sleep(checkpointstore.SaveRecoveryInterval)
			synctest.Wait()
		}
		restore()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		shutdownResult := make(chan error, 1)
		go func() { shutdownResult <- pipeline.Shutdown(ctx) }()
		synctest.Wait()
		require.Empty(t, shutdownResult, "通常Shutdownは上流の受理済み通知を待つ")
		select {
		case <-dispatcher.Done():
			t.Fatal("上流の受理済み通知を渡す前に後段dispatcherが閉じた")
		default:
		}
		resumeCallback()
		synctest.Wait()
		require.NoError(t, <-shutdownResult)
		require.EqualValues(t, 2, notifications.Load())
		waitFailureArchiveRunnerNotificationsDone(t, pipeline)
	})
}

func TestFailureArchiveRunnerReconciliationFailureClosesCheckpointsWithoutStartingRunners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newRunnerCheckpointShutdownConfig(t)
		cfg.enableFileCompress = true
		codec := &unexpectedFailureArchiveCompressionCodec{format: pkgfsstore.CompressGzip}
		cfg.compressionCodec = codec
		retryableDir, err := failurearchive.GetFileArchiveDirectoryPath(cfg, cfg.dirCaseRetryable)
		require.NoError(t, err)
		permanentDir, err := failurearchive.GetFileArchiveDirectoryPath(cfg, cfg.dirCasePermanent)
		require.NoError(t, err)
		rawPath := filepath.Join(retryableDir, "test-app_20260926_1.jsonl")
		content := []byte("retained archive awaiting normal runner processing\n")
		require.NoError(t, os.WriteFile(rawPath, content, 0o600))
		badCompressedPath := filepath.Join(permanentDir, "test-app_20260926_1.jsonl.gz")
		require.NoError(t, os.Mkdir(badCompressedPath, 0o700))

		failed, err := newInlineCheckpointNotificationPipeline(cfg, nil)
		require.Nil(t, failed, "rollback completes even though normal runner processing would fail")
		require.ErrorIs(t, err, appendlog.ErrCompressedPeerNotRegular)
		require.ErrorIs(t, err, ErrFailureArchiveUploadDestinationReconciliation)
		require.Zero(t, codec.compressCalls.Load(), "constructor rollback must not run compression or upload")
		retained, err := os.ReadFile(rawPath)
		require.NoError(t, err)
		require.Equal(t, content, retained)
		require.NoFileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
		synctest.Wait()

		require.NoError(t, os.Remove(badCompressedPath))
		require.NoError(t, os.Remove(rawPath))
		restarted, err := newInlineCheckpointNotificationPipeline(cfg, nil)
		require.NoError(t, err, "failed reconciliation releases the root lease after closing both stores")
		require.NoError(t, restarted.Shutdown(context.Background()))
		synctest.Wait()
		// Both stores were constructed before reconciliation failed. Their workers
		// must also leave this bubble; releasing only the lease is insufficient.
	})
}

func TestFailureArchiveRunnerShutdownClosesBothStoresOnceAndJoinsCloseErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		retryableErr := errors.New("retryable runner checkpoint remains unsaved")
		permanentErr := errors.New("permanent runner checkpoint remains unsaved")
		stores := []*closeObservingArchiveCheckpointStore{
			{Store: newFailureArchiveCompressedLogCheckpointStoreForTest(t, t.TempDir(), "runner-retryable"), closeErr: retryableErr},
			{Store: newFailureArchiveCompressedLogCheckpointStoreForTest(t, t.TempDir(), "runner-permanent"), closeErr: permanentErr},
		}
		runners := make([]*logfilerunner.Runner, 0, len(stores))
		for _, store := range stores {
			runners = append(runners, newCheckpointShutdownRunner(t, store))
		}
		pipeline := &FailureArchivePipeline{runnerRuntime: newFailureArchiveRunnerRuntime(runners)}
		pipeline.runnerRuntime.start(context.Background())
		synctest.Wait()
		err := pipeline.Shutdown(context.Background())
		require.ErrorIs(t, err, retryableErr)
		require.ErrorIs(t, err, permanentErr)
		require.Equal(t, shutdownStateCompleted, pipeline.shutdownState)
		for _, store := range stores {
			require.EqualValues(t, 1, store.closeCalls.Load())
		}
		require.NoError(t, pipeline.Shutdown(context.Background()))
		for _, store := range stores {
			require.EqualValues(t, 1, store.closeCalls.Load(), "a completed shutdown must not close either store twice")
		}
	})
}

func TestFailureArchiveRunnerShutdownWaitsForIndependentLogFileProcessingRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &blockingRunnerRecoveryCheckpointStore{
			closeObservingArchiveCheckpointStore: &closeObservingArchiveCheckpointStore{
				Store: newFailureArchiveCompressedLogCheckpointStoreForTest(t, t.TempDir(), "blocked-runner-recovery"),
			},
			entered: make(chan struct{}), stopped: make(chan struct{}), release: make(chan struct{}),
		}
		runner := newCheckpointShutdownRunner(t, store)
		pipeline := &FailureArchivePipeline{
			runnerRuntime: newFailureArchiveRunnerRuntime([]*logfilerunner.Runner{runner}),
		}
		pipeline.runnerRuntime.start(context.Background())
		_, err := runner.ReconcileObjectStorageUploadDestinations(context.Background())
		require.ErrorIs(t, err, errRunnerCheckpointTemporarilyUnavailable)
		synctest.Wait()
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		select {
		case <-store.entered:
		default:
			t.Fatal("independent log file processing recovery did not start")
		}
		require.Zero(t, store.closeCalls.Load())
		store.onClose = func() {
			select {
			case <-store.stopped:
			default:
				t.Error("checkpoint store closed before its independent recovery stopped")
			}
		}

		closed := make(chan error, 1)
		go func() { closed <- pipeline.Shutdown(context.Background()) }()
		synctest.Wait()
		require.Empty(t, closed, "Shutdown waits while recovery holds the lifecycle lock")
		require.Zero(t, store.closeCalls.Load())
		close(store.release)
		synctest.Wait()
		require.NoError(t, <-closed)
		require.EqualValues(t, 1, store.closeCalls.Load())
		require.EqualValues(t, 2, store.registrationCalls.Load(), "Shutdown starts no new final scan")
		time.Sleep(2 * logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.EqualValues(t, 2, store.registrationCalls.Load(), "independent recovery must not resume after shutdown")
	})
}

var errRunnerCheckpointTemporarilyUnavailable = errors.New("checkpoint temporarily unavailable")

type blockingRunnerRecoveryCheckpointStore struct {
	*closeObservingArchiveCheckpointStore
	registrationCalls atomic.Int32
	entered           chan struct{}
	stopped           chan struct{}
	release           chan struct{}
}

func (s *blockingRunnerRecoveryCheckpointStore) RegisterDestinations(ctx context.Context, destinations []objectstorage.DestinationID, existing []checkpointstore.CheckpointKey) error {
	switch s.registrationCalls.Add(1) {
	case 1:
		return errRunnerCheckpointTemporarilyUnavailable
	case 2:
		close(s.entered)
		select {
		case <-ctx.Done():
			close(s.stopped)
			return ctx.Err()
		case <-s.release:
		}
		close(s.stopped)
		return s.Store.RegisterDestinations(ctx, destinations, existing)
	default:
		return s.Store.RegisterDestinations(ctx, destinations, existing)
	}
}

func newCheckpointShutdownRunner(t *testing.T, store checkpointstore.Store) *logfilerunner.Runner {
	t.Helper()
	directory := t.TempDir()
	manager := logfilelifecycle.NewFileSeriesManager(logfilelifecycle.FileSeriesManagerOptions{
		Config: appendlog.NewConfigAdapter(pkgfsstore.Config{
			Layout: pkgfsstore.FsstoreConfig{
				Target: "test-app", LogPath: directory, LogDir: "logs",
				FileNameFormat: "/%s%s_%s_%d.jsonl", FileDateFormat: pkgfsstore.FileDateFormat,
			},
			Lifecycle: pkgfsstore.FileLifecycleConfig{RotationType: pkgfsstore.FileRotationDaily},
		}),
		FileName:        logpattern.FileNamePattern{Dir: directory, Target: "test-app", Extension: "jsonl"},
		CheckpointStore: store,
		Uploader:        newDestinationFailureArchiveUploader("archive"),
	})
	lifecycle, err := logfilelifecycle.NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	return logfilerunner.NewRunner(lifecycle, logfilerunner.StaticRunnerConfig{IntervalValue: time.Hour}, nil)
}

func newRunnerCheckpointShutdownConfig(t *testing.T) testArchiveConfig {
	t.Helper()
	cfg := newInlineCheckpointNotificationConfig(t)
	cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
	cfg.lifecycleRunnerConfig.Interval = time.Hour
	return cfg
}
