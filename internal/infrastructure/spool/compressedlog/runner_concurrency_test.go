package compressedlog

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestRunnerRunWaitsForManualRunOnceBeforeRecoveringProcessing(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		store := replaytest.NewStore(t)
		replaytest.SaveFile(t, store, "app_20260909_1.log.gz")
		ready := make(chan struct{}, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		upload := &replaytest.Uploader{Ready: ready, Block: release}
		worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
			FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
		}})
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		reports := make(chan RunReport, 1)
		runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), runnerReporterFunc(
			func(_ context.Context, report RunReport, _ error) {
				reports <- report
				cancel()
			},
		))
		require.NoError(t, err)
		manualDone := make(chan error, 1)
		go func() {
			_, err := runner.RunOnce(context.Background())
			manualDone <- err
		}()
		select {
		case <-ready:
		case <-time.After(time.Minute):
			t.Fatal("manual RunOnce did not start its upload")
		}
		processingPath := upload.LastPath()
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(ctx) }()

		// 先行する手動uploadがprocessing transitionを所有している間は、
		// 定期recoveryも別の手動cycleもfileを再claimしてはなりません。
		// 待機中の周期は仮想時間で何度も期限を迎えますが、先行するuploadの完了前には進みません。
		waitingContext, cancelWaiting := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancelWaiting()
		_, err = runner.RunOnce(waitingContext)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		synctest.Wait()
		require.Empty(t, reports)
		require.FileExists(t, processingPath)
		active, err := store.ListFiles(context.Background(), pkgspool.StateActive)
		require.NoError(t, err)
		require.Empty(t, active)

		releaseOnce.Do(func() { close(release) })
		require.NoError(t, waitRunnerOperationForTest(t, manualDone))
		require.ErrorIs(t, waitRunnerOperationForTest(t, runDone), context.Canceled)
		report := <-reports
		require.Zero(t, report.RecoveredProcessingCount)
		require.Empty(t, report.CompressedLogFiles)
		_ = replaytest.OnlyFile(t, store, pkgspool.StateDone)
		require.Equal(t, 1, upload.CallCount())
	})
}

func TestRunnerRunCancellationWaitsForInFlightTransition(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		store := replaytest.NewStore(t)
		replaytest.SaveFile(t, store, "app_20260909_2.log.gz")
		ready := make(chan struct{}, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		upload := &replaytest.Uploader{Ready: ready, Block: release}
		worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
			FileName: replaytest.FileNamePattern("app_"),
			Uploader: upload,
		}})
		runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), nil)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		done := make(chan error, 1)
		go func() { done <- runner.Run(ctx) }()
		select {
		case <-ready:
		case <-time.After(time.Minute):
			t.Fatal("periodic upload did not start")
		}
		cancel()
		// 送信側はcontextの中止に応答します。通常の停止要求をそのcontextへ伝えず、
		// 受理済みの送信とdoneへの移動が完了するまでRunが待つことを確認します。
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Run returned before file transition completed: %v", err)
		default:
		}
		require.True(t, runner.IsRunning())
		_ = replaytest.OnlyFile(t, store, pkgspool.StateProcessing)
		releaseOnce.Do(func() { close(release) })
		require.ErrorIs(t, waitRunnerOperationForTest(t, done), context.Canceled)
		require.False(t, runner.IsRunning())
		_ = replaytest.OnlyFile(t, store, pkgspool.StateDone)
	})
}

func TestRunnerRunCancellationLeavesManualRunOnceWithItsCaller(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		store := replaytest.NewStore(t)
		replaytest.SaveFile(t, store, "app_20260909_3.log.gz")
		ready := make(chan struct{}, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		upload := &replaytest.Uploader{Ready: ready, Block: release}
		worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
			FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
		}})
		runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Hour}), nil)
		require.NoError(t, err)
		manualDone := make(chan error, 1)
		go func() {
			_, err := runner.RunOnce(context.Background())
			manualDone <- err
		}()
		select {
		case <-ready:
		case <-time.After(time.Minute):
			t.Fatal("manual upload did not start")
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		runDone := make(chan error, 1)
		go func() { runDone <- runner.Run(ctx) }()
		synctest.Wait()
		require.True(t, runner.IsRunning())
		cancel()
		require.ErrorIs(t, waitRunnerOperationForTest(t, runDone), context.Canceled)
		synctest.Wait()
		select {
		case err := <-manualDone:
			t.Fatalf("Run cancellation interrupted manual upload: %v", err)
		default:
		}
		releaseOnce.Do(func() { close(release) })
		require.NoError(t, waitRunnerOperationForTest(t, manualDone))
		_ = replaytest.OnlyFile(t, store, pkgspool.StateDone)
		require.Equal(t, 1, upload.CallCount())
	})
}

func TestRunnerPurgeSerializesWithManualReplayAndCancelsLockWaits(t *testing.T) {
	stores, store := replaytest.NewDestinationStores(t)
	worker := newReplayTestWorker(t, store, nil)
	purger, err := objectstoragespool.NewTerminalPurger(stores, objectstoragespool.NewPurgeConfigAdapter(objectstoragespool.TerminalPurgeConfig{}))
	require.NoError(t, err)
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Hour, PurgeInterval: time.Hour}), nil,
		WithTerminalPurge(purger, &infrafsstore.ContextRWMutex{}))
	require.NoError(t, err)
	synctest.Test(t, func(t *testing.T) {
		runner.storeOrPurgeMu.Lock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		purgeDone := make(chan error, 1)
		go func() {
			_, err := runner.runPurgeOnce(ctx)
			purgeDone <- err
		}()
		synctest.Wait()
		waitingContext, cancelWaiting := context.WithTimeout(context.Background(), time.Second)
		defer cancelWaiting()
		_, err := runner.RunOnce(waitingContext)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		cancel()
		require.ErrorIs(t, <-purgeDone, context.Canceled)
		runner.storeOrPurgeMu.Unlock()

		// store lockを待つ間にcancelされたら、runMuを解放しなければなりません。
		_, err = runner.runPurgeOnce(context.Background())
		require.NoError(t, err)
	})
}

func waitRunnerOperationForTest(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(hangDetectionTimeout):
		t.Fatal("runner operation did not complete")
		return nil
	}
}
