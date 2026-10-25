package compressedlog

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
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

type runnerReporterFunc func(context.Context, RunReport, error)

func (f runnerReporterFunc) ReportSpoolRunnerRun(ctx context.Context, report RunReport, err error) {
	f(ctx, report, err)
}

// errReporterDidNotStopRunは、reporterがhangDetectionTimeout以内にRunを止めなかったことを表します。
var errReporterDidNotStopRun = errors.New("reporter did not stop Run within the hang-detection timeout")

// runContextStoppedByReporterは、reporterがcancelして止めるRunのcontextを返します。止まらない場合は
// hangDetectionTimeoutで終了し、requireRunStoppedByReporterがその理由で失敗させます。
func runContextStoppedByReporter() (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(context.Background(), hangDetectionTimeout, errReporterDidNotStopRun)
}

// requireRunStoppedByReporterは、ctxで実行したRunが、上限ではなくreporterのcancelで終了したことを確認します。
func requireRunStoppedByReporter(t *testing.T, ctx context.Context, runErr error) {
	t.Helper()
	if cause := context.Cause(ctx); errors.Is(cause, errReporterDidNotStopRun) {
		t.Fatalf("Run() error = %v: %v", runErr, cause)
	}
	require.ErrorIs(t, runErr, context.Canceled)
}

func TestRunnerReporterCanCancelRunContextSynchronously(t *testing.T) {
	for _, test := range []struct {
		name      string
		uploadErr error
		wantState pkgspool.State
	}{
		{name: "uploaded", wantState: pkgspool.StateDone},
		{name: "retryable", uploadErr: errors.New("transient upload failure"), wantState: pkgspool.StateActive},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := replaytest.NewStore(t)
			file := replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
			upload := &replaytest.Uploader{Err: test.uploadErr}
			worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
				FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
			}})
			ctx, cancel := runContextStoppedByReporter()
			defer cancel()
			var got RunReport
			var gotErr error
			runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), runnerReporterFunc(
				func(_ context.Context, report RunReport, runErr error) {
					got, gotErr = report, runErr
					cancel()
				},
			))
			require.NoError(t, err)
			requireRunStoppedByReporter(t, ctx, runner.Run(ctx))
			require.ErrorIs(t, gotErr, test.uploadErr)
			require.Len(t, got.CompressedLogFiles, 1)
			require.Equal(t, test.wantState, got.CompressedLogFiles[0].State)
			require.Nil(t, got.PurgeReport)
			require.Equal(t, file.FileName, replaytest.OnlyFile(t, store, test.wantState).FileName)
			require.Equal(t, 1, upload.CallCount())
			require.False(t, runner.IsRunning())
		})
	}
}

func TestRunnerRunWaitsForSynchronousReporterAfterCancellation(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		store := replaytest.NewStore(t)
		replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
		worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
			FileName: replaytest.FileNamePattern("app_"), Uploader: &replaytest.Uploader{},
		}})
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		entered, release := make(chan context.Context, 1), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		t.Cleanup(unblock)
		runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), runnerReporterFunc(
			func(ctx context.Context, _ RunReport, _ error) {
				entered <- ctx
				<-release
			},
		))
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- runner.Run(ctx) }()
		var notificationContext context.Context
		select {
		case notificationContext = <-entered:
		case <-time.After(time.Minute):
			t.Fatal("reporter did not start")
		}
		cancel()
		require.NoError(t, notificationContext.Err(), "normal stop must not cancel an accepted reporter")
		// 停止要求の後も、受理済みの通知が終わるまでRunは戻りません。
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Run returned before synchronous reporter completed: %v", err)
		default:
		}
		require.True(t, runner.IsRunning())
		_ = replaytest.OnlyFile(t, store, pkgspool.StateDone)
		unblock()
		require.ErrorIs(t, waitRunnerOperationForTest(t, done), context.Canceled)
		require.False(t, runner.IsRunning())
		require.NoError(t, notificationContext.Err(), "normal completion must leave queued downstream notifications usable")
	})
}

func TestRunnerReporterPreservesOrderAndContinuesAfterPanic(t *testing.T) {
	synctestFromWallClockForTest(t, func(t *testing.T) {
		store := replaytest.NewStore(t)
		wantNames := []string{"app_20260906_1.log.gz", "app_20260906_2.log.gz", "app_20260906_3.log.gz"}
		oldest := time.Now().Add(-time.Hour)
		for index, name := range wantNames {
			file := replaytest.SaveFile(t, store, name)
			mtime := oldest.Add(time.Duration(index) * time.Second)
			require.NoError(t, os.Chtimes(file.Path, mtime, mtime))
		}
		upload := &replaytest.Uploader{}
		worker := newReplayTestWorker(t, store, compressedlogworker.RouteRegistry{{
			FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
		}})
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		var active atomic.Int32
		var overlapped atomic.Bool
		completed := make(chan string, len(wantNames))
		secondEntered, releaseSecond := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblockSecond := func() { releaseOnce.Do(func() { close(releaseSecond) }) }
		t.Cleanup(unblockSecond)
		reporter := runnerReporterFunc(func(_ context.Context, report RunReport, _ error) {
			if active.Add(1) != 1 {
				overlapped.Store(true)
			}
			defer active.Add(-1)
			if len(report.CompressedLogFiles) == 0 {
				return
			}
			name := report.CompressedLogFiles[0].FileName
			defer func() { completed <- name }()
			if name == wantNames[0] {
				panic("reporter failure")
			}
			if name == wantNames[1] {
				close(secondEntered)
				<-releaseSecond
			}
		})
		runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), reporter)
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- runner.Run(ctx) }()
		select {
		case <-secondEntered:
		case <-time.After(time.Minute):
			t.Fatal("second reporter did not start after first reporter panic")
		}
		// 二件目の通知が終わるまでは、再送周期を何度過ぎても次のuploadを始めません。
		time.Sleep(30 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, 2, upload.CallCount())
		unblockSecond()
		gotNames := make([]string, 0, len(wantNames))
		for range wantNames {
			select {
			case name := <-completed:
				gotNames = append(gotNames, name)
			case <-time.After(time.Minute):
				t.Fatalf("reporter stopped after notifications %v", gotNames)
			}
		}
		cancel()
		require.ErrorIs(t, waitRunnerOperationForTest(t, done), context.Canceled)
		require.Equal(t, wantNames, gotNames)
		require.False(t, overlapped.Load())
		require.Equal(t, len(wantNames), upload.CallCount())
		files, err := store.ListFiles(context.Background(), pkgspool.StateDone)
		require.NoError(t, err)
		require.Len(t, files, len(wantNames))
	})
}

func TestRunnerReportsReplayPanicAndContinuesAfterRecovery(t *testing.T) {
	store := replaytest.NewStore(t)
	replaytest.SaveFile(t, store, "app_20260906_1.log.gz")
	_, claimed, err := store.ClaimOldestActive(context.Background())
	require.NoError(t, err)
	require.True(t, claimed)
	attempts := 0
	routes := compressedlogworker.RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: &replaytest.Uploader{},
	}}
	worker, err := compressedlogworker.NewWorker(store, routes, 5*time.Minute, compressedlogworker.WithRouteSnapshot(func() compressedlogworker.RouteRegistry {
		attempts++
		if attempts == 1 {
			panic("route snapshot failed")
		}
		return routes
	}))
	require.NoError(t, err)
	ctx, cancel := runContextStoppedByReporter()
	defer cancel()
	var reports []RunReport
	var runErrors []error
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), runnerReporterFunc(
		func(_ context.Context, report RunReport, runErr error) {
			reports = append(reports, report)
			runErrors = append(runErrors, runErr)
			if len(reports) == 2 {
				cancel()
			}
		},
	))
	require.NoError(t, err)
	requireRunStoppedByReporter(t, ctx, runner.Run(ctx))
	require.Len(t, reports, 2)
	requirePanicError(t, runErrors[0], ErrReplayPanic, "route snapshot failed")
	require.NoError(t, runErrors[1])
	require.Equal(t, 1, reports[1].RecoveredProcessingCount)
	require.Len(t, reports[1].CompressedLogFiles, 1)
	_ = replaytest.OnlyFile(t, store, pkgspool.StateDone)
}

func TestRunnerReportsIndependentPurgeCycles(t *testing.T) {
	stores, store := replaytest.NewDestinationStores(t)
	worker := newReplayTestWorker(t, store, nil)
	ctx, cancel := runContextStoppedByReporter()
	defer cancel()
	var reports []RunReport
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{
		Interval: time.Hour, PurgeInterval: time.Millisecond,
	}), runnerReporterFunc(func(_ context.Context, report RunReport, runErr error) {
		require.NoError(t, runErr)
		reports = append(reports, report)
		if len(reports) == 2 {
			cancel()
		}
	}))
	require.NoError(t, err)
	runner.purger, err = objectstoragespool.NewTerminalPurger(stores, objectstoragespool.NewPurgeConfigAdapter(objectstoragespool.TerminalPurgeConfig{}))
	require.NoError(t, err)
	requireRunStoppedByReporter(t, ctx, runner.Run(ctx))
	require.Len(t, reports, 2)
	for _, report := range reports {
		require.NotNil(t, report.PurgeReport)
		require.Empty(t, report.CompressedLogFiles)
	}
}

func TestRunnerReportsPurgePanicAndContinues(t *testing.T) {
	stores, store := replaytest.NewDestinationStores(t)
	worker := newReplayTestWorker(t, store, nil)
	config := &panicOnceRunnerPurgeConfig{PurgeConfigProvider: objectstoragespool.NewPurgeConfigAdapter(objectstoragespool.TerminalPurgeConfig{})}
	purger, err := objectstoragespool.NewTerminalPurger(stores, config)
	require.NoError(t, err)
	config.armed = true
	ctx, cancel := runContextStoppedByReporter()
	defer cancel()
	var reports []RunReport
	var runErrors []error
	runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{
		Interval: time.Hour, PurgeInterval: time.Millisecond,
	}), runnerReporterFunc(func(_ context.Context, report RunReport, runErr error) {
		reports = append(reports, report)
		runErrors = append(runErrors, runErr)
		if len(reports) == 2 {
			cancel()
		}
	}), WithTerminalPurge(purger, &infrafsstore.ContextRWMutex{}))
	require.NoError(t, err)
	requireRunStoppedByReporter(t, ctx, runner.Run(ctx))
	require.Len(t, reports, 2)
	require.NotNil(t, reports[0].PurgeReport)
	requirePanicError(t, runErrors[0], ErrTerminalPurgePanic, "purge provider failed")
	require.NotNil(t, reports[1].PurgeReport)
	require.NoError(t, runErrors[1])
}

type panicOnceRunnerPurgeConfig struct {
	objectstoragespool.PurgeConfigProvider
	armed bool
}

func (c *panicOnceRunnerPurgeConfig) DonePurge() objectstoragespool.StatePurgeConfigProvider {
	if c.armed {
		c.armed = false
		panic("purge provider failed")
	}
	return c.PurgeConfigProvider.DonePurge()
}

// requirePanicErrorは、回復したpanicを種類とpanicの値で確認します。
func requirePanicError(t *testing.T, err error, kind error, value any) {
	t.Helper()
	require.ErrorIs(t, err, kind)
	var panicErr *PanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, value, panicErr.Value)
}
