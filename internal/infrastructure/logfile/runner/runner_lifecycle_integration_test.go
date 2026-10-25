package runner

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loglifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type panicOnceRunnerUploader struct {
	calls   atomic.Int64
	onFirst func()
}

func (u *panicOnceRunnerUploader) Upload(context.Context, uploader.UploadRequest) error {
	if u.calls.Add(1) == 1 {
		if u.onFirst != nil {
			u.onFirst()
		}
		panic("initial uploader panic")
	}
	return nil
}

func (*panicOnceRunnerUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"archive"}
}

func (u *panicOnceRunnerUploader) UploadDestinations(ctx context.Context, request uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	err := u.Upload(ctx, request)
	report := uploader.UploadReport{}
	for _, destination := range destinations {
		report.Results = append(report.Results, uploader.DestinationResult{DestinationID: destination, Err: err})
	}
	return report
}

func TestRunnerShutdownDrainsInitialPanicAfterSupervisorCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, target := resolvedTempDirForTest(t), "supervisor-panic-drain"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		path := filepath.Join(directory, target+"_"+currentLogDate()+"_1.log.gz")
		writeGzipFile(t, path, []byte("accepted upload"))
		writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		remote := &panicOnceRunnerUploader{onFirst: cancel}
		destinations, store := testFileSeriesUploadDependencies(t, remote)
		manager := loglifecycle.NewFileSeriesManager(loglifecycle.FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, false), Uploader: destinations, CheckpointStore: store,
		})
		t.Cleanup(func() {
			_ = manager.ForceClose()
			_ = boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose)
		})
		lifecycle, err := loglifecycle.NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		runner := NewRunner(lifecycle, StaticRunnerConfig{IntervalValue: time.Second}, nil,
			WithSupervisor(StaticSupervisorConfig{RestartDelayValue: time.Hour, MaxRestartsValue: 1}))
		require.ErrorIs(t, runner.Run(ctx), context.Canceled)
		require.EqualValues(t, 1, remote.calls.Load())
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		require.EqualValues(t, 2, remote.calls.Load())
		require.NoFileExists(t, path)
	})
}

type forceAwareRunnerUploader struct {
	entered chan struct{}
	calls   atomic.Int64
}

func (u *forceAwareRunnerUploader) Upload(ctx context.Context, _ uploader.UploadRequest) error {
	if u.calls.Add(1) == 1 {
		close(u.entered)
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestRunnerForceCloseCancelsAcceptedCycle(t *testing.T) {
	// 通常の中止で受理済みの送信を打ち切らないことは、実時間の待機ではなく、
	// bubble内の全goroutineが停止した時点で確認します。
	synctest.Test(t, func(t *testing.T) {
		root, target := resolvedTempDirForTest(t), "force-cycle"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
		writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 2)
		remote := &forceAwareRunnerUploader{entered: make(chan struct{})}
		destinations, store := testFileSeriesUploadDependencies(t, remote)
		manager := loglifecycle.NewFileSeriesManager(loglifecycle.FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 0, 0, 0, false), Uploader: destinations, CheckpointStore: store,
		})
		lifecycle, err := loglifecycle.NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		runner := NewRunner(lifecycle, StaticRunnerConfig{IntervalValue: time.Millisecond}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		// 途中で失敗しても、受理済みの送信を中止してから資源を解放します。
		t.Cleanup(func() {
			_ = runner.ForceClose()
			_ = boundedwait.CloseInBubble(t, "runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes, runner.ForceClose)
		})
		done := make(chan error, 1)
		go func() { done <- runner.Run(ctx) }()
		select {
		case <-remote.entered:
		case <-time.After(time.Minute):
			t.Fatal("accepted cycle did not enter upload")
		}
		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("normal cancellation abandoned accepted upload: %v", err)
		default:
		}
		require.ErrorIs(t, runner.ForceClose(), shutdown.ErrForcedClose)
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Minute):
			t.Fatal("force did not cancel accepted upload")
		}
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes, runner.ForceClose), shutdown.ErrForcedClose)
	})
}

type panicOnceDestinationUploader struct {
	destination objectstorage.DestinationID
	mu          sync.Mutex
	paths       []string
	panicked    bool
}

func (u *panicOnceDestinationUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{u.destination}
}

func (u *panicOnceDestinationUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, request, u.DestinationIDs()).Err()
}

func (u *panicOnceDestinationUploader) UploadDestinations(_ context.Context, request uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.paths = append(u.paths, request.FilePath())
	if !u.panicked {
		u.panicked = true
		panic("object storage uploader panic")
	}
	report := uploader.UploadReport{}
	for _, destination := range destinations {
		report.Results = append(report.Results, uploader.DestinationResult{DestinationID: destination})
	}
	return report
}

func TestSupervisorRetriesPanickedUploadWithoutSkippingCheckpointGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root, target := resolvedTempDirForTest(t), "checkpoint-panic-retry"
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		period := currentLogDate()
		first := writeFileSeriesCheckpointLog(t, directory, target, period, 1)
		second := writeFileSeriesCheckpointLog(t, directory, target, period, 2)
		writeFileSeriesCheckpointLog(t, directory, target, period, 3)
		checkpointDirectory := filepath.Join(root, "checkpoints")
		require.NoError(t, checkpointstore.PrepareDirectory(checkpointDirectory))
		store, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "default-log-series")
		require.NoError(t, err)
		remote := &panicOnceDestinationUploader{destination: "archive"}
		require.NoError(t, store.RegisterDestinations(context.Background(), remote.DestinationIDs(), nil))
		manager := loglifecycle.NewFileSeriesManager(loglifecycle.FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false), Uploader: remote, CheckpointStore: store,
		})
		t.Cleanup(func() {
			_ = manager.ForceClose()
			_ = boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose)
		})
		lifecycle, err := loglifecycle.NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		runner := NewRunner(lifecycle, StaticRunnerConfig{IntervalValue: time.Millisecond},
			lifecycleRunReporterFunc(func(_ context.Context, _ loglifecycle.RunReport, err error) {
				require.NoError(t, err)
				cancel()
			}), WithSupervisor(StaticSupervisorConfig{RestartDelayValue: time.Millisecond, MaxRestartsValue: 1}))
		// 周期の報告だけで停止し、受理済みの復旧はmanager.Closeが完了まで待ちます。
		require.ErrorIs(t, runner.Run(ctx), context.Canceled)
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		remote.mu.Lock()
		paths := append([]string(nil), remote.paths...)
		remote.mu.Unlock()
		require.Equal(t, []string{first, first, second}, paths)
	})
}
