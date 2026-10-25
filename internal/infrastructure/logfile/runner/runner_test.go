package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	loglifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

func TestRunnerRejectsMissingOrNonPositiveInterval(t *testing.T) {
	tests := []struct {
		name   string
		config RunnerConfig
	}{
		{name: "nil config"},
		{name: "zero interval", config: StaticRunnerConfig{}},
		{name: "negative interval", config: StaticRunnerConfig{IntervalValue: -time.Nanosecond}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := NewRunner(&loglifecycle.DirectoryLifecycle{}, test.config, nil)
			require.ErrorIs(t, runner.Run(context.Background()), ErrInvalidRunnerInterval)
		})
	}
}

func TestRunnerDirectoryPathReturnsItsManagedPhysicalDirectory(t *testing.T) {
	root := t.TempDir()
	target := "runner-directory-identity"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))
	manager := loglifecycle.NewFileSeriesManager(loglifecycle.FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
	})
	directoryLifecycle, err := loglifecycle.NewDirectoryLifecycle(manager)
	require.NoError(t, err)

	runner := NewRunner(directoryLifecycle, nil, nil)
	got, err := runner.DirectoryPath()

	require.NoError(t, err)
	// 一時directoryの祖先がlinkでも、runnerはlifecycleが管理する実体directoryを返します。
	physical, err := filepath.EvalSymlinks(logDir)
	require.NoError(t, err)
	require.Equal(t, pathidentity.AbsoluteOrCleanPath(physical), got)
}

func TestRunnerDirectoryPathRejectsMissingRunnerDependencies(t *testing.T) {
	var nilRunner *Runner
	_, err := nilRunner.DirectoryPath()
	require.ErrorIs(t, err, ErrNilRunner)

	_, err = (&Runner{}).DirectoryPath()
	require.ErrorIs(t, err, ErrNilRunnerDirectoryLifecycle)
}

func TestRunnerRunWaitsBeforeFirstRun(t *testing.T) {
	// 初回の待機は実時間ではなくbubble内の時刻で確認します。
	synctest.Test(t, func(t *testing.T) {
		const interval = 50 * time.Millisecond
		// 空のlifecycleはRunOnceで検証errorを返すため、StopOnErrorにより初回の実行を観測できます。
		runner := NewRunner(&loglifecycle.DirectoryLifecycle{}, StaticRunnerConfig{
			IntervalValue:    interval,
			StopOnErrorValue: true,
		}, nil)
		run := func(ctx context.Context) <-chan error {
			errCh := make(chan error, 1)
			go func() { errCh <- runner.Run(ctx) }()
			return errCh
		}

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		errCh := run(ctx)
		time.Sleep(interval - time.Nanosecond)
		synctest.Wait()
		select {
		case err := <-errCh:
			t.Fatalf("runner ran before the first interval elapsed: %v", err)
		default:
		}
		cancel()
		require.ErrorIs(t, boundedwait.ReceiveInBubble(t, errCh, "receive from errCh"), context.Canceled)

		// 間隔が経過すると、最初のRunOnceを実行します。
		errCh = run(context.Background())
		time.Sleep(interval)
		synctest.Wait()
		select {
		case err := <-errCh:
			require.ErrorIs(t, err, ErrRunnerStoppedOnError)
			require.ErrorIs(t, err, loglifecycle.ErrDirectoryLifecycleHasNoFileSeriesManager)
		default:
			t.Fatal("runner did not run after the first interval elapsed")
		}
	})
}

func TestRunnerCancellationWaitsForAcceptedCycle(t *testing.T) {
	for _, supervised := range []bool{false, true} {
		name := "runner"
		if supervised {
			name = "supervisor"
		}
		t.Run(name, func(t *testing.T) {
			// 受理済み周期が戻らないことは、実時間の待機ではなくbubble内の全goroutineの停止後に確認します。
			synctest.Test(t, func(t *testing.T) {
				remote := &shutdownBlockingUploader{entered: make(chan context.Context, 1), release: make(chan struct{})}
				runner := newFailingArchiveRunnerWithUploader(t, false, nil, remote)
				if supervised {
					WithSupervisor(StaticSupervisorConfig{RestartDelayValue: time.Millisecond, MaxRestartsValue: 1})(runner)
				}
				// fixtureの終了処理より先に解放し、途中で失敗しても送信の完了待ちで止まらないようにします。
				release := sync.OnceFunc(func() { close(remote.release) })
				t.Cleanup(release)
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				stopped := make(chan error, 1)
				go func() { stopped <- runner.Run(ctx) }()
				var cycleCtx context.Context
				select {
				case cycleCtx = <-remote.entered:
				case <-time.After(time.Minute):
					t.Fatal("accepted cycle did not enter upload")
				}
				cancel()
				require.NoError(t, cycleCtx.Err())
				synctest.Wait()
				select {
				case err := <-stopped:
					t.Fatalf("runner canceled an accepted cycle: %v", err)
				default:
				}
				release()
				require.ErrorIs(t, boundedwait.ReceiveInBubble(t, stopped, "receive from stopped"), context.Canceled)
				require.NoError(t, boundedwait.CloseInBubble(t, "runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes, runner.ForceClose))
			})
		})
	}
}

type shutdownBlockingUploader struct {
	entered chan context.Context
	release chan struct{}
}

func (u *shutdownBlockingUploader) Upload(ctx context.Context, _ uploader.UploadRequest) error {
	u.entered <- ctx
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-u.release:
		return nil
	}
}

func TestRunnerRunIntegrationOperatesRealLogFiles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	root := resolvedTempDirForTest(t)
	target := "delivery-runner-integration"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	currentDate := currentLogDate()
	inactivePath := filepath.Join(logDir, target+"_"+currentDate+"_1.log")
	activePath := filepath.Join(logDir, target+"_"+currentDate+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, []byte("inactive runner file"), 0o600))
	require.NoError(t, os.WriteFile(activePath, []byte("active runner file"), 0o600))

	recorder := newNotifyingUploader()
	destinations, store := testFileSeriesUploadDependencies(t, recorder)
	manager := loglifecycle.NewFileSeriesManager(loglifecycle.FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, true),
		Uploader: destinations, CheckpointStore: store,
	})
	t.Cleanup(func() {
		_ = manager.ForceClose()
		_ = boundedwait.Close(t, "manager.Close()", manager.Close, boundedwait.ForceCloserOf(manager))
	})
	directoryLifecycle, err := loglifecycle.NewDirectoryLifecycle(manager)
	require.NoError(t, err)
	runner := NewRunner(directoryLifecycle, StaticRunnerConfig{
		IntervalValue:    time.Millisecond,
		StopOnErrorValue: true,
	}, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	compressedPath := inactivePath + pkgfsstore.ExtensionCompressGzip
	var uploadedPath string
	select {
	case uploadedPath = <-recorder.uploaded:
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("runner did not operate real log files before timeout")
	}

	cancel()
	require.ErrorIs(t, boundedwait.Receive(t, errCh, "runner stop after cancellation"), context.Canceled)

	require.Equal(t, compressedPath, uploadedPath)
	require.NoFileExists(t, inactivePath)
	require.NoFileExists(t, compressedPath)
	require.FileExists(t, activePath)
}

func TestRunnerRunReportsErrorAndContinuesWhenStopOnErrorIsFalse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	reported := make(chan lifecycleRunnerReportCall, 4)
	runner := newFailingArchiveRunner(t, false, lifecycleRunReporterFunc(func(
		ctx context.Context,
		report loglifecycle.RunReport,
		err error,
	) {
		_ = ctx
		reported <- lifecycleRunnerReportCall{report: report, err: err}
	}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	for callIndex := 0; callIndex < 2; callIndex++ {
		select {
		case call := <-reported:
			if callIndex == 0 {
				require.ErrorIs(t, call.err, errTestArchiveUpload)
			} else {
				// 失敗した世代は独立した復旧が所有し、次周期は重ねて送信しません。
				require.NoError(t, call.err)
			}
			require.Positive(t, call.report.ScannedFiles)
		case <-time.After(boundedwait.HangTimeout):
			t.Fatalf("reporter was not called for lifecycle cycle %d", callIndex+1)
		}
	}
	require.True(t, runner.running.Load(), "runner must continue after a reported error")

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("runner did not stop after context cancellation")
	}
}

func TestRunnerRunContinuesAfterNestedContextErrorWhileParentContextIsAlive(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		operationErr error
	}{
		{name: "durable save deadline", operationErr: errors.Join(errors.New("durable save exceeded its budget"), context.DeadlineExceeded)},
		{name: "remote deadline", operationErr: context.DeadlineExceeded},
		{name: "remote cancellation", operationErr: context.Canceled},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			reported := make(chan lifecycleRunnerReportCall, 2)
			runner := newFailingArchiveRunnerWithUploader(t, false, lifecycleRunReporterFunc(func(ctx context.Context, report loglifecycle.RunReport, err error) {
				select {
				case reported <- lifecycleRunnerReportCall{report: report, err: err}:
				case <-ctx.Done():
				}
			}), failingUploader{err: errors.Join(errTestArchiveUpload, testCase.operationErr)})
			// 保存処理やupload自身の期限超過は、Runnerの親contextの終了ではありません。
			// RunOnceまで実ファイル処理を通し、子処理のerrorを返すuploaderを構築時に渡します。
			stopped := make(chan error, 1)
			go func() { stopped <- runner.Run(ctx) }()
			for cycle := 0; cycle < 2; cycle++ {
				select {
				case call := <-reported:
					if cycle == 0 {
						require.ErrorIs(t, call.err, errTestArchiveUpload)
						require.ErrorIs(t, call.err, testCase.operationErr)
					} else {
						require.NoError(t, call.err)
					}
					require.Positive(t, call.report.ScannedFiles)
				case err := <-stopped:
					t.Fatalf("runner stopped after a child operation error instead of continuing: %v", err)
				case <-time.After(boundedwait.HangTimeout):
					t.Fatal("next lifecycle cycle did not run after a child operation error")
				}
			}
			require.NoError(t, ctx.Err())
			require.True(t, runner.running.Load())
			cancel()
			select {
			case err := <-stopped:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(boundedwait.HangTimeout):
				t.Fatal("runner did not stop after explicit parent cancellation")
			}
		})
	}
}

func TestRunnerRunOnceReturnsResultWithoutInvokingReporter(t *testing.T) {
	reported := false
	runner := newFailingArchiveRunner(t, false, lifecycleRunReporterFunc(func(
		context.Context,
		loglifecycle.RunReport,
		error,
	) {
		reported = true
	}))

	report, err := runner.RunOnce(context.Background())

	require.ErrorIs(t, err, errTestArchiveUpload)
	require.Positive(t, report.ScannedFiles)
	require.False(t, reported, "RunOnce caller receives the result directly and must not invoke Reporter")
}

func TestRunnerRunReportsErrorBeforeStoppingWhenStopOnErrorIsTrue(t *testing.T) {
	reported := make(chan lifecycleRunnerReportCall)
	runner := newFailingArchiveRunner(t, true, lifecycleRunReporterFunc(func(
		ctx context.Context,
		report loglifecycle.RunReport,
		err error,
	) {
		_ = ctx
		reported <- lifecycleRunnerReportCall{report: report, err: err}
	}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background())
	}()

	// unbuffered channelで通知を先に受けることで、停止判定より前にReporterが
	// 同期的に呼ばれる契約を確認する。
	select {
	case call := <-reported:
		require.ErrorIs(t, call.err, errTestArchiveUpload)
		require.Positive(t, call.report.ScannedFiles)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("reporter was not called before runner stopped")
	}

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrRunnerStoppedOnError)
		require.ErrorIs(t, err, errTestArchiveUpload)
	case <-time.After(boundedwait.HangTimeout):
		t.Fatal("runner did not stop after the reported lifecycle failure")
	}
}

type lifecycleRunnerReportCall struct {
	report loglifecycle.RunReport
	err    error
}

func newFailingArchiveRunner(
	t *testing.T,
	stopOnError bool,
	reporter loglifecycle.RunReporter,
) *Runner {
	return newFailingArchiveRunnerWithUploader(t, stopOnError, reporter, failingUploader{err: errTestArchiveUpload})
}

func newFailingArchiveRunnerWithUploader(
	t *testing.T,
	stopOnError bool,
	reporter loglifecycle.RunReporter,
	remote uploader.Uploader,
) *Runner {
	t.Helper()
	return newArchiveRunnerWithFailure(t, stopOnError, reporter, remote, nil)
}

func newRegistrationFailingArchiveRunner(t *testing.T, reporter loglifecycle.RunReporter, err error) *Runner {
	t.Helper()
	return newArchiveRunnerWithFailure(t, true, reporter, failingUploader{err: err}, err)
}

func newArchiveRunnerWithFailure(
	t *testing.T,
	stopOnError bool,
	reporter loglifecycle.RunReporter,
	remote uploader.Uploader,
	registrationErr error,
) *Runner {
	t.Helper()

	root := resolvedTempDirForTest(t)
	target := "failing-archive-runner"
	logDir := filepath.Join(root, target, "logs")
	require.NoError(t, os.MkdirAll(logDir, 0o750))

	date := currentLogDate()
	inactivePath := filepath.Join(logDir, target+"_"+date+"_1.log")
	latestPath := filepath.Join(logDir, target+"_"+date+"_2.log")
	require.NoError(t, os.WriteFile(inactivePath, []byte("archive failure"), 0o600))
	require.NoError(t, os.WriteFile(latestPath, []byte("latest"), 0o600))

	destinations, store := testFileSeriesUploadDependencies(t, remote)
	if registrationErr != nil {
		store = &registrationFailingRunnerCheckpointStore{Store: store, err: registrationErr}
	}
	manager := loglifecycle.NewFileSeriesManager(loglifecycle.FileSeriesManagerOptions{
		FileName: logpattern.FileNamePattern{Dir: logDir, Target: target, Extension: "log"},
		Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
		Uploader: destinations, CheckpointStore: store,
	})
	directoryLifecycle, err := loglifecycle.NewDirectoryLifecycle(manager)
	require.NoError(t, err)

	runner := NewRunner(directoryLifecycle, StaticRunnerConfig{
		IntervalValue:    time.Millisecond,
		StopOnErrorValue: stopOnError,
	}, reporter)
	// managerの復旧Reporterは指定せず、通常周期の通知だけを測定します。
	t.Cleanup(func() {
		_ = runner.ForceClose()
		_ = runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
	})
	return runner
}

type lifecycleRunReporterFunc func(context.Context, loglifecycle.RunReport, error)

func (f lifecycleRunReporterFunc) ReportRun(ctx context.Context, report loglifecycle.RunReport, err error) {
	f(ctx, report, err)
}

type notifyingUploader struct {
	uploaded chan string
}

func newNotifyingUploader() *notifyingUploader {
	return &notifyingUploader{
		uploaded: make(chan string, 1),
	}
}

func (u *notifyingUploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	_ = ctx
	select {
	case u.uploaded <- input.FilePath():
	default:
	}
	return nil
}
