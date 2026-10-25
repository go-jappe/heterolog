package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"sync"

	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
)

// failureArchiveRunnerRuntimeはrunner modeでretryable/permanent directoryの周期保守を
// 所有します。Pipelineの終了時には新しい周期を止め、受理済み周期と復旧を完了してから
// checkpoint Storeを含む依存resourceを閉じます。
type failureArchiveRunnerRuntime struct {
	runners []*logfilerunner.Runner
	cancel  context.CancelFunc
	done    chan struct{}

	errMu sync.Mutex
	errs  []error
}

func newFailureArchiveRunnerRuntime(
	runners []*logfilerunner.Runner,
) *failureArchiveRunnerRuntime {
	return &failureArchiveRunnerRuntime{
		runners: append([]*logfilerunner.Runner(nil), runners...),
	}
}

// reconcileObjectStorageUploadDestinationsはfailure archive handlerを利用可能にする前に、
// retryable/permanent runnerの現在のfile一覧とupload宛先を確認し、不要になった
// checkpoint記録を片付けます。fileの圧縮、upload、purgeは行いません。
func (r *failureArchiveRunnerRuntime) reconcileObjectStorageUploadDestinations(ctx context.Context) error {
	if r == nil {
		return nil
	}
	ctx = contextOrBackground(ctx)
	var errs []error
	for index, runner := range r.runners {
		report, err := runner.ReconcileObjectStorageUploadDestinations(ctx)
		if runner != nil && runner.Reporter != nil && err != nil {
			runner.Reporter.ReportRun(ctx, report, err)
		}
		if err != nil {
			errs = append(errs, identifyLifecycleRunnerError(index, runner, err))
		}
	}
	return errors.Join(errs...)
}

// startはretryable/permanentのRunner.Runをそれぞれ開始します。Runner factoryで
// WithSupervisorが付与されていれば、Supervisorによる再起動はRunner.Run内で行われます。
func (r *failureArchiveRunnerRuntime) start(ctx context.Context) {
	if r == nil || len(r.runners) == 0 || r.done != nil {
		return
	}

	runCtx, cancel := context.WithCancel(contextWithoutCancel(ctx))
	r.cancel = cancel
	r.done = make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(len(r.runners))
	for index, runner := range r.runners {
		go func(index int, runner *logfilerunner.Runner) {
			defer wg.Done()
			if err := runner.Run(runCtx); err != nil && !isLifecycleContextError(err) {
				r.errMu.Lock()
				r.errs = append(r.errs, identifyLifecycleRunnerError(index, runner, err))
				r.errMu.Unlock()
			}
		}(index, runner)
	}
	go func() {
		wg.Wait()
		close(r.done)
	}()
}

// stopPeriodicは新しい周期を止めます。受理済み周期と復旧は継続し、
// そのStoreと退避先の登録を利用可能に保ちます。
func (r *failureArchiveRunnerRuntime) stopPeriodic() {
	if r == nil {
		return
	}
	if r.cancel != nil {
		r.cancel()
	}
}

// closeLogFileProcessingRecoveryは受理済み周期の再開処理登録後に、復旧の完了を待ちます。
func (r *failureArchiveRunnerRuntime) closeLogFileProcessingRecovery() error {
	if r == nil {
		return nil
	}
	var errs []error
	for index, runner := range r.runners {
		if runner == nil || runner.DirectoryLifecycle == nil {
			continue
		}
		if err := runner.DirectoryLifecycle.CloseLogFileProcessingRecovery(); err != nil {
			errs = append(errs, identifyLifecycleRunnerError(index, runner, err))
		}
	}
	return errors.Join(errs...)
}

// waitStoppedは中止要求済みの全runnerについて、Runが戻るまで待ちます。
// context timeout時もrunnerの停止処理自体はbackgroundで継続し、次のShutdownは
// 同じdone channelを待ち直せます。
func (r *failureArchiveRunnerRuntime) waitStopped(ctx context.Context) error {
	if r == nil || r.done == nil {
		return nil
	}
	return waitForArchiveShutdown(ctx, "failure archive lifecycle runners", r.done)
}

// closeResourcesは受理済み処理の完了後、または構築失敗時に、runnerが所有するStoreなどを
// すべて閉じます。周期loopを開始していない場合も対象です。個別の終了エラーがあっても
// 残りを閉じ、runner側で保持される終了結果をまとめて返します。
func (r *failureArchiveRunnerRuntime) closeResources() error {
	if r == nil {
		return nil
	}
	var errs []error
	for index, runner := range r.runners {
		if runner == nil || runner.DirectoryLifecycle == nil {
			continue
		}
		if err := runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes(); err != nil {
			errs = append(errs, identifyLifecycleRunnerError(index, runner, err))
		}
	}
	r.errMu.Lock()
	errs = append(errs, r.errs...)
	r.errs = nil
	r.errMu.Unlock()
	return errors.Join(errs...)
}

func identifyLifecycleRunnerError(
	index int,
	runner *logfilerunner.Runner,
	err error,
) error {
	directory := ""
	if runner != nil {
		if path, pathErr := runner.DirectoryPath(); pathErr == nil {
			directory = path
		}
	}
	if directory == "" {
		return fmt.Errorf("failure archive lifecycle runner[%d] failed: %w", index, err)
	}
	return fmt.Errorf(
		"failure archive lifecycle runner[%d] directory=%q failed: %w",
		index,
		directory,
		err,
	)
}

func isLifecycleContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
