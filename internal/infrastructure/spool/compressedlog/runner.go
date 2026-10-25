package compressedlog

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
)

var (
	ErrNilWorker             = errors.New("compressed log file upload failure spool worker is nil")
	ErrNilRunnerConfig       = errors.New("compressed log file upload failure spool runner config provider is nil")
	ErrInvalidRunnerInterval = errors.New("compressed log file upload failure spool runner interval must be positive")
	ErrRunnerAlreadyRunning  = errors.New("compressed log file upload failure spool runner is already running")
)

type RunnerConfig struct {
	Interval      time.Duration
	PurgeInterval time.Duration
}

// RunReportは一回の再送またはterminal purgeの結果です。PurgeReportがnilなら
// 中断fileの回復と最大一件の再送、非nilならpurgeの結果を表します。
// terminal fileは、routeが変わっても再送対象へ戻しません。
type RunReport struct {
	RecoveredProcessingCount int
	CompressedLogFiles       []compressedlogworker.CompressedLogRunReport
	PurgeReport              *objectstoragespool.TerminalPurgeReport
	// RetryAfterMarkersは、宛先の待機期限を保存したmarkerについて通知が必要な結果です。
	RetryAfterMarkers []compressedlogworker.RetryAfterMarkerReport
}

// RunnerReporterは各周期の結果を順番に同期通知します。通知中にRunの呼び出し元が
// 所有するcontextをcancelできます。Runは、この通知が完了してから返ります。
type RunnerReporter interface {
	ReportSpoolRunnerRun(context.Context, RunReport, error)
}

type Runner struct {
	worker   *compressedlogworker.Worker
	config   RunnerConfigProvider
	reporter RunnerReporter

	purger         *objectstoragespool.TerminalPurger
	storeOrPurgeMu *infrafsstore.ContextRWMutex

	// runMuは回復・upload・状態遷移と周期の結果通知を直列化します。
	// routeの解放もこのlockで受理済み周期の完了を待ちます。
	// storeOrPurgeMuより先に取得します。
	runMu   infrafsstore.ContextMutex
	running atomic.Bool
}

type RunnerOption func(*Runner)

// WithTerminalPurgeはpurge部品と、保存との同時実行を防ぐ共有ロックを構築時に渡します。
// 再送だけを実行するRunnerでは指定しません。
func WithTerminalPurge(purger *objectstoragespool.TerminalPurger, storeOrPurgeMu *infrafsstore.ContextRWMutex) RunnerOption {
	return func(r *Runner) {
		r.purger = purger
		r.storeOrPurgeMu = storeOrPurgeMu
	}
}

func NewRunner(worker *compressedlogworker.Worker, config RunnerConfigProvider, reporter RunnerReporter, opts ...RunnerOption) (*Runner, error) {
	if worker == nil {
		return nil, ErrNilWorker
	}
	if config == nil {
		return nil, ErrNilRunnerConfig
	}
	if config.Interval() <= 0 {
		return nil, ErrInvalidRunnerInterval
	}
	runner := &Runner{worker: worker, config: config, reporter: reporter}
	for _, opt := range opts {
		opt(runner)
	}
	if runner.purger != nil && config.PurgeInterval() <= 0 {
		return nil, fmt.Errorf("%w: lifecycle interval must be positive", ErrInvalidCompressedLogFileUploadSpoolConfig)
	}
	return runner, nil
}

// Runはctxが終了するまで再送と、設定されたterminal purgeを実行します。
// 再送の初回もinterval分待ち、以後は処理と結果通知の完了後から次の待機を始めます。
// purgeの周期は独立していますが、実行中の再送が終わるまで待ちます。
// contextやgoroutineは保持せず、Runの同時呼び出しを拒否し、返却後は再実行できます。
// 手動RunOnceのcontextと終了待ちは、その呼び出し元が所有します。
// ctxの中止は次の周期の受付を止めます。受理済み周期と通知は完了まで待ちます。
func (r *Runner) Run(ctx context.Context) error {
	return r.runWithContexts(ctx, context.WithoutCancel(ctx))
}

// runWithContextsは周期の受付停止と、受理済み処理の明示的な強制中止を分離します。
func (r *Runner) runWithContexts(stopCtx, workCtx context.Context) error {
	if r == nil || r.worker == nil {
		return ErrNilWorker
	}
	if r.config == nil {
		return ErrNilRunnerConfig
	}
	if r.config.Interval() <= 0 {
		return ErrInvalidRunnerInterval
	}
	if r.purger != nil && r.config.PurgeInterval() <= 0 {
		return fmt.Errorf("%w: lifecycle interval must be positive", ErrInvalidCompressedLogFileUploadSpoolConfig)
	}
	if !r.running.CompareAndSwap(false, true) {
		return ErrRunnerAlreadyRunning
	}
	defer r.running.Store(false)

	var runPurge func()
	if r.purger != nil {
		runPurge = func() {
			r.runCycle(stopCtx, workCtx, true)
		}
	}
	return runReplayAndPurgeLoop(stopCtx, r.config.Interval(), func() {
		r.runCycle(stopCtx, workCtx, false)
	}, r.config.PurgeInterval(), runPurge)
}

func (r *Runner) runCycle(stopCtx, workCtx context.Context, purge bool) {
	if err := r.runMu.LockContext(stopCtx); err != nil {
		return
	}
	defer r.runMu.Unlock()
	if stopCtx.Err() != nil {
		return
	}
	if purge {
		report, err := r.runPurgeOnceLocked(workCtx)
		r.reportRun(workCtx, RunReport{PurgeReport: &report}, err)
		return
	}
	report, err := r.runReplayOnceLocked(workCtx)
	r.reportRun(workCtx, report, err)
}

func (r *Runner) IsRunning() bool {
	return r != nil && r.running.Load()
}

// RunOnceはWorkerの一周期を、他の再送・purgeと重ならないよう実行します。
// 宛先確認・中断fileの回復・再送・状態遷移はWorkerが担当します。
// 競合処理の完了待ちはctxでcancelできます。
func (r *Runner) RunOnce(ctx context.Context) (RunReport, error) {
	if r == nil || r.worker == nil {
		return RunReport{}, ErrNilWorker
	}
	if err := r.runMu.LockContext(ctx); err != nil {
		return RunReport{}, err
	}
	defer r.runMu.Unlock()
	return r.runReplayOnceLocked(ctx)
}

func (r *Runner) runReplayOnceLocked(ctx context.Context) (report RunReport, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &PanicError{Kind: ErrReplayPanic, Value: recovered}
		}
	}()
	if err := ctx.Err(); err != nil {
		return RunReport{}, err
	}
	workReport, err := r.worker.RunOnce(ctx)
	report = RunReport{
		RecoveredProcessingCount: workReport.RecoveredProcessingCount,
		RetryAfterMarkers:        workReport.RetryAfterMarkers,
	}
	if workReport.File.Claimed {
		report.CompressedLogFiles = append(report.CompressedLogFiles, workReport.File)
	}
	return report, err
}

func (r *Runner) runPurgeOnce(ctx context.Context) (objectstoragespool.TerminalPurgeReport, error) {
	if err := r.runMu.LockContext(ctx); err != nil {
		return objectstoragespool.TerminalPurgeReport{}, err
	}
	defer r.runMu.Unlock()
	return r.runPurgeOnceLocked(ctx)
}

func (r *Runner) runPurgeOnceLocked(ctx context.Context) (objectstoragespool.TerminalPurgeReport, error) {
	if r.storeOrPurgeMu != nil {
		if err := r.storeOrPurgeMu.LockContext(ctx); err != nil {
			return objectstoragespool.TerminalPurgeReport{}, err
		}
		defer r.storeOrPurgeMu.Unlock()
	}
	return runTerminalPurgeWithPanicRecovery(ctx, r.purger)
}

func (r *Runner) reportRun(ctx context.Context, report RunReport, err error) {
	if r.reporter == nil {
		return
	}
	// 通知先のpanicで後続周期を停止させません。呼び出し元のcontextがcancelされても、
	// この通知は同期実行を保ちます。
	defer func() { _ = recover() }()
	r.reporter.ReportSpoolRunnerRun(ctx, report, err)
}

func runTerminalPurgeWithPanicRecovery(
	ctx context.Context,
	purger *objectstoragespool.TerminalPurger,
) (report objectstoragespool.TerminalPurgeReport, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &PanicError{Kind: ErrTerminalPurgePanic, Value: recovered}
		}
	}()
	return purger.Run(ctx)
}
