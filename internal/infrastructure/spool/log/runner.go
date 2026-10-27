package logspool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	lifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
)

var (
	ErrNilRunner             = errors.New("spool runner is nil")
	ErrNilWorker             = errors.New("spool runner worker is nil")
	ErrNilLifecycle          = errors.New("spool runner lifecycle is nil")
	ErrRunnerAlreadyRunning  = errors.New("spool runner is already running")
	ErrInvalidRunnerInterval = errors.New("spool runner interval must be positive")
	ErrRunnerStoppedOnError  = errors.New("spool runner stopped on error")
)

// RunOnceが失敗した処理の段階を表すerrorの種類です。原因のerrorはerrors.Is・errors.Asで辿れます。
var (
	// ErrRunLifecycleは、終了済みファイルの削除処理が失敗したことを表します。
	ErrRunLifecycle = errors.New("run spool lifecycle")
	// ErrRunProcessingFileTransitionRetryは、processing fileの状態遷移の再試行が失敗したことを表します。
	ErrRunProcessingFileTransitionRetry = errors.New("run processing file transition retry")
	// ErrRunSpoolFileProcessは、spool fileの再送処理が失敗したことを表します。
	ErrRunSpoolFileProcess = errors.New("run spool file process")
)

// NewRunnerは、構築済みの再送workerとファイル削除用managerを使うRunnerを返します。
// 必須のworker・managerがなければエラーを返し、構築後はこの依存関係を変更しません。
// 周期実行は開始せず、周期設定の検証はRunの開始時に行います。
func NewRunner(
	worker *worker.Worker,
	lifecycle *lifecycle.Manager,
	cfg RunnerConfig,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*Runner, error) {
	if worker == nil {
		return nil, ErrNilWorker
	}
	if lifecycle == nil {
		return nil, ErrNilLifecycle
	}
	return &Runner{
		worker:    worker,
		lifecycle: lifecycle,
		Config:    cfg,
		Tracer:    tracer,
		Meter:     meter,
	}, nil
}

// Runnerは、spoolへ保存されたログレコードをbackendへ再送する処理を周期実行します。
// Workerがactive・processingのファイルからレコードを読み、OpenSearchやLokiなどへ再送します。
// 送信結果に応じた保存先への振り分けと、失敗した保存・移動・削除の再試行もWorkerが担当します。
// Lifecycleは、done・dead・quarantineの終了済みファイルを保持設定に従って削除します。
// 圧縮ログファイル自体をobject storageへ送るspool/compressedlogのRunnerとは用途が異なります。
// 必須依存をNewRunnerで確定して使用してください。ゼロ値での実行はサポートしません。
type Runner struct {
	// workerは、ログレコードの再送と、失敗した保存・移動・削除の再試行を担当します。
	// NewRunnerで必須確認し、構築後は差し替えません。
	worker *worker.Worker

	// lifecycleは、done・dead・quarantineのファイルを保持期限・容量上限に従って削除します。
	// NewRunnerで必須確認し、構築後は差し替えません。
	lifecycle *lifecycle.Manager

	// Configは、周期間の待機時間と、エラー時に周期実行を停止するかを指定します。
	Config RunnerConfig

	// Reporterは、Runが各RunOnceの結果とエラーを同期的に渡す先です。
	// Runの直接呼び出しでは開始時の値を使い、nilなら結果通知を行いません。
	// Supervisor経由では、Supervisorがまず結果を受け取り、自身のReporterへ渡します。
	// RunOnceの直接呼び出しでは、この通知先を呼ばず戻り値だけを返します。
	Reporter RunnerReporter

	// runMuは、定期実行と手動呼び出しを含むRunOnce同士のファイル処理を直列化します。
	// 結果通知と周期間の待機では保持しません。取得待ちはctxで中止できます。
	runMu infrafsstore.ContextMutex
	// runningは、周期実行の二重起動を防ぎます。手動RunOnceの実行では変更しません。
	running atomic.Bool

	Tracer pkgobserver.Tracer
	Meter  pkgobserver.Meter
}

// RunnerConfigは、周期間の待機時間とエラー時の停止方針を提供します。
type RunnerConfig interface {
	// Intervalは、一周期の処理と結果通知が完了してから、次の周期を始めるまでの待機時間です。
	// 正の値が必須で、0や負値を既定値へ補正しません。
	Interval() time.Duration

	// StopOnErrorは、RunOnceがキャンセル・期限切れ以外のエラーを返した場合に、周期実行を止めるかを指定します。
	// trueなら、元のエラーとErrRunnerStoppedOnErrorを含むエラーを返してRunを終了します。
	// Supervisor経由の実行でも、この停止を理由にRunnerを再起動しません。
	// falseなら、結果を通知した後、Intervalの待機を挟んで次の周期へ進みます。
	// キャンセル・期限切れのエラーでは、この設定にかかわらず終了します。
	StopOnError() bool
}

// RunnerReporterは、周期実行の結果とエラーを同期的に受け取る通知先です。
// 通知先が戻るまで、次の周期へ進まず、Runも終了しません。
type RunnerReporter interface {
	ReportSpoolRunnerRun(ctx context.Context, report RunReport, err error)
}

// RunReportは、一周期のファイル削除・後処理の再試行・ログレコード再送の結果です。
// 途中で終了した場合、実行していない段階の結果はゼロ値です。
type RunReport struct {
	// LifecycleReportは、done・dead・quarantineのファイルを保持設定に従って削除した結果です。
	LifecycleReport lifecyclereport.RunReport

	// ProcessingFileTransitionRetryReportは、失敗したprocessingファイルの保存・移動・削除を再試行した結果です。
	// backendへの送信結果ではありません。
	ProcessingFileTransitionRetryReport worker.ProcessingFileTransitionRetryReport

	// ProcessingFileTransitionRetryHandledは、今回、後処理の再試行対象が見つかり、処理を試みた場合にtrueです。
	// 成功を意味せず、trueなら同じ周期でログレコードの再送は行いません。
	// falseでも、再試行時刻を待つ対象が残っている場合があります。
	ProcessingFileTransitionRetryHandled bool

	// ProcessReportは、spoolファイル最大一件の読み込み・レコード再送・保存先への振り分けの結果です。
	// 後処理の再試行を優先した場合など、この段階へ進まなければゼロ値です。
	ProcessReport worker.ProcessReport
}

// Runは、RunOnceによる一周期の処理と結果通知を繰り返します。
// 初回は待機せず、以後は処理と結果通知が完了してからIntervalだけ待って次の周期を始めます。
// 処理と結果通知は呼び出し元のgoroutineで直列に実行します。
// ctxのキャンセル・期限切れは、次の周期の開始と周期間の待機を止めます。
// 開始済みの一周期と結果通知はそのctxでは中断せず、完了を待ってから返ります。
// Runの同時呼び出しはErrRunnerAlreadyRunningを返します。
func (r *Runner) Run(ctx context.Context) error {
	if r == nil {
		return ErrNilRunner
	}
	return r.run(ctx, r.Reporter)
}

// runは、ctxで次の周期の受付を止め、開始済みの処理と結果通知には
// ctxのキャンセル・期限切れを引き継がないcontextを渡します。
func (r *Runner) run(ctx context.Context, reporter RunnerReporter) error {
	return r.runUntilStopped(ctx, context.WithoutCancel(ctx), reporter)
}

// runUntilStoppedは、周期実行の停止と、開始済み処理への中止要求を分けて扱います。
// stopCtxは次の周期の開始と周期間の待機を止め、operationCtxはRunOnceと結果通知へ渡します。
// 強制終了の呼び出し側はoperationCtxをキャンセルして処理の中止を要求します。
// キャンセルは実行中の処理を強制的に打ち切るものではなく、その処理が戻るまで待ちます。
func (r *Runner) runUntilStopped(stopCtx, operationCtx context.Context, reporter RunnerReporter) error {
	if err := r.validateRunConfig(); err != nil {
		return err
	}

	if !r.running.CompareAndSwap(false, true) {
		return ErrRunnerAlreadyRunning
	}
	defer r.running.Store(false)

	interval := r.Config.Interval()

	for {
		if err := stopCtx.Err(); err != nil {
			return err
		}
		report, err := r.RunOnce(operationCtx)

		if reporter != nil {
			reporter.ReportSpoolRunnerRun(operationCtx, report, err)
		}

		if err != nil {
			if isContextError(err) {
				return err
			}
			if r.Config.StopOnError() {
				return fmt.Errorf("%w: %w", ErrRunnerStoppedOnError, err)
			}
		}

		if err := waitContext(stopCtx, interval); err != nil {
			return err
		}
	}
}

// RunOnceは、backendへの再送用spoolを一周期分処理します。
//
// 実行順序:
//  1. done・dead・quarantineのファイルを、保持期限・容量上限に従って必要なら削除する。
//  2. 前回失敗したprocessingファイルの保存・移動・削除を、最大一件再試行する。
//     この段階ではbackendへの送信は行わない。
//  3. 2で再試行する対象がなければ、spoolファイルを最大一件読み、
//     ログレコードをbackendへ再送して、結果に応じて保存先を振り分ける。
//
// 2の後処理を試みた周期では、成否にかかわらず3の再送を行いません。
// 後処理が未完了のファイルを優先し、送信結果をspoolへ反映できていない状態を
// 増やさないためです。
// backendがRetry-Afterで指定した時刻までは3の再送を行いませんが、1と2は続けます。
// 同じRunnerへの並行呼び出しは、前のRunOnceによるファイル処理の完了を待ちます。
// 待機中にctxがキャンセルされた場合は、削除・後処理・再送を始めず戻ります。
// 結果は戻り値で返し、Reporterは呼びません。
func (r *Runner) RunOnce(ctx context.Context) (RunReport, error) {
	var report RunReport

	if r == nil {
		return report, ErrNilRunner
	}
	if err := r.runMu.LockContext(ctx); err != nil {
		return report, err
	}
	defer r.runMu.Unlock()
	if err := ctx.Err(); err != nil {
		return report, err
	}

	var errs []error

	lifecycleReport, err := r.lifecycle.RunOnce(ctx)
	report.LifecycleReport = lifecycleReport
	if err != nil {
		err = fmt.Errorf("%w: %w", ErrRunLifecycle, err)

		if isContextError(err) {
			return report, err
		}

		// 終了済みファイルの削除に失敗しても、後処理の再試行とログレコードの再送は続けます。
		errs = append(errs, err)
	}

	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
		return report, errors.Join(errs...)
	}

	processingFileTransitionRetryReport, handled, err := r.worker.RunOnceProcessingFileTransitionRetry(ctx)
	report.ProcessingFileTransitionRetryReport = processingFileTransitionRetryReport
	report.ProcessingFileTransitionRetryHandled = handled
	if err != nil {
		errs = append(errs, fmt.Errorf("%w: %w", ErrRunProcessingFileTransitionRetry, err))
		return report, errors.Join(errs...)
	}

	if handled {
		return report, errors.Join(errs...)
	}

	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
		return report, errors.Join(errs...)
	}

	processReport, err := r.worker.RunOnceSpoolFileProcess(ctx)
	report.ProcessReport = processReport
	if err != nil {
		errs = append(errs, fmt.Errorf("%w: %w", ErrRunSpoolFileProcess, err))
	}

	return report, errors.Join(errs...)
}

// RetryAfterDeadlineは、このrunnerが再送する送信先のRetry-Afterによる待機期限を返します。
// 同じspool rootへ書き込む通常送信の保存処理へ渡すと、通常送信で受け取った期限を保存し、
// 再送中のrunnerも次の周期からその送信先のすべてのログに適用します。
func (r *Runner) RetryAfterDeadline() *worker.RetryAfterDeadline {
	if r == nil || r.worker == nil {
		return nil
	}
	return r.worker.RetryAfterDeadline()
}

// IsRunningは周期実行中かを返します。結果通知中や周期間の待機中もtrueです。
// RunOnceを単独で呼んでいるだけの場合はtrueになりません。
func (r *Runner) IsRunning() bool {
	if r == nil {
		return false
	}

	return r.running.Load()
}

func (r *Runner) validateRunConfig() error {
	if r.Config.Interval() <= 0 {
		return ErrInvalidRunnerInterval
	}

	return nil
}

func waitContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ErrInvalidRunnerInterval
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

// RunnerConfigAdapterは、構築時に受け取った待機時間と停止方針を値として保持します。
type RunnerConfigAdapter struct {
	interval    time.Duration
	stopOnError bool
}

// NewRunnerConfigAdapterは、設定値を補完・検証せず、RunnerConfigとして参照できる形にします。
func NewRunnerConfigAdapter(cfg spoolrunner.RunnerConfig) RunnerConfig {
	return &RunnerConfigAdapter{
		interval:    cfg.Interval,
		stopOnError: cfg.StopOnError,
	}
}

func (a *RunnerConfigAdapter) Interval() time.Duration {
	return a.interval
}

func (a *RunnerConfigAdapter) StopOnError() bool {
	return a.stopOnError
}
