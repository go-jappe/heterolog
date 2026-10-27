package logspool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

var (
	ErrNilRunnerSupervisor            = errors.New("spool runner supervisor is nil")
	ErrRunnerSupervisorAlreadyRunning = errors.New("spool runner supervisor is already running")
	ErrInvalidSupervisorRestartDelay  = errors.New("spool runner supervisor restart delay must be positive")
	ErrInvalidSupervisorMaxRestarts   = errors.New("spool runner supervisor max restarts must not be negative")
	ErrRunnerStoppedUnexpectedly      = errors.New("spool runner stopped unexpectedly")
	ErrSupervisorRestartLimitExceeded = errors.New("spool runner supervisor restart limit exceeded")
	// ErrRunnerPanicは、Runner.Runがpanicで中断したことを表します。
	// 回復したpanicの値はRunnerPanicErrorが保持します。
	ErrRunnerPanic = errors.New("spool runner panic")
)

// RunnerPanicErrorは、Runner.Runのpanicを回復したerrorです。
// errors.Is(err, ErrRunnerPanic)で判定でき、Valueにpanicの値を保持します。
type RunnerPanicError struct {
	Value any
}

func (e *RunnerPanicError) Error() string {
	return fmt.Sprintf("%v: %v", ErrRunnerPanic, e.Value)
}

func (e *RunnerPanicError) Unwrap() error {
	return ErrRunnerPanic
}

// RestartLimitExceededErrorは、再起動回数が上限に達した後でRunnerが再び異常停止したため、
// Supervisorが再起動せずに停止したことを表します。errors.Is(err, ErrSupervisorRestartLimitExceeded)で
// 判定でき、最後の異常停止の原因もerrors.Is・errors.Asで辿れます。
type RestartLimitExceededError struct {
	// RestartCountは、停止までに実際に再起動した回数です。
	RestartCount int
	// LastErrは、上限超過で停止した最後の異常停止の原因です。
	LastErr error
}

func (e *RestartLimitExceededError) Error() string {
	return fmt.Sprintf("%v: count=%d last_error=%v", ErrSupervisorRestartLimitExceeded, e.RestartCount, e.LastErr)
}

// Isは、ErrSupervisorRestartLimitExceededと一致します。
func (e *RestartLimitExceededError) Is(target error) bool {
	return target == ErrSupervisorRestartLimitExceeded
}

// Unwrapは、最後の異常停止の原因を返します。
func (e *RestartLimitExceededError) Unwrap() error { return e.LastErr }

type RunnerSupervisor struct {
	// Runner は、監視・再起動対象の spool runner。
	Runner *Runner

	// Config は、Runner 異常停止時の再起動方針。
	Config RunnerSupervisorConfig

	// Reporter は、Runner の各 RunOnce 結果を外へ通知する。
	//
	// nil の場合、通知は行わない。
	// Supervisor 自身も Snapshot 用に report を保持する。
	Reporter RunnerReporter

	running atomic.Bool

	mu       sync.RWMutex
	snapshot RunnerSupervisorSnapshot

	Tracer pkgobserver.Tracer
	Meter  pkgobserver.Meter
}

type RunnerSupervisorConfig interface {
	// RestartDelay は、Runner が異常停止した後、
	// 次に Runner.Run を呼び直すまでの待機時間。
	//
	// 0以下は設定ミスとして error にする。
	RestartDelay() time.Duration

	// MaxRestarts は、最大再起動回数。
	//
	// 実際に再起動した回数がこの値に達した後で Runner が異常停止した場合は、
	// 再起動せずに ErrSupervisorRestartLimitExceeded で停止する。
	// 0 の場合は無制限。
	// 負数は設定ミスとして error にする。
	MaxRestarts() int
}

type RunnerSupervisorState string

const (
	RunnerSupervisorStateIdle       RunnerSupervisorState = "idle"
	RunnerSupervisorStateRunning    RunnerSupervisorState = "running"
	RunnerSupervisorStateRestarting RunnerSupervisorState = "restarting"
	RunnerSupervisorStateStopped    RunnerSupervisorState = "stopped"
)

type RunnerSupervisorSnapshot struct {
	State RunnerSupervisorState

	StartedAt time.Time

	LastRunnerStartedAt time.Time
	LastRunnerStoppedAt time.Time
	// LastRunnerStopErrorは、LastRunnerStopErrの表示用文字列です。
	LastRunnerStopError string
	// LastRunnerStopErrは、Runnerの最後の停止理由です。errors.Is・errors.Asで判定できます。
	// Runnerの実行中はnilです。
	LastRunnerStopErr error

	// RestartCount は、再起動の待機後に実際に Runner を再実行した回数。
	// 待機中の停止要求や上限超過で再起動しなかった停止は数えない。
	RestartCount int
	// LastRestartedAt は、最後に Runner を再実行した時刻。再起動していなければ zero value。
	LastRestartedAt time.Time

	LastRunReport RunReport
	// LastRunErrorは、LastRunErrの表示用文字列です。
	LastRunError string
	// LastRunErrは、最後に通知された周期のerrorです。errors.Is・errors.Asで判定できます。
	LastRunErr        error
	LastRunReportedAt time.Time
}

func NewRunnerSupervisor(
	runner *Runner,
	cfg RunnerSupervisorConfig,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) RunnerSupervisor {
	return RunnerSupervisor{
		Runner: runner,
		Config: cfg,
		Tracer: tracer,
		Meter:  meter,
	}
}

// Run は、Runner.Run を監視し、異常停止した場合に再起動する。
//
// ctx が cancel / deadline exceeded になった場合は正常な停止要求として扱い、
// 受理済みの一周期と結果通知を終えてから、再起動せずに終了する。
func (s *RunnerSupervisor) Run(ctx context.Context) error {
	return s.RunUntilStopped(ctx, context.WithoutCancel(ctx))
}

// RunUntilStoppedはstopCtxで次の周期・再起動を止め、開始済みの処理と通知には
// operationCtxを使います。operationCtxの中止は明示的な強制終了に限ります。
func (s *RunnerSupervisor) RunUntilStopped(ctx, operationCtx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}

	if !s.running.CompareAndSwap(false, true) {
		return ErrRunnerSupervisorAlreadyRunning
	}
	defer s.running.Store(false)

	s.markStarted()

	restarting := false
	for {
		if err := ctx.Err(); err != nil {
			s.markStopped(err)
			return err
		}

		s.markRunnerStarted(restarting)

		err := s.runRunner(ctx, operationCtx)
		if err == nil && ctx.Err() == nil {
			err = ErrRunnerStoppedUnexpectedly
			s.reportUnexpectedRunnerFailure(operationCtx, err)
		}

		if isContextError(err) {
			s.markStopped(err)
			return err
		}

		if isNonRestartableRunnerError(err) {
			s.markStopped(err)
			return err
		}

		// 再起動回数は実際に再起動した回数です。上限に達していれば再起動せずに停止します。
		if restartCount := s.restartCount(); s.Config.MaxRestarts() > 0 && restartCount >= s.Config.MaxRestarts() {
			limitErr := &RestartLimitExceededError{RestartCount: restartCount, LastErr: err}
			s.markStopped(limitErr)
			return limitErr
		}

		s.markRestarting(err)
		if err := waitSupervisorContext(ctx, s.Config.RestartDelay()); err != nil {
			s.markStopped(err)
			return err
		}
		restarting = true
	}
}

// ReportSpoolRunnerRun は、Runner.RunOnce の結果を Snapshot 用に保存する。
func (s *RunnerSupervisor) ReportSpoolRunnerRun(
	ctx context.Context,
	report RunReport,
	err error,
) {
	s.mu.Lock()
	s.snapshot.LastRunReport = report
	s.snapshot.LastRunError = errorText(err)
	s.snapshot.LastRunErr = err
	s.snapshot.LastRunReportedAt = time.Now()
	s.mu.Unlock()

	if s.Reporter != nil {
		s.Reporter.ReportSpoolRunnerRun(ctx, report, err)
	}
}

// Snapshot は、現在の runner supervisor 状態を返す。
//
// 外部から状態確認したい場合はこの値を見る。
func (s *RunnerSupervisor) Snapshot() RunnerSupervisorSnapshot {
	if s == nil {
		return RunnerSupervisorSnapshot{
			State: RunnerSupervisorStateStopped,
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshot
}

func (s *RunnerSupervisor) IsRunning() bool {
	if s == nil {
		return false
	}

	return s.running.Load()
}

func (s *RunnerSupervisor) runRunner(ctx, operationCtx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &RunnerPanicError{Value: recovered}
			// RunOnceが結果を返せなかった場合も、Logger側のOnErrorへ通知します。
			s.reportUnexpectedRunnerFailure(operationCtx, err)
		}
	}()

	return s.Runner.runUntilStopped(ctx, operationCtx, s)
}

// Reporter自身のpanicでRunnerが中断した場合にも呼ばれるため、通知の再panicを隔離します。
// 元のエラーはRunの戻り値とSnapshotへ残し、Supervisorの停止・再起動判定は継続します。
func (s *RunnerSupervisor) reportUnexpectedRunnerFailure(ctx context.Context, err error) {
	defer func() { _ = recover() }()
	s.ReportSpoolRunnerRun(ctx, RunReport{}, err)
}

func (s *RunnerSupervisor) validate() error {
	if s == nil {
		return ErrNilRunnerSupervisor
	}
	if s.Runner == nil {
		return ErrNilRunner
	}
	if err := s.Runner.validateRunConfig(); err != nil {
		return err
	}
	if s.Config.RestartDelay() <= 0 {
		return ErrInvalidSupervisorRestartDelay
	}
	if s.Config.MaxRestarts() < 0 {
		return ErrInvalidSupervisorMaxRestarts
	}

	return nil
}

func (s *RunnerSupervisor) markStarted() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	s.snapshot.State = RunnerSupervisorStateRunning
	s.snapshot.StartedAt = now
}

// markRunnerStartedはRunner.Runの開始を記録します。restartedがtrueなら、
// 再起動の待機後に実際にRunnerを再実行した時点として再起動回数と時刻も更新します。
func (s *RunnerSupervisor) markRunnerStarted(restarted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	s.snapshot.State = RunnerSupervisorStateRunning
	s.snapshot.LastRunnerStartedAt = now
	s.snapshot.LastRunnerStopError = ""
	s.snapshot.LastRunnerStopErr = nil
	if restarted {
		s.snapshot.RestartCount++
		s.snapshot.LastRestartedAt = now
	}
}

// markRestartingは異常停止したRunnerの再起動待機を記録します。
// 待機中の停止要求で再起動しない場合もあるため、再起動回数と時刻はまだ更新しません。
func (s *RunnerSupervisor) markRestarting(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot.State = RunnerSupervisorStateRestarting
	s.snapshot.LastRunnerStoppedAt = time.Now()
	s.snapshot.LastRunnerStopError = errorText(err)
	s.snapshot.LastRunnerStopErr = err
}

// restartCountは、これまでに実際に再起動した回数を返します。
func (s *RunnerSupervisor) restartCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.snapshot.RestartCount
}

func (s *RunnerSupervisor) markStopped(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot.State = RunnerSupervisorStateStopped
	s.snapshot.LastRunnerStoppedAt = time.Now()
	s.snapshot.LastRunnerStopError = errorText(err)
	s.snapshot.LastRunnerStopErr = err
}

// isNonRestartableRunnerError は、Runner.Run が返した error のうち、
// Supervisor が Runner.Run の再実行に進まず、停止扱いにする error かどうかを判定する。
//
// 判定対象には、StopOnError による明示停止と、
// Runner の構成・設定に関する error を含める。
//
// true の場合、Supervisor は Runner.Run を再実行せず、
// その error を保持して停止状態に遷移する。
//
// false の場合、context error など別の停止条件に該当しない限り、
// Supervisor の再起動対象として扱われる。
func isNonRestartableRunnerError(err error) bool {
	return errors.Is(err, ErrRunnerStoppedOnError) ||
		errors.Is(err, ErrNilRunner) ||
		errors.Is(err, ErrNilWorker) ||
		errors.Is(err, ErrNilLifecycle) ||
		errors.Is(err, ErrRunnerAlreadyRunning) ||
		errors.Is(err, ErrInvalidRunnerInterval)
}

func waitSupervisorContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
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

func errorText(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

type ConfigAdapter struct {
	retryDelay  time.Duration
	maxRestarts int
}

func NewConfigAdapter(cfg pkgspoolsupervisor.RunnerSupervisorConfig) RunnerSupervisorConfig {
	return &ConfigAdapter{
		retryDelay:  cfg.RestartDelay,
		maxRestarts: cfg.MaxRestarts,
	}
}

func (a *ConfigAdapter) RestartDelay() time.Duration {
	return a.retryDelay
}

func (a *ConfigAdapter) MaxRestarts() int {
	return a.maxRestarts
}
