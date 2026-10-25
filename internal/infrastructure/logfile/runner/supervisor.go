package runner

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"
)

var (
	ErrRunnerSupervisorAlreadyRunning = errors.New("appendlog runner supervisor is already running")
	ErrNilSupervisorConfig            = errors.New("appendlog runner supervisor config is nil")
	ErrInvalidSupervisorRestartDelay  = errors.New("appendlog runner supervisor restart delay must be positive")
	ErrInvalidSupervisorMaxRestarts   = errors.New("appendlog runner supervisor max restarts must be positive")
	ErrRunnerStoppedUnexpectedly      = errors.New("appendlog runner stopped unexpectedly")
	ErrRunnerPanicked                 = errors.New("appendlog runner panicked")
	ErrSupervisorRestartLimitExceeded = errors.New("appendlog runner supervisor restart limit exceeded")
)

// SupervisorRestartLimitErrorは、設定回数の再起動を終えた後でRunnerが再び終了したことを表します。
// Restartsは実行した再起動の回数、Errは最後のRunnerの終了原因です。
// errors.IsではErrSupervisorRestartLimitExceededと一致し、Errもerrors.Is/Asで判定できます。
type SupervisorRestartLimitError struct {
	Restarts int
	Err      error
}

func (e *SupervisorRestartLimitError) Error() string {
	return fmt.Sprintf("%v: count=%d: %v", ErrSupervisorRestartLimitExceeded, e.Restarts, e.Err)
}

func (e *SupervisorRestartLimitError) Unwrap() []error {
	return []error{ErrSupervisorRestartLimitExceeded, e.Err}
}

// RunnerPanicErrorは、Supervisorが監視するRunnerのpanicを表します。Valueはrecoverで受け取った値、
// Stackはpanic時のstack traceです。errors.IsではErrRunnerPanickedと一致し、Valueがerrorなら
// その原因もerrors.Is/Asで判定できます。
type RunnerPanicError struct {
	Value any
	Stack []byte
}

func (e *RunnerPanicError) Error() string {
	return fmt.Sprintf("%v: %v\n%s", ErrRunnerPanicked, e.Value, e.Stack)
}

func (e *RunnerPanicError) Unwrap() []error {
	if err, ok := e.Value.(error); ok {
		return []error{ErrRunnerPanicked, err}
	}
	return []error{ErrRunnerPanicked}
}

// SupervisorConfigは、Runner異常終了後の再起動間隔と上限を定義します。
type SupervisorConfig interface {
	RestartDelay() time.Duration
	MaxRestarts() int
}

// StaticSupervisorConfigはSupervisorConfigを固定値で指定する実装です。
type StaticSupervisorConfig struct {
	// RestartDelayValueはRunner終了後に再起動するまでの待機時間です。0以下は設定エラーです。
	RestartDelayValue time.Duration
	// MaxRestartsValueは一回のRunで許可する最大再起動回数です。0以下は設定エラーです。
	MaxRestartsValue int
}

func (c StaticSupervisorConfig) RestartDelay() time.Duration { return c.RestartDelayValue }
func (c StaticSupervisorConfig) MaxRestarts() int            { return c.MaxRestartsValue }

// Supervisorは一つのappendlog lifecycle Runnerを監視し、Runが通常の周期継続を
// 維持できず終了した場合に、設定されたdelayと上限に従って同じRunnerを再起動します。
// Runへ渡されたcontextの終了と構成不正は再起動せず、そのまま呼び出し元へ返します。
type Supervisor struct {
	runner  *Runner
	config  SupervisorConfig
	running atomic.Bool
}

func newSupervisor(runner *Runner, config SupervisorConfig) *Supervisor {
	return &Supervisor{runner: runner, config: config}
}

// RunはRunnerの終了を監視します。StopOnErrorによる終了とpanicは再起動対象です。
// 設定回数を再起動した後でRunnerが再び終了した場合は、最後のRunner errorを保持した
// ErrSupervisorRestartLimitExceededを返します。
func (s *Supervisor) Run(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}
	if !s.running.CompareAndSwap(false, true) {
		return ErrRunnerSupervisorAlreadyRunning
	}
	defer s.running.Store(false)

	restartCount := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := s.runRunner(ctx)
		if err == nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			err = ErrRunnerStoppedUnexpectedly
		}
		if isContextError(err) && ctx.Err() != nil {
			return err
		}
		if isNonRestartableRunnerError(err) {
			return err
		}

		maxRestarts := s.config.MaxRestarts()
		if restartCount >= maxRestarts {
			return &SupervisorRestartLimitError{Restarts: restartCount, Err: err}
		}
		restartCount++
		if err := waitContext(ctx, s.config.RestartDelay()); err != nil {
			return err
		}
	}
}

func (s *Supervisor) runRunner(ctx context.Context) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &RunnerPanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	return s.runner.run(ctx)
}

func (s *Supervisor) validate() error {
	if s == nil || s.runner == nil {
		return ErrNilRunner
	}
	if s.config == nil {
		return ErrNilSupervisorConfig
	}
	if err := s.runner.validate(); err != nil {
		return err
	}
	if s.config.RestartDelay() <= 0 {
		return ErrInvalidSupervisorRestartDelay
	}
	if s.config.MaxRestarts() <= 0 {
		return ErrInvalidSupervisorMaxRestarts
	}
	return nil
}

func isNonRestartableRunnerError(err error) bool {
	return errors.Is(err, ErrNilRunner) ||
		errors.Is(err, ErrNilRunnerDirectoryLifecycle) ||
		errors.Is(err, ErrRunnerAlreadyRunning) ||
		errors.Is(err, ErrInvalidRunnerInterval)
}
