package runner

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// ErrFailureArchiveRunnerModeRequiredは周期処理が無効な設定からRunnerを作ろうとしたことを表します。
var ErrFailureArchiveRunnerModeRequired = errors.New("failure archive lifecycle runner requires runner mode")

// FailureArchiveRunnerModeErrorは、runner以外のlifecycle modeを指定した設定を表します。
// Modeは設定されていた値で、errors.IsではErrFailureArchiveRunnerModeRequiredと一致します。
type FailureArchiveRunnerModeError struct {
	Mode pkgfsstore.FileLifecycleMode
}

func (e *FailureArchiveRunnerModeError) Error() string {
	return fmt.Sprintf("%v: got %q", ErrFailureArchiveRunnerModeRequired, e.Mode)
}

func (e *FailureArchiveRunnerModeError) Is(target error) bool {
	return target == ErrFailureArchiveRunnerModeRequired
}

// NewLogFileRunnersはFileLoggerの設定から、物理directoryごとに一つの周期Runnerを作ります。
// runnerCfgがnilならopts.Config.RunnerConfigを使い、Supervisorが有効な場合は各Runnerに
// 独立したSupervisorを付けます。構築だけでは周期処理を開始しません。
// checkpoint保存先は呼び出し前に準備してください。
func NewLogFileRunners(opts lifecycle.LogFileOptions, runnerCfg RunnerConfig) ([]*Runner, error) {
	return newRunners(opts.Config.RunnerConfig, runnerCfg, opts.Reporter, func() ([]*lifecycle.DirectoryLifecycle, error) {
		return lifecycle.NewLogFileLifecycles(opts)
	}, "appendlog")
}

// NewDeliveryFileRunnersはDeliveryFileLoggerの設定から、物理directoryごとに周期Runnerを作ります。
// runnerCfgの扱いと、checkpoint保存先の準備はNewLogFileRunnersと同じです。
func NewDeliveryFileRunners(opts lifecycle.DeliveryFileOptions, runnerCfg RunnerConfig) ([]*Runner, error) {
	return newRunners(opts.Config.RunnerConfig, runnerCfg, opts.Reporter, func() ([]*lifecycle.DirectoryLifecycle, error) {
		return lifecycle.NewDeliveryFileLifecycles(opts)
	}, "appendlog")
}

// NewFailureArchiveRunnersはbackend送信失敗archiveのretryable/permanent保存先を周期実行します。
// runner modeの設定だけを受け付けます。runnerCfgがnilならConfigの実行設定を使い、
// 各Runnerへ独立したSupervisorを付けます。checkpoint保存先は呼び出し前に準備してください。
func NewFailureArchiveRunners(opts lifecycle.FailureArchiveOptions, runnerCfg RunnerConfig) ([]*Runner, error) {
	if opts.Config == nil {
		return nil, lifecycle.ErrNilConfig
	}
	if mode := opts.Config.LifecycleMode(); mode != pkgfsstore.FileLifecycleModeRunner {
		return nil, &FailureArchiveRunnerModeError{Mode: mode}
	}
	return newRunners(opts.Config.LifecycleRunnerConfig(), runnerCfg, opts.Reporter, func() ([]*lifecycle.DirectoryLifecycle, error) {
		return lifecycle.NewFailureArchiveLifecycles(opts)
	}, "failure archive")
}

// newRunnersは周期設定を先に検証し、共通lifecycleの構築結果を周期Runnerで包みます。
// file処理と復旧通知の構築はlifecycle側が担当します。
func newRunners(
	config pkgfsstore.AppendLogRunnerConfig,
	runnerConfig RunnerConfig,
	reporter lifecycle.RunReporter,
	build func() ([]*lifecycle.DirectoryLifecycle, error),
	name string,
) ([]*Runner, error) {
	if runnerConfig == nil {
		if err := pkgfsstore.ValidateAppendLogRunnerConfig(config); err != nil {
			return nil, fmt.Errorf("validate %s runner config: %w", name, err)
		}
	}
	lifecycles, err := build()
	if err != nil {
		return nil, err
	}
	var options []RunnerOption
	if runnerConfig == nil {
		runnerConfig, options = configuredRunnerDependencies(config)
	}
	runners := make([]*Runner, 0, len(lifecycles))
	for _, directory := range lifecycles {
		runners = append(runners, NewRunner(directory, runnerConfig, reporter, options...))
	}
	return runners, nil
}
