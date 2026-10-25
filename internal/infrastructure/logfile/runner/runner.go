package runner

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
)

var (
	ErrNilRunner                   = errors.New("appendlog lifecycle runner is nil")
	ErrNilRunnerDirectoryLifecycle = errors.New("appendlog lifecycle runner directory lifecycle is nil")
	ErrRunnerAlreadyRunning        = errors.New("appendlog lifecycle runner is already running")
	ErrInvalidRunnerInterval       = errors.New("appendlog lifecycle runner interval must be positive")
	ErrRunnerStoppedOnError        = errors.New("appendlog lifecycle runner stopped on error")
)

type RunnerConfig interface {
	Interval() time.Duration
	StopOnError() bool
}

// RunnerOptionはNewRunnerが構築するRunnerへoptionalな実行機能を追加します。
type RunnerOption func(*Runner)

// WithSupervisorはRunner.RunをSupervisor経由で実行し、StopOnErrorによる終了やpanicを
// 設定された再起動方針で処理します。nil configはSupervisorを有効にしません。
func WithSupervisor(config SupervisorConfig) RunnerOption {
	return func(runner *Runner) {
		if runner != nil && config != nil {
			runner.supervisor = newSupervisor(runner, config)
		}
	}
}

// StaticRunnerConfigは、appendlog lifecycle RunnerがRunOnceを開始する周期と、
// RunOnce失敗後もRunnerの周期loopを継続するかを固定値で指定するRunnerConfig実装です。
type StaticRunnerConfig struct {
	// IntervalValueはRunnerが各RunOnceを開始するまでの待機時間です。
	// Runner起動後の初回RunOnce前にも待機します。0以下は設定エラーです。
	IntervalValue time.Duration
	// StopOnErrorValueがtrueの場合、RunOnceがcontext終了以外のerrorを返すとRunnerの
	// 周期loopを終了します。falseの場合はReporterへ結果を通知した後、次の周期を継続します。
	// Runへ渡されたcontextの終了時は、この値にかかわらず終了します。個々のupload等が
	// 独自の期限・キャンセルで失敗しても、Runのcontextが有効ならこの値に従います。
	// 一時的なfilesystemやobject storageの障害を次周期で再試行する通常運用ではfalseを
	// 使用します。trueはWithSupervisorを指定し、Runnerを終了させてからdelay付きで
	// 再起動する特殊な運用向けです。Supervisorなしでtrueにすると、最初の失敗後に
	// lifecycle処理が停止したままになるため、applicationの通常配線では使用しません。
	StopOnErrorValue bool
}

func (c StaticRunnerConfig) Interval() time.Duration {
	return c.IntervalValue
}

func (c StaticRunnerConfig) StopOnError() bool {
	return c.StopOnErrorValue
}

// Runnerは、ログの書き込み先に残るローテーション済みファイルの保守を周期的に実行します。
// 一つのRunnerの管理範囲は、DirectoryLifecycleに登録した一つの保存ディレクトリと、
// その中のログ系列です。同じディレクトリの通常・error・debug系列はまとめて扱います。
// ログ系列はファイル名のprefix・target・元の拡張子で識別します。
// 保存先が異なる系列は、別のRunnerで管理します。
//
// 管理対象は次のファイルと、それぞれの圧縮済みファイルです。名前や保存先は設定で変わります。
//
//   - FileLogger・DeliveryFileLoggerの通常ログ: app_20261008_1.log、error_app_20261008_1.log、debug_app_20261008_1.log
//   - backend送信失敗レコードの保存ファイル: retryable/app_20261008_1.jsonl、permanent/app_20261008_1.jsonl
//
// 各周期ではDirectoryLifecycleを通して、次の保守処理を進めます。
//
//   - 圧縮完了後に残った元ログの削除待ちファイルの回収
//   - 圧縮が有効な場合の未圧縮ファイルの圧縮と、圧縮元の削除
//   - 未完了宛先へのファイル単位のobject storage保存と、送信・退避完了後の圧縮ファイル削除
//   - 保持期限・世代数・ディレクトリ容量の設定に応じたpurge
//
// 削除待ちの例はapp_20261008_1.log.compression-source-delete-pending.tmpです。
// 最新の未圧縮ファイルや保護中のファイルは、圧縮・送信・purgeの対象から外します。
// Runner自身は周期の実行と結果通知を管理し、各ファイルの処理はDirectoryLifecycleへ委譲します。
type Runner struct {
	DirectoryLifecycle *lifecycle.DirectoryLifecycle
	Config             RunnerConfig
	// Reporterは周期処理の結果報告先です。ファイル処理の復旧結果の報告先は、
	// lifecycle.FileSeriesManagerOptions.Reporterで構築時に指定します。
	Reporter   lifecycle.RunReporter
	supervisor *Supervisor

	// running は同一 Runner インスタンスの多重 Run を防ぐ。
	// 複数インスタンスや複数プロセスの同時起動は呼び出し側で管理する。
	running atomic.Bool
}

// NewRunnerは、構築済みのDirectoryLifecycleを指定間隔で実行するRunnerを作ります。
// reporterは各周期の結果報告先です。復旧結果の報告先はlifecycleの構築時に指定します。
func NewRunner(
	directoryLifecycle *lifecycle.DirectoryLifecycle,
	cfg RunnerConfig,
	reporter lifecycle.RunReporter,
	options ...RunnerOption,
) *Runner {
	runner := &Runner{
		DirectoryLifecycle: directoryLifecycle,
		Config:             cfg,
		Reporter:           reporter,
	}
	for _, option := range options {
		if option != nil {
			option(runner)
		}
	}
	return runner
}

// DirectoryPathは、このRunnerが一つの実行単位として管理する物理directoryを
// DirectoryLifecycleの構築結果から返します。返り値はwriterとの排他に使うpathと
// 同じ方法で正規化され、呼び出し側は同型runnerの論理identityに利用できます。
func (r *Runner) DirectoryPath() (string, error) {
	if r == nil {
		return "", ErrNilRunner
	}
	if r.DirectoryLifecycle == nil {
		return "", ErrNilRunnerDirectoryLifecycle
	}
	return r.DirectoryLifecycle.DirectoryPath()
}

func (r *Runner) RunOnce(ctx context.Context) (lifecycle.RunReport, error) {
	if r == nil {
		return lifecycle.RunReport{}, ErrNilRunner
	}
	if r.DirectoryLifecycle == nil {
		return lifecycle.RunReport{}, ErrNilRunnerDirectoryLifecycle
	}
	return r.DirectoryLifecycle.RunOnce(ctx)
}

// ReconcileObjectStorageUploadDestinationsは周期loopを開始せず、設定されたobject storage
// upload先とcheckpointの起動時整合だけを行います。compress、upload、purgeは実行しません。
func (r *Runner) ReconcileObjectStorageUploadDestinations(ctx context.Context) (lifecycle.RunReport, error) {
	if r == nil {
		return lifecycle.RunReport{}, ErrNilRunner
	}
	if r.DirectoryLifecycle == nil {
		return lifecycle.RunReport{}, ErrNilRunnerDirectoryLifecycle
	}
	return r.DirectoryLifecycle.ReconcileObjectStorageUploadDestinations(ctx)
}

// CloseObjectStorageCompressedLogFileUploadFailureRoutesは、factoryがこのRunnerのfile series用に取得した
// compressed log file upload failure store登録を解放します。RunがSupervisorにより再起動される途中ではなく、
// 外側のownerが最終的にRunを停止し、そのgoroutineの終了を待った後に呼びます。
func (r *Runner) CloseObjectStorageCompressedLogFileUploadFailureRoutes() error {
	if r == nil {
		return ErrNilRunner
	}
	if r.DirectoryLifecycle == nil {
		return ErrNilRunnerDirectoryLifecycle
	}
	return r.DirectoryLifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()
}

// ForceCloseは通常終了の待機を打ち切るため、受理済み周期と復旧へ中止を通知します。
// 依存resourceの解放は、runnerが戻った後の通常Close経路で行います。
func (r *Runner) ForceClose() error {
	if r == nil {
		return ErrNilRunner
	}
	if r.DirectoryLifecycle == nil {
		return ErrNilRunnerDirectoryLifecycle
	}
	return r.DirectoryLifecycle.ForceClose()
}

func (r *Runner) Run(ctx context.Context) error {
	if r != nil && r.supervisor != nil {
		return r.supervisor.Run(ctx)
	}
	return r.run(ctx)
}

// runはSupervisorを経由しない単一の周期loopを実行します。SupervisorはRunner.Runを
// 再帰的に呼ばず、この境界を再実行することで同じRunnerを監視します。
func (r *Runner) run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}

	if !r.running.CompareAndSwap(false, true) {
		return ErrRunnerAlreadyRunning
	}
	defer r.running.Store(false)

	interval := r.Config.Interval()
	for {
		// 初回も interval 待ってから実行する。
		// lifecycle は起動直後の必須処理ではなく、通常処理と競合しない周期処理として扱う。
		if err := waitContext(ctx, interval); err != nil {
			return err
		}

		// 中止要求は次の周期の受付を止めます。受理済み周期は完了まで実行し、
		// 必要な復旧を登録してから、所有者が復旧queueとcheckpoint Storeを閉じます。
		if err := ctx.Err(); err != nil {
			return err
		}
		cycleCtx := context.WithoutCancel(ctx)
		report, err := r.RunOnce(cycleCtx)
		if r.Reporter != nil {
			r.Reporter.ReportRun(cycleCtx, report, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if isContextError(err) && ctx.Err() != nil {
				return err
			}
			if r.Config != nil && r.Config.StopOnError() {
				return fmt.Errorf("%w: %w", ErrRunnerStoppedOnError, err)
			}
		}

	}
}

func (r *Runner) validate() error {
	if r == nil {
		return ErrNilRunner
	}
	if r.DirectoryLifecycle == nil {
		return ErrNilRunnerDirectoryLifecycle
	}
	if r.Config == nil || r.Config.Interval() <= 0 {
		return ErrInvalidRunnerInterval
	}
	return nil
}

func waitContext(ctx context.Context, d time.Duration) error {
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
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
