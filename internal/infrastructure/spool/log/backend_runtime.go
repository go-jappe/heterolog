package logspool

import (
	"context"
	"fmt"
	"strings"
	"time"

	fsstorelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/lifecycle"
	fsstoreretryafter "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsstoreworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	lifecyclefsstore "github.com/go-jappe/heterolog/pkg/spool/lifecyclemanager/lifecyclefsstore"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

// BackendRuntimeOptionsは、一つのspoolからbackendへ再送するための送信処理と、
// ファイル保存・再送処理の構築に必要な共通設定をまとめます。
type BackendRuntimeOptions struct {
	Backend          string
	SpoolConfig      pkgdurable.SpoolConfig
	RunnerConfig     pkgspoolrunner.BackendSpoolRunnerConfig
	SupervisorConfig pkgspoolsupervisor.RunnerSupervisorConfig
	Sender           worker.Sender
	// OnWarnは再送後の保存先を塞いでいた項目の保全成功を通知します。
	// runnerの終了を待つcallbackは、呼び出し側で別goroutineへ通知を登録してください。
	OnWarn logging.LoggerHandlerFunc
}

// NewBackendRunnerSupervisorは、spoolの書き込み側と同じディレクトリ構成を使い、
// 再送用のworker、lifecycle manager、runner、supervisorを組み立てます。
func NewBackendRunnerSupervisor(
	clk clock.Clock,
	opts BackendRuntimeOptions,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*RunnerSupervisor, error) {
	backend := strings.TrimSpace(opts.Backend)
	if backend == "" {
		return nil, fmt.Errorf("spool backend is empty")
	}
	if clk == nil {
		return nil, fmt.Errorf("spool clock is nil")
	}
	if opts.Sender == nil {
		return nil, fmt.Errorf("spool sender is nil: backend=%s", backend)
	}
	transitionRetry, err := worker.NewProcessingFileTransitionRetry(
		worker.NewProcessingFileTransitionRetryConfigAdapter(worker.ProcessingFileTransitionRetryConfig{
			RetryDelay: opts.RunnerConfig.Worker.Finalize.RetryDelay,
			MaxEntries: opts.RunnerConfig.Worker.Finalize.MaxEntries,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("build processing file transition retry for %s: %w", backend, err)
	}

	spoolConfig := loggerdurable.NewSpoolConfigAdapter(opts.SpoolConfig)
	activeStore, err := fsstorestore.NewSpoolStore(
		clk,
		spoolConfig,
		tracer,
		meter,
	)
	if err != nil {
		return nil, fmt.Errorf("build active spool store for %s: %w", backend, err)
	}
	activeStore.OnWarn = opts.OnWarn
	stateStore, err := fsstoreworker.NewStateStore(spoolConfig)
	if err != nil {
		return nil, fmt.Errorf("build spool state store for %s: %w", backend, err)
	}
	// Retry-Afterによる待機期限は、このrunnerが専有するspool root内へbackend名で保存します。
	retryAfterStore, err := fsstoreretryafter.NewLogSpoolStore(stateStore.RootDir.String(), backend)
	if err != nil {
		return nil, fmt.Errorf("build spool retry-after store for %s: %w", backend, err)
	}

	spoolWorker, err := worker.New(
		stateStore,
		backendActiveSpoolWriter{store: activeStore},
		opts.Sender,
		worker.WithTargetBackendService(backend),
		worker.WithObservers(tracer, meter),
		worker.WithProcessingFileTransitionRetry(transitionRetry),
		worker.WithMaxRetryAfter(opts.RunnerConfig.Worker.MaxRetryAfter),
		worker.WithRetryAfterStore(logSpoolRetryAfterStore{store: retryAfterStore}),
		// 保存に失敗した期限は、runnerの周期から始めて間隔を倍にしながら保存し直します。
		worker.WithRetryAfterSaveRetryInterval(opts.RunnerConfig.Runner.Interval),
	)
	if err != nil {
		return nil, fmt.Errorf("build spool worker for %s: %w", backend, err)
	}

	lifecycleConfig := fsstorelifecycle.NewConfigAdapter(lifecyclefsstore.LifeCycleFsstoreConfig{
		SpoolPath: opts.SpoolConfig.SpoolPath,
		Target:    opts.SpoolConfig.Target,
		Dir:       opts.SpoolConfig.Dir,

		EnablePurgeDone:       opts.RunnerConfig.LifeCycle.EnablePurgeDone,
		EnablePurgeDead:       opts.RunnerConfig.LifeCycle.EnablePurgeDead,
		EnablePurgeQuarantine: opts.RunnerConfig.LifeCycle.EnablePurgeQuarantine,

		DoneMaxFileAge: opts.RunnerConfig.LifeCycle.DoneMaxFileAge,
		DoneMaxDirSize: opts.RunnerConfig.LifeCycle.DoneMaxDirSize,

		DeadMaxFileAge: opts.RunnerConfig.LifeCycle.DeadMaxFileAge,
		DeadMaxDirSize: opts.RunnerConfig.LifeCycle.DeadMaxDirSize,

		QuarantineMaxFileAge: opts.RunnerConfig.LifeCycle.QuarantineMaxFileAge,
		QuarantineMaxDirSize: opts.RunnerConfig.LifeCycle.QuarantineMaxDirSize,

		SizePurgeInterval:        opts.RunnerConfig.LifeCycle.SizePurgeInterval,
		EnableSizeEstimateCache:  opts.RunnerConfig.LifeCycle.EnableSizeEstimateCache,
		SizeFullScanEveryNChecks: opts.RunnerConfig.LifeCycle.SizeFullScanEveryNChecks,
		SizeFullScanMaxInterval:  opts.RunnerConfig.LifeCycle.SizeFullScanMaxInterval,
	})
	lifecycleStore, err := fsstorelifecycle.NewFileOperator(
		clk,
		lifecycleConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("build spool lifecycle store for %s: %w", backend, err)
	}
	lifecycleManager, err := lifecycle.NewManager(lifecycleStore, tracer, meter)
	if err != nil {
		return nil, fmt.Errorf("build spool lifecycle manager for %s: %w", backend, err)
	}

	runner, err := NewRunner(
		spoolWorker,
		lifecycleManager,
		NewRunnerConfigAdapter(pkgspoolrunner.RunnerConfig{
			Interval:    opts.RunnerConfig.Runner.Interval,
			StopOnError: opts.RunnerConfig.Runner.StopOnError,
		}),
		tracer,
		meter,
	)
	if err != nil {
		return nil, fmt.Errorf("build spool runner for %s: %w", backend, err)
	}
	supervisor := NewRunnerSupervisor(
		runner,
		NewConfigAdapter(opts.SupervisorConfig),
		tracer,
		meter,
	)
	if err := supervisor.validate(); err != nil {
		return nil, fmt.Errorf("validate spool runner supervisor for %s: %w", backend, err)
	}

	return &supervisor, nil
}

// logSpoolRetryAfterStoreは、spool root内のmarkerをworkerの待機期限の保存先として使います。
type logSpoolRetryAfterStore struct {
	store *fsstoreretryafter.Store
}

func (s logSpoolRetryAfterStore) Load(now time.Time, maxWait time.Duration) (worker.RetryAfterState, *pkgspool.RetryAfterClamp, error) {
	state, clamp, err := s.store.Load(now, maxWait)
	return worker.RetryAfterState{NotBefore: state.NotBefore, AppliedThrough: state.AppliedThrough}, clamp, err
}

func (s logSpoolRetryAfterStore) Save(state worker.RetryAfterState) error {
	return s.store.Save(fsstoreretryafter.State{NotBefore: state.NotBefore, AppliedThrough: state.AppliedThrough})
}

func (s logSpoolRetryAfterStore) Remove() error {
	return s.store.Remove()
}

type backendActiveSpoolWriter struct {
	store *fsstorestore.SpoolStore
}

func (w backendActiveSpoolWriter) StoreActive(
	ctx context.Context,
	records []pkgspool.SpoolRecord,
) error {
	return w.store.StoreByState(ctx, pkgspool.StateActive, records)
}
