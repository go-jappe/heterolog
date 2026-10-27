package logger

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

type failureSpoolPipelineID struct {
	contractKind pkglogger.LoggerContractKind
	backend      pkglogger.LogOutputDestination
}

// failure spool pipelineの構築・writer登録の失敗の種類です。FailureSpoolPipelineErrorを
// errors.Isで判定します。
var (
	ErrDuplicateFailureSpoolPipeline        = errors.New("failure spool pipeline is configured more than once")
	ErrFailureSpoolPipelineLeaseMissing     = errors.New("failure spool pipeline has no directory lease")
	ErrFailureSpoolPipelineRunnerMissing    = errors.New("failure spool pipeline has no runner")
	ErrFailureSpoolPipelineWriterCount      = errors.New("failure spool pipeline does not have exactly one writer")
	ErrFailureSpoolWriterRegistrationClosed = errors.New("failure spool writer registration is closed")
	ErrUnplannedFailureSpoolWriter          = errors.New("failure spool writer is not planned")
	ErrFailureSpoolWriterStoreRequired      = errors.New("failure spool writer has no store")
	ErrFailureSpoolWriterAlreadyRegistered  = errors.New("failure spool writer is already registered")
	ErrFailureSpoolWriterRootMismatch       = errors.New("failure spool writer root does not match its pipeline")
)

// FailureSpoolPipelineErrorは、一つのLogger契約種別とbackendのfailure spool pipelineを
// 構築・登録できないことを表します。errors.IsでKindの種類を判定し、ContractKindと
// Backendでpipelineを特定できます。Writersはwriter数の検証時の件数、WriterRootと
// PipelineRootはroot不一致の場合の各pathです。
type FailureSpoolPipelineError struct {
	ContractKind pkglogger.LoggerContractKind
	Backend      pkglogger.LogOutputDestination
	Kind         error
	Writers      int
	WriterRoot   string
	PipelineRoot string
	message      string
}

func (e *FailureSpoolPipelineError) Error() string { return e.message }

func (e *FailureSpoolPipelineError) Is(target error) bool { return target == e.Kind }

// newFailureSpoolPipelineErrorは、従来と同じ文言を保ってFailureSpoolPipelineErrorを作ります。
// formatの最初の二つの%sはLogger契約種別とbackendで、argsはその後に続く値です。
func newFailureSpoolPipelineError(
	id failureSpoolPipelineID,
	kind error,
	format string,
	args ...any,
) *FailureSpoolPipelineError {
	return &FailureSpoolPipelineError{
		ContractKind: id.contractKind,
		Backend:      id.backend,
		Kind:         kind,
		message:      fmt.Sprintf(format, append([]any{id.contractKind, id.backend}, args...)...),
	}
}

// failureSpoolPipelineは、一つのLoggerContractKindとbackendについて、failure recordを
// 保存するwriter、同じrootを排出するrunner、そのrootのprocess-local Leaseを結合します。
// writerはbackendのCompositeへ借用されますが、Shutdownする責任はこのpipelineにあります。
type failureSpoolPipeline struct {
	id             failureSpoolPipelineID
	root           string
	writers        []*failureSpoolOwnedWriter
	runner         *failureSpoolRunnerRuntime
	directoryLease *directorylease.Lease
	// maxRetryAfterは、runnerと同じRetry-After待機の上限です。writerが保存する
	// 再送下限時刻にも同じ上限を適用します。
	maxRetryAfter time.Duration
}

type failureSpoolOwnedWriter struct {
	handler          *spoolstorehandler.SpoolStoreHandler
	shutdownComplete bool
}

// failureSpoolPipelineSetは、一つのtop-level Logger contract instanceで有効なfailure
// spool pipelineを直接所有します。backendの終了後にwriter、runner、Leaseの順で閉じるため、
// writerとrunnerを同じ設定から別々に構築するだけの間接所有にはしません。
type failureSpoolPipelineSet struct {
	pipelines []*failureSpoolPipeline
	byID      map[failureSpoolPipelineID]*failureSpoolPipeline
	writers   []*failureSpoolOwnedWriter
	runtime   *FailureSpoolRuntime
	leases    *failureSpoolDirectoryLeaseSet

	registrationMu     sync.Mutex
	registrationErrs   []error
	registrationSealed bool

	closeMu        sync.Mutex
	writersClosed  bool
	runtimeClosed  bool
	runtimeErr     error
	leasesReleased bool
}

func newFailureSpoolPipelineSet(
	ctx context.Context,
	plan []failureSpoolPipelinePlan,
	spoolConfig *pkgspool.SpoolConfig,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) (*failureSpoolPipelineSet, error) {
	if len(plan) == 0 {
		return nil, nil
	}
	if err := validateFailureSpoolRuntimeConstructionContext(ctx); err != nil {
		return nil, err
	}
	if spoolConfig == nil {
		return nil, ErrFailureSpoolRuntimeConfigRequired
	}

	leases, err := acquireFailureSpoolDirectoryLeases(plan)
	if err != nil {
		return nil, err
	}
	runtime, err := buildFailureSpoolRuntime(plan, spoolConfig, tracer, meter)
	if err != nil {
		leases.Release()
		return nil, err
	}

	set := &failureSpoolPipelineSet{
		pipelines: make([]*failureSpoolPipeline, 0, len(plan)),
		byID:      make(map[failureSpoolPipelineID]*failureSpoolPipeline, len(plan)),
		runtime:   runtime,
		leases:    leases,
	}
	for index, planned := range plan {
		id := failureSpoolPipelineID{
			contractKind: planned.contractKind,
			backend:      planned.backend,
		}
		if _, duplicated := set.byID[id]; duplicated {
			set.registrationErrs = append(set.registrationErrs, newFailureSpoolPipelineError(
				id, ErrDuplicateFailureSpoolPipeline,
				"failure spool pipeline %s/%s is configured more than once",
			))
			continue
		}
		lease, ok := leases.leaseFor(planned.backend)
		if !ok {
			set.registrationErrs = append(set.registrationErrs, newFailureSpoolPipelineError(
				id, ErrFailureSpoolPipelineLeaseMissing,
				"failure spool pipeline %s/%s has no directory lease",
			))
		}
		pipeline := &failureSpoolPipeline{
			id:             id,
			root:           planned.root,
			runner:         &runtime.runners[index],
			directoryLease: lease,
			maxRetryAfter:  failureSpoolMaxRetryAfter(spoolConfig, planned.backend),
		}
		set.pipelines = append(set.pipelines, pipeline)
		set.byID[id] = pipeline
	}
	return set, nil
}

// failureSpoolMaxRetryAfterは、backendのspool runnerに設定したRetry-After待機の上限を返します。
func failureSpoolMaxRetryAfter(
	spoolConfig *pkgspool.SpoolConfig,
	backend pkglogger.LogOutputDestination,
) time.Duration {
	if spoolConfig == nil {
		return 0
	}
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		return spoolConfig.RunnerConfig.OpensearchConfig.Worker.MaxRetryAfter
	case pkglogger.LogOutputLoki:
		return spoolConfig.RunnerConfig.LokiConfig.Worker.MaxRetryAfter
	case pkglogger.LogOutputCloudWatchLogs:
		return spoolConfig.RunnerConfig.CloudWatchLogsConfig.Worker.MaxRetryAfter
	case pkglogger.LogOutputClickHouse:
		return spoolConfig.RunnerConfig.ClickHouseConfig.Worker.MaxRetryAfter
	default:
		return 0
	}
}

// registrarForは、backend factoryが作成したraw spool writerを対応するpipelineへ移譲する
// callbackを返します。Compositeにはfactory側でShutdownを公開しない借用viewが渡されます。
func (s *failureSpoolPipelineSet) registrarFor(
	contractKind pkglogger.LoggerContractKind,
	backend pkglogger.LogOutputDestination,
) flushfailure.SpoolHandlerRegistrar {
	if s == nil {
		return nil
	}
	return func(handler *spoolstorehandler.SpoolStoreHandler) error {
		return s.registerWriter(contractKind, backend, handler)
	}
}

func (s *failureSpoolPipelineSet) registerWriter(
	contractKind pkglogger.LoggerContractKind,
	backend pkglogger.LogOutputDestination,
	handler *spoolstorehandler.SpoolStoreHandler,
) error {
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()

	id := failureSpoolPipelineID{contractKind: contractKind, backend: backend}
	if s.registrationSealed {
		return newFailureSpoolPipelineError(
			id, ErrFailureSpoolWriterRegistrationClosed,
			"failure spool writer registration for %s/%s is closed",
		)
	}
	pipeline := s.byID[id]
	if pipeline == nil {
		return newFailureSpoolPipelineError(
			id, ErrUnplannedFailureSpoolWriter,
			"received unplanned failure spool writer for %s/%s",
		)
	}
	if handler == nil || handler.Store == nil {
		return newFailureSpoolPipelineError(
			id, ErrFailureSpoolWriterStoreRequired,
			"failure spool writer for %s/%s has no store",
		)
	}
	if len(pipeline.writers) != 0 {
		return newFailureSpoolPipelineError(
			id, ErrFailureSpoolWriterAlreadyRegistered,
			"failure spool writer for %s/%s is already registered",
		)
	}
	root, err := filepath.Abs(handler.Store.RootDir.String())
	if err != nil {
		return fmt.Errorf(
			"resolve failure spool writer root for %s/%s: %w",
			contractKind,
			backend,
			err,
		)
	}
	if root != pipeline.root {
		pipelineErr := newFailureSpoolPipelineError(
			id, ErrFailureSpoolWriterRootMismatch,
			"failure spool writer root for %s/%s is %q, want %q", root, pipeline.root,
		)
		pipelineErr.WriterRoot, pipelineErr.PipelineRoot = root, pipeline.root
		return pipelineErr
	}

	owned := &failureSpoolOwnedWriter{handler: handler}
	s.writers = append(s.writers, owned)
	pipeline.writers = append(pipeline.writers, owned)
	// 通常送信で受け取ったRetry-Afterにも、同じrootを再送するrunnerと同じ上限を適用します。
	handler.SetMaxRetryAfter(pipeline.maxRetryAfter)
	// 通常送信で受け取った期限を、同じrootを再送するrunnerの送信先全体の待機期限として記録します。
	// runnerは次の周期から、その送信先のすべてのログに期限を適用します。
	if pipeline.runner != nil && pipeline.runner.supervisor != nil {
		if deadline := pipeline.runner.supervisor.Runner.RetryAfterDeadline(); deadline != nil {
			handler.SetRetryAfterRecorder(deadline)
		}
	}
	// writerと同じ設定で解決済みの内部通知先を、runner起動前に一度だけ接続します。
	// 別のfallback loggerを作ったり、実行のたびに通知先を設定し直したりしません。
	s.runtime.configureRunnerNotifications(pipeline.runner, handler.OnError, handler.OnWarn)
	return nil
}

// validateCompleteは、設定から計画した各pipelineにwriter・runner・Leaseが一つずつ
// 対応していることをrunner起動前に検証し、以後のwriter登録を禁止します。
func (s *failureSpoolPipelineSet) validateComplete() error {
	if s == nil {
		return nil
	}
	s.registrationMu.Lock()
	defer s.registrationMu.Unlock()
	s.registrationSealed = true

	errs := append([]error(nil), s.registrationErrs...)
	for _, pipeline := range s.pipelines {
		if len(pipeline.writers) != 1 {
			pipelineErr := newFailureSpoolPipelineError(
				pipeline.id, ErrFailureSpoolPipelineWriterCount,
				"failure spool pipeline %s/%s has %d writers, want 1", len(pipeline.writers),
			)
			pipelineErr.Writers = len(pipeline.writers)
			errs = append(errs, pipelineErr)
		}
		if pipeline.runner == nil || pipeline.runner.supervisor == nil {
			errs = append(errs, newFailureSpoolPipelineError(
				pipeline.id, ErrFailureSpoolPipelineRunnerMissing,
				"failure spool pipeline %s/%s has no runner",
			))
		}
		if pipeline.directoryLease == nil {
			errs = append(errs, newFailureSpoolPipelineError(
				pipeline.id, ErrFailureSpoolPipelineLeaseMissing,
				"failure spool pipeline %s/%s has no directory lease",
			))
		}
	}
	return errors.Join(errs...)
}

func (s *failureSpoolPipelineSet) Start(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if err := s.validateComplete(); err != nil {
		return err
	}
	return s.runtime.Start(context.WithoutCancel(ctx))
}

// closeは、全writerのShutdown完了後にだけrunnerを停止し、最後にLeaseを解放します。
// complete=falseはwriterの終了待ちが未完了であり、fallback resourceとLeaseを維持したまま
// top-level Closeから再試行しなければならないことを表します。
func (s *failureSpoolPipelineSet) close() (complete bool, err error) {
	if s == nil {
		return true, nil
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	// construction errorでvalidateCompleteへ到達しなかった場合も、Close中にwriterが
	// 追加されないよう登録を封印してから現在の所有writerを確定します。
	s.registrationMu.Lock()
	s.registrationSealed = true
	writers := append([]*failureSpoolOwnedWriter(nil), s.writers...)
	s.registrationMu.Unlock()

	if !s.writersClosed {
		// 通常終了では受理済みwriterの完了を待ち、内部で終了期限を設けません。
		shutdownCtx := context.Background()
		var shutdownErrs []error
		for _, writer := range writers {
			if writer.shutdownComplete || writer.handler == nil {
				continue
			}
			if shutdownErr := writer.handler.Shutdown(shutdownCtx); shutdownErr != nil {
				shutdownErrs = append(shutdownErrs, fmt.Errorf(
					"shutdown failure spool writer %T: %w",
					writer.handler,
					shutdownErr,
				))
				continue
			}
			writer.shutdownComplete = true
		}
		if joined := errors.Join(shutdownErrs...); joined != nil {
			return false, joined
		}
		s.writersClosed = true
	}

	if !s.runtimeClosed {
		s.runtimeErr = s.runtime.Close()
		if !s.runtime.ShutdownComplete() {
			return false, s.runtimeErr
		}
		s.runtimeClosed = true
	}
	if !s.leasesReleased {
		s.leases.Release()
		s.leasesReleased = true
	}
	return true, s.runtimeErr
}

func closeFailureSpoolPipelinesAfterConstructionError(
	initErr error,
	pipelines *failureSpoolPipelineSet,
) error {
	complete, closeErr := pipelines.close()
	if closeErr != nil && !complete {
		closeErr = markLoggerOwnedResourceCleanupIncomplete(closeErr)
	}
	return errors.Join(initErr, closeErr)
}
