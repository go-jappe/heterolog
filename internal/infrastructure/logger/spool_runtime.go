package logger

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-jappe/heterolog/internal/application/observer"
	configspool "github.com/go-jappe/heterolog/internal/config/spool"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	cloudwatchlogslogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	infraspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	clickhousespoolsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/clickhouse"
	cloudwatchlogsspoolsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/cloudwatchlogs"
	lokispoolsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/loki"
	opensearchspoolsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

var (
	ErrFailureSpoolRuntimeAlreadyStarted = errors.New("failure spool runtime is already started")
	ErrFailureSpoolRuntimeClosed         = errors.New("failure spool runtime is closed")
	ErrFailureSpoolRuntimeContextNil     = errors.New("failure spool runtime context is nil")
	ErrFailureSpoolRuntimeConfigRequired = errors.New("failure spool runtime config is required")
	ErrUnsupportedFailureSpoolBackend    = errors.New("unsupported failure spool backend")
	// ErrDurableSpoolNotSelectedは、spool runtimeを構築するbackendのfailure storeで
	// spoolが選択されていないことを表します。
	ErrDurableSpoolNotSelected = errors.New("durable spool is not selected")
)

// FailureStoreDurableTypesErrorは、一つのLogger契約種別とbackendのfailure storeで選択された
// durable typeが不正なことを表します。errors.AsでLogger契約種別とbackendを取り出せ、
// Errはpkg/logger/durableの検証errorです。
type FailureStoreDurableTypesError struct {
	ContractKind pkglogger.LoggerContractKind
	Backend      pkglogger.LogOutputDestination
	Err          error
}

func (e *FailureStoreDurableTypesError) Error() string {
	return fmt.Sprintf(
		"validate %s %s failure store durable types: %v",
		e.ContractKind,
		e.Backend,
		e.Err,
	)
}

func (e *FailureStoreDurableTypesError) Unwrap() error { return e.Err }

// FailureSpoolRunnerErrorは、FailureSpoolRuntimeの一つのrunnerが停止要求以外の理由で
// 終了したことを表します。同じconcrete typeのsupervisorが複数あっても、errors.Asで
// Logger契約種別とbackendを取り出して発生元を特定できます。Errは元の原因です。
type FailureSpoolRunnerError struct {
	ContractKind pkglogger.LoggerContractKind
	Backend      pkglogger.LogOutputDestination
	Err          error
}

func (e *FailureSpoolRunnerError) Error() string {
	return fmt.Sprintf(
		"%s failure spool runner for backend %q failed: %v",
		e.ContractKind.DisplayName(),
		e.Backend,
		e.Err,
	)
}

func (e *FailureSpoolRunnerError) Unwrap() error { return e.Err }

// failureSpoolRuntimeStartStateは、FailureSpoolRuntimeがStartを受理できるか、
// 受理できない場合はStart済みとClose後のどちらであるかを表します。
// 個々のrunnerの稼働状態やClose処理の完了状態は表しません。
type failureSpoolRuntimeStartState uint8

const (
	failureSpoolRuntimeStartStateReady failureSpoolRuntimeStartState = iota
	failureSpoolRuntimeStartStateStarted
	failureSpoolRuntimeStartStateClosed
)

// FailureSpoolRuntimeは、DurableTypeで選択されたfailure spoolを再送するsupervisor群を所有します。
// 通常経路ではproducer loggerと同じ生存期間で起動・終了し、recovery経路では
// 既存spoolを排出するrunnerだけの生存期間を管理します。
// 終了時は開始済みの一周期と通知の完了を待ち、保存済みspool全件の再送成功は待ちません。
type FailureSpoolRuntime struct {
	runners []failureSpoolRunnerRuntime
	// notificationsは構築時に通知先がある場合だけ用意します。runnerとは別の
	// goroutineでOnError等を呼びます。通常Closeは受理済み通知の終了も待ちます。
	notifications *notification.Dispatcher
	// failureSpoolDirectoryLeasesは復旧runtimeが直接所有する、logger契約ごとの使用権一覧です。
	// sync・asyncのSetは通常起動と同じ一契約単位で構築し、全runnerの停止後に解放します。
	// 通常のpaired loggerではfailureSpoolPipelineSetがwriter・runnerと同じ単位で
	// Leaseを所有するため、このfieldはnilです。
	failureSpoolDirectoryLeases []*failureSpoolDirectoryLeaseSet

	// lifecycleMuはStartのWaitGroup.AddとCloseのWait開始を直列化します。
	// Close開始後にsupervisorが追加されないよう、startStateとcancelも同じlockで管理します。
	lifecycleMu sync.Mutex
	// startStateはStart可能、Start済み、Close後でStart不可のどの状態かを保持します。
	// runnerが個別に終了してもStartedからReadyへは戻らず、再Startは許可しません。
	startState  failureSpoolRuntimeStartState
	cancel      context.CancelFunc
	forceCancel context.CancelFunc
	// operationContextは受理済み処理と、その保存先から発生する通知へ強制終了を伝えます。
	operationContext context.Context
	wg               sync.WaitGroup
	closeController  shutdown.Controller

	errMu sync.Mutex
	errs  []error
}

// failureSpoolRunnerRuntimeは、一つのLogger契約種別とbackendに対応する
// failure spool runnerの実行状態を保持します。contractKindとbackendは、
// 同じconcrete typeのsupervisorが複数ある場合にerrorの発生元を特定します。
type failureSpoolRunnerRuntime struct {
	contractKind pkglogger.LoggerContractKind
	backend      pkglogger.LogOutputDestination
	supervisor   *infraspool.RunnerSupervisor
	clock        clock.Clock
	// storeOnWarnは構築中に一度だけ接続する、再送後の保存先保全の通知先です。
	// Storeへ渡すcallbackは固定し、内部通知はruntimeのdispatcherへ登録します。
	storeOnWarn logging.LoggerHandlerFunc
}

// FailureSpoolRecoveryConfigsは、recovery対象となり得る既存failure spoolを
// sync/async設定から特定するための設定集合です。Delivery loggerはbackendを問わず
// spoolを使用しないため含めません。
type FailureSpoolRecoveryConfigs struct {
	Sync  pkglogger.LoggerConfig
	Async pkglogger.AsyncBestEffortMultiLoggerConfig
}

type failureSpoolPipelineSource struct {
	contractKind pkglogger.LoggerContractKind
	config       pkglogger.LoggerConfig
}

// failureSpoolPipelinePlanは、一つのLoggerContractKindとbackendの組み合わせについて、failure recordを
// 最初に保存するwriterと、そのrootをdrainするrunnerが共同所有するpipelineを表します。
// rootのLeaseもsupervisorもこの単位で一度だけ構築します。
type failureSpoolPipelinePlan struct {
	contractKind pkglogger.LoggerContractKind
	backend      pkglogger.LogOutputDestination
	loggerConfig pkglogger.LoggerConfig
	root         string
	// senderは試験がbackendへの送信だけを置き換えるための差し替え口です。nilなら
	// loggerConfigのclient設定からbackend別のsenderを構築します。保存先、worker、
	// runner、通知の接続は差し替えの有無にかかわらず同じ経路で構築します。
	sender worker.Sender
}

// NewFailureSpoolRecoveryRuntimeは、既存spoolを排出するrunnerだけを明示的に
// 構築します。通常のapplication起動では、公開NewLoggerまたはNewAsyncLoggerが
// writerとrunnerを一緒に構築します。
//
// 対象pipelineは独立したbackend選択設定を持たず、選択済みsync/async backendのうち
// DurableTypeSpoolが選択された設定から全件導出します。
// 呼び出し側はStartで排出を開始し、終了時に必ずCloseを呼び出してrunnerを停止し、
// 対象directoryのLeaseを解放します。
func NewFailureSpoolRecoveryRuntime(
	loggerConfigs FailureSpoolRecoveryConfigs,
	spoolConfig *configspool.SpoolConfig,
	tracer observer.Tracer,
	meter observer.Meter,
	options ...FailureSpoolRecoveryOption,
) (*FailureSpoolRuntime, error) {
	var recoveryOptions failureSpoolRecoveryOptions
	for _, option := range options {
		option(&recoveryOptions)
	}
	sources := failureSpoolRecoveryPipelineSources(loggerConfigs)
	plan, err := buildFailureSpoolPipelinePlan(sources)
	if err != nil {
		return nil, err
	}
	if len(plan) > 0 && spoolConfig == nil {
		return nil, ErrFailureSpoolRuntimeConfigRequired
	}
	for index := range plan {
		plan[index].sender = recoveryOptions.sender
	}
	var leaseSets []*failureSpoolDirectoryLeaseSet
	for _, source := range sources {
		var contractPlan []failureSpoolPipelinePlan
		for _, pipeline := range plan {
			if pipeline.contractKind == source.contractKind {
				contractPlan = append(contractPlan, pipeline)
			}
		}
		leases, err := acquireFailureSpoolDirectoryLeases(contractPlan)
		if err != nil {
			// 後の契約の取得失敗でも、先に取得した契約の使用権を残しません。
			releaseFailureSpoolDirectoryLeaseSets(leaseSets)
			return nil, err
		}
		if leases != nil {
			leaseSets = append(leaseSets, leases)
		}
	}
	runtime, err := buildFailureSpoolRuntime(plan, spoolConfig, tracer, meter)
	if err != nil {
		releaseFailureSpoolDirectoryLeaseSets(leaseSets)
		return nil, err
	}
	runtime.failureSpoolDirectoryLeases = leaseSets
	for index := range runtime.runners {
		runtime.configureRunnerNotifications(&runtime.runners[index], recoveryOptions.onError, recoveryOptions.onWarn)
	}
	return runtime, nil
}

func failureSpoolRecoveryPipelineSources(
	loggerConfigs FailureSpoolRecoveryConfigs,
) []failureSpoolPipelineSource {
	return []failureSpoolPipelineSource{
		{
			contractKind: pkglogger.LoggerContractSync,
			config:       loggerConfigs.Sync,
		},
		{
			contractKind: pkglogger.LoggerContractAsyncBestEffort,
			config:       loggerConfigs.Async.ToLoggerConfig(),
		},
	}
}

// buildFailureSpoolPipelinePlanは、選択済みbackendのDurableTypeSpool設定を正本として
// writerとrunnerが共有する全pipelineを列挙します。独立したrunner enable flagや
// backend allow-listは参照しないため、writerだけが構築される設定を作りません。
// filesystem resourceやgoroutineはまだ作成しません。
func buildFailureSpoolPipelinePlan(
	sources []failureSpoolPipelineSource,
) ([]failureSpoolPipelinePlan, error) {
	plan := make([]failureSpoolPipelinePlan, 0)
	for _, source := range sources {
		for _, backend := range pkglogger.AllLogOutputDestinations() {
			if !logOutputEnabled(source.config.LogOutputDestinations, backend) {
				continue
			}
			failureConfig, ok := spoolFailureStoreConfig(backend, source.config)
			if !ok {
				continue
			}
			if err := pkgdurable.ValidateFailureStoreDurableTypes(failureConfig.DurableType); err != nil {
				return nil, &FailureStoreDurableTypesError{
					ContractKind: source.contractKind,
					Backend:      backend,
					Err:          err,
				}
			}
			if !hasSelectedSpool(failureConfig) {
				continue
			}

			spoolConfig := loggerdurable.NewSpoolConfigAdapter(failureConfig.DurableStoreConfig.SpoolConfig)
			root, err := canonicalSpoolRoot(spoolConfig)
			if err != nil {
				return nil, fmt.Errorf("resolve %s %s spool root: %w", source.contractKind, backend, err)
			}
			plan = append(plan, failureSpoolPipelinePlan{
				contractKind: source.contractKind,
				backend:      backend,
				loggerConfig: source.config,
				root:         root,
			})
		}
	}
	return plan, nil
}

func buildFailureSpoolRuntime(
	plan []failureSpoolPipelinePlan,
	spoolConfig *configspool.SpoolConfig,
	tracer observer.Tracer,
	meter observer.Meter,
) (*FailureSpoolRuntime, error) {
	runtime := &FailureSpoolRuntime{runners: make([]failureSpoolRunnerRuntime, 0, len(plan))}
	for _, planned := range plan {
		timeSource := clock.NewUTCClock()
		// runner一覧の容量は計画件数で確定済みなので、この要素への参照は構築後も
		// 変わりません。保存先の通知callbackをsupervisor構築時に接続します。
		runtime.runners = append(runtime.runners, failureSpoolRunnerRuntime{
			contractKind: planned.contractKind,
			backend:      planned.backend,
			clock:        timeSource,
		})
		runner := &runtime.runners[len(runtime.runners)-1]
		supervisor, err := newBackendSpoolSupervisor(
			planned.backend,
			planned.loggerConfig,
			spoolConfig,
			timeSource,
			tracer,
			meter,
			func(ctx context.Context, entry *logging.LogEntry, tracer observer.Tracer, meter observer.Meter) {
				if runner.storeOnWarn != nil {
					runner.storeOnWarn(ctx, entry, tracer, meter)
				}
			},
			planned.sender,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"build %s %s spool runtime: %w",
				planned.contractKind,
				planned.backend,
				err,
			)
		}
		runner.supervisor = supervisor
	}
	return runtime, nil
}

// validateFailureSpoolRuntimeConstructionContextは、runnerを必ず起動できる状態でpaired
// loggerを構築するため、nilまたはすでに終了したcontextを拒否します。起動後のrunnerは
// callerのcancelから切り離し、paired loggerのCloseだけで停止します。
func validateFailureSpoolRuntimeConstructionContext(ctx context.Context) error {
	if ctx == nil {
		return ErrFailureSpoolRuntimeContextNil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("failure spool runtime construction context is done: %w", err)
	}
	return nil
}

func (r *FailureSpoolRuntime) Start(ctx context.Context) error {
	if r == nil || len(r.runners) == 0 {
		return nil
	}
	if ctx == nil {
		return ErrFailureSpoolRuntimeContextNil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.lifecycleMu.Lock()
	defer r.lifecycleMu.Unlock()
	if r.startState == failureSpoolRuntimeStartStateClosed {
		return ErrFailureSpoolRuntimeClosed
	}
	if r.startState == failureSpoolRuntimeStartStateStarted {
		return ErrFailureSpoolRuntimeAlreadyStarted
	}

	runCtx, cancel := context.WithCancel(ctx)
	operationCtx, forceCancel := context.WithCancel(context.WithoutCancel(ctx))
	r.cancel = cancel
	r.forceCancel = forceCancel
	r.operationContext = operationCtx
	r.startState = failureSpoolRuntimeStartStateStarted
	r.wg.Add(len(r.runners))

	for _, runner := range r.runners {
		runner := runner
		go func() {
			defer r.wg.Done()
			if err := runner.supervisor.RunUntilStopped(runCtx, operationCtx); err != nil &&
				!isFailureSpoolRuntimeContextError(err) {
				r.errMu.Lock()
				r.errs = append(r.errs, &FailureSpoolRunnerError{
					ContractKind: runner.contractKind,
					Backend:      runner.backend,
					Err:          err,
				})
				r.errMu.Unlock()
			}
		}()
	}

	return nil
}

func (r *FailureSpoolRuntime) Close() error {
	if r == nil {
		return nil
	}

	r.lifecycleMu.Lock()
	r.startState = failureSpoolRuntimeStartStateClosed
	cancel := r.cancel
	r.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	return r.closeController.Close("failure spool runtime", func() (bool, error) {
		r.wg.Wait()
		r.notifications.CloseAfterDrain()
		if r.notifications != nil {
			<-r.notifications.Done()
		}
		releaseFailureSpoolDirectoryLeaseSets(r.failureSpoolDirectoryLeases)
		r.errMu.Lock()
		defer r.errMu.Unlock()
		return true, errors.Join(r.errs...)
	})
}

// RequestCloseは終了を開始して戻ります。通知先から同じruntimeを閉じる場合に使います。
// Closeはこの通知自身も待つため、通知中に完了まで待ってはいけません。
func (r *FailureSpoolRuntime) RequestClose() {
	if r != nil {
		r.lifecycleMu.Lock()
		r.startState = failureSpoolRuntimeStartStateClosed
		cancel := r.cancel
		r.lifecycleMu.Unlock()
		if cancel != nil {
			cancel()
		}
		go r.Close()
	}
}

// RequestForceCloseは周期の受付と受理済み処理へ中止を通知し、終了待ちを解除します。
// 実行中の処理が戻るまでは保存先の使用権を解放しません。
func (r *FailureSpoolRuntime) RequestForceClose() error {
	if r == nil {
		return nil
	}
	r.lifecycleMu.Lock()
	r.startState = failureSpoolRuntimeStartStateClosed
	cancel, forceCancel := r.cancel, r.forceCancel
	r.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if forceCancel != nil {
		forceCancel()
	}
	return r.closeController.ForceClose(nil)
}

func (r *FailureSpoolRuntime) ShutdownDone() <-chan struct{} { return r.closeController.Done() }
func (r *FailureSpoolRuntime) ShutdownComplete() bool {
	return r == nil || r.closeController.IsComplete()
}

// newBackendSpoolSupervisorはbackend別のsenderで再送runnerを構築します。senderがnilなら
// loggerConfigのclient設定からsenderを構築し、non-nilならそのsenderだけを使います。
func newBackendSpoolSupervisor(
	backend pkglogger.LogOutputDestination,
	loggerConfig pkglogger.LoggerConfig,
	spoolConfig *configspool.SpoolConfig,
	timeSource clock.Clock,
	tracer observer.Tracer,
	meter observer.Meter,
	onWarn logging.LoggerHandlerFunc,
	sender worker.Sender,
) (*infraspool.RunnerSupervisor, error) {
	backendLabel := backend.String()

	switch backend {
	case pkglogger.LogOutputOpenSearch:
		if loggerConfig.OpenSearch == nil || loggerConfig.OpenSearchClient == nil {
			return nil, errors.New("opensearch logger/client config is nil")
		}
		failureConfig := loggerConfig.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig
		if err := validateSelectedSpoolConfig(backendLabel, failureConfig); err != nil {
			return nil, err
		}
		if sender == nil {
			client, err := infraopensearch.NewClient(
				infraopensearch.NewOpenSearchClientConfigAdapter(loggerConfig.OpenSearchClient),
				opensearchlogger.NewRetryConfigProvider(loggerConfig.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig),
				tracer,
				meter,
			)
			if err != nil {
				return nil, err
			}
			sender, err = opensearchspoolsender.NewOpensearchSpoolSender(
				timeSource,
				client,
				loggerConfig.OpenSearch.MaxBulkRequestBytes,
			)
			if err != nil {
				return nil, err
			}
		}
		return infraspool.NewBackendRunnerSupervisor(timeSource, infraspool.BackendRuntimeOptions{
			Backend:          backendLabel,
			SpoolConfig:      failureConfig.DurableStoreConfig.SpoolConfig,
			RunnerConfig:     spoolConfig.RunnerConfig.OpensearchConfig,
			SupervisorConfig: spoolConfig.SupervisorConfig,
			Sender:           sender,
			OnWarn:           onWarn,
		}, tracer, meter)

	case pkglogger.LogOutputLoki:
		if loggerConfig.Loki == nil || loggerConfig.LokiClient == nil {
			return nil, errors.New("loki logger/client config is nil")
		}
		failureConfig := loggerConfig.Loki.LokiFlushManagerConfig.FailureStoreConfig
		if err := validateSelectedSpoolConfig(backendLabel, failureConfig); err != nil {
			return nil, err
		}
		if sender == nil {
			client, err := infraloki.NewClient(
				*loggerConfig.LokiClient,
				lokilogger.NewRetryConfigProvider(loggerConfig.Loki.LokiRetryableClientCaseTransportErrorConfig),
				tracer,
				meter,
			)
			if err != nil {
				return nil, err
			}
			sender, err = lokispoolsender.NewLokiSpoolSender(
				timeSource,
				client,
				infraloki.PushLimits{
					MaxLineBytes:        loggerConfig.Loki.MaxLineBytes,
					MaxPushRequestBytes: loggerConfig.Loki.MaxPushRequestBytes,
				},
			)
			if err != nil {
				return nil, err
			}
		}
		return infraspool.NewBackendRunnerSupervisor(timeSource, infraspool.BackendRuntimeOptions{
			Backend:          backendLabel,
			SpoolConfig:      failureConfig.DurableStoreConfig.SpoolConfig,
			RunnerConfig:     spoolConfig.RunnerConfig.LokiConfig,
			SupervisorConfig: spoolConfig.SupervisorConfig,
			Sender:           sender,
			OnWarn:           onWarn,
		}, tracer, meter)

	case pkglogger.LogOutputCloudWatchLogs:
		if loggerConfig.CloudWatchLogs == nil || loggerConfig.CloudWatchLogsClient == nil {
			return nil, errors.New("cloudwatch logs logger/client config is nil")
		}
		failureConfig := loggerConfig.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.FailureStoreConfig
		if err := validateSelectedSpoolConfig(backendLabel, failureConfig); err != nil {
			return nil, err
		}
		if sender == nil {
			client, err := infracloudwatchlogs.NewClient(
				*loggerConfig.CloudWatchLogsClient,
				cloudwatchlogslogger.NewRetryConfigProvider(loggerConfig.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig),
				tracer,
				meter,
			)
			if err != nil {
				return nil, err
			}
			sender, err = cloudwatchlogsspoolsender.NewCloudWatchLogsSpoolSender(
				timeSource,
				client,
			)
			if err != nil {
				return nil, err
			}
		}
		return infraspool.NewBackendRunnerSupervisor(timeSource, infraspool.BackendRuntimeOptions{
			Backend:          backendLabel,
			SpoolConfig:      failureConfig.DurableStoreConfig.SpoolConfig,
			RunnerConfig:     spoolConfig.RunnerConfig.CloudWatchLogsConfig,
			SupervisorConfig: spoolConfig.SupervisorConfig,
			Sender:           sender,
			OnWarn:           onWarn,
		}, tracer, meter)

	case pkglogger.LogOutputClickHouse:
		if loggerConfig.ClickHouse == nil || loggerConfig.ClickHouseClient == nil {
			return nil, errors.New("clickhouse logger/client config is nil")
		}
		failureConfig := loggerConfig.ClickHouse.ClickHouseFlushManagerConfig.FailureStoreConfig
		if err := validateSelectedSpoolConfig(backendLabel, failureConfig); err != nil {
			return nil, err
		}
		if sender == nil {
			client, err := infraclickhouse.NewClient(
				*loggerConfig.ClickHouseClient,
				clickhouselogger.NewRetryConfigProvider(loggerConfig.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig),
				tracer,
				meter,
			)
			if err != nil {
				return nil, err
			}
			sender, err = clickhousespoolsender.NewClickHouseSpoolSender(
				timeSource,
				client,
				loggerConfig.ClickHouse.MaxInsertRequestBytes,
			)
			if err != nil {
				return nil, err
			}
		}
		return infraspool.NewBackendRunnerSupervisor(timeSource, infraspool.BackendRuntimeOptions{
			Backend:          backendLabel,
			SpoolConfig:      failureConfig.DurableStoreConfig.SpoolConfig,
			RunnerConfig:     spoolConfig.RunnerConfig.ClickHouseConfig,
			SupervisorConfig: spoolConfig.SupervisorConfig,
			Sender:           sender,
			OnWarn:           onWarn,
		}, tracer, meter)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedFailureSpoolBackend, backend)
	}
}

func spoolFailureStoreConfig(
	backend pkglogger.LogOutputDestination,
	config pkglogger.LoggerConfig,
) (pkgdurable.FailureStoreConfig, bool) {
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		if config.OpenSearch == nil {
			return pkgdurable.FailureStoreConfig{}, false
		}
		return config.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig, true
	case pkglogger.LogOutputLoki:
		if config.Loki == nil {
			return pkgdurable.FailureStoreConfig{}, false
		}
		return config.Loki.LokiFlushManagerConfig.FailureStoreConfig, true
	case pkglogger.LogOutputCloudWatchLogs:
		if config.CloudWatchLogs == nil {
			return pkgdurable.FailureStoreConfig{}, false
		}
		return config.CloudWatchLogs.CloudWatchLogsFlushManagerConfig.FailureStoreConfig, true
	case pkglogger.LogOutputClickHouse:
		if config.ClickHouse == nil {
			return pkgdurable.FailureStoreConfig{}, false
		}
		return config.ClickHouse.ClickHouseFlushManagerConfig.FailureStoreConfig, true
	default:
		return pkgdurable.FailureStoreConfig{}, false
	}
}

// hasSelectedSpoolはDurableTypeだけを正本としてspoolの選択有無を返します。
func hasSelectedSpool(config pkgdurable.FailureStoreConfig) bool {
	for _, durableType := range config.DurableType {
		if durableType == pkgdurable.DurableTypeSpool {
			return true
		}
	}
	return false
}

func validateSelectedSpoolConfig(
	backend string,
	config pkgdurable.FailureStoreConfig,
) error {
	if err := pkgdurable.ValidateFailureStoreDurableTypes(config.DurableType); err != nil {
		return fmt.Errorf("invalid failure store durable types: backend=%s: %w", backend, err)
	}
	if hasSelectedSpool(config) {
		return nil
	}
	return fmt.Errorf("%w: backend=%s", ErrDurableSpoolNotSelected, backend)
}

func isFailureSpoolRuntimeContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
