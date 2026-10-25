package logger

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var (
	errAppendLogMaintenanceRuntimeAlreadyStarted = errors.New("appendlog maintenance runner runtime is already started")
	errAppendLogMaintenanceRuntimeClosed         = errors.New("appendlog maintenance runner runtime is closed")
	errAppendLogMaintenanceRuntimeContextNil     = errors.New("appendlog maintenance runner runtime context is nil")
	// ErrLifecycleDirectoryOverlapは、appendlog maintenance runnerを持つ二つのownerの
	// directoryが同じか親子関係にあることを表します。
	ErrLifecycleDirectoryOverlap = errors.New("appendlog maintenance runner directories overlap")
)

// LifecycleDirectoryOverlapErrorは、Ownerが登録済みのdirectoryとCandidateのdirectoryが
// 重なることを表します。OwnerとCandidateは"base file"などのsourceで、errors.Isでは
// ErrLifecycleDirectoryOverlapと一致します。
type LifecycleDirectoryOverlapError struct {
	Owner     string
	Candidate string
}

func (e *LifecycleDirectoryOverlapError) Error() string {
	return fmt.Sprintf("%v: %s and %s", ErrLifecycleDirectoryOverlap, e.Owner, e.Candidate)
}

func (e *LifecycleDirectoryOverlapError) Is(target error) bool {
	return target == ErrLifecycleDirectoryOverlap
}

// LifecycleDirectoryErrorは、Sourceのappendlog maintenance対象directoryを解決できないことを
// 表します。Directoryは解決しようとしたpath、Errは原因です。
type LifecycleDirectoryError struct {
	Source    string
	Directory string
	Err       error
}

func (e *LifecycleDirectoryError) Error() string {
	return fmt.Sprintf("%s lifecycle directory: %v", e.Source, e.Err)
}

func (e *LifecycleDirectoryError) Unwrap() error { return e.Err }

// AppendLogMaintenanceRunnerErrorは、一つのappendlog maintenance runnerの操作が失敗した
// ことを表します。Ownerは"base file:/var/log/app"のようにsourceと物理directoryを結んだ
// 名前で、同じconcrete typeのrunnerが複数あってもerrors.Asで発生元を特定できます。
// Opは"failed"や"reconcile object storage upload destinations"などの操作、Errは原因です。
type AppendLogMaintenanceRunnerError struct {
	Owner string
	Op    string
	Err   error
}

func (e *AppendLogMaintenanceRunnerError) Error() string {
	return fmt.Sprintf("appendlog maintenance runner %q %s: %v", e.Owner, e.Op, e.Err)
}

func (e *AppendLogMaintenanceRunnerError) Unwrap() error { return e.Err }

// appendLogMaintenanceRuntimeは、一つのLoggerContractKindに属し、appendlog fileの圧縮、
// object storage upload、purge、checkpoint管理を周期実行するrunner群の開始・停止・
// error回収を所有します。loggerを閉じる前に停止することで、runnerの結果reporterが
// close済みfallback loggerへ書き込む競合を防ぎます。ログの追記とrotationは扱いません。
type appendLogMaintenanceRuntime struct {
	entries []appendLogMaintenanceRuntimeEntry

	mu      sync.Mutex
	started bool
	closed  bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	errMu sync.Mutex
	errs  []error
}

// appendLogMaintenanceRuntimeEntryは、同じconcrete typeを持つrunnerと、
// error発生時に物理directoryまで特定できる論理ownerを結び付けます。
type appendLogMaintenanceRuntimeEntry struct {
	owner  string
	runner *logfilerunner.Runner
}

// logFileMaintenanceRunnerSpecは、一つのfile ownerに属するrunner設定、日付判定にも使う
// Clock、owner固有のinternal log Reporterを結び付けます。
type logFileMaintenanceRunnerSpec struct {
	source     string
	config     *pkgfsstore.LogFileLifecycleRunnerConfig
	timeSource clock.Clock
	reporter   logfilelifecycle.RunReporter
}

// deliveryFileMaintenanceRunnerSpecはDelivery file用の同じ結合単位です。
type deliveryFileMaintenanceRunnerSpec struct {
	source     string
	config     *pkgfsstore.DeliveryFileLifecycleRunnerConfig
	timeSource clock.Clock
	reporter   logfilelifecycle.RunReporter
}

func newLogFileMaintenanceRuntime(
	specs []logFileMaintenanceRunnerSpec,
	tracer observer.Tracer,
	meter observer.Meter,
	spoolOwners ...*compressedlog.RootRuntimeBuilder,
) (_ *appendLogMaintenanceRuntime, resultErr error) {
	if len(specs) == 0 {
		return nil, nil
	}
	var entries []appendLogMaintenanceRuntimeEntry
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, (&appendLogMaintenanceRuntime{entries: entries}).Close())
		}
	}()
	for _, spec := range specs {
		if spec.config == nil {
			continue
		}
		upload := activeCompressedLogSpoolUpload(spec.config.ObjectStorageUploadConfig, spec.config.FsstoreConfig.Layout)
		spoolRuntimes, err := buildCompressedLogSpoolRuntimes(upload, spoolOwners...)
		if err != nil {
			return nil, fmt.Errorf("%s compressed log spool: %w", spec.source, err)
		}
		configRunners, err := logfilerunner.NewLogFileRunners(
			logfilelifecycle.LogFileOptions{
				Config:        *spec.config,
				Clock:         spec.timeSource,
				Tracer:        tracer,
				Meter:         meter,
				Reporter:      spec.reporter,
				SpoolRuntimes: spoolRuntimes,
			},
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("%s lifecycle runner: %w", spec.source, err)
		}
		configEntries, err := newAppendLogMaintenanceRuntimeEntries(
			spec.source,
			configRunners,
		)
		if err != nil {
			for _, runner := range configRunners {
				err = errors.Join(err, runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes())
			}
			return nil, err
		}
		entries = append(entries, configEntries...)
	}
	return &appendLogMaintenanceRuntime{entries: entries}, nil
}

func newDeliveryFileMaintenanceRuntime(
	specs []deliveryFileMaintenanceRunnerSpec,
	tracer observer.Tracer,
	meter observer.Meter,
	spoolOwners ...*compressedlog.RootRuntimeBuilder,
) (_ *appendLogMaintenanceRuntime, resultErr error) {
	if len(specs) == 0 {
		return nil, nil
	}
	var entries []appendLogMaintenanceRuntimeEntry
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, (&appendLogMaintenanceRuntime{entries: entries}).Close())
		}
	}()
	for _, spec := range specs {
		if spec.config == nil {
			continue
		}
		upload := activeCompressedLogSpoolUpload(spec.config.ObjectStorageUploadConfig, spec.config.FsstoreConfig.Layout)
		spoolRuntimes, err := buildCompressedLogSpoolRuntimes(upload, spoolOwners...)
		if err != nil {
			return nil, fmt.Errorf("%s compressed log spool: %w", spec.source, err)
		}
		configRunners, err := logfilerunner.NewDeliveryFileRunners(
			logfilelifecycle.DeliveryFileOptions{
				Config:        *spec.config,
				Clock:         spec.timeSource,
				Tracer:        tracer,
				Meter:         meter,
				Reporter:      spec.reporter,
				SpoolRuntimes: spoolRuntimes,
			},
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("%s lifecycle runner: %w", spec.source, err)
		}
		configEntries, err := newAppendLogMaintenanceRuntimeEntries(
			spec.source,
			configRunners,
		)
		if err != nil {
			for _, runner := range configRunners {
				err = errors.Join(err, runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes())
			}
			return nil, err
		}
		entries = append(entries, configEntries...)
	}
	return &appendLogMaintenanceRuntime{entries: entries}, nil
}

// newAppendLogMaintenanceRuntimeEntriesは生成済みrunner自身が管理するdirectoryから
// ownerを作ります。factoryのgroupingや生成順を複製しないため、file roleの構成が
// 変わっても実際の実行単位とidentityがずれません。
func newAppendLogMaintenanceRuntimeEntries(
	source string,
	runners []*logfilerunner.Runner,
) ([]appendLogMaintenanceRuntimeEntry, error) {
	entries := make([]appendLogMaintenanceRuntimeEntry, len(runners))
	for index, runner := range runners {
		directory, err := runner.DirectoryPath()
		if err != nil {
			return nil, fmt.Errorf(
				"%s lifecycle runner[%d] directory identity: %w",
				source,
				index,
				err,
			)
		}
		entries[index] = appendLogMaintenanceRuntimeEntry{
			owner:  source + ":" + directory,
			runner: runner,
		}
	}
	return entries, nil
}

// buildLogFileMaintenanceRuntimeはdirectory所有権の検証、sourceごとのrouting解決、
// runner構築を一つの境界で行います。Reporterが使うhandler候補は有効なroutingを持つ
// sourceごとに新しく構築し、stateful fallback handlerとそのCloserを別sourceと共有しません。
func buildLogFileMaintenanceRuntime(
	ctx context.Context,
	base *pkglogger.FileLoggerConfig,
	internalLogFile *pkglogger.FileLoggerConfig,
	newAvailableHandlers func() handlerStruct,
	fallbackCloserOwner *fallbackHandlerCloserOwner,
	tracer observer.Tracer,
	meter observer.Meter,
	spoolOwners ...*compressedlog.RootRuntimeBuilder,
) (*appendLogMaintenanceRuntime, error) {
	specs, err := logFileMaintenanceRunnerSpecs(base, internalLogFile)
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, nil
	}

	for index := range specs {
		routing := specs[index].config.HandlerConfig
		available := handlerStruct{}
		if handlerRoutingConfigured(routing) && newAvailableHandlers != nil {
			available = newAvailableHandlers()
			fallbackCloserOwner.Register(specs[index].source, available)
		}
		if err := validateAppendLogMaintenanceResultNotifierSelectors(routing, available); err != nil {
			return nil, fmt.Errorf(
				"%s lifecycle handler config is invalid: %w",
				specs[index].source,
				err,
			)
		}
		specs[index].reporter = newAppendLogMaintenanceResultNotifier(
			routing,
			available,
			specs[index].timeSource,
			tracer,
			meter,
		)
	}
	return newLogFileMaintenanceRuntime(specs, tracer, meter, spoolOwners...)
}

// buildDeliveryFileMaintenanceRuntimeはDelivery fileについて、通常版と同じくsourceごとに
// 独立したReporter用handler候補とCloserを組み立ててrunnerへ渡します。
func buildDeliveryFileMaintenanceRuntime(
	ctx context.Context,
	base *pkglogger.DeliveryFileLoggerConfig,
	internalLogFile *pkglogger.DeliveryFileLoggerConfig,
	newAvailableHandlers func() handlerStruct,
	fallbackCloserOwner *fallbackHandlerCloserOwner,
	tracer observer.Tracer,
	meter observer.Meter,
	spoolOwners ...*compressedlog.RootRuntimeBuilder,
) (*appendLogMaintenanceRuntime, error) {
	specs, err := deliveryFileMaintenanceRunnerSpecs(base, internalLogFile)
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, nil
	}

	for index := range specs {
		routing := specs[index].config.HandlerConfig
		available := handlerStruct{}
		if handlerRoutingConfigured(routing) && newAvailableHandlers != nil {
			available = newAvailableHandlers()
			fallbackCloserOwner.Register(specs[index].source, available)
		}
		if err := validateAppendLogMaintenanceResultNotifierSelectors(routing, available); err != nil {
			return nil, fmt.Errorf(
				"%s lifecycle handler config is invalid: %w",
				specs[index].source,
				err,
			)
		}
		specs[index].reporter = newAppendLogMaintenanceResultNotifier(
			routing,
			available,
			specs[index].timeSource,
			tracer,
			meter,
		)
	}
	return newDeliveryFileMaintenanceRuntime(specs, tracer, meter, spoolOwners...)
}

func handlerRoutingConfigured(cfg pkgloggerhandler.HandlerConfig) bool {
	for _, selectors := range [][]string{
		cfg.OnErrorConfig,
		cfg.OnWarnConfig,
		cfg.OnInfoConfig,
		cfg.OnDebugConfig,
	} {
		for _, selector := range selectors {
			if strings.TrimSpace(selector) != "" {
				return true
			}
		}
	}
	return false
}

type lifecycleDirectoryOwner struct {
	source string
	paths  map[string]struct{}
}

// logFileMaintenanceRunnerSpecsはbase fileと、明示的に別directoryへ分けた
// internal-log fileの所有権を検証し、runner modeのrunner単位だけを返します。
// internal-log fileがnilの場合はbase FileLoggerSetを共有するため追加runnerを作りません。
func logFileMaintenanceRunnerSpecs(
	base *pkglogger.FileLoggerConfig,
	internalLogFile *pkglogger.FileLoggerConfig,
) ([]logFileMaintenanceRunnerSpec, error) {
	var (
		specs  []logFileMaintenanceRunnerSpec
		owners []lifecycleDirectoryOwner
	)
	if base != nil {
		owner, err := newLifecycleDirectoryOwner(
			"base file",
			base.FsstoreConfig.Layout,
		)
		if err != nil {
			return nil, err
		}
		owners = append(owners, owner)
		if cfg := base.LifecycleRunnerConfig(); cfg != nil {
			specs = append(specs, logFileMaintenanceRunnerSpec{
				source:     "base file",
				config:     cfg,
				timeSource: clock.NewUTCClock(),
			})
		}
	}
	if internalLogFile == nil {
		return specs, nil
	}
	const source = "internal log file"
	owner, err := newLifecycleDirectoryOwner(source, internalLogFile.FsstoreConfig.Layout)
	if err != nil {
		return nil, err
	}
	if err := registerLifecycleDirectoryOwner(&owners, owner); err != nil {
		return nil, err
	}
	if cfg := internalLogFile.LifecycleRunnerConfig(); cfg != nil {
		specs = append(specs, logFileMaintenanceRunnerSpec{
			source:     source,
			config:     cfg,
			timeSource: clock.NewUTCClock(),
		})
	}
	return specs, nil
}

// deliveryFileMaintenanceRunnerSpecsはdelivery writer用base設定と、専用
// internal-log directoryを同じ所有権規則で検証・収集します。
func deliveryFileMaintenanceRunnerSpecs(
	base *pkglogger.DeliveryFileLoggerConfig,
	internalLogFile *pkglogger.DeliveryFileLoggerConfig,
) ([]deliveryFileMaintenanceRunnerSpec, error) {
	var (
		specs  []deliveryFileMaintenanceRunnerSpec
		owners []lifecycleDirectoryOwner
	)
	if base != nil {
		owner, err := newLifecycleDirectoryOwner(
			"base delivery file",
			base.FsstoreConfig.Layout,
		)
		if err != nil {
			return nil, err
		}
		owners = append(owners, owner)
		if cfg := base.LifecycleRunnerConfig(); cfg != nil {
			specs = append(specs, deliveryFileMaintenanceRunnerSpec{
				source:     "base delivery file",
				config:     cfg,
				timeSource: clock.NewUTCClock(),
			})
		}
	}
	if internalLogFile == nil {
		return specs, nil
	}
	const source = "delivery internal log file"
	owner, err := newLifecycleDirectoryOwner(source, internalLogFile.FsstoreConfig.Layout)
	if err != nil {
		return nil, err
	}
	if err := registerLifecycleDirectoryOwner(&owners, owner); err != nil {
		return nil, err
	}
	if cfg := internalLogFile.LifecycleRunnerConfig(); cfg != nil {
		specs = append(specs, deliveryFileMaintenanceRunnerSpec{
			source:     source,
			config:     cfg,
			timeSource: clock.NewUTCClock(),
		})
	}
	return specs, nil
}

// registerLifecycleDirectoryOwnerは、candidateが所有するdirectoryを登録します。
// 別ownerが同じdirectoryまたはその親子directoryをすでに所有している場合は、
// 複数writerとrunnerが同じ領域を扱う構成になるためエラーにします。
func registerLifecycleDirectoryOwner(
	owners *[]lifecycleDirectoryOwner,
	candidate lifecycleDirectoryOwner,
) error {
	for _, owner := range *owners {
		if lifecycleDirectoryPathsOverlap(owner.paths, candidate.paths) {
			return &LifecycleDirectoryOverlapError{
				Owner:     owner.source,
				Candidate: candidate.source,
			}
		}
	}
	*owners = append(*owners, candidate)
	return nil
}

func newLifecycleDirectoryOwner(
	source string,
	layout pkgfsstore.FsstoreConfig,
) (lifecycleDirectoryOwner, error) {
	// error/debugの有効な保存先も所有します。同一directoryを共有するroleは
	// 初期化時に実体pathへ解決してmapでまとめ、他のownerとの衝突検証へ含めます。
	directories := []string{layout.LogDir}
	if layout.IsSeparateErrorLogsEnabled {
		directories = append(directories, layout.ErrorLogDir)
	}
	if layout.IsSeparateDebugLogsEnabled {
		directories = append(directories, appendlog.NewFsstoreConfigAdapter(layout).DebugLogDir())
	}
	paths := make(map[string]struct{}, len(directories))
	for _, directory := range directories {
		path := filepath.Join(layout.LogPath, layout.Target, directory)
		resolved, err := pathidentity.ResolveDirectory(path)
		if err != nil {
			return lifecycleDirectoryOwner{}, &LifecycleDirectoryError{
				Source:    source,
				Directory: path,
				Err:       err,
			}
		}
		paths[pathidentity.ComparisonKey(resolved)] = struct{}{}
	}
	return lifecycleDirectoryOwner{
		source: source,
		paths:  paths,
	}, nil
}

func lifecycleDirectoryPathsOverlap(left, right map[string]struct{}) bool {
	for leftPath := range left {
		for rightPath := range right {
			if lifecyclePathsOverlap(leftPath, rightPath) {
				return true
			}
		}
	}
	return false
}

func lifecyclePathsOverlap(left, right string) bool {
	if left == right {
		return true
	}
	return lifecyclePathWithin(left, right) || lifecyclePathWithin(right, left)
}

func lifecyclePathWithin(parent, candidate string) bool {
	relative, err := filepath.Rel(parent, candidate)
	if err != nil || relative == "." || relative == ".." {
		return false
	}
	return !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// Startは起動前のcheckpoint整合を完了した後、各directoryのRunner.Runを一つずつ
// goroutineで開始します。Runner factoryでWithSupervisorが付与されている場合、
// Runner.Runの内側でSupervisor経路が選択されるため、runtime側は起動方法を分岐しません。
func (r *appendLogMaintenanceRuntime) Start(ctx context.Context) error {
	if r == nil || len(r.entries) == 0 {
		return nil
	}
	if ctx == nil {
		return errAppendLogMaintenanceRuntimeContextNil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errAppendLogMaintenanceRuntimeClosed
	}
	if r.started {
		return errAppendLogMaintenanceRuntimeAlreadyStarted
	}
	if err := r.reconcileObjectStorageUploadDestinations(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.started = true
	r.wg.Add(len(r.entries))
	for _, entry := range r.entries {
		entry := entry
		go func() {
			defer r.wg.Done()
			if err := entry.runner.Run(runCtx); err != nil &&
				!isAppendLogMaintenanceContextError(err) {
				r.errMu.Lock()
				r.errs = append(r.errs, &AppendLogMaintenanceRunnerError{
					Owner: entry.owner,
					Op:    "failed",
					Err:   err,
				})
				r.errMu.Unlock()
			}
		}()
	}
	return nil
}

// reconcileObjectStorageUploadDestinationsはloggerを呼出側へ公開する前に、全runnerの
// 現在のfile一覧とupload宛先を確認し、不要になったcheckpoint記録を片付けます。
// 周期処理そのものは起動せず、errorだけを各runnerのReporterへ通知します。
func (r *appendLogMaintenanceRuntime) reconcileObjectStorageUploadDestinations(ctx context.Context) error {
	if r == nil || len(r.entries) == 0 {
		return nil
	}
	if ctx == nil {
		return errAppendLogMaintenanceRuntimeContextNil
	}

	var errs []error
	for _, entry := range r.entries {
		report, err := entry.runner.ReconcileObjectStorageUploadDestinations(ctx)
		if entry.runner != nil && entry.runner.Reporter != nil && err != nil {
			entry.runner.Reporter.ReportRun(ctx, report, err)
		}
		if err != nil {
			errs = append(errs, &AppendLogMaintenanceRunnerError{
				Owner: entry.owner,
				Op:    "reconcile object storage upload destinations",
				Err:   err,
			})
		}
	}
	return errors.Join(errs...)
}

func (r *appendLogMaintenanceRuntime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	// 周期の新規開始だけを止め、開始済みの周期が復旧処理を登録し終えるまで待ちます。
	// 復旧と保存が完了するまでは、その処理が使うuploaderとStoreを維持します。
	shutdown.WaitGroup("accepted log file maintenance cycles", &r.wg)
	var closeErrs []error
	for _, entry := range r.entries {
		if entry.runner == nil || entry.runner.DirectoryLifecycle == nil {
			continue
		}
		if err := entry.runner.DirectoryLifecycle.CloseLogFileProcessingRecovery(); err != nil {
			closeErrs = append(closeErrs, &AppendLogMaintenanceRunnerError{
				Owner: entry.owner,
				Op:    "close log file processing recovery",
				Err:   err,
			})
		}
	}
	// appendlogの周期処理が完全に停止してから、factoryが登録したobject-storage
	// compressed log file routeを解放します。Supervisorによる一時的なrunner再起動ではこの
	// runtime自体を閉じないため、再起動中に共有compressed log file workerを失いません。
	for _, entry := range r.entries {
		if entry.runner == nil {
			continue
		}
		if err := entry.runner.CloseObjectStorageCompressedLogFileUploadFailureRoutes(); err != nil {
			closeErrs = append(closeErrs, &AppendLogMaintenanceRunnerError{
				Owner: entry.owner,
				Op:    "close object storage compressed log file routes",
				Err:   err,
			})
		}
	}
	for _, entry := range r.entries {
		if entry.runner == nil || entry.runner.DirectoryLifecycle == nil {
			continue
		}
		// 内部・外部の通知先とも、復旧・保存・退避先の最後の通知まで届けます。
		// 通知内から終了する場合は別goroutineで終了を要求し、通知自身を待ちません。
		shutdown.Wait("log file maintenance notifications: "+entry.owner, entry.runner.DirectoryLifecycle.NotificationsDone())
	}

	r.errMu.Lock()
	runErrs := append([]error(nil), r.errs...)
	r.errMu.Unlock()
	return errors.Join(append(runErrs, closeErrs...)...)
}

func isAppendLogMaintenanceContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
