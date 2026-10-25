package compressedlog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var (
	ErrInvalidCompressedLogFileUploadSpoolConfig = errors.New("compressed log file upload failure spool config is invalid")
	ErrSpoolRootAlreadyOwned                     = errors.New("object storage compressed log file spool root already has an owner")
	ErrRootRuntimeClosed                         = errors.New("compressed log file spool root runtime is closed")
	ErrNilDestinationUploader                    = errors.New("object storage compressed log file destination uploader is nil")
	ErrInvalidRouteDestination                   = compressedlogworker.ErrInvalidRouteDestination
	ErrMaintenanceModeMismatch                   = errors.New("compressed log spool routes have incompatible maintenance modes")
	// ErrInitializeSpoolは、route登録時にspoolのdirectory構成を準備できなかったことを表します。
	ErrInitializeSpool = errors.New("initialize compressed log file upload failure spool")
	// ErrRemoveAbandonedStagingFilesは、宛先の初回登録時に中断した保存の一時fileを削除できなかったことを表します。
	ErrRemoveAbandonedStagingFiles = errors.New("remove abandoned object storage compressed log staging files")
)

// ManagedRunObserverは受理済み周期の結果を同期通知します。通常のCloseはcallbackの
// contextを中止せず完了を待ち、実行中のcallbackへはroot・routeの強制終了を伝えます。
// callback内からの終了要求にはRequestCloseを使ってください。
type ManagedRunObserver func(
	context.Context,
	*RunReport,
	*objectstoragespool.TerminalPurgeReport,
	error,
)

type rootRuntimeState uint8

const (
	// rootRuntimeNotStartedは、保存先を予約しただけで周期処理はまだ開始していない状態です。
	rootRuntimeNotStarted rootRuntimeState = iota
	// rootRuntimeRunningは、周期処理が稼働中で、新規routeも登録できる状態です。
	rootRuntimeRunning
	// rootRuntimeRegistrationClosedは、新規routeの登録を締め切り、既存routeの
	// 保存・再送・purgeを維持している状態です。最後のrouteのCloseで停止に進みます。
	rootRuntimeRegistrationClosed
	// rootRuntimeShutdownStartedは、停止処理を開始した状態です。完了はshutdownCompleteで確認します。
	rootRuntimeShutdownStarted
)

// RootRuntimeは一つのspool保存先の設定と管理処理を所有します。
// 設定はNewRootRuntimeで一度だけ受け取り、各uploaderはRegisterRouteで登録します。
// routeが一時的にゼロでも所有権を保持し、lazy生成される別系列へ同じruntimeを渡します。
// NewRootRuntimeで構築し、コピーせずポインタを共有してください。
type RootRuntime struct {
	store *objectstoragespool.DestinationStores
	// runnerは再送・purgeの周期実行を担当します。起動・停止と終了待ちはRootRuntimeが所有します。
	runner *Runner

	// storeOrPurgeMuは保存とpurgeで共有するロックです。保存側がread lock、
	// purge側がwrite lockをそれぞれ取得し、両処理の同時実行を防ぎます。
	// 保存結果を呼び出し元へ返す前に、保存したファイルがpurgeされることを防ぎます。
	storeOrPurgeMu infrafsstore.ContextRWMutex
	// saveOrClaimMuはspoolへの保存と、再送対象をactiveからprocessingへ移す処理で共有します。
	// 保存と対象確保の同時実行を防ぎます。object storageへのupload中は保持しません。
	saveOrClaimMu infrafsstore.ContextMutex

	mu     sync.Mutex
	routes map[*ManagedRoute]routeRegistration
	// initializedDestinationsは起動中に初期化済みの宛先です。registryのlockで保護します。
	initializedDestinations map[objectstorage.DestinationID]bool
	// stateは未開始・稼働中・新規登録締切済み・停止処理開始済みを表します。muで保護します。
	state rootRuntimeState
	// registryのlockを保持し、最初のroute登録時に固定します。
	maintenanceDisabled bool
	// runErrはRunner.Runが返した停止理由です。通常の終了要求によるctxエラーは記録しません。
	// muで保護し、Closeで返します。周期ごとの処理エラーはReporterから通知します。
	runErr error

	// cancelは最後のownerの解放時に再送・purge周期処理を中止します。
	// 対応するContextはRunner.Runの引数で渡します。
	cancel context.CancelFunc
	// forceCancelは強制終了でだけ受理済み周期を中止します。
	forceCancel        context.CancelFunc
	closeController    shutdown.Controller
	notifications      sync.WaitGroup
	routesReleased     chan struct{}
	routesReleasedOnce sync.Once
	// runLoopStoppedは、再送・purgeを実行するRunner.Runが終了すると閉じます。
	// 開始せずに終了する場合も閉じます。保存処理の終了待ちは含みません。
	runLoopStopped chan struct{}
	// shutdownCompleteは、受理済みの保存と周期処理の終了を待ち、
	// 保存先の登録を解除した後に閉じます。同じ保存先を再取得する側は、この通知を待ちます。
	shutdownComplete chan struct{}
}

// NewRootRuntimeは設定を固定して保存先の所有権を予約します。
// ディレクトリ作成と周期処理の開始は最初のRegisterRouteで行います。
// 同じ保存先を使うuploaderへは、この返り値を共有してください。
// 別のRootRuntimeで同じ保存先を再初期化することは、設定値が同じでも拒否します。
func NewRootRuntime(config ConfigProvider) (*RootRuntime, error) {
	root, fixedConfig, err := resolveRootRuntimeConfig(config)
	if err != nil {
		return nil, err
	}
	return newRootRuntime(root, fixedConfig)
}

// resolveRootRuntimeConfigは保存先を解決し、同じrootの共有判定に使う設定を固定します。
// 保存先の予約・ディレクトリ作成・周期処理の開始は行いません。
func resolveRootRuntimeConfig(config ConfigProvider) (objectstoragespool.Root, rootRuntimeConfigAdapter, error) {
	if config == nil {
		return objectstoragespool.Root{}, rootRuntimeConfigAdapter{}, fmt.Errorf("%w: config provider is nil", ErrInvalidCompressedLogFileUploadSpoolConfig)
	}
	contract, err := parseLoggerContract(config.LoggerContract())
	if err != nil {
		return objectstoragespool.Root{}, rootRuntimeConfigAdapter{}, err
	}
	root, err := objectstoragespool.NewRoot(config.SpoolPath(), config.Target(), config.Service(), contract)
	if err != nil {
		return objectstoragespool.Root{}, rootRuntimeConfigAdapter{}, fmt.Errorf("%w: %w", ErrInvalidCompressedLogFileUploadSpoolConfig, err)
	}
	fixedConfig := newRootRuntimeConfigAdapter(config)
	if err := validateRootRuntimeConfig(fixedConfig); err != nil {
		return objectstoragespool.Root{}, rootRuntimeConfigAdapter{}, err
	}
	return root, fixedConfig, nil
}

func newRootRuntime(root objectstoragespool.Root, fixedConfig rootRuntimeConfigAdapter) (*RootRuntime, error) {
	store, err := objectstoragespool.NewDestinationStores(root)
	if err != nil {
		return nil, err
	}
	purger, err := objectstoragespool.NewTerminalPurger(store, fixedConfig)
	if err != nil {
		return nil, err
	}
	rootKey := pathidentity.ComparisonKey(root.Path())
	for {
		rootRuntimeRegistry.Lock()
		if existing := rootRuntimeRegistry.byRoot[rootKey]; existing != nil {
			existing.mu.Lock()
			shutdownStarted := existing.state == rootRuntimeShutdownStarted
			existing.mu.Unlock()
			rootRuntimeRegistry.Unlock()
			if shutdownStarted {
				// 旧管理処理の保存・再送が終了するまで、新しい初期化を開始しません。
				<-existing.shutdownComplete
				continue
			}
			return nil, fmt.Errorf("%w: %q", ErrSpoolRootAlreadyOwned, rootKey)
		}
		runtime := &RootRuntime{
			store:                   store,
			routes:                  make(map[*ManagedRoute]routeRegistration),
			initializedDestinations: make(map[objectstorage.DestinationID]bool),
			runLoopStopped:          make(chan struct{}),
			shutdownComplete:        make(chan struct{}),
			routesReleased:          make(chan struct{}),
		}
		worker, err := compressedlogworker.NewDestinationWorker(
			store, nil, fixedConfig.CircuitBreakerOpenCooldown(),
			compressedlogworker.WithClaimLock(&runtime.saveOrClaimMu),
			compressedlogworker.WithRouteSnapshot(runtime.copyRouteRegistry),
			compressedlogworker.WithMaxRetryAfter(fixedConfig.MaxRetryAfter()),
			// 保存に失敗した宛先の期限は、runnerの周期から始めて間隔を倍にしながら保存し直します。
			compressedlogworker.WithRetryAfterSaveRetryInterval(fixedConfig.Interval()),
		)
		if err != nil {
			rootRuntimeRegistry.Unlock()
			return nil, err
		}
		runner, err := NewRunner(worker, fixedConfig, runtime,
			WithTerminalPurge(purger, &runtime.storeOrPurgeMu),
		)
		if err != nil {
			rootRuntimeRegistry.Unlock()
			return nil, err
		}
		runtime.runner = runner
		rootRuntimeRegistry.byRoot[rootKey] = runtime
		rootRuntimeRegistry.Unlock()
		return runtime, nil
	}
}

// Rootは予約済み保存先を返します。設定の再読込やfilesystemへのアクセスは行いません。
func (r *RootRuntime) Root() objectstoragespool.Root {
	return r.store.Root()
}

// RegisterRouteは準備済みspoolへ一系列の再送先と通知先を登録します。
// 保持設定・周期設定は受け取らず、RootRuntimeの構築時の設定を使います。
func (r *RootRuntime) RegisterRoute(
	cfg ManagedRouteConfig,
	destinationUploader compressedlogworker.Uploader,
) (*ManagedRoute, error) {
	if destinationUploader == nil {
		return nil, ErrNilDestinationUploader
	}
	if err := compressedlogworker.ValidateRouteFileName(cfg.FileName); err != nil {
		return nil, err
	}
	destinationIDs := destinationUploader.DestinationIDs()
	if len(destinationIDs) != 1 || strings.TrimSpace(string(destinationIDs[0])) == "" {
		return nil, ErrInvalidRouteDestination
	}
	if r == nil || r.store == nil {
		return nil, ErrRootRuntimeClosed
	}
	rootRuntimeRegistry.Lock()
	defer rootRuntimeRegistry.Unlock()
	r.mu.Lock()
	closed := (r.state != rootRuntimeNotStarted && r.state != rootRuntimeRunning) || r.closeController.IsForced()
	needsStart := r.state == rootRuntimeNotStarted
	modeMismatch := !needsStart && r.maintenanceDisabled != cfg.DisableMaintenance
	r.mu.Unlock()
	if closed {
		return nil, ErrRootRuntimeClosed
	}
	if modeMismatch {
		return nil, ErrMaintenanceModeMismatch
	}
	if needsStart {
		if err := r.store.EnsureLayout(context.Background()); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInitializeSpool, err)
		}
	}
	store, err := r.store.ForDestination(destinationIDs[0])
	if err != nil {
		return nil, err
	}
	if !r.initializedDestinations[destinationIDs[0]] {
		if err := store.EnsureLayout(context.Background()); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInitializeSpool, err)
		}
		// この宛先の初回登録前だけ掃除します。既存routeの保存処理には触れません。
		if !cfg.DisableMaintenance {
			_, cleanupErr := store.RemoveAbandonedStagingFiles(context.Background())
			if cleanupErr != nil {
				return nil, fmt.Errorf("%w: %w", ErrRemoveAbandonedStagingFiles, cleanupErr)
			}
		}
		r.initializedDestinations[destinationIDs[0]] = true
	}
	forceCtx, forceCancel := context.WithCancel(context.Background())
	route := &ManagedRoute{
		rootRuntime: r, store: store, forceCtx: forceCtx, forceCancel: forceCancel,
		classifyRetry: destinationUploader.ClassifyRetry,
	}
	if err := r.addRoute(route, cfg.FileName, routeUploader{Uploader: destinationUploader, forceCtx: forceCtx}, cfg.RunObserver, cfg.OnWarn); err != nil {
		forceCancel()
		return nil, err
	}
	if needsStart {
		ctx, cancel := context.WithCancel(context.Background())
		workCtx, forceCancel := context.WithCancel(context.Background())
		r.mu.Lock()
		r.cancel = cancel
		r.forceCancel = forceCancel
		r.state = rootRuntimeRunning
		r.maintenanceDisabled = cfg.DisableMaintenance
		r.mu.Unlock()
		if r.closeController.IsForced() {
			cancel()
			forceCancel()
		}
		if cfg.DisableMaintenance {
			close(r.runLoopStopped)
		} else {
			go r.run(ctx, workCtx)
		}
	}
	return route, nil
}

// Closeは新規route登録を止め、この保存先の所有権を解放します。
// 登録済みrouteが残る場合は、それらのCloseまで保存・再送処理を維持します。
// 最後のrouteも終了したときに、周期処理と受理済み保存を待って保存先の予約を解除します。
func (r *RootRuntime) Close() error {
	if r == nil || r.store == nil {
		return nil
	}
	rootRuntimeRegistry.Lock()
	r.mu.Lock()
	var finish bool
	switch r.state {
	case rootRuntimeNotStarted:
		// 周期処理を開始していないため、停止待ちをせず終了処理に進みます。
		r.state = rootRuntimeShutdownStarted
		close(r.runLoopStopped)
		r.routesReleasedOnce.Do(func() { close(r.routesReleased) })
		finish = true
	case rootRuntimeRunning:
		r.state = rootRuntimeRegistrationClosed
		finish = r.stopIfUnusedLocked()
		r.stopIfRoutesClosingLocked()
	}
	shutdownStarted := r.state == rootRuntimeShutdownStarted
	r.mu.Unlock()
	rootRuntimeRegistry.Unlock()
	if finish {
		return r.waitShutdown()
	} else if shutdownStarted {
		return r.waitShutdown()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closeController.IsForced() {
		return errors.Join(shutdown.ErrForcedClose, r.runErr)
	}
	return r.runErr
}

// RequestCloseは通知callbackから終了を要求するための、完了を待たない入口です。
// Closeはcallback自身も含めて待つため、callbackの中ではこちらを使います。
func (r *RootRuntime) RequestClose() {
	if r != nil && r.store != nil {
		go r.Close()
	}
}

// RequestForceCloseはこのroot全体の周期を止め、受理済み処理へ中止を通知します。
// 呼び出し元の待機だけを解き、処理が戻るまで資源とrootの予約を保持します。
func (r *RootRuntime) RequestForceClose() error {
	if r == nil || r.store == nil {
		return nil
	}
	return r.closeController.ForceClose(func() {
		r.mu.Lock()
		stop, force := r.cancel, r.forceCancel
		r.mu.Unlock()
		if stop != nil {
			stop()
		}
		if force != nil {
			force()
		}
	})
}

func (r *RootRuntime) ForceClose() error {
	err := r.RequestForceClose()
	r.RequestClose()
	return err
}

// ShutdownDoneは資源とrootの予約の実際の解放を知らせます。forceだけでは閉じません。
func (r *RootRuntime) ShutdownDone() <-chan struct{} { return r.closeController.Done() }

func (r *RootRuntime) ShutdownComplete() bool { return r.closeController.IsComplete() }

func (r *RootRuntime) waitShutdown() error {
	return r.closeController.Close("compressed log file spool root", func() (bool, error) {
		r.finishShutdown()
		r.mu.Lock()
		defer r.mu.Unlock()
		return true, r.runErr
	})
}

// validateRootRuntimeConfigは、周期処理の構築前に必要な期間設定を検証します。
func validateRootRuntimeConfig(config rootRuntimeConfigProvider) error {
	if config.Interval() <= 0 {
		return fmt.Errorf(
			"%w: runner interval must be positive",
			ErrInvalidCompressedLogFileUploadSpoolConfig,
		)
	}
	if config.PurgeInterval() <= 0 {
		return fmt.Errorf(
			"%w: lifecycle interval must be positive",
			ErrInvalidCompressedLogFileUploadSpoolConfig,
		)
	}
	if config.CircuitBreakerOpenCooldown() <= 0 {
		return fmt.Errorf(
			"%w: %w",
			ErrInvalidCompressedLogFileUploadSpoolConfig,
			compressedlogworker.ErrInvalidCircuitBreakerOpenCooldown,
		)
	}
	if config.MaxRetryAfter() < 0 {
		return fmt.Errorf(
			"%w: %w: %s",
			ErrInvalidCompressedLogFileUploadSpoolConfig,
			compressedlogworker.ErrInvalidMaxRetryAfter,
			config.MaxRetryAfter(),
		)
	}
	return nil
}

func parseLoggerContract(value string) (pkglogger.LoggerContractKind, error) {
	contract := pkglogger.LoggerContractKind(strings.TrimSpace(value))
	switch contract {
	case pkglogger.LoggerContractSync,
		pkglogger.LoggerContractAsyncBestEffort,
		pkglogger.LoggerContractDelivery:
		return contract, nil
	default:
		return "", fmt.Errorf(
			"%w: logger contract %q is unsupported",
			ErrInvalidCompressedLogFileUploadSpoolConfig,
			value,
		)
	}
}

// routeRegistrationは、一つのログ系列の再送対象・送信先・結果通知先をまとめた登録情報です。
// ファイルごとの送信状態ではありません。RootRuntimeが保持し、対応するManagedRouteのCloseで削除します。
type routeRegistration struct {
	// fileNameは、この登録が担当する圧縮ログのファイル名patternです。
	fileName logpattern.FileNamePattern
	// uploaderは対象ファイルを送信し、そのサービス固有の失敗分類を返します。
	uploader compressedlogworker.Uploader
	// observerは再送・purgeの実行結果やエラーを利用側へ通知します。nilなら通知しません。
	observer ManagedRunObserver
	// onWarnは保全済みファイルの確認を利用側へ通知します。処理失敗とは区別します。
	onWarn func(context.Context, error)
}

var rootRuntimeRegistry = struct {
	sync.Mutex
	byRoot map[string]*RootRuntime
}{byRoot: make(map[string]*RootRuntime)}

func (r *RootRuntime) addRoute(
	route *ManagedRoute,
	fileName logpattern.FileNamePattern,
	destinationUploader compressedlogworker.Uploader,
	observer ManagedRunObserver,
	onWarn func(context.Context, error),
) error {
	if r == nil || destinationUploader == nil {
		return ErrNilDestinationUploader
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if (r.state != rootRuntimeNotStarted && r.state != rootRuntimeRunning) || r.closeController.IsForced() {
		return ErrRootRuntimeClosed
	}
	if observer != nil {
		notify := observer
		observer = func(ctx context.Context, replay *RunReport, purge *objectstoragespool.TerminalPurgeReport, err error) {
			callbackCtx, detach := route.notificationContext(ctx)
			defer detach()
			notify(callbackCtx, replay, purge, err)
		}
	}
	if onWarn != nil {
		notify := onWarn
		onWarn = func(ctx context.Context, err error) {
			callbackCtx, detach := route.notificationContext(ctx)
			defer detach()
			notify(callbackCtx, err)
		}
	}
	r.routes[route] = routeRegistration{
		fileName: fileName,
		uploader: destinationUploader,
		observer: observer,
		onWarn:   onWarn,
	}
	return nil
}

// stopIfUnusedLockedは保存先ownerと全routeの解放後に、一度だけ停止を開始します。
// 呼び出し元はregistry lockとr.muの両方を保持します。
func (r *RootRuntime) stopIfUnusedLocked() bool {
	if (r.state != rootRuntimeRegistrationClosed && r.state != rootRuntimeShutdownStarted) || len(r.routes) != 0 {
		return false
	}
	r.state = rootRuntimeShutdownStarted
	r.cancel()
	r.routesReleasedOnce.Do(func() { close(r.routesReleased) })
	return true
}

// 登録を処理終了まで保持したまま、最後のrouteの終了要求で次の周期を止めます。
func (r *RootRuntime) stopIfRoutesClosingLocked() {
	if r.state != rootRuntimeRegistrationClosed {
		return
	}
	for route := range r.routes {
		if !route.closing {
			return
		}
	}
	r.state = rootRuntimeShutdownStarted
	r.cancel()
}

func (r *RootRuntime) finishShutdown() {
	<-r.routesReleased
	// 受理済みの保存が使うstagingを、新しい所有者が残骸と誤認して削除しないよう待ちます。
	r.storeOrPurgeMu.Lock()
	r.storeOrPurgeMu.Unlock()
	<-r.runLoopStopped
	r.notifications.Wait()
	rootRuntimeRegistry.Lock()
	rootKey := pathidentity.ComparisonKey(r.store.Root().Path())
	if rootRuntimeRegistry.byRoot[rootKey] == r {
		delete(rootRuntimeRegistry.byRoot, rootKey)
	}
	close(r.shutdownComplete)
	rootRuntimeRegistry.Unlock()
}

func (r *RootRuntime) isShutdownStarted() bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state == rootRuntimeShutdownStarted
}

func (r *RootRuntime) run(ctx, workCtx context.Context) {
	defer close(r.runLoopStopped)
	err := r.runner.runWithContexts(ctx, workCtx)
	if err != nil && !errors.Is(err, ctx.Err()) {
		r.mu.Lock()
		r.runErr = err
		r.mu.Unlock()
		r.ReportSpoolRunnerRun(workCtx, RunReport{}, err)
	}
}

// ReportSpoolRunnerRunはRunnerの結果を、現在登録されている通知先へ渡します。
// 受理済み通知は全て完了まで待ちます。callbackから終了を要求するときは
// 同じcallbackの終了待ちになるCloseではなく、RequestCloseを使います。
// 今回の再送・purgeのファイル操作は通知前に完了しています。
// 今回の保全結果と全宛先の.preservedに残る保全件数をOnWarnへ渡します。
// 保全結果の取得や保全ディレクトリの走査が失敗した場合は、実行エラーへ追加します。
// 件数は再起動後もfilesystemから取得します。保全ファイルの削除・再送は行いません。
func (r *RootRuntime) ReportSpoolRunnerRun(ctx context.Context, report RunReport, err error) {
	observers, warnObservers, release := r.copyRunNotifications()
	defer release()
	if len(observers) == 0 && len(warnObservers) == 0 {
		return
	}
	var warnings []error
	if ctx.Err() == nil {
		preservationWarnings, takeErr := r.store.TakePreservationWarnings(ctx)
		warnings = append(warnings, preservationWarnings...)
		if takeErr != nil {
			err = errors.Join(err, fmt.Errorf("collect compressed log file spool preservation warnings: %w", takeErr))
		}
		count, scanErr := r.store.CountPreservedFiles(ctx)
		if scanErr != nil {
			err = errors.Join(err, fmt.Errorf("inspect preserved compressed log file spool files: %w", scanErr))
		}
		if count != 0 {
			warnings = append(warnings, &PreservedFilesWarning{Count: count, Root: r.store.Root().Path()})
		}
	}
	// upload先が上限を超えるRetry-Afterを指定した場合は、短縮して待機したことを一回ずつ通知します。
	for _, file := range report.CompressedLogFiles {
		if clamp := file.RetryAfterClamp; clamp != nil {
			warnings = append(warnings, &RetryAfterClampWarning{
				DestinationID: file.DestinationID,
				Requested:     clamp.Requested,
				MaxRetryAfter: clamp.MaxRetryAfter,
				NotBefore:     clamp.NotBefore,
			})
		}
	}
	// 再起動前に保存した期限の短縮と、期限を保存したmarkerの失敗も一回ずつ通知します。
	for _, marker := range report.RetryAfterMarkers {
		if clamp := marker.Clamp; clamp != nil {
			warnings = append(warnings, &RetryAfterClampWarning{
				DestinationID: marker.DestinationID,
				Requested:     clamp.Requested,
				MaxRetryAfter: clamp.MaxRetryAfter,
				NotBefore:     clamp.NotBefore,
				Stored:        clamp.Stored,
			})
		}
		if marker.Err != nil {
			warnings = append(warnings, &RetryAfterMarkerWarning{DestinationID: marker.DestinationID, Err: marker.Err})
		}
	}
	var replay *RunReport
	if report.PurgeReport == nil {
		replay = &report
	}
	r.notifyRunObservers(ctx, observers, replay, report.PurgeReport, err)
	for _, warning := range warnings {
		for _, onWarn := range warnObservers {
			func() {
				defer func() { _ = recover() }()
				onWarn(ctx, warning)
			}()
		}
	}
}

func (r *RootRuntime) copyRouteRegistry() compressedlogworker.RouteRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	routes := make(compressedlogworker.RouteRegistry, 0, len(r.routes))
	for _, registration := range r.routes {
		routes = append(routes, compressedlogworker.RouteRegistration{
			FileName: registration.fileName,
			Uploader: registration.uploader,
		})
	}
	return routes
}

func (r *RootRuntime) copyRunNotifications() ([]ManagedRunObserver, []func(context.Context, error), func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	observers := make([]ManagedRunObserver, 0, len(r.routes))
	warnObservers := make([]func(context.Context, error), 0, len(r.routes))
	var accepted []*ManagedRoute
	for route, registration := range r.routes {
		if registration.observer != nil || registration.onWarn != nil {
			route.notifications.Add(1)
			accepted = append(accepted, route)
		}
		if registration.observer != nil {
			observers = append(observers, registration.observer)
		}
		if registration.onWarn != nil {
			warnObservers = append(warnObservers, registration.onWarn)
		}
	}
	if len(accepted) != 0 {
		r.notifications.Add(1)
	}
	return observers, warnObservers, func() {
		for _, route := range accepted {
			route.notifications.Done()
		}
		if len(accepted) != 0 {
			r.notifications.Done()
		}
	}
}

func (r *RootRuntime) notifyRunObservers(
	ctx context.Context,
	observers []ManagedRunObserver,
	replay *RunReport,
	purgeReport *objectstoragespool.TerminalPurgeReport,
	err error,
) {
	for _, observer := range observers {
		func() {
			defer func() { _ = recover() }()
			observer(ctx, replay, purgeReport, err)
		}()
	}
}
