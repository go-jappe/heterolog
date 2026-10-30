// Package compressedlogfailureは、通常ログの.logとbackend送信失敗レコードの.jsonlの
// 圧縮ファイルについて、object storageへの送信と失敗時の退避先への保存を合成します。
package compressedlogfailure

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/retryafter"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	spooluploader "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/uploader"
	"github.com/go-jappe/heterolog/internal/infrastructure/storageowner"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

var (
	ErrInvalidConfig          = errors.New("object storage compressed log file upload failure store config is invalid")
	ErrNilDestinationUploader = errors.New("object storage compressed log file destination uploader is nil")
)

// DefaultSaveTimeoutはupload失敗後の退避先への保存に与える時間です。
// 期限切れでは保存を中止し、保存処理と後片付けの終了を確認してから戻ります。
// OS内部で停止したfilesystem I/Oの強制終了は保証しません。
const DefaultSaveTimeout = time.Minute

// UploadProgressStoreはManagedUploaderが送信・退避の再開に使うcheckpoint操作です。
// 宛先登録や完了cursorの更新、Storeの終了は呼び出し元が担当します。
type UploadProgressStore interface {
	Load(context.Context, checkpoint.CheckpointKey) (checkpoint.Checkpoint, error)
	LoadUploadProgress(context.Context, checkpoint.CheckpointKey, objectstorage.DestinationID) (checkpoint.UploadProgress, bool, error)
	SaveUploadProgress(context.Context, checkpoint.UploadProgress) error
}

// ManagedUploaderConfigは一つのログファイル系列について、object storage送信失敗時の
// 保存先と、宛先別spool内で再送するログ系列の命名規則を設定します。
type ManagedUploaderConfig struct {
	// DisableMaintenanceは手動復旧用に、退避先への保存だけを有効にします。
	// spool再送・archiveのpurge・起動時の残骸掃除は行いません。同じ保存先を使う
	// 通常アプリを停止してから使用し、同一rootのownerは同じ値で構築してください。
	DisableMaintenance bool
	// Clockはarchiveの保持期限を判定する時刻源です。nilならUTC Clockを使います。
	Clock clock.Clock
	// SaveTimeoutはupload失敗後の保存期限です。0ならDefaultSaveTimeoutを使います。
	// 直接送信の期限とは独立し、期限切れでは保存の中止と後片付けを待ちます。
	SaveTimeout                         time.Duration
	CompressedLogFileUploadFailureStore ConfigProvider
	// SpoolRuntimesは構築側でbackendごとに用意し、同じ保存先を使う各uploaderへ共有します。
	// このuploaderはrouteだけを所有し、RootRuntime自体はCloseしません。
	SpoolRuntimes compressedlog.RootRuntimes
	// Notifierは送信失敗・退避・checkpoint保存に関する内部ログの通知先です。
	Notifier Notifier
	// 通知先は、宛先のbackendに対応する保存先を確定してから構築します。
	SpoolRunObserverForRoot func(*compressedlog.RootRuntime) compressedlog.ManagedRunObserver
	// SpoolOnWarnForRootはspoolの保全済みファイルを確認する警告通知先を構築します。
	SpoolOnWarnForRoot          func(*compressedlog.RootRuntime) func(context.Context, error)
	ArchivePurgeObserverForRoot func(compressedlogarchive.Root) ArchivePurgeObserver
	// CheckpointStoreはこの系列の送信・退避の途中経過を保存します。
	// nilなら、このuploaderが生存する間だけ失敗中の途中経過を保持します。
	CheckpointStore UploadProgressStore
	// FileNameは、送信元の退避対象の判定・checkpointの世代解析・spoolの再送経路の選択に使う命名規則です。
	// 退避対象はDir直下の、この系列の正規の圧縮ファイルです。Dir・Target・Extensionは必須です。
	// 相対Dirは構築時の作業ディレクトリを基準に絶対化し、以後の作業ディレクトリ変更の影響を受けません。
	// spoolでは移動後のファイル名を照合し、元のDirや現在のDate・Indexは照合しません。
	FileName logpattern.FileNamePattern
}

// ManagedUploaderはファイル・宛先ごとのupload結果と退避済み保存先をcheckpointへ記録し、
// 再試行では未完了の保存先だけを処理します。初回のuploadに失敗した圧縮log fileは
// 選択済みのdurable storeへ並列保存します。保存期限を過ぎたら中止を要求し、全保存先の終了と
// 後片付けを待ちます。保存処理を裏に残して戻らず、すべて成功した場合だけ受理成功を返します。
// この受理成功も呼び出し側のcheckpointへ完了として記録されます。S3への到達とは区別し、
// 実際のupload失敗はnotifierへ通知します。spoolの再送は元ファイルのcheckpointとは独立します。
type ManagedUploader struct {
	// nextは、S3などへの実際のuploadを行う委譲先です。
	next uploader.DestinationUploader
	// notifierは、upload失敗と保存結果を利用側へ通知する関数です。
	notifier Notifier

	// failureTargetsは保存・再利用確認・終了処理が共用する宛先別の退避先一覧です。
	failureTargets map[objectstorage.DestinationID][]failureTarget

	checkpointStore UploadProgressStore
	fileName        logpattern.FileNamePattern
	// attemptLocksは宛先ごとの途中経過の読み取りから送信・保存完了までを排他します。
	// 別宛先は並列実行し、同じ宛先の再試行だけを直列化します。構築後は変更しません。
	attemptLocks map[objectstorage.DestinationID]*infrafsstore.ContextMutex
	attemptsMu   sync.Mutex
	attempts     map[uploadAttemptKey]*uploadAttempt
	// notifierMuは複数宛先が同時に完了しても通知先を並列に呼ばないための排他です。
	notifierMu sync.Mutex

	saveTimeout time.Duration
	// stateMuは受付とClose開始だけを排他します。送信・保存中は保持しません。
	stateMu sync.Mutex
	closing bool
	// inFlightは受理済みの同期呼び出しです。Closeが使用中の保存先を閉じることを防ぎます。
	inFlight        sync.WaitGroup
	closeController shutdown.Controller
	forceOnce       sync.Once
	forceContext    context.Context
	forceCancel     context.CancelFunc
}

type archiveRoute struct {
	store *compressedlogarchive.Store
	owner *archiveLifecycleOwner
}

var _ uploader.DestinationUploader = (*ManagedUploader)(nil)

// NewManagedUploaderは設定が未指定、またはdurable typeが空・noneならnextを変更せず返します。
// 複数宛先の場合はspoolとarchiveを宛先別に登録し、保存結果を混同しません。
// 宛先IDは宛先別の退避先とRetry-Afterのmarkerの名前になるため、retryafter.ValidateDestinationIDの
// 規則に合わない宛先があれば、どの退避先も登録せずに*InvalidDestinationIDErrorを返します。
func NewManagedUploader(
	cfg ManagedUploaderConfig,
	next uploader.Uploader,
) (uploader.Uploader, error) {
	if cfg.CompressedLogFileUploadFailureStore == nil {
		return next, nil
	}
	types := cfg.CompressedLogFileUploadFailureStore.DurableType()
	if len(types) == 0 || (len(types) == 1 && types[0] == failurestore.DurableTypeNone) {
		return next, nil
	}
	if cfg.SaveTimeout < 0 {
		return nil, fmt.Errorf("%w: save timeout must not be negative", ErrInvalidConfig)
	}
	if err := failurestore.ValidateDurableTypes(
		"object storage compressed log file upload failure store",
		types,
		failurestore.DurableTypeFileArchive,
		failurestore.DurableTypeSpool,
	); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	destinationUploader, ok := next.(uploader.DestinationUploader)
	if !ok || destinationUploader == nil {
		return nil, fmt.Errorf("%w: uploader %T", ErrNilDestinationUploader, next)
	}
	if strings.TrimSpace(cfg.FileName.Dir) == "" || strings.TrimSpace(cfg.FileName.Target) == "" || strings.TrimSpace(cfg.FileName.Extension) == "" {
		return nil, fmt.Errorf("%w: source file directory, target and extension are required", ErrInvalidConfig)
	}
	fileName := cfg.FileName
	directory, err := filepath.Abs(fileName.Dir)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve source file directory: %w", ErrInvalidConfig, err)
	}
	fileName.Dir = directory
	destinationIDs := destinationUploader.DestinationIDs()
	if len(destinationIDs) == 0 {
		return nil, fmt.Errorf("%w: no destinations", ErrInvalidConfig)
	}
	services, ok := next.(uploader.DestinationServiceProvider)
	if !ok {
		return nil, fmt.Errorf("%w: uploader %T has no destination services", ErrInvalidConfig, next)
	}
	servicesByDestination := make(map[objectstorage.DestinationID]objectstorage.UploadTarget, len(destinationIDs))
	locks := make(map[objectstorage.DestinationID]*infrafsstore.ContextMutex, len(destinationIDs))
	for _, id := range destinationIDs {
		if strings.TrimSpace(string(id)) == "" || locks[id] != nil {
			return nil, fmt.Errorf("%w: empty or duplicate destination %q", ErrInvalidConfig, id)
		}
		// 宛先IDは宛先別spool・file archiveのdirectoryとRetry-Afterのmarkerの名前になるため、
		// どの退避先も登録する前に、markerと同じ規則で一度だけ検証します。
		if err := retryafter.ValidateDestinationID(string(id)); err != nil {
			return nil, &InvalidDestinationIDError{Destination: id, Err: err}
		}
		locks[id] = &infrafsstore.ContextMutex{}
		service, exists := services.DestinationService(id)
		if !exists || strings.TrimSpace(string(service)) == "" {
			return nil, fmt.Errorf("%w: %w %q", ErrInvalidConfig, ErrNoDestinationService, id)
		}
		servicesByDestination[id] = service
	}

	spoolSelected := slices.Contains(types, failurestore.DurableTypeSpool)

	managed := &ManagedUploader{
		next:            destinationUploader,
		notifier:        cfg.Notifier,
		saveTimeout:     cfg.SaveTimeout,
		checkpointStore: cfg.CheckpointStore,
		fileName:        fileName,
		attemptLocks:    locks,
		failureTargets:  make(map[objectstorage.DestinationID][]failureTarget),
	}
	if managed.saveTimeout == 0 {
		managed.saveTimeout = DefaultSaveTimeout
	}
	if spoolSelected {
		for _, destination := range destinationIDs {
			service := servicesByDestination[destination]
			runtime := cfg.SpoolRuntimes[service]
			if runtime == nil || runtime.Root().Service() != service {
				_ = managed.Close()
				return nil, &SpoolRuntimeMismatchError{Destination: destination, Service: service}
			}
			replayUploader, err := spooluploader.New(service, destinationUploadRoute{next: destinationUploader, destination: destination})
			if err != nil {
				_ = managed.Close()
				return nil, fmt.Errorf("%w: create spool uploader: %w", ErrInvalidConfig, err)
			}
			var observer compressedlog.ManagedRunObserver
			if cfg.SpoolRunObserverForRoot != nil {
				observer = cfg.SpoolRunObserverForRoot(runtime)
			}
			var onWarn func(context.Context, error)
			if cfg.SpoolOnWarnForRoot != nil {
				onWarn = cfg.SpoolOnWarnForRoot(runtime)
			}
			route, err := runtime.RegisterRoute(
				compressedlog.ManagedRouteConfig{
					DisableMaintenance: cfg.DisableMaintenance,
					FileName:           fileName,
					RunObserver:        observer,
					OnWarn:             onWarn,
				},
				replayUploader,
			)
			if err != nil {
				_ = managed.Close()
				return nil, fmt.Errorf("create compressed log file upload failure spool route: %w", err)
			}
			managed.failureTargets[destination] = append(managed.failureTargets[destination], spoolFailureTarget{route: route})
		}
	}
	if slices.Contains(types, failurestore.DurableTypeFileArchive) {
		archiveConfig := cfg.CompressedLogFileUploadFailureStore.FileArchiveConfig()
		if archiveConfig == nil {
			_ = managed.Close()
			return nil, fmt.Errorf("%w: archive config provider is nil", ErrInvalidConfig)
		}
		contract, err := parseLoggerContract(archiveConfig.LoggerContract())
		if err != nil {
			_ = managed.Close()
			return nil, err
		}
		for _, destination := range destinationIDs {
			root, err := compressedlogarchive.NewRoot(
				archiveConfig.RootPath(), servicesByDestination[destination], contract,
			)
			if err != nil {
				_ = managed.Close()
				return nil, fmt.Errorf("create object storage compressed log file upload failure archive root: %w", err)
			}
			destinationRoot, err := root.ForDestination(destination)
			if err != nil {
				_ = managed.Close()
				return nil, fmt.Errorf("create object storage compressed log file upload failure archive destination: %w", err)
			}
			store, err := compressedlogarchive.NewStore(destinationRoot)
			if err != nil {
				_ = managed.Close()
				return nil, fmt.Errorf("create object storage compressed log file upload failure archive store: %w", err)
			}
			var observer ArchivePurgeObserver
			if cfg.ArchivePurgeObserverForRoot != nil {
				observer = cfg.ArchivePurgeObserverForRoot(destinationRoot)
			}
			owner, coordinator, err := acquireArchiveLifecycleWithMaintenance(
				store, archiveConfig, cfg.Clock, observer, cfg.DisableMaintenance,
			)
			if err != nil {
				_ = managed.Close()
				return nil, fmt.Errorf("create object storage compressed log file upload failure archive lifecycle: %w", err)
			}
			route := archiveRoute{store: coordinator.store, owner: owner}
			// 通知する退避先の順序（archive、spool）を維持します。
			managed.failureTargets[destination] = append(
				[]failureTarget{archiveFailureTarget{route: route}},
				managed.failureTargets[destination]...,
			)
		}
	}
	// 設定した退避先を一覧へ登録し忘れても、送信失敗後に空の一覧を
	// 「保存完了」として受理しないよう、起動時に照合します。
	for _, destination := range destinationIDs {
		targets := managed.failureTargets[destination]
		if len(targets) != len(types) {
			_ = managed.Close()
			return nil, fmt.Errorf("%w: destination %q has %d failure targets for %d configured types", ErrInvalidConfig, destination, len(targets), len(types))
		}
		for _, kind := range types {
			if !slices.ContainsFunc(targets, func(target failureTarget) bool { return target.kind() == kind }) {
				_ = managed.Close()
				return nil, fmt.Errorf("%w: destination %q has no failure target for %q", ErrInvalidConfig, destination, kind)
			}
		}
	}
	return managed, nil
}

func (u *ManagedUploader) DestinationIDs() []objectstorage.DestinationID {
	if u == nil || u.next == nil {
		return nil
	}
	return slices.Clone(u.next.DestinationIDs())
}

func (u *ManagedUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	if u == nil || u.next == nil {
		return ErrNilDestinationUploader
	}
	return u.UploadDestinations(ctx, request, u.DestinationIDs()).Err()
}

// UploadDestinationsは宛先ごとの途中経過を確認し、送信失敗済みなら未完了の退避先への
// 保存だけを実行します。記録がない場合も、設定された全退避先に同名・同内容の
// ファイルがあれば送信を省略し、それ以外の場合にuploadを実行します。
func (u *ManagedUploader) UploadDestinations(
	ctx context.Context,
	request uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	if u == nil || u.next == nil {
		return failedUploadReport(destinationIDs, ErrNilDestinationUploader)
	}
	if err := request.Validate(); err != nil {
		return failedUploadReport(destinationIDs, err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	u.stateMu.Lock()
	if u.closing {
		u.stateMu.Unlock()
		return failedUploadReport(destinationIDs, ErrUploaderClosed)
	}
	u.inFlight.Add(1)
	u.stateMu.Unlock()
	defer u.inFlight.Done()
	ctx, cancel := u.contextUntilForced(ctx)
	defer cancel()
	// 元の保存先と、この系列の正規の圧縮ファイル名が一致する場合に退避処理を適用します。
	if logpattern.CompressedFilePathMatcher(u.fileName)(request.FilePath()) {
		if err := ctx.Err(); err != nil {
			return failedUploadReport(destinationIDs, err)
		}
		if len(destinationIDs) == 0 {
			return uploader.UploadReport{}
		}
		seen := make(map[objectstorage.DestinationID]bool, len(destinationIDs))
		for _, id := range destinationIDs {
			if u.attemptLocks[id] == nil || seen[id] {
				return failedUploadReport(destinationIDs, fmt.Errorf("%w: unknown or duplicate destination %q", ErrInvalidConfig, id))
			}
			seen[id] = true
		}
		report := uploader.UploadReport{Results: make([]uploader.DestinationResult, len(destinationIDs))}
		var pending sync.WaitGroup
		for i, id := range destinationIDs {
			pending.Add(1)
			go func() {
				defer pending.Done()
				report.Results[i] = uploader.DestinationResult{DestinationID: id}
				defer func() {
					if recovered := recover(); recovered != nil {
						report.Results[i].Err = &PanicError{Kind: ErrDestinationUploadPanic, Destination: id, Value: recovered}
					}
				}()
				lock := u.attemptLocks[id]
				if err := lock.LockContext(ctx); err != nil {
					report.Results[i].Err = err
					return
				}
				defer lock.Unlock()
				report.Results[i].Err = u.uploadWithProgress(ctx, request, id).Err()
			}()
		}
		pending.Wait()
		return report
	}
	// remote送信は従来のuploaderへ同期的に委譲し、job/goroutineで包みません。
	report := u.next.UploadDestinations(ctx, request, destinationIDs)
	_, contractErr := normalizeUploadResults(destinationIDs, report)
	if contractErr != nil {
		return failedUploadReport(destinationIDs, errors.Join(report.Err(), contractErr))
	}
	return report
}

// saveFailedUploadはuploadに失敗したfileの保存だけを担当します。object storageへ再送しません。
// 送信側contextは既に期限切れの可能性があるため、保存専用の期限をここで開始します。
func (u *ManagedUploader) saveFailedUpload(
	ctx context.Context,
	request uploader.UploadRequest,
	attempt *uploadAttempt,
) (notification Notification) {
	durabilityContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), u.saveTimeout)
	defer cancel()
	durabilityContext, forceCancel := u.contextUntilForced(durabilityContext)
	defer forceCancel()
	failureCategory := uploader.FailureCategoryRetryable
	if attempt.progress.UploadResult == checkpoint.UploadResultPermanentFailure {
		failureCategory = uploader.FailureCategoryPermanent
	}
	destination := attempt.progress.DestinationID
	targets := u.failureTargets[destination]
	notification = Notification{DestinationID: destination, SourcePath: request.FilePath(), UploadErr: attempt.uploadErr, FailureCategory: failureCategory}
	releases := make([]func(), len(targets))
	defer func() {
		// 成功結果の通知まで保存先のpurgeを止め、その後に自分の保護だけを解除します。
		notification.CheckpointErr = attempt.checkpointErr
		notification.PreservationWarning = attempt.takePreservationWarnings()
		u.notify(context.WithoutCancel(ctx), notification)
		for _, release := range releases {
			if release != nil {
				release()
			}
		}
	}()
	if len(targets) == 0 {
		notification.SaveErr = fmt.Errorf("%w: no failure target for destination %q", ErrInvalidConfig, destination)
		return notification
	}
	// 直接uploadが一時的な失敗とともにRetry-Afterを返した場合は、ファイルを保存する前に、その宛先の
	// 待機期限を宛先別spoolへ記録します。稼働中の再送workerも次の周期から期限まで送信しません。
	if failureCategory == uploader.FailureCategoryRetryable && attempt.uploadErr != nil {
		for _, target := range targets {
			if recorder, ok := target.(retryAfterRecordingTarget); ok {
				recorder.recordUploadRetryAfter(ctx, attempt.uploadErr)
			}
		}
	}
	files := make([]failureTargetFile, len(targets))
	saves := make([]func() error, len(targets))
	for index, target := range targets {
		if previous, saved := attempt.savedTarget(target.kind(), target.root()); saved {
			files[index].saved = previous
			continue
		}
		saves[index] = func() error {
			release, err := target.lock(durabilityContext)
			if err != nil {
				return err
			}
			releases[index] = release
			file, err := target.save(durabilityContext, request.FilePath(), failureCategory)
			files[index] = file
			if err != nil {
				return err
			}
			if err := validateFailureTargetFile(target, file); err != nil {
				return err
			}
			return u.recordSavedTarget(durabilityContext, request, attempt, file.saved)
		}
	}
	// 各保存先のlock取得から並列に進め、一方のlock待ちで他方の保存を妨げません。
	// 全保存先の終了までpurgeからの保護を維持し、通知内容はこのgoroutineで組み立てます。
	saveErrors := saveFailureStores(saves)
	// 後続の保存や同期に失敗しても、完了済みの保全への名前変更は通知します。
	// 保全済みファイルは失敗時も残し、管理者が確認できるようにします。
	var saveErrs []error
	for index, target := range targets {
		notification.ConflictRenames = append(notification.ConflictRenames, files[index].conflicts...)
		if err := saveErrors[index]; err != nil {
			saveErrs = append(saveErrs, fmt.Errorf("save %s: %w", target.name(), err))
			continue
		}
		result, err := target.savedFileDetails(files[index])
		if err != nil {
			saveErrs = append(saveErrs, err)
			continue
		}
		notification.SavedTargets = append(notification.SavedTargets, result)
	}
	notification.SaveErr = errors.Join(saveErrs...)
	return notification
}

// saveFailureStoresは選択された保存処理を並列実行し、全ての終了を待ちます。
// nilは保存済みの退避先です。一つの失敗・panicでも他を打ち切らず、入力順で結果を返します。
func saveFailureStores(saves []func() error) []error {
	results := make([]error, len(saves))
	var pending sync.WaitGroup
	for index, save := range saves {
		if save == nil {
			continue
		}
		pending.Add(1)
		go func() {
			defer pending.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					results[index] = &PanicError{Kind: ErrDurableStoreSavePanic, Value: recovered}
				}
			}()
			results[index] = save()
		}()
	}
	pending.Wait()
	return results
}

func classifyUploadFailure(
	destinationIDs []objectstorage.DestinationID,
	results map[objectstorage.DestinationID]uploader.DestinationResult,
	contractErr error,
) checkpoint.UploadResult {
	if contractErr != nil {
		return checkpoint.UploadResultRetryableFailure
	}
	for _, destinationID := range destinationIDs {
		result := results[destinationID]
		if result.Err == nil {
			continue
		}
		// 分類は宛先の送信実装が決めます。未分類の失敗も再送可能として保持します。
		if result.FailureCategory != uploader.FailureCategoryPermanent {
			return checkpoint.UploadResultRetryableFailure
		}
	}
	return checkpoint.UploadResultPermanentFailure
}

// notifyは退避・事前確認の結果を構築時の通知先へ渡します。
// 通知先がpanicしてもupload処理の結果は変えません。
func (u *ManagedUploader) notify(ctx context.Context, notification Notification) {
	if u == nil || u.notifier == nil {
		return
	}
	defer func() { _ = recover() }()
	u.notifierMu.Lock()
	defer u.notifierMu.Unlock()
	// 通知元の期限切れでは通知を捨てず、明示的な強制終了だけを通知先へ伝えます。
	ctx, cancel := u.contextUntilForced(context.WithoutCancel(ctx))
	defer cancel()
	u.notifier(ctx, notification)
}

func parseLoggerContract(value string) (pkglogger.LoggerContractKind, error) {
	contract := pkglogger.LoggerContractKind(strings.TrimSpace(value))
	if storageowner.Supported(contract) {
		return contract, nil
	}
	return "", fmt.Errorf("%w: logger contract %q is unsupported", ErrInvalidConfig, value)
}

func normalizeUploadResults(
	expected []objectstorage.DestinationID,
	report uploader.UploadReport,
) (map[objectstorage.DestinationID]uploader.DestinationResult, error) {
	expectedSet := make(map[objectstorage.DestinationID]struct{}, len(expected))
	for _, destinationID := range expected {
		expectedSet[destinationID] = struct{}{}
	}
	results := make(map[objectstorage.DestinationID]uploader.DestinationResult, len(expected))
	seen := make(map[objectstorage.DestinationID]struct{}, len(report.Results))
	var contractErrors []error
	for _, result := range report.Results {
		if _, wanted := expectedSet[result.DestinationID]; !wanted {
			contractErrors = append(contractErrors, fmt.Errorf(
				"unexpected upload result destination %q",
				result.DestinationID,
			))
			continue
		}
		if _, duplicated := seen[result.DestinationID]; duplicated {
			contractErrors = append(contractErrors, fmt.Errorf(
				"duplicate upload result destination %q",
				result.DestinationID,
			))
			results[result.DestinationID] = uploader.DestinationResult{DestinationID: result.DestinationID, Err: uploader.ErrDuplicateDestinationID}
			continue
		}
		seen[result.DestinationID] = struct{}{}
		results[result.DestinationID] = result
	}
	for _, destinationID := range expected {
		if _, exists := seen[destinationID]; exists {
			continue
		}
		err := fmt.Errorf("upload result for destination %q is missing", destinationID)
		contractErrors = append(contractErrors, err)
		results[destinationID] = uploader.DestinationResult{DestinationID: destinationID, Err: err}
	}
	return results, errors.Join(contractErrors...)
}

func failedUploadReport(
	destinationIDs []objectstorage.DestinationID,
	err error,
) uploader.UploadReport {
	report := uploader.UploadReport{Results: make([]uploader.DestinationResult, len(destinationIDs))}
	for index, destinationID := range destinationIDs {
		report.Results[index] = uploader.DestinationResult{
			DestinationID: destinationID,
			Err:           fmt.Errorf("durable object storage upload: %w", err),
		}
	}
	return report
}

func successfulUploadReport(destinationIDs []objectstorage.DestinationID) uploader.UploadReport {
	report := uploader.UploadReport{Results: make([]uploader.DestinationResult, len(destinationIDs))}
	for index, destinationID := range destinationIDs {
		report.Results[index].DestinationID = destinationID
	}
	return report
}
