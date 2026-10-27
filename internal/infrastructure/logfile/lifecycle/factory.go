package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	uploadercompose "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/compose"
	s3uploaderbuilder "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/uploader/s3"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// fileClassは、一つのFileLogger構成から作られるdefault、error、debugの
// file系列を識別します。file名patternとobject storage upload設定は、
// この分類ごとに選択します。
type fileClass string

const (
	fileClassDefault fileClass = "default"
	fileClassError   fileClass = "error"
	fileClassDebug   fileClass = "debug"
)

// directoryLifecycleFactoryOptionsは、一つのappendlog構成から次の実行単位を
// 組み立てるための共通入力です。
//
//   - default、error、debugのfile系列ごとのFileSeriesManager
//   - 同じ物理directoryのFileSeriesManagerをまとめたDirectoryLifecycle
//
// Configは管理対象のfile系列を、ObjectStorageUploadとArchiveCheckpointは
// 系列ごとのuploadとcheckpointを決めます。
type directoryLifecycleFactoryOptions struct {
	manualRecovery      bool
	checkpointSaveRetry bool
	// validateDefaultPurgeは、default系列の保持設定をerror/debugと同じ下限で検証します。
	// 通常FileLoggerはwriterの構築時にdefault系列を検証済みです。Delivery writerは
	// 保持設定を持たないため、周期処理用のlifecycleを作るこの境界で検証します。
	// 停止中の手動復旧はpurgeを行わないため、保持設定を検証しません。
	validateDefaultPurge bool
	Config               appendlog.ConfigProvider
	ObjectStorageUpload  objectstorage.LogFileUploadConfig
	SpoolRuntimes        compressedlog.RootRuntimes
	ArchiveCheckpoint    pkgfsstore.ArchiveCheckpointConfig
	Clock                clock.Clock
	Tracer               pkgobserver.Tracer
	Meter                pkgobserver.Meter
	// Reporterは受理済みファイル処理の復旧結果と、Store内部の保存再試行エラーの通知先です。
	// 保存再試行の通知先はStoreの構築時に渡します。
	Reporter RunReporter
}

// newDirectoryLifecyclesは、有効なfile classごとに一つのFileSeriesManagerを作り、
// 正規化後の物理directoryが同じFileSeriesManagerを一つのDirectoryLifecycleへまとめます。
//
// FileSeriesManagerは担当するfile系列の圧縮、object storage upload、保持期間・世代数による
// purgeを行います。DirectoryLifecycleは同じdirectory内の全file系列をまとめ、
// directory合計sizeによるpurgeを一度だけ行います。
// checkpoint保存先は呼び出し元が起動時に準備済みであることを前提とし、ここでは作成しません。
func newDirectoryLifecycles(
	opts directoryLifecycleFactoryOptions,
) (lifecycles []*DirectoryLifecycle, resultErr error) {
	var acquiredRouteOwners []ObjectStorageUploadFailureRouteOwner
	var acquiredManagers []*FileSeriesManager
	var unboundCheckpointStore checkpointstore.Store
	defer func() {
		panicked := recover()
		retryCleanup := opts.manualRecovery && opts.checkpointSaveRetry
		if resultErr != nil || panicked != nil {
			forceCleanup := retryCleanup || panicked != nil
			if unboundCheckpointStore != nil {
				resultErr = errors.Join(resultErr, closeUnpublishedCheckpointStore(unboundCheckpointStore, forceCleanup, retryCleanup))
			}
			for _, manager := range acquiredManagers {
				resultErr = errors.Join(resultErr, closeUnpublishedFileSeriesManager(manager, forceCleanup, retryCleanup))
			}
			resultErr = errors.Join(
				resultErr,
				closeObjectStorageCompressedLogFileUploadFailureRouteOwners(acquiredRouteOwners),
			)
		}
		if panicked != nil {
			panic(panicked)
		}
	}()

	if opts.Config == nil {
		return nil, ErrNilConfig
	}
	// Clockのタイムゾーンはupload失敗archiveの保持期限判定にも使います。
	if opts.Clock == nil {
		opts.Clock = clock.NewUTCClock()
	}
	if err := validateLogDirectories(opts.Config.Layout()); err != nil {
		return nil, err
	}
	if err := pkgfsstore.ValidateUsageCacheTTL(
		opts.Config.Purge().UsageCacheTTL(),
	); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidUsageCacheConfiguration, err)
	}
	if err := pkgfsstore.ValidateFileRotationWeekday(
		opts.Config.Rotation().FileRotationType(),
		opts.Config.Rotation().FileRotationDayOfWeek(),
	); err != nil {
		return nil, fmt.Errorf("appendlog lifecycle rotation config: %w", err)
	}
	if opts.validateDefaultPurge {
		// default系列の保持設定も、error/debugと同じ下限で検証します。分離が無効なら
		// default系列だけを管理するため、ここで検証しないと0の保持設定を受け付けてしまいます。
		if err := infrafsstore.ValidateFileLifecycleConfigProvider(defaultLogFilePurgeValidationConfig{
			FilePurgeConfigProvider: opts.Config.Purge(),
			maxFileSize:             opts.Config.Lifecycle().MaxFileSize(),
		}); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidDefaultLogPurgeConfiguration, err)
		}
	}

	// file名に使う設定値は保存先directoryの解決より前に検証します。targetはdirectory名にも
	// 使うため、Windowsではcolonを含むtargetがpathの構文errorとなり、名前の検証より先に失敗します。
	if err := appendlog.ValidateLogFileNameConfigProvider(opts.Config.Layout()); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidFileNameConfig, err)
	}
	// 保持設定が不正な場合にdirectoryを作らないよう、ここではread-onlyで解決します。
	// alias経由で同じ保存先を使う系列も、通常ログのdirectory容量上限を共有します。
	layout := opts.Config.Layout()
	resolveDirectory := func(enabled bool, directory string) (string, error) {
		if !enabled {
			return "", nil
		}
		return pathidentity.ResolveDirectory(filepath.Join(layout.LogPath(), layout.Target(), directory))
	}
	normalDirectory, normalErr := resolveDirectory(true, layout.LogDir())
	errorDirectory, errorErr := resolveDirectory(layout.IsSeparateErrorLogsEnabled(), layout.ErrorLogDir())
	debugDirectory, debugErr := resolveDirectory(layout.IsSeparateDebugLogsEnabled(), layout.DebugLogDir())
	if err := errors.Join(normalErr, errorErr, debugErr); err != nil {
		return nil, fmt.Errorf("resolve appendlog lifecycle directories: %w", err)
	}
	opts.Config = appendlog.WithResolvedLogFilePurgeDirectories(
		opts.Config, normalDirectory, errorDirectory, debugDirectory,
	)
	if err := appendlog.ValidateLogFilePurgeConfigs(opts.Config); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRoleLogPurgeConfiguration, err)
	}
	targets, err := lifecycleTargets(opts.Config)
	if err != nil {
		return nil, err
	}
	// 新しい宛先の登録境界は、構築時刻が属するrotation期間で全系列に共通です。
	activePeriod, err := currentRotationPeriod(opts.Config, opts.Clock.Now())
	if err != nil {
		return nil, err
	}
	type directoryGroup struct {
		fileSeriesManagers                        []*FileSeriesManager
		compressedLogFileUploadFailureRouteOwners []ObjectStorageUploadFailureRouteOwner
	}
	groups := make(map[string]*directoryGroup, len(targets))
	groupOrder := make([]string, 0, len(targets))
	for _, target := range targets {
		purgeConfig := opts.Config.Purge()
		switch target.class {
		case fileClassError:
			purgeConfig = opts.Config.ErrorPurge()
		case fileClassDebug:
			purgeConfig = opts.Config.DebugPurge()
		}
		targetUploadCfg := uploadTargetConfigForFileClass(
			opts.ObjectStorageUpload,
			target.class,
		)
		rawUploader, err := objectStorageUploaderForFileClass(
			opts.ObjectStorageUpload,
			target.class,
		)
		if err != nil {
			return nil, err
		}
		checkpointOptions := archiveCheckpointFactoryOptions{
			disableSaveRetry:  opts.manualRecovery && !opts.checkpointSaveRetry,
			acceptPendingSave: opts.manualRecovery && opts.checkpointSaveRetry,
			activePeriod:      activePeriod,
		}
		checkpointStore, err := newArchiveCheckpointStore(
			opts.ArchiveCheckpoint,
			string(target.class),
			target.fileName,
			opts.Config.Layout(),
			rawUploader,
			opts.Reporter,
			checkpointOptions,
		)
		if err != nil {
			return nil, fmt.Errorf("create %s archive checkpoint: %w", target.class, err)
		}
		// managerへ渡し終えるまで、wrapや依存のpanicでもこのfactoryがStoreを解放します。
		unboundCheckpointStore = checkpointStore
		objectStorageUploader, routeOwner, err := WrapObjectStorageUploader(
			targetUploadCfg,
			opts.SpoolRuntimes,
			rawUploader,
			target.fileName,
			opts.Clock,
			opts.Reporter,
			checkpointStore,
			opts.manualRecovery,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"create %s object storage compressed log file upload failure store: %w",
				target.class,
				err,
			)
		}
		if routeOwner != nil {
			acquiredRouteOwners = append(acquiredRouteOwners, routeOwner)
		}
		fileSeriesManager := NewFileSeriesManager(FileSeriesManagerOptions{
			Config:          appendlog.WithPurgeConfig(opts.Config, purgeConfig),
			FileName:        target.fileName,
			Uploader:        objectStorageUploader,
			CheckpointStore: checkpointStore,
			Reporter:        opts.Reporter,
			Clock:           opts.Clock,
			Tracer:          opts.Tracer,
			Meter:           opts.Meter,
		})
		fileSeriesManager.manualRecovery = opts.manualRecovery
		acquiredManagers = append(acquiredManagers, fileSeriesManager)
		unboundCheckpointStore = nil

		dir := pathidentity.ComparisonKey(target.fileName.Dir)
		group := groups[dir]
		if group == nil {
			group = &directoryGroup{}
			groups[dir] = group
			groupOrder = append(groupOrder, dir)
		}
		group.fileSeriesManagers = append(group.fileSeriesManagers, fileSeriesManager)
		if routeOwner != nil {
			group.compressedLogFileUploadFailureRouteOwners = append(group.compressedLogFileUploadFailureRouteOwners, routeOwner)
		}
	}

	lifecycles = make([]*DirectoryLifecycle, 0, len(groupOrder))
	for _, dir := range groupOrder {
		directoryLifecycle, err := NewDirectoryLifecycle(groups[dir].fileSeriesManagers...)
		if err != nil {
			return nil, err
		}
		directoryLifecycle.checkpointSaveRetryEnabled = opts.manualRecovery && opts.checkpointSaveRetry
		directoryLifecycle.ownObjectStorageCompressedLogFileUploadFailureRoutes(
			groups[dir].compressedLogFileUploadFailureRouteOwners...,
		)
		lifecycles = append(lifecycles, directoryLifecycle)
	}
	return lifecycles, nil
}

// LogFileOptionsは通常FileLoggerのログ系列を処理するDirectoryLifecycleの構築入力です。
// 保存・圧縮・送信・保持設定と実行時依存を受け取り、writerや周期Runnerは生成しません。
type LogFileOptions struct {
	Config pkgfsstore.LogFileLifecycleRunnerConfig
	Clock  clock.Clock
	// SpoolRuntimesは呼び出し元が構築・所有するbackend別の共有rootです。
	// spool退避を使うbackendのruntimeは必須です。spool退避が無効ならnilを渡せます。
	// 呼び出し元は、生成した全DirectoryLifecycleの終了後にruntimeを閉じてください。
	SpoolRuntimes compressedlog.RootRuntimes
	Tracer        pkgobserver.Tracer
	Meter         pkgobserver.Meter
	// Reporterは受理済みファイル処理の復旧結果と、checkpoint保存再試行のエラーを
	// 非同期で通知する宛先です。一巡処理の結果は戻り値で返します。nilなら通知しません。
	Reporter RunReporter
}

// NewLogFileLifecyclesは通常FileLoggerのrunner mode用設定から、正規化済みの
// 物理directoryごとにDirectoryLifecycleを作ります。Runnerへの包装や起動は行いません。
// checkpointを設定する場合、保存先は呼び出し前に準備してください。
func NewLogFileLifecycles(opts LogFileOptions) ([]*DirectoryLifecycle, error) {
	return newDirectoryLifecycles(directoryLifecycleFactoryOptions{
		Config:              appendlog.NewConfigAdapter(opts.Config.FsstoreConfig),
		ObjectStorageUpload: opts.Config.ObjectStorageUploadConfig,
		SpoolRuntimes:       opts.SpoolRuntimes,
		ArchiveCheckpoint:   opts.Config.ArchiveCheckpointConfig,
		Clock:               opts.Clock,
		Tracer:              opts.Tracer,
		Meter:               opts.Meter,
		Reporter:            opts.Reporter,
	})
}

// DeliveryFileOptionsはDeliveryFileLoggerのDirectoryLifecycleを構築する入力です。
// Delivery writerはinline lifecycleを実行しないため、
// compress、archive、purgeに必要な設定と実行時依存をwriter設定から分けて受け取ります。
type DeliveryFileOptions struct {
	Config pkgfsstore.DeliveryFileLifecycleRunnerConfig
	Clock  clock.Clock
	// SpoolRuntimesは呼び出し元が構築・所有するbackend別の共有rootです。
	// spool退避を使うbackendのruntimeは必須です。spool退避が無効ならnilを渡せます。
	// 呼び出し元は、生成した全DirectoryLifecycleの終了後にruntimeを閉じてください。
	SpoolRuntimes compressedlog.RootRuntimes
	Tracer        pkgobserver.Tracer
	Meter         pkgobserver.Meter
	// Reporterは受理済みファイル処理の復旧結果と、checkpoint保存再試行のエラーを
	// 非同期で通知する宛先です。一巡処理の結果は戻り値で返します。nilなら通知しません。
	Reporter RunReporter
}

// NewDeliveryFileLifecyclesはDeliveryFileLogger用設定から、正規化済みの
// 物理directoryごとにDirectoryLifecycleを作ります。Runnerへの包装や起動は行いません。
// default系列の保持設定もerror/debugと同じ下限で、directoryやcheckpointを作る前に検証します。
// checkpointを設定する場合、保存先は呼び出し前に準備してください。
func NewDeliveryFileLifecycles(opts DeliveryFileOptions) ([]*DirectoryLifecycle, error) {
	return newDirectoryLifecycles(directoryLifecycleFactoryOptions{
		validateDefaultPurge: true,
		Config:               appendlog.NewDeliveryFileLifecycleStoreConfigAdapter(opts.Config.FsstoreConfig),
		ObjectStorageUpload:  opts.Config.ObjectStorageUploadConfig,
		SpoolRuntimes:        opts.SpoolRuntimes,
		ArchiveCheckpoint:    opts.Config.ArchiveCheckpointConfig,
		Clock:                opts.Clock,
		Tracer:               opts.Tracer,
		Meter:                opts.Meter,
		Reporter:             opts.Reporter,
	})
}

// defaultLogFilePurgeValidationConfigは、default系列の保持設定と1fileの最大サイズを
// fsstoreの検証へ渡します。error/debugの検証と同じ下限を適用します。
type defaultLogFilePurgeValidationConfig struct {
	appendlog.FilePurgeConfigProvider
	maxFileSize uint64
}

func (c defaultLogFilePurgeValidationConfig) MaxFileSize() uint64 { return c.maxFileSize }

type lifecycleTarget struct {
	class    fileClass
	fileName logpattern.FileNamePattern
}

// lifecycleTargetsはConfigが現在管理するfile classとfile名patternを列挙します。
// defaultは常に対象とし、error/debugは分離ログ設定が有効な場合だけ対象にします。
//
// 分離を無効にすると、新しいerror/debug eventはwriter側でdefault fileへ集約されます。
// 以前のerror_/debug_ fileとcheckpointはdefaultへ移行せず、この列挙から外したまま
// 保持します。再び分離を有効にすれば同じfile classのcheckpointから処理を再開でき、
// 再有効化しない場合の旧file整理は運用者が明示的に行います。
func lifecycleTargets(cfg appendlog.ConfigProvider) ([]lifecycleTarget, error) {
	layout := cfg.Layout()
	if err := appendlog.ValidateLogFileNameConfigProvider(layout); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidFileNameConfig, err)
	}
	if err := validateLogDirectories(layout); err != nil {
		return nil, err
	}
	// targetの解決はdirectoryを作成するため、role配置の検証を先に済ませます。
	if layout.IsSeparateErrorLogsEnabled() && layout.IsSeparateDebugLogsEnabled() {
		defaultDir, err := pathidentity.ResolveDirectory(filepath.Join(layout.LogPath(), layout.Target(), layout.LogDir()))
		if err != nil {
			return nil, fmt.Errorf("resolve default log directory: %w", err)
		}
		errorDir, err := pathidentity.ResolveDirectory(filepath.Join(layout.LogPath(), layout.Target(), layout.ErrorLogDir()))
		if err != nil {
			return nil, fmt.Errorf("resolve error log directory: %w", err)
		}
		debugDir, err := pathidentity.ResolveDirectory(filepath.Join(layout.LogPath(), layout.Target(), layout.DebugLogDir()))
		if err != nil {
			return nil, fmt.Errorf("resolve debug log directory: %w", err)
		}
		if pathidentity.ComparisonKey(errorDir) == pathidentity.ComparisonKey(debugDir) &&
			pathidentity.ComparisonKey(errorDir) != pathidentity.ComparisonKey(defaultDir) {
			return nil, &RoleDirectoryConflictError{
				SharedRoles: [2]string{"error", "debug"}, SharedDirectory: errorDir,
				OtherRole: "default", OtherDirectory: defaultDir,
			}
		}
	}
	targets := make([]lifecycleTarget, 0, 3)

	defaultTarget, err := lifecycleTargetFor(layout.LogPath(), layout.Target(), layout.LogDir(), "", fileClassDefault)
	if err != nil {
		return nil, err
	}
	targets = append(targets, defaultTarget)

	if layout.IsSeparateErrorLogsEnabled() {
		errorTarget, err := lifecycleTargetFor(
			layout.LogPath(),
			layout.Target(),
			layout.ErrorLogDir(),
			layout.SeparateErrorLogsPrefix(),
			fileClassError,
		)
		if err != nil {
			return nil, err
		}
		targets = append(targets, errorTarget)
	}

	if layout.IsSeparateDebugLogsEnabled() {
		debugTarget, err := lifecycleTargetFor(
			layout.LogPath(),
			layout.Target(),
			layout.DebugLogDir(),
			layout.SeparateDebugLogsPrefix(),
			fileClassDebug,
		)
		if err != nil {
			return nil, err
		}
		targets = append(targets, debugTarget)
	}

	return targets, nil
}

func validateLogDirectories(layout appendlog.FsstoreLayoutConfigProvider) error {
	if err := errors.Join(
		pkgfsstore.ValidateLogDirectory(layout.LogDir()),
		pkgfsstore.ValidateDebugLogDirectory(layout.IsSeparateDebugLogDirEnabled(), layout.DebugLogDir()),
	); err != nil {
		return fmt.Errorf("appendlog lifecycle directory config: %w", err)
	}
	return nil
}

// lifecycleTargetForは、一つのfile classの保存directoryを解決し、
// FileSeriesManagerがそのfile系列を識別するための固定部分を作ります。
// 日付と世代番号は実行時に個々のfile名から取得するため、ここで保持するのは
// directory、prefix、target、extensionだけです。
func lifecycleTargetFor(logPath, target, logDir, prefix string, class fileClass) (lifecycleTarget, error) {
	dir, err := infrafsstore.GetFileSystemStoreDirectoryPath(logPath, target, logDir)
	if err != nil {
		return lifecycleTarget{}, fmt.Errorf("%w: %w", ErrInvalidFileName, err)
	}
	return lifecycleTarget{
		class: class,
		fileName: logpattern.FileNamePattern{
			Dir:       dir,
			Prefix:    prefix,
			Target:    target,
			Extension: defaultLogFileExtension,
		},
	}, nil
}

// objectStorageUploaderForFileClassは、指定file classのobject storage upload設定から
// uploaderを作ります。upload全体または指定file classが無効な場合はnilを返し、
// FileSeriesManagerはobject storage uploadを行いません。
func objectStorageUploaderForFileClass(uploadCfg objectstorage.LogFileUploadConfig, class fileClass) (uploader.Uploader, error) {
	if !uploadCfg.Enabled {
		return nil, nil
	}
	targetCfg := uploadTargetConfigForFileClass(uploadCfg, class)
	if !targetCfg.Enabled {
		return nil, nil
	}
	return uploadercompose.FromTargets(objectstorage.NewUploadTargetConfigAdapter(targetCfg), s3uploaderbuilder.Builders())
}

type archiveCheckpointFactoryOptions struct {
	disableSaveRetry  bool
	acceptPendingSave bool
	// activePeriodは、構築時刻が属するrotation期間の開始日（YYYYMMDD）です。
	// 新しい宛先の登録では、この期間以降の元ログだけを書き込み中の可能性がある世代として扱います。
	activePeriod string
}

// currentRotationPeriodは、configのrotation設定でatが属する期間の開始日を返します。
// writerがfile名に使う期間と同じ規則で、新しい宛先の登録境界の判定に使います。
func currentRotationPeriod(cfg appendlog.ConfigProvider, at time.Time) (string, error) {
	period, err := logpattern.ResolveDateStringForFileNameAt(appendlog.NewNameLogPatternConfigAdapter(cfg), at)
	if err != nil {
		return "", fmt.Errorf("resolve appendlog current rotation period: %w", err)
	}
	return period, nil
}

// newArchiveCheckpointStoreは、この保存先・命名規則に属するログファイルの
// upload成功記録を扱います。通常ログのsource ID例は logger_file_default-<SHA-256> です。
// upload無効時は、既存の記録も開かずにそのまま残します。
// reporterがある場合は、保存再試行のOnErrorと非同期通知をここで組み立てます。
// 返すStoreのCloseで受理済み保存を完了してから通知受付を閉じます。
func newArchiveCheckpointStore(
	cfg pkgfsstore.ArchiveCheckpointConfig,
	class string,
	fileName logpattern.FileNamePattern,
	layout appendlog.FsstoreLayoutConfigProvider,
	objectStorageUploader uploader.Uploader,
	reporter RunReporter,
	options ...archiveCheckpointFactoryOptions,
) (createdStore checkpointstore.Store, resultErr error) {
	var settings archiveCheckpointFactoryOptions
	if len(options) > 0 {
		settings = options[0]
	}
	var acquiredStore checkpointstore.Store
	defer func() {
		panicked := recover()
		if acquiredStore != nil && (resultErr != nil || panicked != nil) {
			resultErr = errors.Join(resultErr, closeUnpublishedCheckpointStore(
				acquiredStore, settings.acceptPendingSave || panicked != nil, settings.acceptPendingSave,
			))
		}
		if panicked != nil {
			panic(panicked)
		}
	}()
	directoryConfigured := strings.TrimSpace(cfg.Directory) != ""
	sourceConfigured := strings.TrimSpace(cfg.SourcePrefix) != ""
	if objectStorageUploader == nil && !directoryConfigured && !sourceConfigured {
		return nil, nil
	}
	if !directoryConfigured || !sourceConfigured {
		return nil, fmt.Errorf("%w: %w", ErrInvalidCheckpointConfiguration, ErrIncompleteCheckpointConfiguration)
	}
	if err := validateArchiveCheckpointFileNaming(fileName, layout); err != nil {
		return nil, err
	}
	sourceID := archiveCheckpointSourceID(cfg.SourcePrefix, class, fileName)
	if objectStorageUploader == nil {
		if err := checkpointstore.ValidateSourceID(sourceID); err != nil {
			return nil, err
		}
		directory, err := filepath.Abs(cfg.Directory)
		if err != nil {
			return nil, err
		}
		if _, err := pathidentity.ResolveAndValidateManagedDirectory(filepath.Dir(directory), filepath.Base(directory)); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var onError func(error)
	if reporter != nil {
		onError = func(err error) {
			reporter.ReportRun(context.Background(), RunReport{DirectoryPath: fileName.Dir}, err)
		}
	}
	storeOptions := []checkpointstore.FilesystemStoreOption{
		checkpointstore.WithMissingCheckpointRecovery(cfg.AutoRecoverMissingCheckpoint),
	}
	if settings.disableSaveRetry {
		storeOptions = append(storeOptions, checkpointstore.WithSaveRetryDisabled())
	}
	checkpointStore, err := logfile.NewCheckpointStore(
		cfg.Directory,
		sourceID,
		onError,
		storeOptions...,
	)
	if err != nil {
		return nil, err
	}
	acquiredStore = checkpointStore
	client, ok := objectStorageUploader.(uploader.DestinationUploader)
	if !ok {
		return nil, ErrInvalidCheckpointConfiguration
	}
	err = appendlog.DirectoryGuardForPath(fileName.Dir).RunWithLifecycleLock(func() error {
		existingKeys, err := checkpointstore.ScanRegistrationFileKeys(fileName, settings.activePeriod)
		if err != nil {
			return err
		}
		return checkpointStore.RegisterDestinations(context.Background(), client.DestinationIDs(), existingKeys)
	})
	if err != nil {
		if settings.acceptPendingSave && checkpointstore.IsPendingSaveOnly(err) {
			// 宛先登録はmemoryへ受理済みです。所有者を公開して停止要求を受けられるようにし、
			// RecoverOnceは保存完了まで待機します。ここでCloseして無期限に構築を止めません。
			return checkpointStore, nil
		}
		return nil, fmt.Errorf("register log file upload destinations: %w", err)
	}
	return checkpointStore, nil
}

// closeUnpublishedCheckpointStoreは、構築失敗で返せないStoreを解放します。
// 再試行を有効にした手動ownerは公開前には外から停止できないため、保存待ちを中止します。
func closeUnpublishedCheckpointStore(store checkpointstore.Store, force, waitNotifications bool) error {
	var err error
	if force {
		if stoppable, ok := store.(interface{ ForceClose() error }); ok {
			err = stoppable.ForceClose()
		}
	}
	err = errors.Join(err, store.Close())
	if waitNotifications {
		if notifications, ok := store.(interface{ NotificationsDone() <-chan struct{} }); ok {
			<-notifications.NotificationsDone()
		}
	}
	return err
}

// closeUnpublishedFileSeriesManagerは構築失敗・panicで外へ返せないmanagerを解放します。
// 手動再試行では、構築の再試行や最終結果の後へ通知が残らないよう排出完了も待ちます。
func closeUnpublishedFileSeriesManager(manager *FileSeriesManager, force, waitNotifications bool) error {
	var err error
	if force {
		err = manager.ForceClose()
	}
	err = errors.Join(err, manager.Close())
	if waitNotifications {
		if manager.logFileProcessingRecovery != nil {
			<-manager.logFileProcessingRecovery.NotificationsDone()
		}
		if manager.objectStorageUpload != nil {
			manager.objectStorageUpload.stateMu.Lock()
			store := manager.objectStorageUpload.checkpointStore
			manager.objectStorageUpload.stateMu.Unlock()
			if notifications, ok := store.(interface{ NotificationsDone() <-chan struct{} }); ok {
				<-notifications.NotificationsDone()
			}
		}
	}
	return err
}

// validateArchiveCheckpointFileNamingは、FileSeriesManagerが対象file名からPeriodとIndexを
// 取得できる命名規則であることを、Runnerの構築時に検証します。
//
// FileNameFormatへ固定のvalidationPeriodとvalidationIndexを代入し、生成結果が
// parseFileGenerationの前提とする`/<prefix><target>_<period>_<index>.<extension>`と
// 一致するか確認します。application mappingを経由せずexported factoryを
// 直接使った場合も、命名規則の不一致によって対象fileを黙って無視することを
// 防ぎます。
func validateArchiveCheckpointFileNaming(
	fileName logpattern.FileNamePattern,
	layout appendlog.FsstoreLayoutConfigProvider,
) error {
	const (
		validationPeriod = "20000101"
		validationIndex  = 1
	)

	if layout == nil {
		return fmt.Errorf("%w: file layout is nil", ErrInvalidCheckpointConfiguration)
	}
	if layout.FileDateFormat() != pkgfsstore.FileDateFormat {
		return fmt.Errorf(
			"%w: archive checkpoint requires file date format %q, got %q",
			ErrInvalidCheckpointConfiguration,
			pkgfsstore.FileDateFormat,
			layout.FileDateFormat(),
		)
	}
	extension := strings.TrimPrefix(fileName.Extension, ".")
	want := fmt.Sprintf(
		"/%s%s_%s_%d.%s",
		fileName.Prefix,
		fileName.Target,
		validationPeriod,
		validationIndex,
		extension,
	)
	got := fmt.Sprintf(
		layout.FileNameFormat(),
		fileName.Prefix,
		fileName.Target,
		validationPeriod,
		validationIndex,
	)
	if got != want {
		return fmt.Errorf(
			"%w: archive checkpoint cannot parse file name format %q (example %q, want %q)",
			ErrInvalidCheckpointConfiguration,
			layout.FileNameFormat(),
			got,
			want,
		)
	}
	return nil
}

// archiveCheckpointSourceIDは保存先・命名規則ごとのupload成功記録を識別します。
// directory、prefix、target、extensionが変われば別IDになります。
// 生成規則はinlineと共通のcheckpointstore.ArchiveCheckpointSourceIDを使用します。
func archiveCheckpointSourceID(
	sourcePrefix string,
	class string,
	fileName logpattern.FileNamePattern,
) string {
	return checkpointstore.ArchiveCheckpointSourceID(
		sourcePrefix,
		class,
		fileName,
	)
}

// uploadTargetConfigForFileClassは、default/error/debugの分類に対応する
// object storage upload設定を返します。
func uploadTargetConfigForFileClass(uploadCfg objectstorage.LogFileUploadConfig, class fileClass) objectstorage.UploadTargetConfig {
	switch class {
	case fileClassError:
		return uploadCfg.Error
	case fileClassDebug:
		return uploadCfg.Debug
	default:
		return uploadCfg.Default
	}
}
