package lifecycle

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	failurearchiveuploader "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// FailureArchiveRunnerConfigProviderは、archiveの保存・保持設定とrunnerの実行設定を
// 必須の契約としてまとめます。runner設定を型アサーションで補う必要はありません。
type FailureArchiveRunnerConfigProvider interface {
	failurearchive.FileArchiveConfigProvider
	LifecycleRunnerConfig() pkgfsstore.AppendLogRunnerConfig
	ObjectStorageUpload() objectstorage.UploadTargetConfig
	ArchiveCheckpoint() pkgfsstore.ArchiveCheckpointConfig
}

// FailureArchiveOptionsは、failure archiveのretryable/permanent directoryを
// 既存appendlog lifecycleで管理するための設定と実行時依存です。
// writerの生成・起動は行わず、compress、object storage upload、purgeだけを扱います。
type FailureArchiveOptions struct {
	Config FailureArchiveRunnerConfigProvider
	Clock  clock.Clock
	Tracer pkgobserver.Tracer
	Meter  pkgobserver.Meter
	// SpoolRuntimesは呼び出し元が構築・所有する送信backend別の共有spoolです。
	// spool退避を使うbackendのruntimeは必須です。spool退避が無効ならnilを渡せます。
	// 呼び出し元は、生成した全DirectoryLifecycleの終了後にruntimeを閉じてください。
	SpoolRuntimes compressedlog.RootRuntimes
	// Reporterは受理済みファイル処理の復旧結果と、checkpoint保存再試行のエラーを
	// 非同期で通知する宛先です。一巡処理の結果は戻り値で返します。
	// nilなら通知しません。
	Reporter RunReporter
}

type failureArchiveTarget struct {
	failureCategory failurearchive.FailureCategory
	terminalDir     string
}

// failureArchiveTargetsは、failure archive lifecycleが管理するretryable/permanentの
// 物理保存先を同じ順序で返します。
func failureArchiveTargets(cfg failurearchive.FileArchiveConfigProvider) []failureArchiveTarget {
	return []failureArchiveTarget{
		{
			failureCategory: failurearchive.FailureCategoryRetryable,
			terminalDir:     cfg.DirCaseRetryable(),
		},
		{
			failureCategory: failurearchive.FailureCategoryPermanent,
			terminalDir:     cfg.DirCasePermanent(),
		},
	}
}

// NewFailureArchiveLifecyclesはretryable/permanentの物理directoryごとに、
// failure archiveと同じJSONL命名規則を扱うDirectoryLifecycleを作ります。
// 各FileSeriesManagerは既存appendlog実装を使ってcompress、upload、purgeを行います。
// checkpoint保存先は呼び出し元が起動時に準備済みであることを前提とし、ここでは作成しません。
func NewFailureArchiveLifecycles(
	opts FailureArchiveOptions,
) ([]*DirectoryLifecycle, error) {
	return newFailureArchiveLifecyclesWithMode(opts, false)
}

func newFailureArchiveLifecyclesWithMode(
	opts FailureArchiveOptions,
	manualRecovery bool,
	options ...RecoveryOption,
) (lifecycles []*DirectoryLifecycle, resultErr error) {
	settings := resolveRecoveryOptions(options)
	var acquiredRouteOwners []ObjectStorageUploadFailureRouteOwner
	var acquiredManagers []*FileSeriesManager
	var unboundCheckpointStore checkpointstore.Store
	defer func() {
		panicked := recover()
		retryCleanup := manualRecovery && settings.checkpointSaveRetry
		if resultErr != nil || panicked != nil {
			forceCleanup := retryCleanup || panicked != nil
			if unboundCheckpointStore != nil {
				resultErr = errors.Join(resultErr, closeUnpublishedCheckpointStore(unboundCheckpointStore, forceCleanup, retryCleanup))
			}
			for _, manager := range acquiredManagers {
				resultErr = errors.Join(resultErr, closeUnpublishedFileSeriesManager(manager, forceCleanup, retryCleanup))
			}
			resultErr = errors.Join(resultErr, closeObjectStorageCompressedLogFileUploadFailureRouteOwners(acquiredRouteOwners))
		}
		if panicked != nil {
			panic(panicked)
		}
	}()
	if opts.Config == nil {
		return nil, ErrNilConfig
	}
	if err := failurearchive.ValidateLayoutConfigProvider(opts.Config); err != nil {
		return nil, fmt.Errorf("failure archive lifecycle layout config: %w", err)
	}
	if err := failurearchive.ValidateFileArchiveLifecycleConfigProvider(opts.Config); err != nil {
		return nil, err
	}
	cfg := newFailureArchiveLifecycleConfigAdapter(opts.Config)
	targets := failureArchiveTargets(opts.Config)
	uploadConfig := opts.Config.ObjectStorageUpload()
	registrationClock := opts.Clock
	if registrationClock == nil {
		registrationClock = clock.NewUTCClock()
	}
	// 新しい宛先の登録境界は、構築時刻が属するrotation期間で両分類に共通です。
	activePeriod, err := currentRotationPeriod(cfg, registrationClock.Now())
	if err != nil {
		return nil, err
	}

	lifecycles = make([]*DirectoryLifecycle, 0, len(targets))
	for _, target := range targets {
		dir, err := failurearchive.GetFileArchiveDirectoryPath(opts.Config, target.terminalDir)
		if err != nil {
			return nil, fmt.Errorf(
				"resolve %s failure archive lifecycle directory: %w",
				target.failureCategory,
				err,
			)
		}
		rawUploader, err := failurearchiveuploader.New(
			uploadConfig,
			target.failureCategory,
		)
		if err != nil {
			return nil, &FailureArchiveUploaderError{FailureCategory: target.failureCategory, Err: err}
		}
		fileName := logpattern.FileNamePattern{
			Dir:       dir,
			Prefix:    "",
			Target:    opts.Config.Target(),
			Extension: failurearchive.FileExtension,
		}
		checkpointOptions := archiveCheckpointFactoryOptions{
			disableSaveRetry:  manualRecovery && !settings.checkpointSaveRetry,
			acceptPendingSave: manualRecovery && settings.checkpointSaveRetry,
			activePeriod:      activePeriod,
		}
		checkpointStore, err := newArchiveCheckpointStore(
			opts.Config.ArchiveCheckpoint(),
			string(target.failureCategory),
			fileName,
			cfg.Layout(),
			rawUploader,
			opts.Reporter,
			checkpointOptions,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"create %s failure archive checkpoint: %w",
				target.failureCategory,
				err,
			)
		}
		unboundCheckpointStore = checkpointStore
		managedUploader, routeOwner, err := WrapObjectStorageUploader(
			uploadConfig, opts.SpoolRuntimes, rawUploader, fileName,
			opts.Clock, opts.Reporter, checkpointStore, manualRecovery,
		)
		if err != nil {
			return nil, fmt.Errorf("create %s failure archive upload failure stores: %w", target.failureCategory, err)
		}
		if routeOwner != nil {
			acquiredRouteOwners = append(acquiredRouteOwners, routeOwner)
		}

		fileSeriesManager := NewFileSeriesManager(FileSeriesManagerOptions{
			Config:          cfg,
			FileName:        fileName,
			Uploader:        managedUploader,
			CheckpointStore: checkpointStore,
			Reporter:        opts.Reporter,
			Clock:           opts.Clock,
			Tracer:          opts.Tracer,
			Meter:           opts.Meter,
		})
		fileSeriesManager.manualRecovery = manualRecovery
		acquiredManagers = append(acquiredManagers, fileSeriesManager)
		unboundCheckpointStore = nil
		directoryLifecycle, err := NewDirectoryLifecycle(fileSeriesManager)
		if err != nil {
			return nil, fmt.Errorf(
				"create %s failure archive directory lifecycle: %w",
				target.failureCategory,
				err,
			)
		}
		directoryLifecycle.checkpointSaveRetryEnabled = manualRecovery && settings.checkpointSaveRetry
		if routeOwner != nil {
			directoryLifecycle.ownObjectStorageCompressedLogFileUploadFailureRoutes(routeOwner)
		}
		lifecycles = append(lifecycles, directoryLifecycle)
	}

	return lifecycles, nil
}

// newFailureArchiveLifecycleConfigAdapterはfailure archiveのimmutableな設定を、
// 既存appendlog ConfigAdapterが受け取る値へsnapshotします。独自codecがある場合は
// その同じinstanceをrunnerのFileStoreへ渡し、inlineと圧縮形式が分岐しないようにします。
// FileNamePatternには実directoryを明示するため、Layoutのpath値は命名・日付解決用です。
func newFailureArchiveLifecycleConfigAdapter(
	source failurearchive.FileArchiveConfigProvider,
) appendlog.ConfigProvider {
	var compressionCodec pkgfsstore.CompressionCodec
	if provider, ok := source.(failurearchive.CompressionCodecProvider); ok {
		compressionCodec = provider.CompressionCodec()
	}

	return appendlog.NewConfigAdapter(pkgfsstore.Config{
		Layout: pkgfsstore.FsstoreConfig{
			Target:         source.Target(),
			LogPath:        source.ArchivePath(),
			LogDir:         source.Dir(),
			FileNameFormat: source.FileNameFormat(),
			FileDateFormat: source.FileDateFormat(),
		},
		CompressionCodec: compressionCodec,
		Lifecycle: pkgfsstore.FileLifecycleConfig{
			EnableRotate:       source.EnableRotate(),
			RotationType:       source.RotationType(),
			RotationDayOfWeek:  source.RotationDayOfWeek(),
			MaxFileBackups:     source.MaxFileBackups(),
			MaxFileAge:         source.MaxFileAge(),
			MaxFileSize:        source.MaxFileSize(),
			MaxDirSize:         source.MaxDirSize(),
			EnableFileCompress: source.EnableFileCompress(),
			CompressFormat:     source.CompressFormat(),
		},
		Retry: pkgfsstore.RetryConfig{
			MaxWriteRetries:            source.MaxWriteRetries(),
			FileOperationRetryWaitTime: source.FileOperationRetryWaitTime(),
		},
	})
}
