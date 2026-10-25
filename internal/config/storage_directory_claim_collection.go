package config

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/filelifecycle"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// collectStorageDirectoryClaimsは、現在有効な設定に基づき、各構成内で同じ保存先を使う
// file role群またはstoreごとに、local filesystem上で占有するdirectoryを一覧にします。
// 戻り値のstorageDirectoryClaimは、そのrole群またはstoreをdirectoryの所有単位とする
// 所有権宣言です。この関数は宣言の収集だけを担当し、宣言同士の衝突判定は
// validateStorageDirectoryClaimsが行います。
//
// `.env`に値が残っていても、通常出力または内部handlerとして使われないfile設定、
// logger出力先として選択されていないbackend、DurableType=noneの設定は、
// runtimeでdirectoryを所有しないため列挙しません。
//
// file logger、failure archive、record spool、compressed log file upload failure storeの
// 種類を問わず、列挙したlocal filesystem上の保存領域はownerごとに独立して
// いなければなりません。S3のbucket/prefixについては
// local directoryの所有権宣言を生成せず、異なるownerによる重複を許容します。
func (a AppConfig) collectStorageDirectoryClaims() ([]storageDirectoryClaim, error) {
	claims := make([]storageDirectoryClaim, 0, 32)
	var errs []error
	// sync/async/Deliveryごとの「失敗時の保存設定を扱う必要があるか」（bool）です。
	// 有効な圧縮ログupload経路の有無を判定し、fileArchive/spoolの選択は後段で確認します。
	objectStorageCompressedLogRequired := objectStorageCompressedLogFileUploadFailureStoreRequirements(a)
	// 同じ契約ごとの「upload先サービス一覧」（例: s3）です。
	// 重複を除き、file archive/spoolの保存先をサービス別に分けるために使います。
	objectStorageCompressedLogFileUploadFailureServices := objectStorageCompressedLogFileUploadFailureServices(a)

	// file loggerはLoggerContractKind（sync、async best-effort、delivery）ごとに独立して
	// directoryを所有します。同じLoggerContractKind内ではdefaultを含むrole群だけが
	// file name prefixでdirectoryを共有できます。実際に有効なroleを列挙した後、
	// 正規化済みpath単位で所有権宣言をまとめます。
	appendFileClaims := func(
		source string,
		enabled bool,
		rawLayout pkgfsstore.FsstoreConfig,
	) {
		if !enabled {
			return
		}
		if err := errors.Join(
			pkgfsstore.ValidateLogDirectory(rawLayout.LogDir),
			pkgfsstore.ValidateDebugLogDirectory(rawLayout.IsSeparateDebugLogDirEnabled, rawLayout.DebugLogDir),
		); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", source, err))
			return
		}

		layout := appendlog.NewFsstoreConfigAdapter(rawLayout)
		fileClaims, err := newFileLogDirectoryClaims(
			source,
			layout.LogPath(),
			layout.Target(),
			layout.LogDir(),
			layout.ErrorLogDir(),
			layout.DebugLogDir(),
			layout.IsSeparateErrorLogsEnabled(),
			layout.IsSeparateDebugLogsEnabled(),
		)
		if err != nil {
			errs = append(errs, err)
		}
		claims = append(claims, fileClaims...)
	}

	appendContractFileClaims := func(
		contract pkglogger.LoggerContractKind,
		normalSelected bool,
		internalSelected bool,
		base logconfig.FileSystemStoreConfig,
		override logconfig.InternalLogDestinationConfig,
	) {
		layout := pkgfsstore.FsstoreConfig{
			LogPath:                      a.FsstoreConfig.LogPath,
			Target:                       a.RuntimeConfig.Target,
			LogDir:                       base.LogDir,
			ErrorLogDir:                  base.ErrorLogDir,
			DebugLogDir:                  base.DebugLogDir,
			IsSeparateDebugLogDirEnabled: base.IsSeparateDebugLogDirEnabled,
			IsSeparateErrorLogsEnabled:   base.IsSeparateErrorLogsEnabled,
			IsSeparateDebugLogsEnabled:   base.IsSeparateDebugLogsEnabled,
		}
		dedicatedInternal := internalSelected && internalLogFileDestinationOverridden(override, layout)
		// 専用internal logしか使用しない場合、未使用baseのdirectoryは解決しません。
		// 両方使用する場合だけ、表記が違っても実体が同じlayoutを共有へまとめます。
		if internalSelected && normalSelected {
			var err error
			dedicatedInternal, err = resolvedInternalLogFileDestinationOverridden(override, layout)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s internal log directory: %w", contract, err))
				return
			}
		}
		appendFileClaims(
			loggerOutputSource(contract, logconfig.LogOutputFile),
			normalSelected || (internalSelected && !dedicatedInternal),
			layout,
		)
		if dedicatedInternal {
			overrideInternalLogFileLayout(&layout, override)
			appendFileClaims(
				loggerInternalLogOutputSource(contract, logconfig.LogOutputFile),
				true,
				layout,
			)
		}
	}

	appendContractFileClaims(
		pkglogger.LoggerContractSync,
		a.Logger.LogOutputDestinations.Has(logconfig.LogOutputFile),
		loggerUsesInternalHandlerTarget(a.Logger, logconfig.LogOutputFile),
		a.Logger.FileConfig.FileSystemStoreConfig,
		a.Logger.InternalLogDestinationConfig,
	)
	appendContractFileClaims(
		pkglogger.LoggerContractAsyncBestEffort,
		a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(logconfig.LogOutputFile),
		asyncLoggerUsesInternalHandlerTarget(
			a.AsyncBestEffortMultiLoggerConfig,
			logconfig.LogOutputFile,
		),
		a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig,
		a.AsyncBestEffortMultiLoggerConfig.InternalLogDestinationConfig,
	)
	appendContractFileClaims(
		pkglogger.LoggerContractDelivery,
		a.DeliveryLogger.LogOutputDestinations.Has(logconfig.LogOutputFile),
		deliveryLoggerUsesInternalHandlerTarget(
			a.DeliveryLogger,
			logconfig.LogOutputFile,
		),
		logconfig.FileSystemStoreConfig{
			LogDir:                       a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LogDir,
			ErrorLogDir:                  a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.ErrorLogDir,
			DebugLogDir:                  a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.DebugLogDir,
			IsSeparateDebugLogDirEnabled: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.IsSeparateDebugLogDirEnabled,
			IsSeparateErrorLogsEnabled:   a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled,
			IsSeparateDebugLogsEnabled:   a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
		},
		a.DeliveryLogger.InternalLogDestinationConfig,
	)

	// 通常のfailure handlerはfileArchiveとspoolを複数選択できます。
	// DurableTypeに含まれるstoreだけについて、owner固有のdirectory所有権宣言を
	// 生成します。
	appendFailureClaims := func(
		source string,
		enabled bool,
		cfg failure.FailureHandlerConfig,
	) {
		if !enabled {
			return
		}

		if containsDurableType(cfg.DurableType, durable.DurableTypeFileArchive) {
			archiveClaim, err := a.newArchiveDirectoryClaim(source, cfg.Fsstore.FileArchiveConfig)
			if err != nil {
				errs = append(errs, err)
			} else {
				claims = append(claims, archiveClaim)
			}
		}

		if !containsDurableType(cfg.DurableType, durable.DurableTypeSpool) {
			return
		}

		claim, err := a.newSpoolDirectoryClaim(source, cfg.Fsstore.SpoolConfig)
		if err != nil {
			errs = append(errs, err)
		} else {
			claims = append(claims, claim)
		}
	}

	// Delivery failure handlerはbackendを問わずfileArchiveのみを所有します。
	// 実行境界にかかわらずwriterごとに専用directoryが必要なため、通常の
	// failure archiveと同じく他の保存先との重複を検証します。
	appendDeliveryArchiveClaims := func(
		source string,
		enabled bool,
		cfg failure.DeliveryFailureHandlerConfig,
	) {
		if !enabled || !containsDurableType(cfg.DurableType, durable.DurableTypeFileArchive) {
			return
		}

		archiveCfg := failure.FileArchiveConfig{
			Dir:              cfg.FileArchiveConfig.Dir,
			DirCaseRetryable: cfg.FileArchiveConfig.DirCaseRetryable,
			DirCasePermanent: cfg.FileArchiveConfig.DirCasePermanent,
		}
		archiveClaim, err := a.newArchiveDirectoryClaim(source, archiveCfg)
		if err != nil {
			errs = append(errs, err)
			return
		}
		claims = append(claims, archiveClaim)
	}

	for _, entry := range a.failureStoreCatalog() {
		if entry.support != failureStoreSupportSupported {
			continue
		}
		appendFailureClaims(
			entry.syncStore.source,
			entry.syncStore.selected,
			entry.syncStore.config,
		)
		appendFailureClaims(
			entry.asyncStore.source,
			entry.asyncStore.selected,
			entry.asyncStore.config,
		)
		appendDeliveryArchiveClaims(
			entry.deliveryStore.source,
			entry.deliveryStore.selected,
			entry.deliveryStore.config,
		)
	}

	// compressed log file upload failure storeは通常ログとbackend送信失敗archiveの
	// 圧縮ファイルを対象とし、physical rootとlifecycle coordinatorをobject storage
	// serviceとLoggerContractKindごとに共有します。
	// そのため、一つの契約に複数のログ系列があっても、選択された
	// durable typeごとに一つだけownership claimを追加します。宛先別のdirectoryへ元basenameで
	// 保存し、同名・別内容の既存ファイルは上書きせず保全します。
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync,
		pkglogger.LoggerContractAsyncBestEffort,
		pkglogger.LoggerContractDelivery,
	} {
		if !objectStorageCompressedLogRequired[contract] {
			continue
		}
		failureStore := a.CompressedLogFileUploadFailureStore.Standard
		if contract == pkglogger.LoggerContractDelivery {
			failureStore = a.CompressedLogFileUploadFailureStore.Delivery
		}
		if containsFailureStoreDurableType(
			failureStore.DurableType,
			failurestore.DurableTypeFileArchive,
		) {
			for _, service := range objectStorageCompressedLogFileUploadFailureServices[contract] {
				claim, err := a.newObjectStorageCompressedLogFileUploadFailureArchiveDirectoryClaim(
					contract,
					failureStore.DurableStoreConfig.FileArchiveConfig,
					service,
				)
				if err != nil {
					errs = append(errs, err)
				} else {
					claims = append(claims, claim)
				}
			}
		}
		if containsFailureStoreDurableType(
			failureStore.DurableType,
			failurestore.DurableTypeSpool,
		) {
			for _, service := range objectStorageCompressedLogFileUploadFailureServices[contract] {
				claim, err := a.newObjectStorageCompressedLogFileUploadSpoolDirectoryClaim(
					contract,
					service,
				)
				if err != nil {
					errs = append(errs, err)
				} else {
					claims = append(claims, claim)
				}
			}
		}
	}

	// archive checkpointの保存先はapplication内部の予約directoryです。現在checkpointを
	// 使うsourceがなくても常に一件だけ追加し、ほかの保存先への割り当てを禁止します。
	checkpointClaim, err := a.newArchiveCheckpointDirectoryClaim()
	if err != nil {
		errs = append(errs, err)
	} else {
		claims = append(claims, checkpointClaim)
	}

	return claims, errors.Join(errs...)
}

func compressedLogFileUploadFailureStoreRequired(
	fileSelected bool,
	lifecycleEnabled bool,
	cfg objectstorage.LogFileUploadConfig,
	separateError bool,
	separateDebug bool,
) bool {
	if !fileSelected || !lifecycleEnabled || !cfg.Enabled {
		return false
	}
	return cfg.Default.Enabled ||
		separateError && cfg.Error.Enabled ||
		separateDebug && cfg.Debug.Enabled
}

// objectStorageCompressedLogFileUploadFailureStoreRequirementsは、object storageへcompressed log fileを送信する
// producerが実際に構築され、かつそのlifecycleと物理upload targetが有効なlogger契約だけを
// 返します。config loadingとdirectory ownershipで同じ判定を共有し、未選択sourceに残った
// 設定をfailure storeの有効化へ波及させません。
func objectStorageCompressedLogFileUploadFailureStoreRequirements(
	a AppConfig,
) map[pkglogger.LoggerContractKind]bool {
	required := map[pkglogger.LoggerContractKind]bool{
		pkglogger.LoggerContractSync: compressedLogFileUploadFailureStoreRequired(
			requiresLoggerTargetConfig(
				a.Logger.LogOutputDestinations,
				a.Logger.HandlerTargetDestinations,
				pkglogger.LogOutputFile,
			),
			filelifecycle.UsesObjectStorageUpload(
				a.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode,
			),
			a.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
			a.Logger.FileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled,
			a.Logger.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
		),
		pkglogger.LoggerContractAsyncBestEffort: compressedLogFileUploadFailureStoreRequired(
			requiresLoggerTargetConfig(
				a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
				a.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
				pkglogger.LogOutputFile,
			),
			filelifecycle.UsesObjectStorageUpload(
				a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.
					LifecycleMode,
			),
			a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.
				ObjectStorageUploadConfig,
			a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.
				IsSeparateErrorLogsEnabled,
			a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.
				IsSeparateDebugLogsEnabled,
		),
		pkglogger.LoggerContractDelivery: compressedLogFileUploadFailureStoreRequired(
			requiresLoggerTargetConfig(
				a.DeliveryLogger.LogOutputDestinations,
				a.DeliveryLogger.HandlerTargetDestinations,
				pkglogger.LogOutputFile,
			),
			a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleMode ==
				pkgfsstore.FileLifecycleModeRunner,
			a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.
				ObjectStorageUploadConfig,
			a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.
				IsSeparateErrorLogsEnabled,
			a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.
				IsSeparateDebugLogsEnabled,
		),
	}

	for contract, uploads := range backendFailureArchiveUploadTargets(a) {
		if len(uploads) != 0 {
			required[contract] = true
		}
	}
	return required
}

// backendFailureArchiveUploadTargetsは、有効なbackendのfileArchiveがinlineまたはrunnerで
// object storageへ送信する設定を、logger契約ごとに返します。未選択の保存先は含めません。
func backendFailureArchiveUploadTargets(a AppConfig) map[pkglogger.LoggerContractKind][]objectstorage.UploadTargetConfig {
	uploads := make(map[pkglogger.LoggerContractKind][]objectstorage.UploadTargetConfig, 3)
	appendUpload := func(contract pkglogger.LoggerContractKind, selected bool, types []durable.DurableType, mode pkgfsstore.FileLifecycleMode, upload objectstorage.UploadTargetConfig) {
		if selected && containsDurableType(types, durable.DurableTypeFileArchive) &&
			filelifecycle.UsesObjectStorageUpload(mode) && upload.Enabled {
			uploads[contract] = append(uploads[contract], upload)
		}
	}
	for _, entry := range a.failureStoreCatalog() {
		if entry.support != failureStoreSupportSupported {
			continue
		}
		for contract, store := range map[pkglogger.LoggerContractKind]configuredFailureStore{
			pkglogger.LoggerContractSync:            entry.syncStore,
			pkglogger.LoggerContractAsyncBestEffort: entry.asyncStore,
		} {
			archive := store.config.Fsstore.FileArchiveConfig
			appendUpload(contract, store.selected, store.config.DurableType, archive.LifecycleMode, archive.ObjectStorageUploadConfig)
		}
		store := entry.deliveryStore
		archive := store.config.FileArchiveConfig
		appendUpload(pkglogger.LoggerContractDelivery, store.selected, store.config.DurableType, archive.LifecycleMode, archive.ObjectStorageUploadConfig)
	}
	return uploads
}

// objectStorageCompressedLogFileUploadFailureServicesは、実際にcompressed log file uploadを行うproducerから
// logger契約別のobject storage service集合を収集します。file archiveとspoolのrootは
// serviceごとに分離するため、同じserviceは一件へまとめます。
func objectStorageCompressedLogFileUploadFailureServices(
	a AppConfig,
) map[pkglogger.LoggerContractKind][]objectstorage.UploadTarget {
	services := make(map[pkglogger.LoggerContractKind][]objectstorage.UploadTarget, 3)
	seen := make(map[pkglogger.LoggerContractKind]map[objectstorage.UploadTarget]struct{}, 3)
	appendUpload := func(
		contract pkglogger.LoggerContractKind,
		upload objectstorage.UploadTargetConfig,
	) {
		if !upload.Enabled {
			return
		}
		if seen[contract] == nil {
			seen[contract] = make(map[objectstorage.UploadTarget]struct{})
		}
		for _, service := range upload.Targets {
			if _, exists := seen[contract][service]; exists {
				continue
			}
			seen[contract][service] = struct{}{}
			services[contract] = append(services[contract], service)
		}
	}
	appendLogFileUploads := func(
		contract pkglogger.LoggerContractKind,
		fileSelected bool,
		lifecycleEnabled bool,
		cfg objectstorage.LogFileUploadConfig,
		separateError bool,
		separateDebug bool,
	) {
		if !fileSelected || !lifecycleEnabled || !cfg.Enabled {
			return
		}
		appendUpload(contract, cfg.Default)
		if separateError {
			appendUpload(contract, cfg.Error)
		}
		if separateDebug {
			appendUpload(contract, cfg.Debug)
		}
	}

	appendLogFileUploads(
		pkglogger.LoggerContractSync,
		requiresLoggerTargetConfig(
			a.Logger.LogOutputDestinations,
			a.Logger.HandlerTargetDestinations,
			pkglogger.LogOutputFile,
		),
		filelifecycle.UsesObjectStorageUpload(
			a.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode,
		),
		a.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
		a.Logger.FileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled,
		a.Logger.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
	)
	appendLogFileUploads(
		pkglogger.LoggerContractAsyncBestEffort,
		requiresLoggerTargetConfig(
			a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
			a.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
			pkglogger.LogOutputFile,
		),
		filelifecycle.UsesObjectStorageUpload(
			a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LifecycleMode,
		),
		a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
		a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled,
		a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
	)
	appendLogFileUploads(
		pkglogger.LoggerContractDelivery,
		requiresLoggerTargetConfig(
			a.DeliveryLogger.LogOutputDestinations,
			a.DeliveryLogger.HandlerTargetDestinations,
			pkglogger.LogOutputFile,
		),
		a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleMode ==
			pkgfsstore.FileLifecycleModeRunner,
		a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
		a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled,
		a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
	)

	for contract, uploads := range backendFailureArchiveUploadTargets(a) {
		for _, upload := range uploads {
			appendUpload(contract, upload)
		}
	}
	return services
}

// overrideOrBaseは上書き設定を優先し、空文字の場合だけ基本設定を返します。
func overrideOrBase(override, base string) string {
	if override != "" {
		return override
	}
	return base
}
