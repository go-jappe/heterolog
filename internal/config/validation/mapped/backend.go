package mappedvalidation

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/filelifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

const fileSourceLabelSuffix = "file"

// ValidateSyncLoggerConfigは、同期・非同期loggerの設定の不足や不正値を、アプリ起動時に検証します。
// 読み込んだ値をpkglogger.LoggerConfigへ変換した後に呼び出します。
// 非同期loggerも同じ設定型へ変換して、この関数を使用します。
//
// 主な検証対象は次のとおりです。
//   - 使用するログ出力先に必要な設定と、送信に失敗した場合の再試行設定。
//   - ログファイルの命名、保持・削除条件、使用量cacheの更新間隔、定期処理・uploadの設定。
//   - logger内部で発生したエラーなどの通知先と、内部ログ専用のファイル保存設定。
//   - ログ送信に失敗したレコードを保存するfileArchive・spoolの設定。
//   - 圧縮ログのupload失敗時の保存先で、通常ログと内部ログのファイル名が衝突しないこと。
//
// 実際に使う出力先・保存方式・動作modeに応じて検証し、検出したエラーをまとめて返します。
// この関数では環境変数の読み込みやloggerの生成、外部サービスへの送信は行いません。
func ValidateSyncLoggerConfig(sourceLabel string, cfg pkglogger.LoggerConfig) error {
	var errs []error
	errs = append(errs, pkglogger.ValidateLoggerBackendConfigs(sourceLabel, cfg))
	if cfg.File != nil && pkglogger.LoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputFile) {
		fileCfg := cfg.File
		fileSourceLabel := sourceLabel + " " + fileSourceLabelSuffix
		errs = append(errs, validateMappedLogFileOverloadConfig(fileSourceLabel, fileCfg.FsstoreConfig.Overload))
		errs = append(errs, validateMappedLogFileDirectories(fileSourceLabel, fileCfg.FsstoreConfig.Layout))
		errs = append(errs, validateMappedLogFileNameConfig(fileSourceLabel, fileCfg.FsstoreConfig.Layout))
		if filelifecycle.UsesUsageCache(fileCfg.LifecycleMode) {
			errs = append(errs, validateMappedLogFilePurgeConfigs(
				fileSourceLabel,
				appendlog.NewConfigAdapter(fileCfg.FsstoreConfig),
			))
			errs = append(errs, validateMappedUsageCacheTTL(
				fileSourceLabel,
				fileCfg.FsstoreConfig.Lifecycle.UsageCacheTTL,
			))
		}
		errs = append(errs, validateMappedFileLifecycleMode(fileSourceLabel, fileCfg.LifecycleMode))
		errs = append(errs, validateMappedAppendLogRunnerConfig(
			fileSourceLabel,
			fileCfg.LifecycleMode,
			fileCfg.LifecycleRunnerSettings,
		))
		if filelifecycle.UsesObjectStorageUpload(fileCfg.LifecycleMode) {
			errs = append(errs, objectstoragevalidation.ValidateLogFileUploadConfig(
				fileSourceLabel,
				fileCfg.ObjectStorageUploadConfig,
				fileCfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled,
				fileCfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled,
			))
		}
		errs = append(errs, validateMappedArchiveCheckpointConfig(
			fileSourceLabel,
			fileCfg.ArchiveCheckpointConfig,
		))
	}
	errs = append(errs, pkglogger.ValidateLoggerHandlerRouting(sourceLabel, cfg))
	errs = append(errs, validateMappedInternalLogFileConfig(
		sourceLabel,
		cfg.InternalLogDestinations,
	))
	errs = append(errs, validateMappedSyncCompressedLogFileUploadNameCollisions(
		sourceLabel,
		cfg,
	))
	errs = append(errs, pkglogger.ValidateLoggerRetryClientConfigs(sourceLabel, cfg))
	errs = append(errs, validateMappedFailureStores(sourceLabel, cfg))
	return errors.Join(errs...)
}

// ValidateDeliveryLoggerConfigはDelivery loggerの変換済み設定に同じ検証境界を適用します。
func ValidateDeliveryLoggerConfig(
	sourceLabel string,
	cfg pkglogger.DeliveryLoggerConfig,
) error {
	var errs []error
	errs = append(errs, pkglogger.ValidateDeliveryLoggerBackendConfigs(sourceLabel, cfg))
	if cfg.File != nil && pkglogger.DeliveryLoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputFile) {
		fileCfg := cfg.File
		fileSourceLabel := sourceLabel + " " + fileSourceLabelSuffix
		errs = append(errs, validateMappedLogFileDirectories(fileSourceLabel, fileCfg.FsstoreConfig.Layout))
		errs = append(errs, validateMappedLogFileNameConfig(fileSourceLabel, fileCfg.FsstoreConfig.Layout))
		errs = append(
			errs,
			validateMappedDeliveryFileLifecycleMode(fileSourceLabel, fileCfg.LifecycleMode),
		)
		errs = append(errs, validateMappedAppendLogRunnerConfig(
			fileSourceLabel,
			fileCfg.LifecycleMode,
			fileCfg.LifecycleRunnerSettings.RunnerConfig,
		))
		if lifecycleCfg := fileCfg.LifecycleRunnerConfig(); lifecycleCfg != nil {
			errs = append(errs, validateMappedLogFilePurgeConfigs(
				fileSourceLabel,
				appendlog.NewDeliveryFileLifecycleStoreConfigAdapter(lifecycleCfg.FsstoreConfig),
			))
			errs = append(errs, validateMappedUsageCacheTTL(
				fileSourceLabel,
				lifecycleCfg.FsstoreConfig.Lifecycle.UsageCacheTTL,
			))
			errs = append(errs, objectstoragevalidation.ValidateLogFileUploadConfig(
				fileSourceLabel,
				lifecycleCfg.ObjectStorageUploadConfig,
				lifecycleCfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled,
				lifecycleCfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled,
			))
		}
		errs = append(errs, validateMappedArchiveCheckpointConfig(
			fileSourceLabel,
			fileCfg.LifecycleRunnerSettings.ArchiveCheckpointConfig,
		))
	}
	errs = append(errs, pkglogger.ValidateDeliveryLoggerHandlerRouting(sourceLabel, cfg))
	errs = append(errs, validateMappedDeliveryInternalLogFileConfig(
		sourceLabel,
		cfg.InternalLogDestinations,
	))
	errs = append(errs, validateMappedDeliveryCompressedLogFileUploadNameCollisions(
		sourceLabel,
		cfg,
	))
	errs = append(errs, pkglogger.ValidateDeliveryLoggerRetryClientConfigs(sourceLabel, cfg))
	errs = append(errs, validateMappedDeliveryFailureStores(sourceLabel, cfg))
	return errors.Join(errs...)
}
