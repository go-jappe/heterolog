package mappedvalidation

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/filelifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

// validateMappedInternalLogFileConfigは、同期・非同期loggerの内部エラーや警告などを、
// 通常ログとは別のfile loggerへ出力する場合の設定を検証します。
// 専用設定がなければ、この検証は行いません。通常側のfile設定は呼び出し元で別途扱います。
//
// 最初に圧縮・削除などの実行方式（inline・runner・disable）を検証します。
// その後、ファイル名・通常ログとdebugの保存先・checkpointの保存先などを確認し、inline/runnerで
// 使用する保持設定・使用量cacheの更新間隔・upload設定も検証します。
// 定期実行や自動再起動の設定はrunnerの場合だけ検証します。
// 実際にファイルを開いたりuploadしたりする処理ではありません。
func validateMappedInternalLogFileConfig(
	sourceLabel string,
	destinations *pkglogger.InternalLogDestinationConfig,
) error {
	if destinations == nil || destinations.File == nil {
		return nil
	}
	fileCfg := destinations.File
	if err := validateMappedFileLifecycleMode(
		sourceLabel+" internal log file",
		fileCfg.LifecycleMode,
	); err != nil {
		return err
	}
	fileSource := sourceLabel + " internal log file"
	var errs []error
	errs = append(errs, validateMappedLogFileOverloadConfig(fileSource, fileCfg.FsstoreConfig.Overload))
	errs = append(errs, validateMappedLogFileDirectories(fileSource, fileCfg.FsstoreConfig.Layout))
	errs = append(errs, validateMappedLogFileNameConfig(fileSource, fileCfg.FsstoreConfig.Layout))
	if filelifecycle.UsesUsageCache(fileCfg.LifecycleMode) {
		errs = append(errs, validateMappedLogFilePurgeConfigs(
			fileSource,
			appendlog.NewConfigAdapter(fileCfg.FsstoreConfig),
		))
		errs = append(errs, validateMappedUsageCacheTTL(
			fileSource,
			fileCfg.FsstoreConfig.Lifecycle.UsageCacheTTL,
		))
	}
	errs = append(errs, validateMappedAppendLogRunnerConfig(
		fileSource,
		fileCfg.LifecycleMode,
		fileCfg.LifecycleRunnerSettings,
	))
	if filelifecycle.UsesObjectStorageUpload(fileCfg.LifecycleMode) {
		errs = append(errs, objectstoragevalidation.ValidateLogFileUploadConfig(
			fileSource,
			fileCfg.ObjectStorageUploadConfig,
			fileCfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled,
			fileCfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled,
		))
	}
	errs = append(errs, validateMappedArchiveCheckpointConfig(
		fileSource,
		fileCfg.ArchiveCheckpointConfig,
	))
	return errors.Join(errs...)
}

// validateMappedDeliveryInternalLogFileConfigは、Delivery loggerの内部エラーや警告などを
// 専用のfile loggerへ出力する場合の設定を検証します。専用設定がなければ何もしません。
//
// Deliveryのfile loggerではinlineを使えないため、実行方式はrunnerまたはdisableに限定します。
// ファイル名・通常ログとdebugの保存先・checkpointの保存先はどちらでも検証し、保持設定・使用量cacheの
// 更新間隔・upload・定期実行の設定はrunnerの場合だけ検証します。
// 通常ログとは別の設定なので、通常側の検証結果だけで済ませず、こちらも個別に確認します。
func validateMappedDeliveryInternalLogFileConfig(
	sourceLabel string,
	destinations *pkglogger.DeliveryInternalLogDestinationConfig,
) error {
	if destinations == nil || destinations.File == nil {
		return nil
	}
	fileCfg := destinations.File
	if err := validateMappedDeliveryFileLifecycleMode(
		sourceLabel+" internal log file",
		fileCfg.LifecycleMode,
	); err != nil {
		return err
	}
	lifecycleCfg := fileCfg.LifecycleRunnerConfig()
	fileSource := sourceLabel + " internal log file"
	var errs []error
	errs = append(errs, validateMappedLogFileDirectories(fileSource, fileCfg.FsstoreConfig.Layout))
	errs = append(errs, validateMappedLogFileNameConfig(fileSource, fileCfg.FsstoreConfig.Layout))
	errs = append(errs, validateMappedAppendLogRunnerConfig(
		fileSource,
		fileCfg.LifecycleMode,
		fileCfg.LifecycleRunnerSettings.RunnerConfig,
	))
	if lifecycleCfg != nil {
		errs = append(errs, validateMappedLogFilePurgeConfigs(
			fileSource,
			appendlog.NewDeliveryFileLifecycleStoreConfigAdapter(lifecycleCfg.FsstoreConfig),
		))
		errs = append(errs, validateMappedUsageCacheTTL(
			fileSource,
			lifecycleCfg.FsstoreConfig.Lifecycle.UsageCacheTTL,
		))
		errs = append(errs, objectstoragevalidation.ValidateLogFileUploadConfig(
			fileSource,
			lifecycleCfg.ObjectStorageUploadConfig,
			lifecycleCfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled,
			lifecycleCfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled,
		))
	}
	errs = append(errs, validateMappedArchiveCheckpointConfig(
		fileSource,
		fileCfg.LifecycleRunnerSettings.ArchiveCheckpointConfig,
	))
	return errors.Join(errs...)
}
