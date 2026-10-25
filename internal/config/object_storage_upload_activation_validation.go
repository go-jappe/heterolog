package config

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/filelifecycle"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// validateLogFileObjectStorageUploadActivationは、実際に起動するfile lifecycleに
// object storage uploadを有効化した場合、少なくとも一つの物理file classにupload先が
// 割り当てられていることを検証します。
//
// 個々のupload targetやS3設定の妥当性はmapperとpkg/objectstorage側で検証します。
// ここで扱うのは、loggerの選択状態、lifecycle mode、error/debug file分離設定を
// 組み合わせなければ判断できないapplication-levelの整合性だけです。
func validateLogFileObjectStorageUploadActivation(a AppConfig) error {
	syncStore := a.Logger.FileConfig.FileSystemStoreConfig
	asyncStore := a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig
	deliveryStore := a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig

	return errors.Join(
		validateLogFileUploadActivation(
			pkglogger.LoggerContractSync.DisplayName()+" file",
			requiresLoggerTargetConfig(
				a.Logger.LogOutputDestinations,
				a.Logger.HandlerTargetDestinations,
				pkglogger.LogOutputFile,
			) && filelifecycle.UsesObjectStorageUpload(syncStore.LifecycleMode),
			syncStore.ObjectStorageUploadConfig,
			syncStore.IsSeparateErrorLogsEnabled,
			syncStore.IsSeparateDebugLogsEnabled,
		),
		validateLogFileUploadActivation(
			pkglogger.LoggerContractAsyncBestEffort.DisplayName()+" file",
			requiresLoggerTargetConfig(
				a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
				a.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations,
				pkglogger.LogOutputFile,
			) && filelifecycle.UsesObjectStorageUpload(asyncStore.LifecycleMode),
			asyncStore.ObjectStorageUploadConfig,
			asyncStore.IsSeparateErrorLogsEnabled,
			asyncStore.IsSeparateDebugLogsEnabled,
		),
		validateLogFileUploadActivation(
			pkglogger.LoggerContractDelivery.DisplayName()+" file",
			requiresLoggerTargetConfig(
				a.DeliveryLogger.LogOutputDestinations,
				a.DeliveryLogger.HandlerTargetDestinations,
				pkglogger.LogOutputFile,
			) && deliveryStore.LifecycleMode == pkgfsstore.FileLifecycleModeRunner,
			deliveryStore.ObjectStorageUploadConfig,
			deliveryStore.IsSeparateErrorLogsEnabled,
			deliveryStore.IsSeparateDebugLogsEnabled,
		),
	)
}

func validateLogFileUploadActivation(
	sourceLabel string,
	lifecycleEnabled bool,
	cfg objectstorage.LogFileUploadConfig,
	separateError bool,
	separateDebug bool,
) error {
	if !lifecycleEnabled || !cfg.Enabled {
		return nil
	}
	if cfg.Default.Enabled ||
		separateError && cfg.Error.Enabled ||
		separateDebug && cfg.Debug.Enabled {
		return nil
	}
	return &FileUploadActivationError{Source: sourceLabel}
}

// ErrNoApplicableFileUploadは、file lifecycleのobject storage uploadを有効にしたのに、実際に
// 生成するdefault・error・debugのいずれのfile classでもuploadが有効でないことを表します。
var ErrNoApplicableFileUpload = errors.New(
	"object storage upload is enabled but no applicable file upload is enabled",
)

// FileUploadActivationErrorは、設定元Sourceのfile出力でErrNoApplicableFileUploadが起きたことを
// 表します。errors.IsでErrNoApplicableFileUploadと一致します。
type FileUploadActivationError struct {
	// Sourceは"logger file"など、uploadを有効にしたfile出力の設定元です。
	Source string
}

func (e *FileUploadActivationError) Error() string {
	return e.Source + " " + ErrNoApplicableFileUpload.Error()
}

func (e *FileUploadActivationError) Unwrap() error { return ErrNoApplicableFileUpload }
