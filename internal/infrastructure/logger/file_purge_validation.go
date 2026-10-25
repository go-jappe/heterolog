package logger

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// validateLoggerFilePurgeConfigsは、backend、checkpoint、uploadのresource取得前に
// 構築対象のfile保存先を検証します。internal-log専用保存先のdirectory容量は、
// 上書き後のlayoutから共有の有無を解決します。
func validateLoggerFilePurgeConfigs(sourceLabel string, cfg pkglogger.LoggerConfig) error {
	var errs []error
	if pkglogger.LoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputFile) {
		errs = append(errs, validateFileLoggerPurgeConfig(sourceLabel+" file", cfg.File))
	}
	if cfg.InternalLogDestinations != nil {
		errs = append(errs, validateFileLoggerPurgeConfig(
			sourceLabel+" internal log file", cfg.InternalLogDestinations.File,
		))
	}
	return errors.Join(errs...)
}

// filePurgeConfigFieldsは、file loggerの系列ごとの保持設定をbackend設定内のfield pathで表します。
var filePurgeConfigFields = map[appendlog.PurgeRole]pkglogger.ConfigField{
	appendlog.PurgeRoleError: "FsstoreConfig.Lifecycle.ErrorPurge",
	appendlog.PurgeRoleDebug: "FsstoreConfig.Lifecycle.DebugPurge",
}

// deliveryFilePurgeConfigFieldsは、delivery file loggerのlifecycle runnerへ渡す保持設定の
// field pathです。default系列の値はLifecycleRunnerSettingsに直接置きます。
var deliveryFilePurgeConfigFields = map[appendlog.PurgeRole]pkglogger.ConfigField{
	appendlog.PurgeRoleDefault: "LifecycleRunnerSettings",
	appendlog.PurgeRoleError:   "LifecycleRunnerSettings.ErrorPurge",
	appendlog.PurgeRoleDebug:   "LifecycleRunnerSettings.DebugPurge",
}

func validateFileLoggerPurgeConfig(sourceLabel string, cfg *pkglogger.FileLoggerConfig) error {
	if cfg == nil {
		return nil
	}
	switch cfg.LifecycleMode {
	case "", pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner:
		return filePurgeConfigFieldErrors(
			sourceLabel,
			filePurgeConfigFields,
			appendlog.ValidateLogFilePurgeConfigs(appendlog.NewConfigAdapter(cfg.FsstoreConfig)),
		)
	}
	return nil
}

// filePurgeConfigFieldErrorsは、系列ごとのappendlog.PurgeConfigErrorを、設定の出所と
// field pathを持つpkg/logger.ConfigFieldErrorへ変換します。errors.Asで出所と系列を、
// errors.Isでfsstoreの項目別sentinelを判定できます。文言は従来どおり
// "<source> lifecycle config is invalid: <系列> log purge config: ..."です。
func filePurgeConfigFieldErrors(
	sourceLabel string,
	fields map[appendlog.PurgeRole]pkglogger.ConfigField,
	err error,
) error {
	var errs []error
	for _, cause := range flattenJoinedErrors(err) {
		var field pkglogger.ConfigField
		var purgeErr *appendlog.PurgeConfigError
		if errors.As(cause, &purgeErr) {
			field = fields[purgeErr.Role]
		}
		errs = append(errs, &pkglogger.ConfigFieldError{
			Source:  sourceLabel,
			Backend: pkglogger.LogOutputFile,
			Field:   field,
			Detail:  "lifecycle config is invalid: " + cause.Error(),
			Err:     cause,
		})
	}
	return errors.Join(errs...)
}

// flattenJoinedErrorsはerrors.Joinでまとめたerrorを、入れ子のJoinも含めて個々のerrorへ
// 展開します。Joinでないerrorはそのまま一件として返し、nilなら空です。
func flattenJoinedErrors(err error) []error {
	if err == nil {
		return nil
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return []error{err}
	}
	var flattened []error
	for _, cause := range joined.Unwrap() {
		flattened = append(flattened, flattenJoinedErrors(cause)...)
	}
	return flattened
}

func validateDeliveryLoggerFilePurgeConfigs(sourceLabel string, cfg pkglogger.DeliveryLoggerConfig) error {
	var errs []error
	if pkglogger.DeliveryLoggerConfigRequiresBaseBackend(cfg, pkglogger.LogOutputFile) {
		errs = append(errs, validateDeliveryFileLoggerPurgeConfig(sourceLabel+" file", cfg.File))
	}
	if cfg.InternalLogDestinations != nil {
		errs = append(errs, validateDeliveryFileLoggerPurgeConfig(
			sourceLabel+" internal log file", cfg.InternalLogDestinations.File,
		))
	}
	return errors.Join(errs...)
}

// validateDeliveryFileLoggerPurgeConfigは、delivery writerとは別に渡すlifecycle runner設定の
// 保持設定を検証します。delivery writer自身は保持設定を持たないため、default系列も
// error/debugと同じ下限でここで検証します。分離が無効な場合もdefault系列の0の
// 保持世代数・保持日数や、1fileより小さい容量上限を受け付けません。
func validateDeliveryFileLoggerPurgeConfig(sourceLabel string, cfg *pkglogger.DeliveryFileLoggerConfig) error {
	lifecycleCfg := cfg.LifecycleRunnerConfig()
	if lifecycleCfg == nil {
		return nil
	}
	storeCfg := appendlog.NewDeliveryFileLifecycleStoreConfigAdapter(lifecycleCfg.FsstoreConfig)
	var errs []error
	if err := fsstore.ValidateFileLifecycleConfigProvider(deliveryDefaultPurgeValidationConfig{
		FilePurgeConfigProvider: storeCfg.Purge(),
		maxFileSize:             storeCfg.Lifecycle().MaxFileSize(),
	}); err != nil {
		errs = append(errs, &appendlog.PurgeConfigError{Role: appendlog.PurgeRoleDefault, Err: err})
	}
	if err := appendlog.ValidateLogFilePurgeConfigs(storeCfg); err != nil {
		errs = append(errs, err)
	}
	return filePurgeConfigFieldErrors(sourceLabel, deliveryFilePurgeConfigFields, errors.Join(errs...))
}

// deliveryDefaultPurgeValidationConfigは、delivery lifecycleのdefault系列の保持設定と
// 1fileの最大サイズをfsstoreの検証へ渡します。
type deliveryDefaultPurgeValidationConfig struct {
	appendlog.FilePurgeConfigProvider
	maxFileSize uint64
}

func (c deliveryDefaultPurgeValidationConfig) MaxFileSize() uint64 { return c.maxFileSize }
