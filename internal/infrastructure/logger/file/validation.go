package file

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

// validateLogTimestampConfigは、log本文に表示する時刻の設定（local time表示のtimezoneと
// 日時の書式）を、fileやdirectoryを使う前に検証します。内部ログの通知先としてだけ使うroleの
// loggerは最初の通知で作られるため、ここで検証しないと設定の誤りが構築時のerrorにならず、
// 通知のたびの作成失敗として残ります。errors.Isでlogging.ErrInvalidTimeZoneや
// timestamp.ErrLayoutRequiredと判別できます。
func validateLogTimestampConfig(subject string, timeConfig loggertime.TimeConfigProvider, body LogBodyConfigProvider) error {
	if _, err := logging.NewTimestampFormatter(
		timeConfig.EnableLocalTime(),
		timeConfig.TimeZone(),
		body.LogLineDateFormat(),
	); err != nil {
		return fmt.Errorf("%s time config: %w", subject, err)
	}
	return nil
}

func validateFileLoggerConfigProvider(cfg FileLoggerConfigProvider) error {
	if err := pkgfsstore.ValidateLogDirectory(cfg.Fsstore().Layout().LogDir()); err != nil {
		return fmt.Errorf("file logger directory config: %w", err)
	}
	if err := validateFileLoggerDirectoryLayout(cfg.Fsstore().Layout()); err != nil {
		return err
	}
	var errs []error
	errs = append(errs, validateLogTimestampConfig("file logger", cfg.Time(), cfg.LogBody()))
	if overload := cfg.Overload(); overload != nil {
		if err := pkgfsstore.ValidateFileOverloadConfig(overload.OverloadPolicy(), overload.SpillDestination()); err != nil {
			errs = append(errs, err)
		}
	}

	if err := pkgfsstore.ValidateFileRotationWeekday(
		cfg.Fsstore().Rotation().FileRotationType(),
		cfg.Fsstore().Rotation().FileRotationDayOfWeek(),
	); err != nil {
		errs = append(errs, fmt.Errorf("file logger rotation config: %w", err))
	}

	if err := fsstore.ValidateFileLifecycleConfigProvider(newValidationLifecycleConfigAdapter(cfg.Fsstore())); err != nil {
		errs = append(errs, fmt.Errorf("file logger fsstore lifecycle config: %w", err))
	}

	if err := pkgfsstore.ValidateFileLifecycleMode(cfg.LifecycleMode()); err != nil {
		errs = append(errs, fmt.Errorf("file logger lifecycle mode: %w", err))
	}

	if usesInlineLifecycle(cfg.LifecycleMode()) ||
		cfg.LifecycleMode() == pkgfsstore.FileLifecycleModeRunner {
		directories, err := resolveLoggerDirectoryPaths(cfg.Fsstore().Layout())
		if err != nil {
			errs = append(errs, fmt.Errorf("file logger purge directory config: %w", err))
		} else if err := appendlog.ValidateLogFilePurgeConfigs(appendlog.WithResolvedLogFilePurgeDirectories(
			cfg.Fsstore(), directories[DefaultLogger], directories[ErrorLogger], directories[DebugLogger],
		)); err != nil {
			errs = append(errs, fmt.Errorf("file logger role purge config: %w", err))
		}
		if err := pkgfsstore.ValidateUsageCacheTTL(cfg.Fsstore().Purge().UsageCacheTTL()); err != nil {
			errs = append(errs, fmt.Errorf("file logger usage cache config: %w", err))
		}
	}

	if usesInlineLifecycle(cfg.LifecycleMode()) {
		layout := cfg.Fsstore().Layout()
		if err := objectstoragevalidation.ValidateLogFileUploadConfigProvider(
			"file logger",
			cfg.ObjectStorageUpload(),
			layout.IsSeparateErrorLogsEnabled(),
			layout.IsSeparateDebugLogsEnabled(),
		); err != nil {
			errs = append(errs, err)
		}
	}

	if err := sampling.ValidateSamplingConfigProvider(cfg.Sampling()); err != nil {
		errs = append(errs, fmt.Errorf("file logger sampling config: %w", err))
	}

	return errors.Join(errs...)
}

// validateDeliveryFileLoggerConfigProviderは、delivery writerが実装している
// lifecycle modeだけを受け付けます。DeliveryLoggerSetと単体constructorの双方が
// この境界を使用し、上位factoryを迂回しても同じ契約を維持します。
// 1ファイルの最大サイズには、rotationの有無にかかわらず通常のfile loggerと同じ下限を適用します。
// roleは今回開くfileを表し、Setの検証では必ず使用するDefaultLoggerを渡します。
// 単体のerror、または保存先を個別指定したdebugでは、未使用のLogDirを検証しません。
func validateDeliveryFileLoggerConfigProvider(cfg DeliveryFileLoggerConfigProvider, role loggerType) error {
	layout := cfg.Fsstore().Layout()
	if role == DefaultLogger || (role == DebugLogger && !layout.IsSeparateDebugLogDirEnabled()) {
		if err := pkgfsstore.ValidateLogDirectory(layout.LogDir()); err != nil {
			return fmt.Errorf("file logger directory config: %w", err)
		}
	}
	if err := validateFileLoggerDirectoryLayout(cfg.Fsstore().Layout()); err != nil {
		return err
	}
	if err := pkgfsstore.ValidateFileRotationWeekday(
		cfg.Fsstore().Rotation().FileRotationType(),
		cfg.Fsstore().Rotation().FileRotationDayOfWeek(),
	); err != nil {
		return fmt.Errorf("delivery file logger rotation config: %w", err)
	}
	if err := validateLogFileMaxFileSize(cfg.Fsstore().Rotation().MaxFileSize()); err != nil {
		return fmt.Errorf("delivery file logger rotation config: %w", err)
	}
	if err := pkgfsstore.ValidateDeliveryFileLifecycleMode(cfg.LifecycleMode()); err != nil {
		return fmt.Errorf("delivery file logger lifecycle mode: %w", err)
	}
	return validateLogTimestampConfig("delivery file logger", cfg.Time(), cfg.LogBody())
}

// validateLogFileMaxFileSizeは、通常のfile loggerがlifecycle設定の検証で適用する
// 1ファイルの最大サイズの下限を、保持設定を持たないdelivery writerにも適用します。
// 下限値と誤りの文言は、通常のfile loggerと同じfsstoreの定義を使います。
// 通常のfile loggerと同じく、errors.Isでfsstore.ErrInvalidMaxFileSizeと判別できます。
func validateLogFileMaxFileSize(maxFileSize uint64) error {
	if maxFileSize < fsstore.MinAllowedFileSizeBytes {
		// 文言のtemplateはsentinelの文言で始まるため、その部分をsentinel自体のwrapに置き換えます。
		detail := strings.TrimPrefix(fsstore.ErrorMsgMinAllowedFileSizeTemplate, fsstore.ErrInvalidMaxFileSize.Error())
		return fmt.Errorf("%w"+detail, fsstore.ErrInvalidMaxFileSize, fsstore.MinAllowedFileSizeBytes, maxFileSize)
	}
	return nil
}

func validateFileLoggerDirectoryLayout(layout appendlog.FsstoreLayoutConfigProvider) error {
	// ファイル名の検証は、保存先のlease取得やdirectory作成より前に全constructorで行います。
	if err := appendlog.ValidateLogFileNameConfigProvider(layout); err != nil {
		return fmt.Errorf("file logger filename config: %w", err)
	}
	if err := pkgfsstore.ValidateDebugLogDirectory(
		layout.IsSeparateDebugLogDirEnabled(),
		layout.DebugLogDir(),
	); err != nil {
		return fmt.Errorf("file logger directory config: %w", err)
	}
	if !layout.IsSeparateErrorLogsEnabled() || !layout.IsSeparateDebugLogsEnabled() {
		return nil
	}
	_, err := resolveLoggerDirectoryPaths(layout)
	return err
}

type validationLifecycleConfigAdapter struct {
	lifecycle appendlog.FileLifecycleConfigProvider
	purge     appendlog.FilePurgeConfigProvider
}

func newValidationLifecycleConfigAdapter(cfg appendlog.ConfigProvider) fsstore.ValidationLifecycleConfigProvider {
	return validationLifecycleConfigAdapter{
		lifecycle: cfg.Lifecycle(),
		purge:     cfg.Purge(),
	}
}

func (a validationLifecycleConfigAdapter) MaxFileBackups() int {
	return a.purge.MaxFileBackups()
}
func (a validationLifecycleConfigAdapter) MaxFileAge() int {
	return a.purge.MaxFileAge()
}
func (a validationLifecycleConfigAdapter) MaxFileSize() uint64 {
	return a.lifecycle.MaxFileSize()
}
func (a validationLifecycleConfigAdapter) MaxDirSize() uint64 {
	return a.purge.MaxDirSize()
}
