package objectstoragecompressedlog

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

// Settingは、SettingErrorで不正な設定項目を表す名前です。
type Setting string

const (
	// SettingFileArchiveDirNameは、file archiveの保存directory名です。
	SettingFileArchiveDirName Setting = "failure archive directory name"
	// SettingFileArchiveLifecycleIntervalは、file archiveの圧縮・削除などを実行する間隔です。
	SettingFileArchiveLifecycleInterval Setting = "failure archive lifecycle interval"
	// SettingSpoolRunnerIntervalは、spoolの再送runnerの実行間隔です。
	SettingSpoolRunnerInterval Setting = "spool runner interval"
	// SettingSpoolMaxRetryAfterは、spoolの再送でRetry-Afterに従って待機する上限です。
	SettingSpoolMaxRetryAfter Setting = "spool max retry-after"
	// SettingSpoolCircuitBreakerOpenCooldownは、spoolの再送を止めたcircuit breakerを再開するまでの時間です。
	SettingSpoolCircuitBreakerOpenCooldown Setting = "spool circuit breaker open cooldown"
	// SettingSpoolLifecycleIntervalは、spoolの削除などを実行する間隔です。
	SettingSpoolLifecycleInterval Setting = "spool lifecycle interval"
	// SettingPurgeMaxFileAgeDaysは、Destinationのfileを保持する日数です。
	SettingPurgeMaxFileAgeDays Setting = "max file age days"
	// SettingPurgeMaxDirSizeは、Destinationのdirectory容量の上限です。
	SettingPurgeMaxDirSize Setting = "max directory size"
)

var (
	// ErrInvalidDirectoryNameは、保存directory名が空・前後の空白・path区切り・"."・".."を含むことを表します。
	ErrInvalidDirectoryName = errors.New("compressed log file upload failure directory name is invalid")
	// ErrNonPositiveSettingは、正の値が必要な設定が0以下であることを表します。
	ErrNonPositiveSetting = errors.New("compressed log file upload failure setting must be positive")
	// ErrNegativeSettingは、0以上の値が必要な設定が負値であることを表します。
	ErrNegativeSetting = errors.New("compressed log file upload failure setting must not be negative")
)

// SettingErrorは、Contract（"standard"または"delivery"）のcompressed log file upload failure store
// 設定で、Settingの値Valueが不正であることを表します。Destinationは保持設定の対象
// （"failure archive retryable"、"spool done"など）で、保持設定以外では空です。Errは不正の
// 種類を表すsentinelで、errors.Isで判別できます。ValidateConfigは複数のerrorをerrors.Joinで
// まとめて返します。
type SettingError struct {
	Contract    string
	Destination string
	Setting     Setting
	Value       any
	Err         error

	message string
}

func newSettingError(
	contract string,
	destination string,
	setting Setting,
	value any,
	kind error,
	format string,
	args ...any,
) error {
	return &SettingError{
		Contract:    contract,
		Destination: destination,
		Setting:     setting,
		Value:       value,
		Err:         kind,
		message:     fmt.Sprintf(format, args...),
	}
}

func (e *SettingError) Error() string { return e.message }

func (e *SettingError) Unwrap() error { return e.Err }

// ValidateConfigは、object storage uploadが有効なlogger契約についてだけ作られた
// failure store設定を検証します。zero valueは該当契約でuploadを使用しない状態です。
func ValidateConfig(cfg Config) error {
	return errors.Join(
		validateCompressedLogFileUploadFailureStoreConfig("standard", cfg.Standard),
		validateCompressedLogFileUploadFailureStoreConfig("delivery", cfg.Delivery),
	)
}

func validateCompressedLogFileUploadFailureStoreConfig(name string, cfg FailureStoreConfig) error {
	if len(cfg.DurableType) == 0 {
		return nil
	}
	if err := failurestore.ValidateDurableTypes(
		name+" compressed log file upload failure",
		cfg.DurableType,
		failurestore.DurableTypeFileArchive,
		failurestore.DurableTypeSpool,
	); err != nil {
		return err
	}
	if slices.Contains(cfg.DurableType, failurestore.DurableTypeNone) {
		return nil
	}

	var errs []error
	if slices.Contains(cfg.DurableType, failurestore.DurableTypeFileArchive) {
		errs = append(errs, validateFileArchiveConfig(
			name,
			cfg.DurableStoreConfig.FileArchiveConfig,
		))
	}
	if slices.Contains(cfg.DurableType, failurestore.DurableTypeSpool) {
		errs = append(errs, validateSpoolConfig(
			name,
			cfg.DurableStoreConfig.SpoolConfig,
		))
	}
	return errors.Join(errs...)
}

func validateFileArchiveConfig(name string, cfg FileArchiveConfig) error {
	var errs []error
	if strings.TrimSpace(cfg.DirName) == "" ||
		strings.TrimSpace(cfg.DirName) != cfg.DirName ||
		filepath.Base(cfg.DirName) != cfg.DirName ||
		cfg.DirName == "." || cfg.DirName == ".." ||
		strings.ContainsAny(cfg.DirName, `/\`+"\x00") {
		errs = append(errs, newSettingError(
			name, "", SettingFileArchiveDirName, cfg.DirName, ErrInvalidDirectoryName,
			"%s compressed log file upload failure archive directory name is invalid: %q",
			name,
			cfg.DirName,
		))
	}
	if cfg.LifecycleInterval <= 0 {
		errs = append(errs, newSettingError(
			name, "", SettingFileArchiveLifecycleInterval, cfg.LifecycleInterval, ErrNonPositiveSetting,
			"%s compressed log file upload failure archive lifecycle interval must be positive: %s",
			name,
			cfg.LifecycleInterval,
		))
	}
	errs = append(
		errs,
		validatePurgeConfig(name, "failure archive retryable", cfg.Retryable),
		validatePurgeConfig(name, "failure archive permanent", cfg.Permanent),
	)
	return errors.Join(errs...)
}

func validateSpoolConfig(name string, cfg SpoolConfig) error {
	var errs []error
	if cfg.Runner.Interval <= 0 {
		errs = append(errs, newSettingError(
			name, "", SettingSpoolRunnerInterval, cfg.Runner.Interval, ErrNonPositiveSetting,
			"%s compressed log file upload failure spool runner interval must be positive: %s",
			name,
			cfg.Runner.Interval,
		))
	}
	if cfg.Runner.MaxRetryAfter < 0 {
		errs = append(errs, newSettingError(
			name, "", SettingSpoolMaxRetryAfter, cfg.Runner.MaxRetryAfter, ErrNegativeSetting,
			"%s compressed log file upload failure spool max retry-after must not be negative: %s",
			name,
			cfg.Runner.MaxRetryAfter,
		))
	}
	if cfg.CircuitBreaker.OpenCooldown <= 0 {
		errs = append(errs, newSettingError(
			name, "", SettingSpoolCircuitBreakerOpenCooldown, cfg.CircuitBreaker.OpenCooldown, ErrNonPositiveSetting,
			"%s compressed log file upload failure spool circuit breaker open cooldown must be positive: %s",
			name,
			cfg.CircuitBreaker.OpenCooldown,
		))
	}
	if cfg.Lifecycle.Interval <= 0 {
		errs = append(errs, newSettingError(
			name, "", SettingSpoolLifecycleInterval, cfg.Lifecycle.Interval, ErrNonPositiveSetting,
			"%s compressed log file upload failure spool lifecycle interval must be positive: %s",
			name,
			cfg.Lifecycle.Interval,
		))
	}

	errs = append(
		errs,
		validatePurgeConfig(name, "spool done", cfg.Lifecycle.Done),
		validatePurgeConfig(name, "spool dead", cfg.Lifecycle.Dead),
		validatePurgeConfig(name, "spool quarantine", cfg.Lifecycle.Quarantine),
	)
	return errors.Join(errs...)
}

func validatePurgeConfig(scope, destination string, cfg PurgeConfig) error {
	if !cfg.Enabled {
		return nil
	}
	var errs []error
	if cfg.MaxFileAgeDays <= 0 {
		errs = append(errs, newSettingError(
			scope, destination, SettingPurgeMaxFileAgeDays, cfg.MaxFileAgeDays, ErrNonPositiveSetting,
			"%s object storage %s max file age days must be positive: %d",
			scope,
			destination,
			cfg.MaxFileAgeDays,
		))
	}
	if cfg.MaxDirSize == 0 {
		errs = append(errs, newSettingError(
			scope, destination, SettingPurgeMaxDirSize, cfg.MaxDirSize, ErrNonPositiveSetting,
			"%s object storage %s max directory size must be positive",
			scope,
			destination,
		))
	}
	return errors.Join(errs...)
}
