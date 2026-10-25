package fsstore

import (
	"errors"
	"fmt"
	"time"
)

// 主にバリデーションに使用される各設定項目の最小値の定義
const (
	// 「(ログ)ファイル最大サイズ」の最小値（バイト単位）　これ以下の場合はエラーを返す
	MinAllowedFileSizeBytes = 1 * 1024 * 1024
	// (ログ)世代最大保持数の最小値
	MinAllowedMaxFileBackups int = 2
	//　バックアップ保持日数の最小値
	MinAllowedMaxFileAge int = 1
	//　「(ログ)ディレクトリ最大サイズ」の最小値
	MinAllowedMaxDirSize uint64 = 10 * 1024 * 1024
)

// 保持設定の項目ごとの検証エラーです。各エラーは項目のsentinelをwrapし、
// errors.Isで拒否した項目を判別できます。文言は下のErrorMsg...Templateと同じです。
var (
	ErrInvalidMaxFileSize         = errors.New(errorMsgInvalidMaxFileSize)
	ErrInvalidMaxFileBackups      = errors.New(errorMsgInvalidMaxFileBackups)
	ErrInvalidMaxFileAge          = errors.New(errorMsgInvalidMaxFileAge)
	ErrInvalidMaxDirSize          = errors.New(errorMsgInvalidMaxDirSize)
	ErrMaxDirSizeBelowMaxFileSize = errors.New(errorMsgMaxDirSizeBelowMaxFileSize)
)

const (
	errorMsgInvalidMaxFileSize         = "invalid file size"
	errorMsgInvalidMaxFileBackups      = "invalid file backups"
	errorMsgInvalidMaxFileAge          = "invalid file backup age"
	errorMsgInvalidMaxDirSize          = "invalid dir size"
	errorMsgMaxDirSizeBelowMaxFileSize = "max dir size must be greater than or equal to max file size"

	errorDetailMinAllowedFileSize    = ": must be at least %d byte, but got %d byte"
	errorDetailMinAllowedFileBackups = ": must be at least %d backups, but got %d "
	errorDetailMinAllowedFileAge     = ": must be at least %d day(s), but got %d day(s)"
	errorDetailMinAllowedMaxDirSize  = ": must be at least %d bytes, but got %d bytes"
	errorDetailMaxDirSizeBelowFile   = ": maxDirSize=%d, maxFileSize=%d"

	errorDetailFailureArchiveFileBackups = ": must be 0 (no limit) or at least %d backups, but got %d"
	errorDetailFailureArchiveFileAge     = ": must be 0 (no limit) or at least %d day(s), but got %d day(s)"
	errorDetailFailureArchiveMaxDirSize  = ": must be 0 (no limit) or at least %d bytes, but got %d bytes"
)

const (
	ErrorMsgMinAllowedFileSizeTemplate    = errorMsgInvalidMaxFileSize + errorDetailMinAllowedFileSize
	ErrorMsgMinAllowedFileBackupsTemplate = errorMsgInvalidMaxFileBackups + errorDetailMinAllowedFileBackups
	ErrorMsgMinAllowedFileAgeTemplate     = errorMsgInvalidMaxFileAge + errorDetailMinAllowedFileAge
	ErrorMsgMinAllowedMaxDirSize          = errorMsgInvalidMaxDirSize + errorDetailMinAllowedMaxDirSize
	ErrorMsgMaxDirSizeMustBeGTEFileSize   = errorMsgMaxDirSizeBelowMaxFileSize + errorDetailMaxDirSizeBelowFile

	// failure archiveの保持上限は0（上限なし）も受け付けるため、通常ログとは別の文言で拒否理由を示します。
	ErrorMsgFailureArchiveFileBackupsTemplate = errorMsgInvalidMaxFileBackups + errorDetailFailureArchiveFileBackups
	ErrorMsgFailureArchiveFileAgeTemplate     = errorMsgInvalidMaxFileAge + errorDetailFailureArchiveFileAge
	ErrorMsgFailureArchiveMaxDirSizeTemplate  = errorMsgInvalidMaxDirSize + errorDetailFailureArchiveMaxDirSize
)

type ValidationLifecycleConfigProvider interface {
	MaxFileBackups() int
	MaxFileAge() int
	MaxFileSize() uint64
	MaxDirSize() uint64
}

func ValidateFileLifecycleConfigProvider(cfg ValidationLifecycleConfigProvider) error {
	var errs []error

	if cfg.MaxFileSize() < MinAllowedFileSizeBytes {
		errs = append(errs, fmt.Errorf("%w"+errorDetailMinAllowedFileSize, ErrInvalidMaxFileSize, MinAllowedFileSizeBytes, cfg.MaxFileSize()))
	}
	if cfg.MaxFileBackups() < MinAllowedMaxFileBackups {
		errs = append(errs, fmt.Errorf("%w"+errorDetailMinAllowedFileBackups, ErrInvalidMaxFileBackups, MinAllowedMaxFileBackups, cfg.MaxFileBackups()))
	}
	if cfg.MaxFileAge() < MinAllowedMaxFileAge {
		errs = append(errs, fmt.Errorf("%w"+errorDetailMinAllowedFileAge, ErrInvalidMaxFileAge, MinAllowedMaxFileAge, cfg.MaxFileAge()))
	}
	if cfg.MaxDirSize() < MinAllowedMaxDirSize {
		errs = append(errs, fmt.Errorf("%w"+errorDetailMinAllowedMaxDirSize, ErrInvalidMaxDirSize, MinAllowedMaxDirSize, cfg.MaxDirSize()))
	}
	if cfg.MaxDirSize() < cfg.MaxFileSize() {
		errs = append(errs, fmt.Errorf("%w"+errorDetailMaxDirSizeBelowFile, ErrMaxDirSizeBelowMaxFileSize, cfg.MaxDirSize(), cfg.MaxFileSize()))
	}

	return errors.Join(errs...)
}

// ValidateFailureArchiveRetentionConfigProviderは、backend送信失敗を保存するfailure archive
// （fileArchiveのretryable・permanent JSONL）のファイル最大サイズと保持上限を検証します。
// 保持世代数・保持日数・ディレクトリ最大サイズの0は「上限なし」で、その条件では削除しません。
// 0以外の値には通常ログと同じ最小値を求め、ディレクトリ最大サイズを指定する場合は
// ファイル最大サイズ以上を求めます。ファイル最大サイズは0を受け付けません。
// 通常ログの検証にはValidateFileLifecycleConfigProviderを使用し、保持上限の0を拒否します。
func ValidateFailureArchiveRetentionConfigProvider(cfg ValidationLifecycleConfigProvider) error {
	var errs []error

	if cfg.MaxFileSize() < MinAllowedFileSizeBytes {
		errs = append(errs, fmt.Errorf("%w"+errorDetailMinAllowedFileSize, ErrInvalidMaxFileSize, MinAllowedFileSizeBytes, cfg.MaxFileSize()))
	}
	if backups := cfg.MaxFileBackups(); backups != 0 && backups < MinAllowedMaxFileBackups {
		errs = append(errs, fmt.Errorf("%w"+errorDetailFailureArchiveFileBackups, ErrInvalidMaxFileBackups, MinAllowedMaxFileBackups, backups))
	}
	if age := cfg.MaxFileAge(); age != 0 && age < MinAllowedMaxFileAge {
		errs = append(errs, fmt.Errorf("%w"+errorDetailFailureArchiveFileAge, ErrInvalidMaxFileAge, MinAllowedMaxFileAge, age))
	}
	if dirSize := cfg.MaxDirSize(); dirSize != 0 {
		if dirSize < MinAllowedMaxDirSize {
			errs = append(errs, fmt.Errorf("%w"+errorDetailFailureArchiveMaxDirSize, ErrInvalidMaxDirSize, MinAllowedMaxDirSize, dirSize))
		}
		if dirSize < cfg.MaxFileSize() {
			errs = append(errs, fmt.Errorf("%w"+errorDetailMaxDirSizeBelowFile, ErrMaxDirSizeBelowMaxFileSize, dirSize, cfg.MaxFileSize()))
		}
	}

	return errors.Join(errs...)
}

const (
	MinCompressWarnThresholdDuration   time.Duration = 1 * time.Millisecond
	MinCompressErrorThresholdDuration  time.Duration = 1 * time.Millisecond
	MinWriteFileWarnThresholdDuration  time.Duration = 1 * time.Millisecond
	MinWriteFileErrorThresholdDuration time.Duration = 1 * time.Millisecond

	ErrorMsgMinCompressWarnThresholdDuration   = errorMsgInvalidCompressWarnThreshold + errorDetailMinDuration
	ErrorMsgMinCompressErrorThresholdDuration  = errorMsgInvalidCompressErrorThreshold + errorDetailMinDuration
	ErrorMsgMinWriteFileWarnThresholdDuration  = errorMsgInvalidWriteFileWarnThreshold + errorDetailMinDuration
	ErrorMsgMinWriteFileErrorThresholdDuration = errorMsgInvalidWriteFileErrorThreshold + errorDetailMinDuration

	ErrorMsgCompressErrorThresholdLessThanWarn  = errorMsgCompressErrorThresholdBelowWarn + errorDetailThresholdBelowWarn
	ErrorMsgWriteFileErrorThresholdLessThanWarn = errorMsgWriteFileErrorThresholdBelowWarn + errorDetailThresholdBelowWarn
)

// 処理時間の閾値の項目ごとの検証エラーです。errors.Isで拒否した項目を判別できます。
var (
	ErrInvalidCompressWarnThreshold     = errors.New(errorMsgInvalidCompressWarnThreshold)
	ErrInvalidCompressErrorThreshold    = errors.New(errorMsgInvalidCompressErrorThreshold)
	ErrInvalidWriteFileWarnThreshold    = errors.New(errorMsgInvalidWriteFileWarnThreshold)
	ErrInvalidWriteFileErrorThreshold   = errors.New(errorMsgInvalidWriteFileErrorThreshold)
	ErrCompressErrorThresholdBelowWarn  = errors.New(errorMsgCompressErrorThresholdBelowWarn)
	ErrWriteFileErrorThresholdBelowWarn = errors.New(errorMsgWriteFileErrorThresholdBelowWarn)
)

const (
	errorMsgInvalidCompressWarnThreshold     = "compress warn threshold duration"
	errorMsgInvalidCompressErrorThreshold    = "compress error threshold duration"
	errorMsgInvalidWriteFileWarnThreshold    = "write file warn threshold duration"
	errorMsgInvalidWriteFileErrorThreshold   = "write file error threshold duration"
	errorMsgCompressErrorThresholdBelowWarn  = "compress error threshold duration must be >= compress warn threshold duration"
	errorMsgWriteFileErrorThresholdBelowWarn = "write file error threshold duration must be >= write file warn threshold duration"

	errorDetailMinDuration        = " must be >= %s, got %s"
	errorDetailThresholdBelowWarn = ", got error=%s, warn=%s"
)

type ValidationDurationConfigProvider interface {
	CompressWarnThresholdDuration() time.Duration
	CompressErrorThresholdDuration() time.Duration
	WriteFileWarnThresholdDuration() time.Duration
	WriteFileErrorThresholdDuration() time.Duration
}

func ValidateDurationConfigProvider(cfg ValidationDurationConfigProvider) error {
	var errs []error
	if cfg.CompressWarnThresholdDuration() < MinCompressWarnThresholdDuration {
		errs = append(errs, fmt.Errorf(
			"%w"+errorDetailMinDuration,
			ErrInvalidCompressWarnThreshold,
			MinCompressWarnThresholdDuration,
			cfg.CompressWarnThresholdDuration(),
		))
	}

	if cfg.CompressErrorThresholdDuration() < MinCompressErrorThresholdDuration {
		errs = append(errs, fmt.Errorf(
			"%w"+errorDetailMinDuration,
			ErrInvalidCompressErrorThreshold,
			MinCompressErrorThresholdDuration,
			cfg.CompressErrorThresholdDuration(),
		))
	}

	if cfg.WriteFileWarnThresholdDuration() < MinWriteFileWarnThresholdDuration {
		errs = append(errs, fmt.Errorf(
			"%w"+errorDetailMinDuration,
			ErrInvalidWriteFileWarnThreshold,
			MinWriteFileWarnThresholdDuration,
			cfg.WriteFileWarnThresholdDuration(),
		))
	}

	if cfg.WriteFileErrorThresholdDuration() < MinWriteFileErrorThresholdDuration {
		errs = append(errs, fmt.Errorf(
			"%w"+errorDetailMinDuration,
			ErrInvalidWriteFileErrorThreshold,
			MinWriteFileErrorThresholdDuration,
			cfg.WriteFileErrorThresholdDuration(),
		))
	}

	if cfg.CompressErrorThresholdDuration() < cfg.CompressWarnThresholdDuration() {
		errs = append(errs, fmt.Errorf(
			"%w"+errorDetailThresholdBelowWarn,
			ErrCompressErrorThresholdBelowWarn,
			cfg.CompressErrorThresholdDuration(),
			cfg.CompressWarnThresholdDuration(),
		))
	}

	if cfg.WriteFileErrorThresholdDuration() < cfg.WriteFileWarnThresholdDuration() {
		errs = append(errs, fmt.Errorf(
			"%w"+errorDetailThresholdBelowWarn,
			ErrWriteFileErrorThresholdBelowWarn,
			cfg.WriteFileErrorThresholdDuration(),
			cfg.WriteFileWarnThresholdDuration(),
		))
	}

	return errors.Join(errs...)
}

const (
	MinWriteRetryAttempts         = 1
	MinFileOperationRetryWaitTime = 1 * time.Millisecond
)

const (
	ErrorMsgMinWriteRetryAttempts         = errorMsgInvalidMaxWriteAttempts + " must be >= %d, got %d"
	ErrorMsgMinFileOperationRetryWaitTime = errorMsgInvalidFileOperationRetryWaitTime + errorDetailMinDuration
)

// 再試行設定の項目ごとの検証エラーです。errors.Isで拒否した項目を判別できます。
var (
	ErrInvalidMaxWriteAttempts           = errors.New(errorMsgInvalidMaxWriteAttempts)
	ErrInvalidFileOperationRetryWaitTime = errors.New(errorMsgInvalidFileOperationRetryWaitTime)
)

const (
	errorMsgInvalidMaxWriteAttempts           = "max write retry attempts"
	errorMsgInvalidFileOperationRetryWaitTime = "file operation retry wait time"
)

type ValidationRetryConfigProvider interface {
	MaxWriteAttempts() int
	FileOperationRetryWaitTime() time.Duration
}

func ValidateRetryConfigProvider(cfg ValidationRetryConfigProvider) error {
	var errs []error

	if cfg.MaxWriteAttempts() < MinWriteRetryAttempts {
		errs = append(errs, fmt.Errorf(
			"%w must be >= %d, got %d",
			ErrInvalidMaxWriteAttempts,
			MinWriteRetryAttempts,
			cfg.MaxWriteAttempts(),
		))
	}

	if cfg.FileOperationRetryWaitTime() < MinFileOperationRetryWaitTime {
		errs = append(errs, fmt.Errorf(
			"%w"+errorDetailMinDuration,
			ErrInvalidFileOperationRetryWaitTime,
			MinFileOperationRetryWaitTime,
			cfg.FileOperationRetryWaitTime(),
		))
	}

	return errors.Join(errs...)
}
