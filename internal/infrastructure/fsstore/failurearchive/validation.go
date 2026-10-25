package failurearchive

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

const (
	ExpectedFileDateFormat = "20060102"
)

const (
	ErrorMsgInvalidTarget               = "target must not be empty"
	ErrorMsgInvalidArchivePath          = "archive path must not be empty"
	ErrorMsgInvalidDirCaseRetryable     = "dir case retryable must not be empty"
	ErrorMsgInvalidDirCasePermanent     = "dir case permanent must not be empty"
	ErrorMsgInvalidFileNameFormat       = "invalid file name format: format=%q err=%w"
	ErrorMsgInvalidFileDateFormat       = "invalid file date format: only %q is allowed"
	ErrorMsgFileNameFormatNoExtension   = "file name format must include an extension"
	ErrorMsgFileNameFormatInvalidPrefix = "file name format must start with %q"
	ErrorMsgFileNameFormatEmptyExt      = "extension must not be empty"
	ErrorMsgFileNameFormatExtWhitespace = "extension must not contain leading or trailing whitespace"
	ErrorMsgFileNameFormatExtSeparator  = "extension must not contain path separators"
	ErrorMsgFileNameFormatExtControl    = "extension must not contain control characters"
	ErrorMsgFileNameFormatMismatch      = "file name format must match failure archive JSONL format %q"
	ErrorMsgArchiveDirectoriesOverlap   = "retryable and permanent archive directories must not be identical or nested"
	ErrorMsgArchiveDirectoryOutsideRoot = "failure archive terminal directory must be a strict descendant of archive root"
)

// fileNameFormatBaseは、failure archiveのfile name formatが拡張子の前に持つ固定部分です。
const fileNameFormatBase = "/%s%s_%s_%d"

// 設定検証の各sentinelは、対応するErrorMsg*と同じ文言です。
var (
	ErrInvalidTarget           = errors.New(ErrorMsgInvalidTarget)
	ErrInvalidArchivePath      = errors.New(ErrorMsgInvalidArchivePath)
	ErrInvalidDirCaseRetryable = errors.New(ErrorMsgInvalidDirCaseRetryable)
	ErrInvalidDirCasePermanent = errors.New(ErrorMsgInvalidDirCasePermanent)
	// ErrInvalidFileNameFormatは、file name formatの検証エラーをwrapします。
	// 個別の理由はErrFileNameFormat*で判別します。
	ErrInvalidFileNameFormat = errors.New("invalid file name format")
	ErrInvalidFileDateFormat = fmt.Errorf(ErrorMsgInvalidFileDateFormat, ExpectedFileDateFormat)

	ErrFileNameFormatNoExtension   = errors.New(ErrorMsgFileNameFormatNoExtension)
	ErrFileNameFormatInvalidPrefix = fmt.Errorf(ErrorMsgFileNameFormatInvalidPrefix, fileNameFormatBase)
	ErrFileNameFormatEmptyExt      = errors.New(ErrorMsgFileNameFormatEmptyExt)
	ErrFileNameFormatExtWhitespace = errors.New(ErrorMsgFileNameFormatExtWhitespace)
	ErrFileNameFormatExtSeparator  = errors.New(ErrorMsgFileNameFormatExtSeparator)
	ErrFileNameFormatExtControl    = errors.New(ErrorMsgFileNameFormatExtControl)
	ErrFileNameFormatMismatch      = fmt.Errorf(ErrorMsgFileNameFormatMismatch, FileNameFormat)

	// ErrArchiveDirectoriesOverlapは、retryableとpermanentの保存先が同一または包含関係にあることを表します。
	ErrArchiveDirectoriesOverlap = errors.New(ErrorMsgArchiveDirectoriesOverlap)
	// ErrArchiveDirectoryOutsideRootは、保存先がarchive rootの外またはroot自身へ解決されることを表します。
	ErrArchiveDirectoryOutsideRoot = errors.New(ErrorMsgArchiveDirectoryOutsideRoot)
)

type ValidationLayoutConfigProvider interface {
	Target() string
	ArchivePath() string      // ex.) /var/lib/{target}/  {/var/lib部分}
	Dir() string              // ex.) /var/lib/{target}/{dir} 空文字許容
	DirCaseRetryable() string // ex.) /var/lib/{target}/{dir}/{retryable}
	DirCasePermanent() string // ex.) /var/lib/{target}/{dir}/{permanent}
	FileNameFormat() string
	FileDateFormat() string
}

func ValidateLayoutConfigProvider(cfg ValidationLayoutConfigProvider) error {
	var errs []error

	if cfg.Target() == "" {
		errs = append(errs, ErrInvalidTarget)
	}
	errs = append(errs,
		pkgfsstore.ValidateLogFileNameValue("target", cfg.Target()),
		pkgfsstore.ValidateLogFileNameValue("file name format", cfg.FileNameFormat()),
		pkgfsstore.ValidateLogFileNameValue("file date format", cfg.FileDateFormat()),
	)

	if cfg.ArchivePath() == "" {
		errs = append(errs, ErrInvalidArchivePath)
	}

	if cfg.DirCaseRetryable() == "" {
		errs = append(errs, ErrInvalidDirCaseRetryable)
	}

	if cfg.DirCasePermanent() == "" {
		errs = append(errs, ErrInvalidDirCasePermanent)
	}

	if err := validateFileArchiveDirectoriesWithinRoot(cfg); err != nil {
		errs = append(errs, err)
	}

	if err := ValidateFileArchiveDirectorySeparationConfigProvider(cfg); err != nil {
		errs = append(errs, err)
	}

	if err := validateFileNameFormat(cfg.FileNameFormat()); err != nil {
		// ErrorMsgInvalidFileNameFormatと同じ文言で、sentinelと理由の両方をwrapします。
		errs = append(errs, fmt.Errorf(
			"%w: format=%q err=%w",
			ErrInvalidFileNameFormat,
			cfg.FileNameFormat(),
			err,
		))
	}

	if cfg.FileDateFormat() != ExpectedFileDateFormat {
		errs = append(errs, ErrInvalidFileDateFormat)
	}

	return errors.Join(errs...)
}

// validateFileArchiveDirectoriesWithinRootは、Leaseを取得するretryable/permanent共通rootと
// 実際のwriter directoryが一致する所有範囲にあることを検証します。解決後のterminal
// directoryがroot自身またはroot外になる設定を拒否し、Lease外への書き込みを防ぎます。
func validateFileArchiveDirectoriesWithinRoot(cfg ValidationLayoutConfigProvider) error {
	root, err := pathidentity.ResolveDirectory(resolveFileArchiveDirectoryPath(cfg, ""))
	if err != nil {
		return fmt.Errorf("resolve failure archive root directory: %w", err)
	}

	directories := []struct {
		failureCategory string
		path            string
	}{
		{failureCategory: string(FailureCategoryRetryable), path: cfg.DirCaseRetryable()},
		{failureCategory: string(FailureCategoryPermanent), path: cfg.DirCasePermanent()},
	}
	var errs []error
	for _, directory := range directories {
		// root外・root自身への入力は従来と同じ診断で拒否します。
		// 管理分類のリンクはこの字句的な包含判定の後、実体へ解決せず検査します。
		resolved := filepath.Join(root, directory.path)
		contained, compareErr := pathContainsOrEquals(root, resolved)
		if compareErr != nil {
			errs = append(errs, fmt.Errorf(
				"compare %s archive directory with archive root: %w",
				directory.failureCategory,
				compareErr,
			))
			continue
		}
		if !contained || pathidentity.ComparisonKey(resolved) == pathidentity.ComparisonKey(root) {
			errs = append(errs, fmt.Errorf(
				"%w: failureCategory=%s root=%q directory=%q",
				ErrArchiveDirectoryOutsideRoot,
				directory.failureCategory,
				root,
				resolved,
			))
			continue
		}
		if _, err := resolveManagedFileArchiveDirectoryPath(cfg, directory.path); err != nil {
			errs = append(errs, fmt.Errorf("resolve %s archive directory: %w", directory.failureCategory, err))
		}
	}
	return errors.Join(errs...)
}

// ValidateFileArchiveDirectorySeparationConfigProvider はretryableとpermanentを
// 別々にrotation、purge、uploadできるよう、解決後の保存先が同一または包含関係に
// なっていないことを検証します。
func ValidateFileArchiveDirectorySeparationConfigProvider(cfg ValidationLayoutConfigProvider) error {
	retryablePath, err := resolveManagedFileArchiveDirectoryPath(cfg, cfg.DirCaseRetryable())
	if err != nil {
		return fmt.Errorf("resolve retryable archive directory: %w", err)
	}
	permanentPath, err := resolveManagedFileArchiveDirectoryPath(cfg, cfg.DirCasePermanent())
	if err != nil {
		return fmt.Errorf("resolve permanent archive directory: %w", err)
	}

	overlaps, err := pathContainsOrEquals(retryablePath, permanentPath)
	if err != nil {
		return fmt.Errorf("compare retryable and permanent archive directories: %w", err)
	}
	if !overlaps {
		overlaps, err = pathContainsOrEquals(permanentPath, retryablePath)
		if err != nil {
			return fmt.Errorf("compare permanent and retryable archive directories: %w", err)
		}
	}
	if overlaps {
		return fmt.Errorf(
			"%w: retryable=%q permanent=%q",
			ErrArchiveDirectoriesOverlap,
			retryablePath,
			permanentPath,
		)
	}

	return nil
}

func pathContainsOrEquals(parent, candidate string) (bool, error) {
	relativePath, err := filepath.Rel(pathidentity.ComparisonKey(parent), pathidentity.ComparisonKey(candidate))
	if err != nil {
		return false, err
	}

	return relativePath == "." ||
		(relativePath != ".." &&
			!strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) &&
			!filepath.IsAbs(relativePath)), nil
}

// ファイル名形式規則のバリデーション
func validateFileNameFormat(format string) error {
	const base = fileNameFormatBase

	if len(format) <= len(base) {
		return ErrFileNameFormatNoExtension
	}

	if !strings.HasPrefix(format, base) {
		return ErrFileNameFormatInvalidPrefix
	}

	ext := format[len(base):]

	if ext == "" {
		return ErrFileNameFormatEmptyExt
	}

	if strings.TrimSpace(ext) != ext {
		return ErrFileNameFormatExtWhitespace
	}

	if strings.ContainsAny(ext, "/\\") {
		return ErrFileNameFormatExtSeparator
	}

	for _, r := range ext {
		if unicode.IsControl(r) {
			return ErrFileNameFormatExtControl
		}
	}

	if format != FileNameFormat {
		return ErrFileNameFormatMismatch
	}

	return nil
}

func ValidateFileArchiveConfigProvider(cfg FileArchiveConfigProvider) error {
	var errs []error

	if err := ValidateFileArchiveLifecycleConfigProvider(cfg); err != nil {
		errs = append(errs, err)
	}

	if err := ValidateLayoutConfigProvider(cfg); err != nil {
		errs = append(errs, fmt.Errorf("failure archive fsstore layout config: %w", err))
	}

	if err := fsstore.ValidateRetryConfigProvider(cfg); err != nil {
		errs = append(errs, fmt.Errorf("failure archive fsstore retry config: %w", err))
	}
	return errors.Join(errs...)
}

// ValidateFileArchiveLifecycleConfigProvider はfailure archiveのファイルサイズ、
// 保持世代数、保持日数、ディレクトリサイズに関する制約を検証します。
// 保持世代数・保持日数・ディレクトリサイズの0は上限なし（その条件では削除しない）として受け付け、
// 0以外の値には通常ログと同じ最小値を求めます。
func ValidateFileArchiveLifecycleConfigProvider(cfg FileArchiveConfigProvider) error {
	var errs []error
	if err := pkgfsstore.ValidateFileRotationWeekday(cfg.RotationType(), cfg.RotationDayOfWeek()); err != nil {
		errs = append(errs, err)
	}
	if err := pkgfsstore.ValidateFileLifecycleMode(cfg.LifecycleMode()); err != nil {
		errs = append(errs, err)
	}
	if cfg.LifecycleMode() == "" ||
		cfg.LifecycleMode() == pkgfsstore.FileLifecycleModeInline ||
		cfg.LifecycleMode() == pkgfsstore.FileLifecycleModeRunner {
		if err := pkgfsstore.ValidateUsageCacheTTL(cfg.UsageCacheTTL()); err != nil {
			errs = append(errs, err)
		}
	}
	if err := fsstore.ValidateFailureArchiveRetentionConfigProvider(cfg); err != nil {
		errs = append(errs, err)
	}

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("failure archive fsstore lifecycle config: %w", err)
	}
	return nil
}
