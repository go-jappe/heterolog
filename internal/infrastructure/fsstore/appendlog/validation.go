package appendlog

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

const (
	ExpectedFileDateFormat = "20060102"
)

const (
	ErrorMsgInvalidTarget               = "target must not be empty"
	ErrorMsgInvalidLogPath              = "log path must not be empty"
	ErrorMsgInvalidFileNameFormat       = "invalid file name format: format=%q err=%w"
	ErrorMsgInvalidFileDateFormat       = "invalid file date format: only %q is allowed"
	ErrorMsgFileNameFormatNoExtension   = "file name format must include an extension"
	ErrorMsgFileNameFormatInvalidPrefix = "file name format must start with %q"
	ErrorMsgFileNameFormatEmptyExt      = "extension must not be empty"
	ErrorMsgFileNameFormatExtWhitespace = "extension must not contain leading or trailing whitespace"
	ErrorMsgFileNameFormatExtSeparator  = "extension must not contain path separators"
	ErrorMsgFileNameFormatExtControl    = "extension must not contain control characters"
)

type ValidationLayoutConfigProvider interface {
	Target() string
	LogPath() string     // ex.) /var/log/{target}/  {/var/log部分}
	LogDir() string      // ex.) /var/log/{target}/{LogDir} 使用時は空文字不可
	ErrorLogDir() string // ex.) /var/log/{target}/{ErrorLogDir} 空文字許容
	FileNameFormat() string
	FileDateFormat() string
}

func ValidateLayoutConfigProvider(cfg ValidationLayoutConfigProvider) error {
	var errs []error

	if cfg.Target() == "" {
		errs = append(errs, errors.New(ErrorMsgInvalidTarget))
	}
	errs = append(errs, pkgfsstore.ValidateLogFileNameValue("target", cfg.Target()))

	if cfg.LogPath() == "" {
		errs = append(errs, errors.New(ErrorMsgInvalidLogPath))
	}
	errs = append(errs, pkgfsstore.ValidateLogDirectory(cfg.LogDir()))

	if err := validateFileNameFormat(cfg.FileNameFormat()); err != nil {
		errs = append(errs, fmt.Errorf(
			ErrorMsgInvalidFileNameFormat,
			cfg.FileNameFormat(),
			err,
		))
	}

	if cfg.FileDateFormat() != ExpectedFileDateFormat {
		errs = append(errs, fmt.Errorf(
			ErrorMsgInvalidFileDateFormat,
			ExpectedFileDateFormat,
		))
	}

	return errors.Join(errs...)
}

// ValidateLogFileNameConfigProviderは、mappingとLogger/runner生成入口で共用する
// file名設定の検証です。directoryのドライブ指定は検証せず、実際のfile名に使用する
// 値だけを対象にします。分離が無効なroleのprefixは使用されないため検証しません。
func ValidateLogFileNameConfigProvider(cfg FsstoreLayoutConfigProvider) error {
	errs := []error{
		pkgfsstore.ValidateLogFileNameValue("target", cfg.Target()),
		pkgfsstore.ValidateLogFileNameValue("file name format", cfg.FileNameFormat()),
		pkgfsstore.ValidateLogFileNameValue("file date format", cfg.FileDateFormat()),
	}
	if cfg.IsSeparateErrorLogsEnabled() {
		errs = append(errs, pkgfsstore.ValidateLogFileNameValue("error log prefix", cfg.SeparateErrorLogsPrefix()))
	}
	if cfg.IsSeparateDebugLogsEnabled() {
		errs = append(errs, pkgfsstore.ValidateLogFileNameValue("debug log prefix", cfg.SeparateDebugLogsPrefix()))
	}
	return errors.Join(errs...)
}

// ファイル名形式規則のバリデーション
func validateFileNameFormat(format string) error {
	const base = "/%s%s_%s_%d"

	if len(format) <= len(base) {
		return errors.New(ErrorMsgFileNameFormatNoExtension)
	}

	if !strings.HasPrefix(format, base) {
		return fmt.Errorf(ErrorMsgFileNameFormatInvalidPrefix, base)
	}

	ext := format[len(base):]

	if ext == "" {
		return errors.New(ErrorMsgFileNameFormatEmptyExt)
	}

	if strings.TrimSpace(ext) != ext {
		return errors.New(ErrorMsgFileNameFormatExtWhitespace)
	}

	if strings.ContainsAny(ext, "/\\") {
		return errors.New(ErrorMsgFileNameFormatExtSeparator)
	}
	if err := pkgfsstore.ValidateLogFileNameValue("file extension", ext); err != nil {
		return err
	}

	for _, r := range ext {
		if unicode.IsControl(r) {
			return errors.New(ErrorMsgFileNameFormatExtControl)
		}
	}

	return nil
}
