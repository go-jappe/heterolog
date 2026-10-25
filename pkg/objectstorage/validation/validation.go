// Package validation は、object storage upload先の型付き設定を検証します。
//
// objectstorage packageが保持するtarget表記の検証と、各provider packageが保持する
// client設定の検証を組み合わせ、config mapperとruntime constructorで同じ契約を
// 再利用できる境界を提供します。
package validation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

// ValidateUploadTargetsは、型付き設定へ格納済みのupload targetを変更せず検証します。
// 表記揺れや重複は暗黙に補正しません。対応providerの有無は利用側が判断するため、
// ここではcanonicalな表記と一意性だけを検証します。空のsliceの必須性も利用側の
// 責務です。不正なtargetは*UploadTargetErrorです。
func ValidateUploadTargets(targets []objectstorage.UploadTarget) error {
	seen := make(map[objectstorage.UploadTarget]struct{}, len(targets))
	for index, target := range targets {
		canonical := objectstorage.UploadTarget(strings.ToLower(strings.TrimSpace(string(target))))
		if canonical == "" {
			return &UploadTargetError{Index: index, Target: target, Err: ErrUploadTargetRequired}
		}
		if target != canonical {
			return &UploadTargetError{
				Index:     index,
				Target:    target,
				Canonical: canonical,
				Err:       ErrNonCanonicalUploadTarget,
			}
		}
		if _, exists := seen[target]; exists {
			return &UploadTargetError{Index: index, Target: target, Err: ErrDuplicateUploadTarget}
		}
		seen[target] = struct{}{}
	}
	return nil
}

// ValidateLogFileUploadConfigは、実際に生成されるfile roleのupload設定を検証します。
//
// error/debugがdefault fileへ合流する構成では、対応する個別upload設定はruntimeで
// 使用されないため検証しません。
func ValidateLogFileUploadConfig(
	scope string,
	cfg objectstorage.LogFileUploadConfig,
	separateError bool,
	separateDebug bool,
) error {
	return ValidateLogFileUploadConfigProvider(scope, objectstorage.NewLogFileUploadConfigAdapter(cfg), separateError, separateDebug)
}

// ValidateLogFileUploadConfigProviderは、実際に生成されるログ系列の送信設定を検証します。
// error・debugを通常ログへまとめる場合、その系列の個別設定は検証しません。
func ValidateLogFileUploadConfigProvider(
	scope string,
	cfg objectstorage.LogFileUploadConfigProvider,
	separateError bool,
	separateDebug bool,
) error {
	if cfg == nil {
		return fmt.Errorf("%s object storage upload config provider is required", scope)
	}
	if !cfg.Enabled() {
		return nil
	}

	var errs []error
	errs = append(errs, ValidateUploadTargetConfigProvider(scope+" default", cfg.Default()))
	if separateError {
		errs = append(errs, ValidateUploadTargetConfigProvider(scope+" error", cfg.Error()))
	}
	if separateDebug {
		errs = append(errs, ValidateUploadTargetConfigProvider(scope+" debug", cfg.Debug()))
	}
	return errors.Join(errs...)
}

// ValidateUploadTargetConfigは、有効なupload先にtargetが指定され、対応providerの
// client設定がそのproviderの契約を満たすことを検証します。
func ValidateUploadTargetConfig(
	scope string,
	cfg objectstorage.UploadTargetConfig,
) error {
	return ValidateUploadTargetConfigProvider(scope, objectstorage.NewUploadTargetConfigAdapter(cfg))
}

// ValidateUploadTargetConfigProviderは、送信先の有効・無効と各サービスの設定を検証します。
// S3の接続設定は、送信用clientと同じ検証規則を使用します。targetの未指定は
// ErrUploadTargetsRequired、S3設定の不正はErrInvalidS3Config、対応providerのないtargetは
// *UnsupportedUploadTargetErrorとerrors.Is・errors.Asで判別できます。
func ValidateUploadTargetConfigProvider(
	scope string,
	cfg objectstorage.UploadTargetConfigProvider,
) error {
	if cfg == nil {
		return fmt.Errorf("%s object storage upload target config provider is required", scope)
	}
	if !cfg.Enabled() {
		return nil
	}
	targets := cfg.Targets()
	if len(targets) == 0 {
		return fmt.Errorf("%s %w", scope, ErrUploadTargetsRequired)
	}
	if err := ValidateUploadTargets(targets); err != nil {
		return fmt.Errorf("%s object storage upload target is invalid: %w", scope, err)
	}

	var errs []error
	for _, target := range targets {
		switch target {
		case objectstorage.UploadTargetS3:
			providers := cfg.S3DestinationConfigs()
			configs := make([]objectstorage.S3Config, len(providers))
			for index, provider := range providers {
				if provider == nil {
					return fmt.Errorf("%s s3 config provider is required at index %d", scope, index)
				}
				configs[index] = s3uploader.ConfigFromProvider(provider)
			}
			if _, err := s3uploader.ResolveDestinationIDs(configs); err != nil {
				errs = append(errs, fmt.Errorf("%s %w: %w", scope, ErrInvalidS3Config, err))
			}
		default:
			errs = append(errs, &UnsupportedUploadTargetError{Scope: scope, Target: target})
		}
	}
	return errors.Join(errs...)
}
