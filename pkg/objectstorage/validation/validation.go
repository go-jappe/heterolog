// Package validation validates the typed settings of object storage upload
// destinations.
//
// It combines the validation of the target notation that the objectstorage
// package holds with the validation of the client settings that each provider
// package holds, and it provides a boundary through which config mappers and
// runtime constructors can reuse the same contract.
package validation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	s3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

// ValidateUploadTargets validates the upload targets already stored in a typed
// configuration without changing them. It does not silently correct variant
// spellings or duplicates. Whether a provider supports a target is decided by
// the caller, so this function validates only the canonical notation and the
// uniqueness. Requiring a non-empty slice is also the caller's responsibility.
// An invalid target is reported as an *UploadTargetError.
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

// ValidateLogFileUploadConfig validates the upload settings of the file roles
// that are actually created.
//
// In a configuration where error or debug logs are merged into the default
// file, the corresponding separate upload settings are not used at runtime, so
// they are not validated.
func ValidateLogFileUploadConfig(
	scope string,
	cfg objectstorage.LogFileUploadConfig,
	separateError bool,
	separateDebug bool,
) error {
	return ValidateLogFileUploadConfigProvider(scope, objectstorage.NewLogFileUploadConfigAdapter(cfg), separateError, separateDebug)
}

// ValidateLogFileUploadConfigProvider validates the upload settings of the log
// series that are actually created. When error or debug logs are merged into
// the normal logs, the separate settings of that series are not validated.
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

// ValidateUploadTargetConfig validates that an enabled upload destination
// specifies targets and that the client settings of each supported provider
// meet the contract of that provider.
func ValidateUploadTargetConfig(
	scope string,
	cfg objectstorage.UploadTargetConfig,
) error {
	return ValidateUploadTargetConfigProvider(scope, objectstorage.NewUploadTargetConfigAdapter(cfg))
}

// ValidateUploadTargetConfigProvider validates the enabled state of the
// destination and the settings of each service. S3 connection settings use the
// same validation rules as the client that uploads. With errors.Is and
// errors.As, a missing target can be identified as ErrUploadTargetsRequired,
// invalid S3 settings as ErrInvalidS3Config, and a target that no provider
// supports as *UnsupportedUploadTargetError.
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
