package sampling

import (
	"errors"
	"strings"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// MaxRequestIDSamplingRateは、request IDの32-bit hashで割り切れずに表せる最大の割合です。
// 設定の検証規則はpkg/loggerのValidateSamplingConfigが所有し、この値は直接呼び出しに
// 対する実行時の防御（ShouldSampleByRequestId）だけに使います。untyped定数なので
// 32-bit環境でもcompileできます。
const MaxRequestIDSamplingRate = 1<<32 - 1

// ErrSamplingConfigRequiredは、検証するsampling設定providerがnilであることを表します。
var ErrSamplingConfigRequired = errors.New("sampling config is nil")

type ValidationSamplingConfigProvider interface {
	IsSamplingEnabled() bool
	EnableLoggingByRequestID() bool
	SamplingDebugRate() int
	SamplingInfoRate() int
	SamplingWarnRate() int
	SamplingErrorRate() int
	LoggingByRequestIDRate() int
}

// ValidateSamplingConfigProviderは、backendを特定しない呼び出し元向けに
// ValidateSamplingConfigProviderForを実行します。呼び出し元は、どのloggerの設定かを
// errorの文脈として付け加えてください。
func ValidateSamplingConfigProvider(cfg ValidationSamplingConfigProvider) error {
	return ValidateSamplingConfigProviderFor("", cfg)
}

// ValidateSamplingConfigProviderForは、providerの値をpkg/loggerのSamplingConfigへ写し、
// pkglogger.ValidateSamplingConfigと同じ規則で検証します。規則は一か所で管理し、範囲外の
// 割合はpkglogger.ErrInvalidSamplingRateを持つpkglogger.ConfigFieldErrorです。Backendと
// Fieldで不正な設定項目を特定でき、文言には"sampling <level> rate"を含みます。
// providerがnilの場合はErrSamplingConfigRequiredを返します。
func ValidateSamplingConfigProviderFor(
	backend pkglogger.LogOutputDestination,
	cfg ValidationSamplingConfigProvider,
) error {
	if cfg == nil {
		return ErrSamplingConfigRequired
	}
	err := pkglogger.ValidateSamplingConfig("", backend, pkglogger.SamplingConfig{
		IsSamplingEnabled:        cfg.IsSamplingEnabled(),
		EnableLoggingByRequestID: cfg.EnableLoggingByRequestID(),
		LoggingByRequestIDRate:   cfg.LoggingByRequestIDRate(),
		SamplingDebugRate:        cfg.SamplingDebugRate(),
		SamplingInfoRate:         cfg.SamplingInfoRate(),
		SamplingWarnRate:         cfg.SamplingWarnRate(),
		SamplingErrorRate:        cfg.SamplingErrorRate(),
	})
	if backend == "" {
		// backend名を前置する文言から、空のbackend名に続く区切りだけを除きます。
		for _, fieldErr := range pkglogger.ConfigFieldErrors(err) {
			fieldErr.Detail = strings.TrimPrefix(fieldErr.Detail, " ")
		}
	}
	return err
}
