package logger

import (
	"errors"
	"fmt"
	"time"
)

// ConfigField is the name that identifies an invalid setting in a
// ConfigFieldError. It is the field path within the backend settings, and a
// field promoted from an embedded struct is named by its field name alone.
type ConfigField string

// These are settings that a ConfigFieldError reports. The settings of the
// backend configurations, such as buffers, send limits and retries, identify
// the field of the same name in each backend configuration. A
// ConfigFieldError can also name other fields by their path, such as
// "FsstoreConfig.Lifecycle.ErrorPurge".
const (
	ConfigFieldLogLevel                 ConfigField = "RuntimeConfig.LogLevel"
	ConfigFieldErrorTraceDepth          ConfigField = "LogBodyConfig.ErrorTraceDepth"
	ConfigFieldErrorTraceDepthDebugMode ConfigField = "LogBodyConfig.ErrorTraceDepthDebugMode"
	ConfigFieldSamplingDebugRate        ConfigField = "SamplingConfig.SamplingDebugRate"
	ConfigFieldSamplingInfoRate         ConfigField = "SamplingConfig.SamplingInfoRate"
	ConfigFieldSamplingWarnRate         ConfigField = "SamplingConfig.SamplingWarnRate"
	ConfigFieldSamplingErrorRate        ConfigField = "SamplingConfig.SamplingErrorRate"
	ConfigFieldSamplingRequestIDRate    ConfigField = "SamplingConfig.LoggingByRequestIDRate"
	// ConfigFieldClient identifies the client settings of a backend, such as
	// OpenSearchClient. For Slack, it identifies the Token and Channel used to
	// connect to the Slack API.
	ConfigFieldClient ConfigField = "Client"
	// ConfigFieldBackendConfig identifies the configuration itself of a backend
	// listed in LogOutputDestinations.
	ConfigFieldBackendConfig ConfigField = "BackendConfig"
	// ConfigFieldLogOutputDestinations identifies the list of regular output
	// destinations.
	ConfigFieldLogOutputDestinations ConfigField = "LogOutputDestinations"
	// ConfigFieldTimeout identifies the Timeout of LoggerConfig,
	// AsyncBestEffortMultiLoggerConfig and DeliveryLoggerConfig.
	ConfigFieldTimeout ConfigField = "Timeout"
	// ConfigFieldMaxParallel identifies
	// AsyncBestEffortMultiLoggerConfig.MaxParallel.
	ConfigFieldMaxParallel ConfigField = "MaxParallel"

	ConfigFieldFlushInterval           ConfigField = "FlushInterval"
	ConfigFieldMaxEntries              ConfigField = "MaxEntries"
	ConfigFieldBufferFlushBytes        ConfigField = "BufferFlushBytes"
	ConfigFieldBufferFlushMaxEvents    ConfigField = "BufferFlushMaxEvents"
	ConfigFieldFallbackQueueMaxEntries ConfigField = "FallbackQueueMaxEntries"
	ConfigFieldFallbackQueueMaxEvents  ConfigField = "FallbackQueueMaxEvents"
	ConfigFieldMaxLineBytes            ConfigField = "MaxLineBytes"
	ConfigFieldMaxPushRequestBytes     ConfigField = "MaxPushRequestBytes"
	ConfigFieldMaxBulkRequestBytes     ConfigField = "MaxBulkRequestBytes"
	ConfigFieldMaxBulkDocument         ConfigField = "MaxBulkDocument"
	ConfigFieldMaxInsertRequestBytes   ConfigField = "MaxInsertRequestBytes"
	ConfigFieldMaxLogsPerNotification  ConfigField = "MaxLogsPerNotification"
	ConfigFieldIntervals               ConfigField = "Intervals"
	ConfigFieldMaxRestThreshold        ConfigField = "MaxRestThreshold"
	ConfigFieldRetryClientMode         ConfigField = "RetryClientMode"
	ConfigFieldRetryMaxRetries         ConfigField = "MaxRetries"
	ConfigFieldRetryBaseDelay          ConfigField = "BaseDelay"
	ConfigFieldRetryMaxDelay           ConfigField = "MaxDelay"
	ConfigFieldLogGroupName            ConfigField = "LogGroupName"
	ConfigFieldLogStreamName           ConfigField = "LogStreamName"
	// ConfigFieldStaticLabels identifies the additional labels attached to every
	// Loki stream.
	ConfigFieldStaticLabels ConfigField = "StaticLabels"
	// ConfigFieldIndex identifies the OpenSearch index that entries are sent to.
	ConfigFieldIndex ConfigField = "Index"
)

var (
	// ErrInvalidLogLevel reports that a log level is not a defined LogLevel.
	ErrInvalidLogLevel = errors.New("log level is invalid")
	// ErrNegativeErrorTraceDepth reports that an error trace depth is negative.
	ErrNegativeErrorTraceDepth = errors.New("error trace depth must not be negative")
	// ErrInvalidSamplingRate reports that a sampling rate is outside the accepted
	// range.
	ErrInvalidSamplingRate = errors.New("sampling rate is out of range")
	// ErrUnpairedInternalLogClient reports that the client settings of a
	// dedicated internal-log destination have no corresponding logger settings.
	// Client settings alone do not replace the destination with the dedicated
	// one.
	ErrUnpairedInternalLogClient = errors.New(
		"internal log destination client config is set without its logger config",
	)
	// ErrClientConfigRequired reports that a backend in use has no client
	// settings.
	ErrClientConfigRequired = errors.New("backend client config is required")
	// ErrBackendConfigRequired reports that a backend listed in
	// LogOutputDestinations has no configuration.
	ErrBackendConfigRequired = errors.New("log output destination config is required")
	// ErrUnsupportedLogOutputDestination reports that LogOutputDestinations
	// contains an unsupported output destination.
	ErrUnsupportedLogOutputDestination = errors.New("unsupported log output destination")
	// ErrNonPositiveSetting reports that a count, byte size or duration setting
	// that must be positive is 0 or less.
	ErrNonPositiveSetting = errors.New("logger setting must be positive")
	// ErrNegativeSetting reports that a count setting for which 0 is valid, such
	// as a retry count, is negative.
	ErrNegativeSetting = errors.New("logger setting must not be negative")
	// ErrInconsistentSetting reports that a setting is larger or smaller than
	// another related setting allows. Limit holds the value of the setting that
	// Value was compared with.
	ErrInconsistentSetting = errors.New("logger setting is inconsistent with a related setting")
	// ErrEmptySetting reports that a setting that needs at least one element is
	// empty.
	ErrEmptySetting = errors.New("logger setting must not be empty")
	// ErrUnsupportedRetryClientMode reports that a retry client mode is not one
	// of the available values.
	ErrUnsupportedRetryClientMode = errors.New("unsupported retry client mode")
	// ErrLogGroupMismatch reports that the logger settings and the client
	// settings of CloudWatch Logs name different log groups.
	ErrLogGroupMismatch = errors.New("cloudwatch logs log group does not match client config")
	// ErrInvalidClientConfig reports that the client settings of a backend do
	// not satisfy the validation rules of that backend's package. The reason is
	// an error of the backend package, which errors.Is and errors.As can
	// identify.
	ErrInvalidClientConfig = errors.New("backend client config is invalid")
	// ErrInvalidSetting reports that the value of a logger setting with
	// backend-specific rules, such as a Loki static label or an OpenSearch
	// index, does not satisfy those rules. The reason is an error of the
	// backend package, which errors.Is and errors.As can identify.
	ErrInvalidSetting = errors.New("logger setting is invalid")
)

const (
	minSamplingRate = 1
	// maxRequestIDSamplingRateは、request IDの32-bit hashで割り切れずに表せる最大の割合です。
	maxRequestIDSamplingRate = 1<<32 - 1
)

// ConfigFieldError reports that one setting of a logger configuration is
// invalid. Retrieved with errors.As, it identifies the invalid setting by
// Source, Backend and Field, and errors.Is matches Err. Use ConfigFieldErrors
// to list several invalid settings combined with errors.Join.
type ConfigFieldError struct {
	// Source is where the setting comes from, such as "logger" or "logger
	// internal log destination".
	Source string
	// Backend is the backend that has the invalid setting.
	Backend LogOutputDestination
	// Field is the invalid setting within the backend configuration.
	Field ConfigField
	// Detail describes what is invalid, and is shown after Source.
	Detail string
	// Value is the invalid value. It is nil for checks that have no value.
	Value any
	// Limit is the value of the related setting that Value was compared with,
	// for example for ErrInconsistentSetting. It is nil for checks that involve
	// no comparison.
	Limit any
	// Err identifies the kind of problem. It is one of the sentinel errors of
	// this package, except for the purge settings of a file logger, where it
	// is the error that describes the invalid purge setting.
	Err error
	// causeは、下位のvalidatorが返した原因です。
	cause error
	// sourceInContextは、SourceとBackendを表示する上位のerrorに包まれていることを表します。
	sourceInContext bool
}

// Error returns Source followed by Detail. When the error is wrapped by an
// outer error that shows Source and Backend, it returns only Detail.
func (e *ConfigFieldError) Error() string {
	if e == nil {
		return "<nil>"
	}
	detail := e.Detail
	if detail == "" && e.Err != nil {
		detail = e.Err.Error()
	}
	if e.Source == "" || e.sourceInContext {
		return detail
	}
	return e.Source + " " + detail
}

// Unwrap returns the kind of problem and, if there is one, the cause returned
// by a lower-level validator.
func (e *ConfigFieldError) Unwrap() []error {
	if e == nil {
		return nil
	}
	if e.cause == nil {
		return []error{e.Err}
	}
	return []error{e.Err, e.cause}
}

// settingErrorは、sourceやbackendを持たない下位validatorの検証項目について、
// 従来どおりのmessageを持つConfigFieldErrorを作ります。
func settingError(field ConfigField, kind error, value any, format string, args ...any) *ConfigFieldError {
	return &ConfigFieldError{Field: field, Detail: fmt.Sprintf(format, args...), Value: value, Err: kind}
}

// backendLabelは、設定errorのmessageでbackendを表す名前です。
func backendLabel(backend LogOutputDestination) string {
	if backend == LogOutputCloudWatchLogs {
		return "cloudwatch logs"
	}
	return backend.String()
}

// wrapBackendSettingErrorsは、下位validatorのerrを、sourceのbackend設定のうちpartの不正として
// "<source> <backend> <part> is invalid: <err>"の形で包みます。errに含まれるSourceのない
// ConfigFieldErrorには、errors.Asで出所を特定できるようSourceとBackendを設定します。
// errがnilならnilを返します。
func wrapBackendSettingErrors(source string, backend LogOutputDestination, part string, err error) error {
	if err == nil {
		return nil
	}
	for _, fieldErr := range ConfigFieldErrors(err) {
		if fieldErr.Source == "" && fieldErr.Backend == "" {
			fieldErr.Source = source
			fieldErr.Backend = backend
			fieldErr.sourceInContext = true
		}
	}
	return fmt.Errorf("%s %s %s is invalid: %w", source, backendLabel(backend), part, err)
}

// backendRuleErrorは、backend packageの検証規則がsourceのbackend設定のfieldを拒否したことを、
// kindを持つConfigFieldErrorとして返します。messageは"<source> <context>: <cause>"で、
// causeはerrors.Isやerrors.Asで辿れます。
func backendRuleError(
	source string,
	backend LogOutputDestination,
	field ConfigField,
	value any,
	kind error,
	context string,
	cause error,
) *ConfigFieldError {
	return &ConfigFieldError{
		Source:  source,
		Backend: backend,
		Field:   field,
		Detail:  fmt.Sprintf("%s: %v", context, cause),
		Value:   value,
		Err:     kind,
		cause:   cause,
	}
}

// ConfigFieldErrors returns the ConfigFieldErrors in the error tree of err, in
// the order in which they appear. It lists every invalid setting in validation
// errors combined with errors.Join or with the %w verb of fmt.Errorf.
func ConfigFieldErrors(err error) []*ConfigFieldError {
	var found []*ConfigFieldError
	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}
		if fieldErr, ok := err.(*ConfigFieldError); ok {
			found = append(found, fieldErr)
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return found
}

// ValidateTimeout validates timeout, the Timeout of the logger configuration
// in source. A timeout that is not positive is reported as a ConfigFieldError
// for ConfigFieldTimeout with the kind ErrNonPositiveSetting.
func ValidateTimeout(source string, timeout time.Duration) error {
	if timeout > 0 {
		return nil
	}
	return &ConfigFieldError{
		Source: source,
		Field:  ConfigFieldTimeout,
		Detail: fmt.Sprintf("timeout must be positive: %s", timeout),
		Value:  timeout,
		Err:    ErrNonPositiveSetting,
	}
}

// ValidateMaxParallel validates maxParallel, the MaxParallel of the async
// logger configuration in source. A value that is not positive is reported as
// a ConfigFieldError for ConfigFieldMaxParallel with the kind
// ErrNonPositiveSetting.
func ValidateMaxParallel(source string, maxParallel int) error {
	if maxParallel > 0 {
		return nil
	}
	return &ConfigFieldError{
		Source: source,
		Field:  ConfigFieldMaxParallel,
		Detail: fmt.Sprintf("max parallel must be positive: %d", maxParallel),
		Value:  maxParallel,
		Err:    ErrNonPositiveSetting,
	}
}

// ValidateLogLevel validates level, the log level set for backend in source.
// An invalid value is reported as a ConfigFieldError with the kind
// ErrInvalidLogLevel.
func ValidateLogLevel(source string, backend LogOutputDestination, level LogLevel) error {
	if level.IsValid() {
		return nil
	}
	return &ConfigFieldError{
		Source:  source,
		Backend: backend,
		Field:   ConfigFieldLogLevel,
		Detail:  fmt.Sprintf("%s log level is invalid: %d", backend, level),
		Value:   level,
		Err:     ErrInvalidLogLevel,
	}
}

// ValidateLogBodyConfig validates body, the display settings of the log body
// set for backend in source. A negative trace depth is reported as a
// ConfigFieldError with the kind ErrNegativeErrorTraceDepth.
func ValidateLogBodyConfig(source string, backend LogOutputDestination, body LogBodyConfig) error {
	var errs []error
	if body.ErrorTraceDepth < 0 {
		errs = append(errs, &ConfigFieldError{
			Source:  source,
			Backend: backend,
			Field:   ConfigFieldErrorTraceDepth,
			Detail: fmt.Sprintf(
				"%s error trace depth must not be negative: %d",
				backend,
				body.ErrorTraceDepth,
			),
			Value: body.ErrorTraceDepth,
			Err:   ErrNegativeErrorTraceDepth,
		})
	}
	if body.ErrorTraceDepthDebugMode < 0 {
		errs = append(errs, &ConfigFieldError{
			Source:  source,
			Backend: backend,
			Field:   ConfigFieldErrorTraceDepthDebugMode,
			Detail: fmt.Sprintf(
				"%s debug error trace depth must not be negative: %d",
				backend,
				body.ErrorTraceDepthDebugMode,
			),
			Value: body.ErrorTraceDepthDebugMode,
			Err:   ErrNegativeErrorTraceDepth,
		})
	}
	return errors.Join(errs...)
}

// ValidateSamplingConfig validates the sampling rates in cfg, set for backend
// in source. When sampling is enabled, the rate of each level must be at
// least 1. When sampling by request ID is also enabled, its rate must be at
// least 1 and at most 2^32-1. A value out of range is reported as a
// ConfigFieldError with the kind ErrInvalidSamplingRate.
func ValidateSamplingConfig(source string, backend LogOutputDestination, cfg SamplingConfig) error {
	if !cfg.IsSamplingEnabled {
		return nil
	}
	var errs []error
	invalid := func(field ConfigField, value int, detail string) {
		errs = append(errs, &ConfigFieldError{
			Source:  source,
			Backend: backend,
			Field:   field,
			Detail:  fmt.Sprintf("%s %s", backend, detail),
			Value:   value,
			Err:     ErrInvalidSamplingRate,
		})
	}
	for _, rate := range []struct {
		field ConfigField
		name  string
		value int
	}{
		{field: ConfigFieldSamplingDebugRate, name: "debug", value: cfg.SamplingDebugRate},
		{field: ConfigFieldSamplingInfoRate, name: "info", value: cfg.SamplingInfoRate},
		{field: ConfigFieldSamplingWarnRate, name: "warn", value: cfg.SamplingWarnRate},
		{field: ConfigFieldSamplingErrorRate, name: "error", value: cfg.SamplingErrorRate},
	} {
		if rate.value < minSamplingRate {
			invalid(rate.field, rate.value, fmt.Sprintf(
				"sampling %s rate must be at least %d, but got %d",
				rate.name,
				minSamplingRate,
				rate.value,
			))
		}
	}
	if cfg.EnableLoggingByRequestID {
		rate := cfg.LoggingByRequestIDRate
		if rate < minSamplingRate || uint64(rate) > maxRequestIDSamplingRate {
			invalid(ConfigFieldSamplingRequestIDRate, rate, fmt.Sprintf(
				"sampling request_id rate must be between %d and %d, but got %d",
				minSamplingRate,
				uint64(maxRequestIDSamplingRate),
				rate,
			))
		}
	}
	return errors.Join(errs...)
}

// missingClientConfigErrorは、使用するbackendにclient設定がないことを、
// ErrClientConfigRequiredを持つConfigFieldErrorとして返します。
func missingClientConfigError(source string, backend LogOutputDestination, name string) error {
	return &ConfigFieldError{
		Source:  source,
		Backend: backend,
		Field:   ConfigFieldClient,
		Detail:  name + " client config is nil",
		Err:     ErrClientConfigRequired,
	}
}
