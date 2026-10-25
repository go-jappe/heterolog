package logger

import (
	"errors"
	"fmt"
)

// ConfigFieldは、ConfigFieldErrorで不正な設定項目を表す名前です。
// backend設定内のfield pathで表し、埋め込みstructから昇格したfieldはfield名だけで表します。
type ConfigField string

// ConfigFieldErrorが報告する設定項目です。backend設定のbuffer・送信上限・retry設定などは、
// 各backend設定で同じ名前のfieldを表します。
const (
	ConfigFieldLogLevel                 ConfigField = "RuntimeConfig.LogLevel"
	ConfigFieldErrorTraceDepth          ConfigField = "LogBodyConfig.ErrorTraceDepth"
	ConfigFieldErrorTraceDepthDebugMode ConfigField = "LogBodyConfig.ErrorTraceDepthDebugMode"
	ConfigFieldSamplingDebugRate        ConfigField = "SamplingConfig.SamplingDebugRate"
	ConfigFieldSamplingInfoRate         ConfigField = "SamplingConfig.SamplingInfoRate"
	ConfigFieldSamplingWarnRate         ConfigField = "SamplingConfig.SamplingWarnRate"
	ConfigFieldSamplingErrorRate        ConfigField = "SamplingConfig.SamplingErrorRate"
	ConfigFieldSamplingRequestIDRate    ConfigField = "SamplingConfig.LoggingByRequestIDRate"
	// ConfigFieldClientは、OpenSearchClientなど、backendのclient設定です。
	// Slackでは、Slack APIへの接続に使うTokenとChannelです。
	ConfigFieldClient ConfigField = "Client"
	// ConfigFieldBackendConfigは、LogOutputDestinationsで指定したbackendの設定自体です。
	ConfigFieldBackendConfig ConfigField = "BackendConfig"
	// ConfigFieldLogOutputDestinationsは、通常出力先の一覧です。
	ConfigFieldLogOutputDestinations ConfigField = "LogOutputDestinations"

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
	ConfigFieldRetryMaxAttempts        ConfigField = "MaxAttempts"
	ConfigFieldRetryBaseDelay          ConfigField = "BaseDelay"
	ConfigFieldRetryMaxDelay           ConfigField = "MaxDelay"
	ConfigFieldLogGroupName            ConfigField = "LogGroupName"
	ConfigFieldLogStreamName           ConfigField = "LogStreamName"
	// ConfigFieldStaticLabelsは、Lokiの全streamへ付与する追加labelです。
	ConfigFieldStaticLabels ConfigField = "StaticLabels"
	// ConfigFieldIndexは、OpenSearchの送信先indexです。
	ConfigFieldIndex ConfigField = "Index"
)

var (
	// ErrInvalidLogLevelは、log levelが定義済みのLogLevelではないことを表します。
	ErrInvalidLogLevel = errors.New("log level is invalid")
	// ErrNegativeErrorTraceDepthは、error trace depthが負値であることを表します。
	ErrNegativeErrorTraceDepth = errors.New("error trace depth must not be negative")
	// ErrInvalidSamplingRateは、samplingの割合が受け付ける範囲外であることを表します。
	ErrInvalidSamplingRate = errors.New("sampling rate is out of range")
	// ErrUnpairedInternalLogClientは、internal-log専用保存先のclient設定に対応する
	// logger設定がないことを表します。client設定だけでは専用保存先へ置き換わりません。
	ErrUnpairedInternalLogClient = errors.New(
		"internal log destination client config is set without its logger config",
	)
	// ErrClientConfigRequiredは、使用するbackendのclient設定がないことを表します。
	ErrClientConfigRequired = errors.New("backend client config is required")
	// ErrBackendConfigRequiredは、LogOutputDestinationsで指定したbackendの設定がないことを表します。
	ErrBackendConfigRequired = errors.New("log output destination config is required")
	// ErrUnsupportedLogOutputDestinationは、LogOutputDestinationsに未対応の出力先が含まれることを表します。
	ErrUnsupportedLogOutputDestination = errors.New("unsupported log output destination")
	// ErrNonPositiveSettingは、正の値が必要な件数・byte数・時間の設定が0以下であることを表します。
	ErrNonPositiveSetting = errors.New("logger setting must be positive")
	// ErrInconsistentSettingは、設定値が関連する別の設定値との大小関係を満たさないことを表します。
	// LimitにはValueと比較した設定値が入ります。
	ErrInconsistentSetting = errors.New("logger setting is inconsistent with a related setting")
	// ErrEmptySettingは、一つ以上の要素が必要な設定が空であることを表します。
	ErrEmptySetting = errors.New("logger setting must not be empty")
	// ErrUnsupportedRetryClientModeは、retry client modeが利用可能な値ではないことを表します。
	ErrUnsupportedRetryClientMode = errors.New("unsupported retry client mode")
	// ErrLogGroupMismatchは、CloudWatch Logsのlogger設定とclient設定でlog groupが異なることを表します。
	ErrLogGroupMismatch = errors.New("cloudwatch logs log group does not match client config")
	// ErrInvalidClientConfigは、backendのclient設定が、そのbackend packageの検証規則を
	// 満たさないことを表します。理由はbackend packageのerrorとして、errors.Isや
	// errors.Asで判別できます。
	ErrInvalidClientConfig = errors.New("backend client config is invalid")
	// ErrInvalidSettingは、Lokiのstatic labelやOpenSearchのindexなど、backend固有の
	// 規則を持つlogger設定の値が、その規則を満たさないことを表します。理由はbackend
	// packageのerrorとして、errors.Isやerrors.Asで判別できます。
	ErrInvalidSetting = errors.New("logger setting is invalid")
)

const (
	minSamplingRate = 1
	// maxRequestIDSamplingRateは、request IDの32-bit hashで割り切れずに表せる最大の割合です。
	maxRequestIDSamplingRate = 1<<32 - 1
)

// ConfigFieldErrorは、logger設定の一つの項目が不正であることを表します。
// errors.Asで取り出すとSource、Backend、Fieldで不正な設定を特定でき、Errのsentinelは
// errors.Isで判定できます。errors.Joinでまとめた複数の不正はConfigFieldErrorsで列挙します。
type ConfigFieldError struct {
	// Sourceは"logger"や"logger internal log destination"など、設定の出所です。
	Source string
	// Backendは不正な設定を持つbackendです。
	Backend LogOutputDestination
	// Fieldはbackend設定内の不正な項目です。
	Field ConfigField
	// Detailは、Sourceに続けて表示する不正な内容の説明です。
	Detail string
	// Valueは不正な値です。値を持たない検証ではnilです。
	Value any
	// Limitは、ErrInconsistentSettingなどでValueと比較した関連設定の値です。
	// 比較を伴わない検証ではnilです。
	Limit any
	// Errは不正の種類を表すsentinelです。
	Err error
	// causeは、下位のvalidatorが返した原因です。
	cause error
	// sourceInContextは、SourceとBackendを表示する上位のerrorに包まれていることを表します。
	sourceInContext bool
}

// Errorは、Sourceに続けてDetailを返します。SourceとBackendを表示する上位のerrorに
// 包まれている場合は、Detailだけを返します。
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

// Unwrapは、不正の種類と、下位のvalidatorが返した原因があればそれを返します。
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

// ConfigFieldErrorsは、errのerror treeに含まれるConfigFieldErrorを出現順に返します。
// errors.Joinやfmt.Errorfの%wでまとめた検証errorから、不正な設定をすべて列挙できます。
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

// ValidateLogLevelは、sourceのbackendに設定されたlog levelを検証します。
// 不正な値はErrInvalidLogLevelを持つConfigFieldErrorです。
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

// ValidateLogBodyConfigは、sourceのbackendに設定されたlog本文の表示設定を検証します。
// 負のtrace depthはErrNegativeErrorTraceDepthを持つConfigFieldErrorです。
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

// ValidateSamplingConfigは、sourceのbackendに設定されたsamplingの割合を検証します。
// samplingが有効な場合、各levelの割合は1以上です。request IDによるsamplingも有効なら、
// その割合は1以上2^32-1以下です。範囲外の値はErrInvalidSamplingRateを持つ
// ConfigFieldErrorです。
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
