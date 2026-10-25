package logger

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

// fieldErrorKeyは、ConfigFieldErrorのうち比較する識別情報です。
type fieldErrorKey struct {
	source  string
	backend LogOutputDestination
	field   ConfigField
	kind    error
}

func fieldErrorKeys(t *testing.T, err error) []fieldErrorKey {
	t.Helper()
	var keys []fieldErrorKey
	for _, fieldErr := range ConfigFieldErrors(err) {
		if fieldErr.Err == nil || !errors.Is(fieldErr, fieldErr.Err) {
			t.Fatalf("ConfigFieldError %+v must unwrap to its sentinel", fieldErr)
		}
		keys = append(keys, fieldErrorKey{fieldErr.Source, fieldErr.Backend, fieldErr.Field, fieldErr.Err})
	}
	return keys
}

func TestValidatePresentationSettingsIdentifyBackendAndField(t *testing.T) {
	if err := ValidateLogLevel("logger", LogOutputStdout, LogLevelInfo); err != nil {
		t.Fatalf("ValidateLogLevel(valid) error = %v", err)
	}
	err := ValidateLogLevel("logger", LogOutputStdout, 999)
	if !errors.Is(err, ErrInvalidLogLevel) || !slices.Equal(fieldErrorKeys(t, err), []fieldErrorKey{
		{"logger", LogOutputStdout, ConfigFieldLogLevel, ErrInvalidLogLevel},
	}) {
		t.Fatalf("ValidateLogLevel(invalid) error = %v", err)
	}

	if err := ValidateLogBodyConfig("logger", LogOutputFile, LogBodyConfig{}); err != nil {
		t.Fatalf("ValidateLogBodyConfig(zero depth) error = %v", err)
	}
	err = ValidateLogBodyConfig("logger internal log destination", LogOutputFile, LogBodyConfig{
		ErrorTraceDepth: -1, ErrorTraceDepthDebugMode: -1,
	})
	if !slices.Equal(fieldErrorKeys(t, err), []fieldErrorKey{
		{"logger internal log destination", LogOutputFile, ConfigFieldErrorTraceDepth, ErrNegativeErrorTraceDepth},
		{"logger internal log destination", LogOutputFile, ConfigFieldErrorTraceDepthDebugMode, ErrNegativeErrorTraceDepth},
	}) {
		t.Fatalf("ValidateLogBodyConfig(negative depths) error = %v", err)
	}
}

// samplingは有効な場合だけ割合を検証し、request IDの割合は32-bit hashで表せる範囲に限ります。
func TestValidateSamplingConfigChecksRatesOnlyWhenEnabled(t *testing.T) {
	if err := ValidateSamplingConfig("logger", LogOutputStdout, SamplingConfig{}); err != nil {
		t.Fatalf("disabled sampling error = %v, want nil", err)
	}
	valid := SamplingConfig{
		IsSamplingEnabled: true, EnableLoggingByRequestID: true, LoggingByRequestIDRate: 1,
		SamplingDebugRate: 1, SamplingInfoRate: 1, SamplingWarnRate: 1, SamplingErrorRate: 1,
	}
	if err := ValidateSamplingConfig("logger", LogOutputStdout, valid); err != nil {
		t.Fatalf("minimum rates error = %v, want nil", err)
	}

	zeroRates := valid
	zeroRates.SamplingDebugRate, zeroRates.SamplingInfoRate = 0, 0
	zeroRates.SamplingWarnRate, zeroRates.SamplingErrorRate = 0, 0
	zeroRates.LoggingByRequestIDRate = 0
	key := func(field ConfigField) fieldErrorKey {
		return fieldErrorKey{"logger", LogOutputStdout, field, ErrInvalidSamplingRate}
	}
	if got := fieldErrorKeys(t, ValidateSamplingConfig("logger", LogOutputStdout, zeroRates)); !slices.Equal(got, []fieldErrorKey{
		key(ConfigFieldSamplingDebugRate),
		key(ConfigFieldSamplingInfoRate),
		key(ConfigFieldSamplingWarnRate),
		key(ConfigFieldSamplingErrorRate),
		key(ConfigFieldSamplingRequestIDRate),
	}) {
		t.Fatalf("zero rates errors = %+v", got)
	}

	// request IDによるsamplingが無効なら、その割合は使われないため検証しません。
	requestIDDisabled := zeroRates
	requestIDDisabled.EnableLoggingByRequestID = false
	if got := fieldErrorKeys(t, ValidateSamplingConfig("logger", LogOutputStdout, requestIDDisabled)); slices.Contains(
		got, key(ConfigFieldSamplingRequestIDRate),
	) {
		t.Fatalf("disabled request-ID sampling was validated: %+v", got)
	}

	if strconv.IntSize < 64 {
		return
	}
	for _, rate := range []uint64{1<<32 - 1, 1 << 32} {
		cfg := valid
		cfg.LoggingByRequestIDRate = int(rate)
		err := ValidateSamplingConfig("logger", LogOutputStdout, cfg)
		if wantErr := rate > 1<<32-1; errors.Is(err, ErrInvalidSamplingRate) != wantErr {
			t.Fatalf("request-ID rate %d error = %v, want error=%t", rate, err, wantErr)
		}
	}
}

func TestConfigFieldErrorsCollectsWrappedAndJoinedErrors(t *testing.T) {
	first := &ConfigFieldError{Source: "logger", Backend: LogOutputFile, Field: ConfigFieldLogLevel, Err: ErrInvalidLogLevel}
	second := &ConfigFieldError{Source: "logger", Backend: LogOutputLoki, Field: ConfigFieldClient, Err: ErrClientConfigRequired}
	err := fmt.Errorf("construct: %w", errors.Join(first, errors.New("other"), fmt.Errorf("nested: %w", second)))
	if got := ConfigFieldErrors(err); !slices.Equal(got, []*ConfigFieldError{first, second}) {
		t.Fatalf("ConfigFieldErrors() = %v, want both field errors in order", got)
	}
	if got := ConfigFieldErrors(nil); len(got) != 0 {
		t.Fatalf("ConfigFieldErrors(nil) = %v, want none", got)
	}
}

func TestValidateMaxLogEntryBytesReturnsSentinel(t *testing.T) {
	if err := ValidateMaxLogEntryBytes(1); err != nil {
		t.Fatalf("ValidateMaxLogEntryBytes(1) error = %v", err)
	}
	for _, value := range []int{0, -1} {
		if err := ValidateMaxLogEntryBytes(value); !errors.Is(err, ErrInvalidMaxLogEntryBytes) {
			t.Fatalf("ValidateMaxLogEntryBytes(%d) error = %v, want ErrInvalidMaxLogEntryBytes", value, err)
		}
	}
}

// 解決できないselectorは、指定したsource、log level、前後の空白を除いたselectorを持つ
// HandlerSelectorErrorです。
func TestHandlerSelectorErrorIdentifiesSourceLevelAndSelector(t *testing.T) {
	err := ValidateLoggerHandlerRouting("logger", LoggerConfig{
		Stdout: &StdoutLoggerConfig{HandlerConfig: loggerhandler.HandlerConfig{OnWarnConfig: []string{" unknown "}}},
	})
	var selectorErr *HandlerSelectorError
	if !errors.As(err, &selectorErr) || !errors.Is(err, ErrHandlerSelectorNotConfigured) {
		t.Fatalf("ValidateLoggerHandlerRouting() error = %v, want HandlerSelectorError", err)
	}
	if want := (HandlerSelectorError{Source: "logger stdout", Level: "warn", Selector: "unknown"}); *selectorErr != want {
		t.Fatalf("HandlerSelectorError = %+v, want %+v", *selectorErr, want)
	}
}

// findFieldErrorは、errに含まれるfieldとkindのConfigFieldErrorを返します。なければnilです。
func findFieldError(err error, field ConfigField, kind error) *ConfigFieldError {
	for _, fieldErr := range ConfigFieldErrors(err) {
		if fieldErr.Field == field && errors.Is(fieldErr, kind) {
			return fieldErr
		}
	}
	return nil
}

// findBackendFieldErrorは、errに含まれるConfigFieldErrorのうち、sourceのbackendのfieldを
// kindの不正として示す最初のものを返します。見つからなければnilです。
func findBackendFieldError(
	err error,
	source string,
	backend LogOutputDestination,
	field ConfigField,
	kind error,
) *ConfigFieldError {
	for _, fieldErr := range ConfigFieldErrors(err) {
		if fieldErr.Source == source && fieldErr.Backend == backend && fieldErr.Field == field &&
			errors.Is(fieldErr, kind) {
			return fieldErr
		}
	}
	return nil
}

// ConfigFieldErrorの表示形式は、利用者がどの設定を直すかを読むerror文字列なので、一度だけ
// 固定します。上位のerrorがSourceとbackendを表示する項目は、Detailだけを表示します。
func TestConfigFieldErrorFormat(t *testing.T) {
	if got, want := ValidateLokiMaxEntries(0).Error(), "max entries must be greater than zero: 0"; got != want {
		t.Fatalf("leaf error = %q, want %q", got, want)
	}

	err := ValidateLoggerBackendConfigs("logger", LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputLoki},
		Loki: &LokiConfig{LokiLoggerBaseConfig: LokiLoggerBaseConfig{
			FlushInterval:           time.Second,
			BufferFlushBytes:        1,
			FallbackQueueMaxEntries: 1,
			MaxLineBytes:            1,
			MaxPushRequestBytes:     2,
		}},
		LokiClient: &pkgloki.Config{},
	})
	// client設定の理由の文言はpkg/lokiが所有するため、そのsentinelの文言から期待値を組み立てます。
	wantClient := "logger loki client config is invalid: " + pkgloki.ErrEndpointRequired.Error()
	want := "logger loki logger config is invalid: max entries must be greater than zero: 0\n" + wantClient
	if err == nil || err.Error() != want {
		t.Fatalf("backend error = %q, want %q", err, want)
	}
	fieldErr := findBackendFieldError(err, "logger", LogOutputLoki, ConfigFieldMaxEntries, ErrNonPositiveSetting)
	if fieldErr == nil || fieldErr.Error() != "max entries must be greater than zero: 0" {
		t.Fatalf("attributed field error = %v, want detail only", fieldErr)
	}
	clientErr := findBackendFieldError(err, "logger", LogOutputLoki, ConfigFieldClient, ErrInvalidClientConfig)
	if clientErr == nil || !errors.Is(clientErr, pkgloki.ErrEndpointRequired) || clientErr.Error() != wantClient {
		t.Fatalf("client config error = %v, want source and backend", clientErr)
	}
}
