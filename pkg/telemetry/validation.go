package telemetry

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

var (
	// ErrNonPositiveSetting reports that a count or duration that must be
	// positive is 0 or less.
	ErrNonPositiveSetting = errors.New("telemetry setting must be positive")
	// ErrNegativeSetting reports that a count for which 0 is valid, such as
	// Retries, is negative.
	ErrNegativeSetting = errors.New("telemetry setting must not be negative")
	// ErrInvalidSetting reports that a setting is outside its accepted values,
	// such as a SamplingRatio above 1 or an endpoint that is not an http or
	// https URL.
	ErrInvalidSetting = errors.New("telemetry setting is invalid")
)

// FieldError reports one invalid setting of a Config. Retrieved with
// errors.As, it names the setting by its path in Field, such as
// "Traces.OTLP.Timeout", and errors.Is matches Err.
type FieldError struct {
	Field string
	// Detail describes what is invalid.
	Detail string
	// Value is the invalid value. It is nil for settings that can contain
	// credentials, such as Traces.OTLP.Endpoint and Traces.OTLP.Headers.
	Value any
	// Err is ErrNonPositiveSetting, ErrNegativeSetting or ErrInvalidSetting.
	Err error
}

// Error returns the setting and what is invalid.
func (e *FieldError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return "telemetry " + e.Field + ": " + e.Detail
}

// Unwrap returns Err.
func (e *FieldError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Validate checks the settings that heterolog.NewTelemetry uses and returns
// a FieldError for each invalid one, joined with errors.Join. Settings of
// destinations that Traces.Exporters and Metrics.Exporters do not list are not
// checked.
func (c Config) Validate() error {
	return errors.Join(c.Traces.validate(), c.Metrics.validate())
}

func (c TracesConfig) validate() error {
	var errs []error
	if math.IsNaN(c.SamplingRatio) || c.SamplingRatio < 0 || c.SamplingRatio > 1 {
		errs = append(errs, &FieldError{
			Field: "Traces.SamplingRatio", Value: c.SamplingRatio, Err: ErrInvalidSetting,
			Detail: fmt.Sprintf("must be from 0 to 1, got %v", c.SamplingRatio),
		})
	}
	if len(c.Exporters) == 0 {
		return errors.Join(errs...)
	}

	seen := make(map[TraceExporter]bool, len(c.Exporters))
	for _, exporter := range c.Exporters {
		switch exporter {
		case TraceExporterOTLP, TraceExporterOpenSearch, TraceExporterFile, TraceExporterStdout:
		default:
			errs = append(errs, &FieldError{
				Field: "Traces.Exporters", Value: exporter, Err: ErrInvalidSetting,
				Detail: fmt.Sprintf("unsupported exporter %q; use otlp, opensearch, file or stdout", exporter),
			})
			continue
		}
		if seen[exporter] {
			errs = append(errs, &FieldError{
				Field: "Traces.Exporters", Value: exporter, Err: ErrInvalidSetting,
				Detail: fmt.Sprintf("exporter %q is listed more than once", exporter),
			})
		}
		seen[exporter] = true
	}
	errs = append(errs,
		positive("Traces.QueueSize", c.QueueSize),
		positive("Traces.BatchSize", c.BatchSize),
		positive("Traces.ExportInterval", c.ExportInterval),
	)
	if c.QueueSize > 0 && c.BatchSize > c.QueueSize {
		errs = append(errs, &FieldError{
			Field: "Traces.BatchSize", Value: c.BatchSize, Err: ErrInvalidSetting,
			Detail: fmt.Sprintf("must not exceed Traces.QueueSize %d, got %d", c.QueueSize, c.BatchSize),
		})
	}
	if seen[TraceExporterOTLP] {
		errs = append(errs, c.OTLP.validate("Traces.OTLP"))
	}
	if seen[TraceExporterOpenSearch] {
		errs = append(errs, c.OpenSearch.validate("Traces.OpenSearch"))
	}
	if seen[TraceExporterFile] && strings.TrimSpace(c.File.Path) == "" {
		errs = append(errs, &FieldError{
			Field: "Traces.File.Path", Value: c.File.Path, Err: ErrInvalidSetting,
			Detail: "must not be empty when the file exporter is used",
		})
	}
	return errors.Join(errs...)
}

// validateは、prefix（Traces.OTLPなど）から始まるfieldの名前で、設定を確かめます。
func (c HTTPConfig) validate(prefix string) error {
	var errs []error
	// endpointはuserinfoなどに認証情報を含み得るため、errorに値を入れません。
	if u, err := url.Parse(c.Endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, &FieldError{
			Field: prefix + ".Endpoint", Err: ErrInvalidSetting,
			Detail: "must be an absolute http or https URL; the value is not shown because it can contain credentials",
		})
	}
	for name, value := range c.Headers {
		if !isHeaderToken(name) || strings.ContainsAny(value, "\r\n\x00") {
			errs = append(errs, &FieldError{
				Field: prefix + ".Headers", Err: ErrInvalidSetting,
				Detail: "must have header names made of token characters and values without line breaks; the value is not shown because it can contain credentials",
			})
			break
		}
	}
	if c.Compression != "gzip" && c.Compression != "none" {
		errs = append(errs, &FieldError{
			Field: prefix + ".Compression", Value: c.Compression, Err: ErrInvalidSetting,
			Detail: fmt.Sprintf("must be gzip or none, got %q", c.Compression),
		})
	}
	errs = append(errs,
		positive(prefix+".Timeout", c.Timeout),
		validateRetries(prefix, c.Retries, c.RetryBaseDelay, c.RetryMaxDelay),
		c.FailureStore.validate(prefix+".FailureStore"),
	)
	return errors.Join(errs...)
}

// validateは、prefix（Traces.OpenSearch）から始まるfieldの名前で、設定を確かめます。接続の
// 設定は、ログの出力先と同じくpkg/opensearchの検証で確かめます。その検証のerrorは、
// 認証情報を含みません。
func (c OpenSearchConfig) validate(prefix string) error {
	var errs []error
	if err := opensearch.ValidateConfig(c.Client); err != nil {
		errs = append(errs, &FieldError{Field: prefix + ".Client", Err: ErrInvalidSetting, Detail: err.Error()})
	}
	if err := opensearch.ValidateIndex(c.Index); err != nil {
		errs = append(errs, &FieldError{Field: prefix + ".Index", Value: c.Index, Err: ErrInvalidSetting, Detail: err.Error()})
	}
	errs = append(errs,
		validateRetries(prefix, c.Retries, c.RetryBaseDelay, c.RetryMaxDelay),
		c.FailureStore.validate(prefix+".FailureStore"),
	)
	return errors.Join(errs...)
}

// validateRetriesは、送り直す回数と、送り直す前に待つ時間の幅を確かめます。待つ時間は、
// 送り直す場合だけ確かめます。
func validateRetries(prefix string, retries int, baseDelay, maxDelay time.Duration) error {
	if retries < 0 {
		return &FieldError{
			Field: prefix + ".Retries", Value: retries, Err: ErrNegativeSetting,
			Detail: fmt.Sprintf("must not be negative, got %d", retries),
		}
	}
	if retries == 0 {
		return nil
	}
	errs := []error{
		positive(prefix+".RetryBaseDelay", baseDelay),
		positive(prefix+".RetryMaxDelay", maxDelay),
	}
	if baseDelay > 0 && maxDelay > 0 && maxDelay < baseDelay {
		errs = append(errs, &FieldError{
			Field: prefix + ".RetryMaxDelay", Value: maxDelay, Err: ErrInvalidSetting,
			Detail: fmt.Sprintf("must not be shorter than %s.RetryBaseDelay %v, got %v", prefix, baseDelay, maxDelay),
		})
	}
	return errors.Join(errs...)
}

// validateは、failure storeの選び方を確かめます。store自体の設定は、ログの出力先と
// 同じく、NewTelemetryがstoreを作るときに確かめます。
func (c FailureStoreConfig) validate(field string) error {
	if len(c.Store.DurableType) == 0 {
		return nil
	}
	if err := durable.ValidateFailureStoreDurableTypes(c.Store.DurableType); err != nil {
		return &FieldError{Field: field + ".Store.DurableType", Value: c.Store.DurableType, Err: ErrInvalidSetting, Detail: err.Error()}
	}
	if !c.Enabled() {
		return nil
	}
	var errs []error
	if err := durable.ValidateFailureHandlerMaxConcurrent(c.Store.MaxConcurrent); err != nil {
		errs = append(errs, &FieldError{Field: field + ".Store.MaxConcurrent", Value: c.Store.MaxConcurrent, Err: ErrNonPositiveSetting, Detail: err.Error()})
	}
	if c.usesSpool() {
		errs = append(errs, positive(field+".Replay.Runner.Interval", c.Replay.Runner.Interval))
	}
	if c.MaxAge < 0 {
		errs = append(errs, &FieldError{
			Field: field + ".MaxAge", Value: c.MaxAge, Err: ErrNegativeSetting,
			Detail: fmt.Sprintf("must not be negative, got %v", c.MaxAge),
		})
	}
	return errors.Join(errs...)
}

func (c MetricsConfig) validate() error {
	if len(c.Exporters) == 0 {
		return nil
	}
	var errs []error
	seen := make(map[MetricExporter]bool, len(c.Exporters))
	for _, exporter := range c.Exporters {
		switch exporter {
		case MetricExporterOTLP, MetricExporterVictoriaMetrics, MetricExporterFile, MetricExporterStdout:
		default:
			errs = append(errs, &FieldError{
				Field: "Metrics.Exporters", Value: exporter, Err: ErrInvalidSetting,
				Detail: fmt.Sprintf("unsupported exporter %q; use otlp, victoriametrics, file or stdout", exporter),
			})
			continue
		}
		if seen[exporter] {
			errs = append(errs, &FieldError{
				Field: "Metrics.Exporters", Value: exporter, Err: ErrInvalidSetting,
				Detail: fmt.Sprintf("exporter %q is listed more than once", exporter),
			})
		}
		seen[exporter] = true
	}
	errs = append(errs,
		positive("Metrics.ExportInterval", c.ExportInterval),
		positive("Metrics.QueueSize", c.QueueSize),
		positive("Metrics.CardinalityLimit", c.CardinalityLimit),
	)
	for i, bound := range c.HistogramBounds {
		if math.IsNaN(bound) || math.IsInf(bound, 0) || (i > 0 && bound <= c.HistogramBounds[i-1]) {
			errs = append(errs, &FieldError{
				Field: "Metrics.HistogramBounds", Value: c.HistogramBounds, Err: ErrInvalidSetting,
				Detail: "must be finite and strictly increasing",
			})
			break
		}
	}
	if seen[MetricExporterOTLP] {
		errs = append(errs, c.OTLP.validate("Metrics.OTLP"))
	}
	if seen[MetricExporterVictoriaMetrics] {
		errs = append(errs, c.VictoriaMetrics.validate("Metrics.VictoriaMetrics"))
	}
	if seen[MetricExporterFile] && strings.TrimSpace(c.File.Path) == "" {
		errs = append(errs, &FieldError{
			Field: "Metrics.File.Path", Value: c.File.Path, Err: ErrInvalidSetting,
			Detail: "must not be empty when the file exporter is used",
		})
	}
	return errors.Join(errs...)
}

// positiveは、valueが正でなければFieldErrorを返します。
func positive[T ~int | ~int64](field string, value T) error {
	if value > 0 {
		return nil
	}
	return &FieldError{
		Field: field, Value: value, Err: ErrNonPositiveSetting,
		Detail: fmt.Sprintf("must be positive, got %v", value),
	}
}

// isHeaderTokenは、nameがHTTPのheader名に使える文字（RFC 9110のtoken）だけでできているかを
// 返します。
func isHeaderToken(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}
