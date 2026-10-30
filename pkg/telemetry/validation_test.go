package telemetry_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/telemetry"
)

func validConfig() telemetry.Config {
	return telemetry.Config{Traces: telemetry.TracesConfig{
		Exporters:      []telemetry.TraceExporter{telemetry.TraceExporterOTLP, telemetry.TraceExporterFile, telemetry.TraceExporterStdout},
		SamplingRatio:  1,
		QueueSize:      10,
		BatchSize:      5,
		ExportInterval: time.Second,
		OTLP: telemetry.OTLPConfig{
			Endpoint:       "https://collector.example:4318/v1/traces",
			Headers:        map[string]string{"Authorization": "Bearer secret"},
			Compression:    "gzip",
			Timeout:        time.Second,
			Retries:        2,
			RetryBaseDelay: time.Second,
			RetryMaxDelay:  2 * time.Second,
		},
		File: telemetry.FileConfig{Path: "spans.jsonl"},
	}}
}

func TestValidateAcceptsAValidConfigAndNoExporters(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Errorf("Validate() = %v", err)
	}
	// 送り先がなければ、送り先の設定は使わないため確かめません。
	if err := (telemetry.Config{}).Validate(); err != nil {
		t.Errorf("Validate() of the zero value = %v", err)
	}
}

func TestValidateReportsEachInvalidSetting(t *testing.T) {
	for _, tc := range []struct {
		field  string
		kind   error
		change func(*telemetry.Config)
	}{
		{"Traces.SamplingRatio", telemetry.ErrInvalidSetting, func(c *telemetry.Config) { c.Traces.SamplingRatio = 1.5 }},
		{"Traces.SamplingRatio", telemetry.ErrInvalidSetting, func(c *telemetry.Config) { c.Traces.SamplingRatio = math.NaN() }},
		{"Traces.Exporters", telemetry.ErrInvalidSetting, func(c *telemetry.Config) { c.Traces.Exporters = []telemetry.TraceExporter{"zipkin"} }},
		{"Traces.Exporters", telemetry.ErrInvalidSetting, func(c *telemetry.Config) {
			c.Traces.Exporters = []telemetry.TraceExporter{telemetry.TraceExporterStdout, telemetry.TraceExporterStdout}
		}},
		{"Traces.QueueSize", telemetry.ErrNonPositiveSetting, func(c *telemetry.Config) { c.Traces.QueueSize = 0 }},
		{"Traces.BatchSize", telemetry.ErrInvalidSetting, func(c *telemetry.Config) { c.Traces.BatchSize = 11 }},
		{"Traces.ExportInterval", telemetry.ErrNonPositiveSetting, func(c *telemetry.Config) { c.Traces.ExportInterval = 0 }},
		{"Traces.OTLP.Endpoint", telemetry.ErrInvalidSetting, func(c *telemetry.Config) { c.Traces.OTLP.Endpoint = "collector:4318" }},
		{"Traces.OTLP.Headers", telemetry.ErrInvalidSetting, func(c *telemetry.Config) {
			c.Traces.OTLP.Headers = map[string]string{"Authorization": "Bearer secret\r\nX: y"}
		}},
		{"Traces.OTLP.Compression", telemetry.ErrInvalidSetting, func(c *telemetry.Config) { c.Traces.OTLP.Compression = "zstd" }},
		{"Traces.OTLP.Timeout", telemetry.ErrNonPositiveSetting, func(c *telemetry.Config) { c.Traces.OTLP.Timeout = 0 }},
		{"Traces.OTLP.Retries", telemetry.ErrNegativeSetting, func(c *telemetry.Config) { c.Traces.OTLP.Retries = -1 }},
		{"Traces.OTLP.RetryBaseDelay", telemetry.ErrNonPositiveSetting, func(c *telemetry.Config) { c.Traces.OTLP.RetryBaseDelay = 0 }},
		{"Traces.OTLP.RetryMaxDelay", telemetry.ErrInvalidSetting, func(c *telemetry.Config) { c.Traces.OTLP.RetryMaxDelay = time.Millisecond }},
		{"Traces.File.Path", telemetry.ErrInvalidSetting, func(c *telemetry.Config) { c.Traces.File.Path = " " }},
	} {
		cfg := validConfig()
		tc.change(&cfg)
		err := cfg.Validate()
		var fieldErr *telemetry.FieldError
		if !errors.As(err, &fieldErr) || fieldErr.Field != tc.field || !errors.Is(err, tc.kind) {
			t.Errorf("%s: Validate() = %v, want a FieldError of the field with %v", tc.field, err, tc.kind)
		}
	}
}

func TestValidateDoesNotShowCredentials(t *testing.T) {
	cfg := validConfig()
	cfg.Traces.OTLP.Endpoint = "ftp://user:password@collector"
	cfg.Traces.OTLP.Headers = map[string]string{"Authorization": "Bearer secret\n"}
	err := cfg.Validate()
	if err == nil || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "secret") {
		t.Errorf("Validate() = %v, want an error without the credentials", err)
	}
}

func TestValidateSkipsTheSettingsOfUnusedExporters(t *testing.T) {
	cfg := validConfig()
	cfg.Traces.Exporters = []telemetry.TraceExporter{telemetry.TraceExporterStdout}
	cfg.Traces.OTLP = telemetry.OTLPConfig{}
	cfg.Traces.File = telemetry.FileConfig{}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want no error for the settings of unused exporters", err)
	}
	// 再試行しなければ、待ち時間は使いません。
	cfg = validConfig()
	cfg.Traces.OTLP.Retries = 0
	cfg.Traces.OTLP.RetryBaseDelay, cfg.Traces.OTLP.RetryMaxDelay = 0, 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want no error for the delays without retries", err)
	}
}

func TestValidateChecksTheFailureStore(t *testing.T) {
	withStore := func(change func(*telemetry.FailureStoreConfig)) telemetry.Config {
		cfg := validConfig()
		cfg.Traces.OTLP.FailureStore = telemetry.FailureStoreConfig{
			Store: durable.FailureStoreConfig{MaxConcurrent: 1, DurableType: []durable.DurableType{durable.DurableTypeSpool}},
		}
		cfg.Traces.OTLP.FailureStore.Replay.Runner.Interval = time.Second
		change(&cfg.Traces.OTLP.FailureStore)
		return cfg
	}
	if err := withStore(func(*telemetry.FailureStoreConfig) {}).Validate(); err != nil {
		t.Errorf("Validate() of a spool = %v", err)
	}
	// 何も選ばなければ、failure storeの設定は確かめません。
	if err := withStore(func(c *telemetry.FailureStoreConfig) { *c = telemetry.FailureStoreConfig{} }).Validate(); err != nil {
		t.Errorf("Validate() without a store = %v", err)
	}
	for _, tc := range []struct {
		field  string
		kind   error
		change func(*telemetry.FailureStoreConfig)
	}{
		{"Traces.OTLP.FailureStore.Store.DurableType", telemetry.ErrInvalidSetting, func(c *telemetry.FailureStoreConfig) {
			c.Store.DurableType = []durable.DurableType{durable.DurableTypeSpool, durable.DurableTypeSpool}
		}},
		{"Traces.OTLP.FailureStore.Store.MaxConcurrent", telemetry.ErrNonPositiveSetting, func(c *telemetry.FailureStoreConfig) {
			c.Store.MaxConcurrent = 0
		}},
		{"Traces.OTLP.FailureStore.Replay.Runner.Interval", telemetry.ErrNonPositiveSetting, func(c *telemetry.FailureStoreConfig) {
			c.Replay.Runner.Interval = 0
		}},
	} {
		err := withStore(tc.change).Validate()
		var fieldErr *telemetry.FieldError
		if !errors.As(err, &fieldErr) || fieldErr.Field != tc.field || !errors.Is(err, tc.kind) {
			t.Errorf("%s: Validate() = %v, want a FieldError of the field with %v", tc.field, err, tc.kind)
		}
	}
}

func validMetricsConfig() telemetry.Config {
	return telemetry.Config{Metrics: telemetry.MetricsConfig{
		Exporters:        []telemetry.MetricExporter{telemetry.MetricExporterOTLP, telemetry.MetricExporterFile},
		ExportInterval:   time.Minute,
		QueueSize:        4,
		HistogramBounds:  []float64{0.1, 1},
		CardinalityLimit: 10,
		OTLP: telemetry.OTLPConfig{
			Endpoint: "http://localhost:4318/v1/metrics", Compression: "gzip", Timeout: time.Second,
		},
		File: telemetry.FileConfig{Path: "metrics.jsonl"},
	}}
}

func TestValidateReportsEachInvalidMetricSetting(t *testing.T) {
	if err := validMetricsConfig().Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	for _, tc := range []struct {
		field  string
		kind   error
		change func(*telemetry.MetricsConfig)
	}{
		{"Metrics.Exporters", telemetry.ErrInvalidSetting, func(c *telemetry.MetricsConfig) { c.Exporters = []telemetry.MetricExporter{"prometheus"} }},
		{"Metrics.Exporters", telemetry.ErrInvalidSetting, func(c *telemetry.MetricsConfig) {
			c.Exporters = []telemetry.MetricExporter{telemetry.MetricExporterFile, telemetry.MetricExporterFile}
		}},
		{"Metrics.ExportInterval", telemetry.ErrNonPositiveSetting, func(c *telemetry.MetricsConfig) { c.ExportInterval = 0 }},
		{"Metrics.QueueSize", telemetry.ErrNonPositiveSetting, func(c *telemetry.MetricsConfig) { c.QueueSize = 0 }},
		{"Metrics.CardinalityLimit", telemetry.ErrNonPositiveSetting, func(c *telemetry.MetricsConfig) { c.CardinalityLimit = 0 }},
		{"Metrics.HistogramBounds", telemetry.ErrInvalidSetting, func(c *telemetry.MetricsConfig) { c.HistogramBounds = []float64{1, 1} }},
		{"Metrics.HistogramBounds", telemetry.ErrInvalidSetting, func(c *telemetry.MetricsConfig) { c.HistogramBounds = []float64{math.Inf(1)} }},
		{"Metrics.OTLP.Endpoint", telemetry.ErrInvalidSetting, func(c *telemetry.MetricsConfig) { c.OTLP.Endpoint = "collector:4318" }},
		{"Metrics.File.Path", telemetry.ErrInvalidSetting, func(c *telemetry.MetricsConfig) { c.File.Path = "" }},
	} {
		cfg := validMetricsConfig()
		tc.change(&cfg.Metrics)
		err := cfg.Validate()
		var fieldErr *telemetry.FieldError
		if !errors.As(err, &fieldErr) || fieldErr.Field != tc.field || !errors.Is(err, tc.kind) {
			t.Errorf("%s: Validate() = %v, want a FieldError of the field with %v", tc.field, err, tc.kind)
		}
	}
}
