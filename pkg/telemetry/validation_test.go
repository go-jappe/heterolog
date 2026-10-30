package telemetry_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/telemetry"
)

func validConfig() telemetry.Config {
	return telemetry.Config{Traces: telemetry.TracesConfig{
		Exporters:      []telemetry.TraceExporter{telemetry.TraceExporterOTLP, telemetry.TraceExporterFile, telemetry.TraceExporterStdout},
		SamplingRatio:  1,
		QueueSize:      10,
		BatchSize:      5,
		ExportInterval: time.Second,
		OTLP: telemetry.OTLPTracesConfig{
			Endpoint:       "https://collector.example:4318/v1/traces",
			Headers:        map[string]string{"Authorization": "Bearer secret"},
			Compression:    "gzip",
			Timeout:        time.Second,
			Retries:        2,
			RetryBaseDelay: time.Second,
			RetryMaxDelay:  2 * time.Second,
		},
		File: telemetry.FileTracesConfig{Path: "spans.jsonl"},
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
	cfg.Traces.OTLP = telemetry.OTLPTracesConfig{}
	cfg.Traces.File = telemetry.FileTracesConfig{}
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
