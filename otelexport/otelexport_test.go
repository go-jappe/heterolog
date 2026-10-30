package otelexport_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/otelexport"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestNewSpanExporterDeliversSDKSpansThroughTheTelemetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.jsonl")
	cfg := heterolog.DefaultTelemetryConfig()
	cfg.Traces.Exporters = []heterolog.TraceExporter{heterolog.TraceExporterFile}
	cfg.Traces.File.Path = path
	tel, err := heterolog.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(otelexport.NewSpanExporter(tel)))
	_, span := provider.Tracer("sdk-lib").Start(context.Background(), "sdk span")
	span.End()
	// SDKが持つspanをTelemetryへ渡してから、Telemetryを止めます。
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(content), `"name":"sdk span"`) || !strings.Contains(string(content), `"name":"sdk-lib"`) {
		t.Errorf("trace file = %q, %v; want the SDK span with its scope", content, err)
	}
}

func TestNewSpanExporterWithoutATelemetryDropsSpans(t *testing.T) {
	exporter := otelexport.NewSpanExporter(nil)
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	_, span := provider.Tracer("sdk-lib").Start(context.Background(), "dropped")
	span.End()
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMetricExporterSendsSDKMetricsThroughTheTelemetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.jsonl")
	cfg := heterolog.DefaultTelemetryConfig()
	cfg.Metrics.Exporters = []heterolog.MetricExporter{heterolog.MetricExporterFile}
	cfg.Metrics.File.Path = path
	tel, err := heterolog.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otelexport.NewMetricExporter(tel))))
	counter, err := provider.Meter("sdk-lib").Int64Counter("sdk_orders")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(context.Background(), 4)
	// MeterProviderを先に止め、最後に集めたmetricsをTelemetryへ渡してから、Telemetryを止めます。
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(content), `"name":"sdk_orders"`) || !strings.Contains(string(content), `"asInt":"4"`) ||
		!strings.Contains(string(content), `"aggregationTemporality":2`) {
		t.Errorf("metrics file = %s, %v; want the SDK counter as a cumulative sum", content, err)
	}

	// metricsの送り先がないTelemetryとnilは、metricsを捨てます。
	for _, target := range []*heterolog.Telemetry{nil, {}} {
		if err := otelexport.NewMetricExporter(target).Export(context.Background(), &metricdata.ResourceMetrics{}); err != nil {
			t.Errorf("Export without destinations = %v", err)
		}
	}
}
