package otelexport_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/otelexport"
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
