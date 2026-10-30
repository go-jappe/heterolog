package heterolog_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog"
	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestTelemetryForwardsToTheImplementation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spans.jsonl")
	cfg := heterolog.DefaultTelemetryConfig()
	cfg.Traces.Exporters = []heterolog.TraceExporter{heterolog.TraceExporterFile}
	cfg.Traces.File.Path = path
	tel, err := heterolog.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.Tracer().StartSpan(context.Background(), "facade span")
	span.End()
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(content), `"name":"facade span"`) {
		t.Errorf("trace file = %q, %v; want the span", content, err)
	}

	if _, err := heterolog.NewTelemetry(nil, cfg); !errors.Is(err, heterolog.ErrNilContext) {
		t.Errorf("NewTelemetry(nil) = %v, want ErrNilContext", err)
	}
}

func TestZeroTelemetryRecordsNothing(t *testing.T) {
	var tel heterolog.Telemetry
	_, span := tel.Tracer().StartSpan(context.Background(), "nothing")
	span.End()
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown of the zero value = %v", err)
	}
}

func TestEnvConfigTelemetryForwardsToTheImplementation(t *testing.T) {
	testutil.UnsetConfigEnvironment(t)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	logDir := t.TempDir()
	path := filepath.Join(t.TempDir(), "heterolog.env")
	lines := []string{
		"HETEROLOG_ENABLE_TRACES=true",
		"HETEROLOG_TRACES_TYPE=file",
		"HETEROLOG_SYSTEM_LOG_DIR=" + logDir,
		"HETEROLOG_TARGET=facade",
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env, err := heterolog.LoadEnvConfig(path)
	if err != nil {
		t.Fatalf("LoadEnvConfig: %v", err)
	}
	want := filepath.Join(logDir, "facade", "traces", "spans.jsonl")
	if got := env.TelemetryConfig().Traces.File.Path; got != want {
		t.Errorf("TelemetryConfig file path = %q, want %q", got, want)
	}
	tel, err := env.NewTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.Tracer().StartSpan(context.Background(), "env span")
	span.End()
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(want); err != nil || !strings.Contains(string(content), `"name":"env span"`) {
		t.Errorf("trace file = %q, %v; want the span", content, err)
	}

	// zero値のEnvConfigからは、何も記録しないtelemetryを作ります。
	var zero heterolog.EnvConfig
	if exporters := zero.TelemetryConfig().Traces.Exporters; len(exporters) != 0 {
		t.Errorf("zero EnvConfig exporters = %v, want none", exporters)
	}
}
