package envconfig_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/presentation/envconfig"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

func TestTelemetryConfigIsDisabledByDefault(t *testing.T) {
	isolateEnvConfigForTest(t)
	loaded, err := envconfig.Load(writeEnvFileForTest(t, "HETEROLOG_APP_NAME=checkout"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := loaded.TelemetryConfig()
	if len(cfg.Traces.Exporters) != 0 {
		t.Errorf("exporters = %v, want none while HETEROLOG_ENABLE_TRACES is false", cfg.Traces.Exporters)
	}
	if cfg.Resource.ServiceName != "checkout" {
		t.Errorf("service name = %q, want the HETEROLOG_APP_NAME value", cfg.Resource.ServiceName)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v", err)
	}
}

func TestTelemetryConfigReadsTheTraceSettings(t *testing.T) {
	isolateEnvConfigForTest(t)
	logDir := t.TempDir()
	loaded, err := envconfig.Load(writeEnvFileForTest(t,
		"HETEROLOG_ENABLE_TRACES=true",
		"HETEROLOG_TRACES_TYPE=otlp, file",
		"HETEROLOG_TRACES_SAMPLING_RATIO=0.25",
		"HETEROLOG_TRACES_EXPORT_INTERVAL_MILLISECOND=250",
		"HETEROLOG_TRACES_OTLP_ENDPOINT=https://collector.example/v1/traces",
		`HETEROLOG_TRACES_OTLP_HEADERS={"Authorization":"Bearer token"}`,
		"HETEROLOG_TRACES_OTLP_RETRIES=0",
		"HETEROLOG_ENABLE_TRACES_OTLP_CIRCUIT_BREAKER=true",
		"HETEROLOG_SYSTEM_LOG_DIR="+logDir,
		"HETEROLOG_TARGET=api",
	))
	if err != nil {
		t.Fatal(err)
	}
	traces := loaded.TelemetryConfig().Traces
	if len(traces.Exporters) != 2 || traces.Exporters[0] != pkgtelemetry.TraceExporterOTLP || traces.Exporters[1] != pkgtelemetry.TraceExporterFile {
		t.Errorf("exporters = %v, want otlp and file", traces.Exporters)
	}
	if traces.SamplingRatio != 0.25 || traces.ExportInterval != 250*time.Millisecond || traces.QueueSize != 2048 {
		t.Errorf("ratio %v, interval %v, queue %d; want the set values and the default queue size",
			traces.SamplingRatio, traces.ExportInterval, traces.QueueSize)
	}
	if traces.OTLP.Endpoint != "https://collector.example/v1/traces" || traces.OTLP.Headers["Authorization"] != "Bearer token" ||
		traces.OTLP.Retries != 0 || traces.OTLP.Timeout != 10*time.Second || !traces.OTLP.CircuitBreaker {
		t.Errorf("OTLP = %+v", traces.OTLP)
	}
	if want := filepath.Join(logDir, "api", "traces", "spans.jsonl"); traces.File.Path != want {
		t.Errorf("file path = %q, want %q", traces.File.Path, want)
	}
}

func TestLoadRejectsInvalidTraceSettings(t *testing.T) {
	isolateEnvConfigForTest(t)
	_, err := envconfig.Load(writeEnvFileForTest(t,
		"HETEROLOG_ENABLE_TRACES=true",
		"HETEROLOG_TRACES_OTLP_ENDPOINT=collector:4318",
		"HETEROLOG_TRACES_SAMPLING_RATIO=2",
	))
	var fieldErr *pkgtelemetry.FieldError
	if !errors.As(err, &fieldErr) || !errors.Is(err, pkgtelemetry.ErrInvalidSetting) || !strings.Contains(err.Error(), "HETEROLOG_TRACES_*") {
		t.Fatalf("Load = %v, want the trace validation errors under HETEROLOG_TRACES_*", err)
	}
	for _, field := range []string{"Traces.OTLP.Endpoint", "Traces.SamplingRatio"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("Load error %q does not name %s", err, field)
		}
	}

	// 無効にしたtraceの設定は読まないため、不正な値でも失敗しません。
	if _, err := envconfig.Load(writeEnvFileForTest(t, "HETEROLOG_TRACES_SAMPLING_RATIO=2")); err != nil {
		t.Errorf("Load with traces disabled = %v", err)
	}
}

func TestTelemetryConfigReadsTheTraceFailureStoreSettings(t *testing.T) {
	isolateEnvConfigForTest(t)
	libDir := t.TempDir()
	loaded, err := envconfig.Load(writeEnvFileForTest(t,
		"HETEROLOG_ENABLE_TRACES=true",
		"HETEROLOG_SYSTEM_LIB_DIR="+libDir,
		"HETEROLOG_TARGET=api",
		"HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_INTERVAL_MILLISECOND=250",
		"HETEROLOG_TRACES_OTLP_SPOOL_RUNNER_MAX_RETRY_AFTER_SECOND=60",
	))
	if err != nil {
		t.Fatal(err)
	}
	failureStore := loaded.TelemetryConfig().Traces.OTLP.FailureStore
	// 既定では、ログの出力先と同じくspoolを選び、traces_otlpに保存します。
	spool := failureStore.Store.DurableStoreConfig.SpoolConfig
	if !failureStore.Enabled() || len(failureStore.Store.DurableType) != 1 || failureStore.Store.DurableType[0] != pkgdurable.DurableTypeSpool ||
		spool.SpoolPath != libDir || spool.Target != "api" || spool.Dir != "traces_otlp" {
		t.Errorf("failure store = %+v, spool %+v; want the default spool under the lib directory", failureStore.Store, spool)
	}
	if failureStore.Replay.Runner.Interval != 250*time.Millisecond || failureStore.Replay.Worker.MaxRetryAfter != time.Minute ||
		failureStore.ReplaySupervisor.RestartDelay <= 0 {
		t.Errorf("replay = %+v, supervisor %+v", failureStore.Replay, failureStore.ReplaySupervisor)
	}

	loaded, err = envconfig.Load(writeEnvFileForTest(t,
		"HETEROLOG_ENABLE_TRACES=true",
		"HETEROLOG_SYSTEM_LIB_DIR="+libDir,
		"HETEROLOG_TRACES_OTLP_DURABLE_TYPES=fileArchive",
		"HETEROLOG_TRACES_OTLP_FAILURE_ARCHIVE_DIR=span_archives",
	))
	if err != nil {
		t.Fatal(err)
	}
	failureStore = loaded.TelemetryConfig().Traces.OTLP.FailureStore
	archive := failureStore.Store.DurableStoreConfig.FileArchiveConfig
	if failureStore.Store.DurableType[0] != pkgdurable.DurableTypeFileArchive || archive.Dir != "span_archives" ||
		archive.ObjectStorageUploadConfig.Enabled {
		t.Errorf("failure store = %+v, archive %+v; want a failure archive without upload", failureStore.Store, archive)
	}
}
