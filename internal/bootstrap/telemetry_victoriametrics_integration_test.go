package bootstrap_test

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// victoriaMetricsSettingsは、VictoriaMetricsの結合テストの設定を読みます。
func victoriaMetricsSettings(t *testing.T) (otlpEndpoint, importEndpoint, queryAddr string) {
	t.Helper()
	settings := testutil.RequireIntegrationEnvironment(t, "VICTORIAMETRICS_INTEGRATION_TEST")
	otlpEndpoint = strings.TrimSpace(settings["HETEROLOG_METRICS_OTLP_ENDPOINT"])
	importEndpoint = strings.TrimSpace(settings["HETEROLOG_METRICS_VICTORIAMETRICS_ENDPOINT"])
	queryAddr = strings.TrimSpace(settings["HETEROLOG_VICTORIAMETRICS_QUERY_ADDR"])
	if otlpEndpoint == "" || importEndpoint == "" || queryAddr == "" {
		t.Fatal("the VictoriaMetrics integration test needs HETEROLOG_METRICS_OTLP_ENDPOINT, " +
			"HETEROLOG_METRICS_VICTORIAMETRICS_ENDPOINT and HETEROLOG_VICTORIAMETRICS_QUERY_ADDR in the dedicated environment file")
	}
	return otlpEndpoint, importEndpoint, queryAddr
}

// waitForVictoriaMetricsSeriesは、VictoriaMetricsの/api/v1/exportから、metricの名前と
// run属性に合う系列を読み、wantを含むまで待ちます。VictoriaMetricsは、受け取ったdataを
// 少し後から読めるようにします。
func waitForVictoriaMetricsSeries(t *testing.T, queryAddr, selector, want string) string {
	t.Helper()
	query := url.Values{"match[]": {selector}}
	exportURL := "http://" + queryAddr + "/api/v1/export?" + query.Encode()
	client := &http.Client{Timeout: 10 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	var body string
	for {
		if resp, err := client.Get(exportURL); err == nil {
			raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			body = string(raw)
			if resp.StatusCode == http.StatusOK && strings.Contains(body, want) {
				return body
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("VictoriaMetrics did not return %s with %q; last response: %.2000s", selector, want, body)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// victoriaMetricsTelemetryConfigは、metricsだけを送る設定です。runは、試験ごとに系列を
// 分けるための属性の値です。
func victoriaMetricsTelemetryConfig(exporter pkgtelemetry.MetricExporter) pkgtelemetry.Config {
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "heterolog-it-metrics"
	cfg.Metrics.Exporters = []pkgtelemetry.MetricExporter{exporter}
	cfg.Metrics.ExportInterval = time.Hour
	return cfg
}

func testRun() string {
	return strings.ReplaceAll(time.Now().UTC().Format("20060102T150405.000000000"), ".", "_")
}

func TestTelemetrySendsMetricsToVictoriaMetricsOverOTLP(t *testing.T) {
	otlpEndpoint, _, queryAddr := victoriaMetricsSettings(t)
	cfg := victoriaMetricsTelemetryConfig(pkgtelemetry.MetricExporterOTLP)
	cfg.Metrics.OTLP.Endpoint = otlpEndpoint
	tel, err := bootstrap.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	run := testRun()
	tel.Meter().Counter("heterolog_it_otlp_orders_total").Add(context.Background(), 3, pkgobserver.Attr{Key: "run", Value: run})
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitForVictoriaMetricsSeries(t, queryAddr, `{__name__=~"heterolog_it_otlp_orders_total.*",run="`+run+`"}`, `"values":[3]`)
}

func TestTelemetrySendsMetricsToVictoriaMetricsInThePrometheusFormat(t *testing.T) {
	_, importEndpoint, queryAddr := victoriaMetricsSettings(t)
	cfg := victoriaMetricsTelemetryConfig(pkgtelemetry.MetricExporterVictoriaMetrics)
	cfg.Metrics.VictoriaMetrics.Endpoint = importEndpoint
	tel, err := bootstrap.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	run := testRun()
	ctx := context.Background()
	tel.Meter().Counter("heterolog_it_import_orders_total").Add(ctx, 5, pkgobserver.Attr{Key: "run", Value: run})
	tel.Meter().Histogram("heterolog_it_import_latency_seconds").Record(ctx, 0.2, pkgobserver.Attr{Key: "run", Value: run})
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	body := waitForVictoriaMetricsSeries(t, queryAddr, `{__name__="heterolog_it_import_orders_total",run="`+run+`"}`, `"values":[5]`)
	if !strings.Contains(body, `"service_name":"heterolog-it-metrics"`) {
		t.Errorf("series = %s, want the resource attributes as labels", body)
	}
	waitForVictoriaMetricsSeries(t, queryAddr, `{__name__="heterolog_it_import_latency_seconds_bucket",run="`+run+`",le="0.25"}`, `"values":[1]`)
}

func TestTelemetryResendsSpooledMetricsToVictoriaMetrics(t *testing.T) {
	_, importEndpoint, queryAddr := victoriaMetricsSettings(t)
	spoolDir := t.TempDir()
	newConfig := func(endpoint string) pkgtelemetry.Config {
		cfg := victoriaMetricsTelemetryConfig(pkgtelemetry.MetricExporterVictoriaMetrics)
		cfg.Metrics.VictoriaMetrics.Endpoint = endpoint
		cfg.Metrics.VictoriaMetrics.Retries = 0
		cfg.Metrics.VictoriaMetrics.FailureStore.Store.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
		cfg.Metrics.VictoriaMetrics.FailureStore.Store.DurableStoreConfig.SpoolConfig = pkgdurable.SpoolConfig{
			SpoolPath: spoolDir, Target: "heterolog-it", Dir: "metrics_victoriametrics",
			OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
				OverloadPolicy: pkgdurable.OverloadPolicyDrop, SpillDestination: pkgdurable.SpillDestinationStdout,
				MaxDirSize: 1 << 30, StatsScanInterval: time.Second, MaxCallsSinceScan: 100,
			},
		}
		return cfg
	}
	// 接続できない受信先へ送ったmetricは、spoolのOTLP/JSONのrecordに残ります。
	tel, err := bootstrap.NewTelemetry(context.Background(), newConfig("http://127.0.0.1:1/api/v1/import/prometheus"))
	if err != nil {
		t.Fatal(err)
	}
	run := testRun()
	tel.Meter().Counter("heterolog_it_spooled_orders_total").Add(context.Background(), 7, pkgobserver.Attr{Key: "run", Value: run})
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 同じspoolを使って再び始めると、Prometheusのtext形式に戻してVictoriaMetricsへ送り直します。
	tel, err = bootstrap.NewTelemetry(context.Background(), newConfig(importEndpoint))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	waitForVictoriaMetricsSeries(t, queryAddr, `{__name__="heterolog_it_spooled_orders_total",run="`+run+`"}`, `"values":[7]`)
}
