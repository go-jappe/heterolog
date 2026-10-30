package bootstrap_test

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

// metricsReceiverは、受け取ったmetricを名前で引けるように保持するOTLP/HTTPの受信側です。
// downの間は503を返します。
type metricsReceiver struct {
	t        *testing.T
	mu       sync.Mutex
	down     bool
	metrics  map[string]*metricspb.Metric
	services map[string]string
}

func newMetricsReceiver(t *testing.T) (*metricsReceiver, string) {
	r := &metricsReceiver{t: t, metrics: map[string]*metricspb.Metric{}, services: map[string]string{}}
	server := httptest.NewServer(r)
	t.Cleanup(server.Close)
	return r, server.URL + "/v1/metrics"
}

func (r *metricsReceiver) setDown(down bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.down = down
}

func (r *metricsReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	down := r.down
	r.mu.Unlock()
	if down {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	zr, err := gzip.NewReader(req.Body)
	if err != nil {
		r.t.Errorf("gzip body: %v", err)
		return
	}
	raw, _ := io.ReadAll(zr)
	var body collectormetrics.ExportMetricsServiceRequest
	if err := proto.Unmarshal(raw, &body); err != nil {
		r.t.Errorf("unmarshal: %v", err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rm := range body.ResourceMetrics {
		service := ""
		for _, kv := range rm.Resource.GetAttributes() {
			if kv.Key == "service.name" {
				service = kv.Value.GetStringValue()
			}
		}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				r.metrics[m.Name] = m
				r.services[m.Name] = service
			}
		}
	}
}

func (r *metricsReceiver) metric(name string) (*metricspb.Metric, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.metrics[name], r.services[name]
}

func otlpMetricsConfig(endpoint string) pkgtelemetry.Config {
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "checkout"
	cfg.Metrics.Exporters = []pkgtelemetry.MetricExporter{pkgtelemetry.MetricExporterOTLP}
	cfg.Metrics.OTLP.Endpoint = endpoint
	cfg.Metrics.OTLP.Retries = 0
	cfg.Metrics.ExportInterval = time.Hour
	return cfg
}

func TestTelemetrySendsMetricsOverOTLPAtShutdown(t *testing.T) {
	receiver, endpoint := newMetricsReceiver(t)
	tel, err := bootstrap.NewTelemetry(context.Background(), otlpMetricsConfig(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	meter := tel.Meter()
	meter.Counter("orders").Add(ctx, 3, pkgobserver.Attr{Key: "region", Value: "eu"})
	meter.Gauge("queue_depth").Record(ctx, 7)
	meter.Histogram("latency_seconds").Record(ctx, 0.2)
	// 集める間隔は1時間ですが、Shutdownが最後に集めて送ります。
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	orders, service := receiver.metric("orders")
	if orders == nil || service != "checkout" || orders.GetSum().DataPoints[0].GetAsInt() != 3 ||
		!orders.GetSum().IsMonotonic || orders.GetSum().DataPoints[0].Attributes[0].Value.GetStringValue() != "eu" {
		t.Errorf("orders = %v (service %q)", orders, service)
	}
	if gauge, _ := receiver.metric("queue_depth"); gauge == nil || gauge.GetGauge().DataPoints[0].GetAsDouble() != 7 {
		t.Errorf("queue_depth = %v", gauge)
	}
	if histogram, _ := receiver.metric("latency_seconds"); histogram == nil || histogram.GetHistogram().DataPoints[0].Count != 1 ||
		len(histogram.GetHistogram().DataPoints[0].ExplicitBounds) != 11 {
		t.Errorf("latency_seconds = %v, want one value in the default buckets", histogram)
	}
}

func TestTelemetryWritesMetricsToAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics", "metrics.jsonl")
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Metrics.Exporters = []pkgtelemetry.MetricExporter{pkgtelemetry.MetricExporterFile}
	cfg.Metrics.File.Path = path
	tel, err := bootstrap.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	tel.Meter().Counter("orders").Add(context.Background(), 1)
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(content), `"name":"orders"`) || !strings.Contains(string(content), `"asInt":"1"`) {
		t.Errorf("metrics file = %s, %v", content, err)
	}
}

func TestTelemetryWithoutMetricExportersRecordsNothing(t *testing.T) {
	tel, err := bootstrap.NewTelemetry(context.Background(), bootstrap.DefaultTelemetryConfig())
	if err != nil {
		t.Fatal(err)
	}
	// 送り先がなければ、何もしないMeterです。
	tel.Meter().Counter("orders").Add(context.Background(), 1)
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTelemetryResendsMetricsFromTheSpoolAfterTheReceiverRecovers(t *testing.T) {
	receiver, endpoint := newMetricsReceiver(t)
	receiver.setDown(true)
	spoolDir := t.TempDir()
	cfg := otlpMetricsConfig(endpoint)
	cfg.Metrics.OTLP.FailureStore.Store.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
	cfg.Metrics.OTLP.FailureStore.Store.DurableStoreConfig.SpoolConfig = pkgdurable.SpoolConfig{
		SpoolPath: spoolDir, Target: "checkout", Dir: "metrics_otlp",
		OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
			OverloadPolicy: pkgdurable.OverloadPolicyDrop, SpillDestination: pkgdurable.SpillDestinationStdout,
			MaxDirSize: 1 << 30, StatsScanInterval: time.Second, MaxCallsSinceScan: 100,
		},
	}
	tel, err := bootstrap.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	tel.Meter().Counter("orders").Add(context.Background(), 5)
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := receiver.metric("orders"); got != nil {
		t.Fatal("the metric reached the receiver while it was down")
	}

	// 再起動した後、受信側が回復すれば、spoolのmetricを送り直します。
	receiver.setDown(false)
	tel, err = bootstrap.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	deadline := time.Now().Add(10 * time.Second)
	for got, _ := receiver.metric("orders"); got == nil; got, _ = receiver.metric("orders") {
		if time.Now().After(deadline) {
			t.Fatal("the spooled metric was not resent")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got, service := receiver.metric("orders"); got.GetSum().DataPoints[0].GetAsInt() != 5 || service != "checkout" {
		t.Errorf("resent orders = %v (service %q), want the stored value and Resource", got, service)
	}
}

func TestTelemetrySendsMetricsToVictoriaMetricsInTheTextFormat(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("gzip body: %v", err)
			return
		}
		raw, _ := io.ReadAll(zr)
		mu.Lock()
		bodies = append(bodies, r.URL.Path+"\n"+string(raw))
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "checkout"
	cfg.Metrics.Exporters = []pkgtelemetry.MetricExporter{pkgtelemetry.MetricExporterVictoriaMetrics}
	cfg.Metrics.VictoriaMetrics.Endpoint = server.URL + "/api/v1/import/prometheus"
	cfg.Metrics.ExportInterval = time.Hour
	tel, err := bootstrap.NewTelemetry(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	tel.Meter().Counter("orders_total").Add(context.Background(), 2, pkgobserver.Attr{Key: "region", Value: "eu"})
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 1 || !strings.HasPrefix(bodies[0], "/api/v1/import/prometheus\n") ||
		!strings.Contains(bodies[0], `orders_total{`) || !strings.Contains(bodies[0], `region="eu"`) ||
		!strings.Contains(bodies[0], `service_name="checkout"`) || !strings.Contains(bodies[0], "} 2 ") {
		t.Errorf("bodies = %q, want the counter in the text format", bodies)
	}
}
