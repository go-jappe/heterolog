package victoriametrics

import (
	"compress/gzip"
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/otlphttp"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/retrying"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

var testTime = time.UnixMilli(1793318400123)

func TestMarshalTextWritesThePrometheusTextFormat(t *testing.T) {
	resource := &telemetrydata.Resource{Attrs: []pkgobserver.Attr{
		{Key: "service.name", Value: "checkout"}, {Key: "region", Value: "us"},
	}}
	items := []metrics.Metric{
		{Name: "orders_total", Kind: metrics.KindSum, Monotonic: true, Points: []metrics.Point{{
			Attrs: []pkgobserver.Attr{{Key: "region", Value: "eu"}, {Key: "note", Value: "a \"b\"\nc\\"}, {Key: "code", Value: 200}},
			Time:  testTime, IsInt: true, Int: 5,
		}}},
		{Name: "queue.depth", Kind: metrics.KindGauge, Resource: &telemetrydata.Resource{}, Points: []metrics.Point{
			{Time: testTime, Float: math.NaN()}, {Time: testTime, Float: math.Inf(-1)},
		}},
		{Name: "1latency_seconds", Kind: metrics.KindHistogram, Resource: &telemetrydata.Resource{}, Points: []metrics.Point{{
			Attrs: []pkgobserver.Attr{{Key: "le", Value: "ignored"}},
			Time:  testTime, Count: 4, Sum: 2.5, Bounds: []float64{0.1, 1}, BucketCounts: []uint64{2, 1, 1},
		}}},
	}
	got := string(MarshalText(items, resource))
	want := `orders_total{code="200",note="a \"b\"\nc\\",region="eu",service_name="checkout"} 5 1793318400123
queue_depth NaN 1793318400123
queue_depth -Inf 1793318400123
_1latency_seconds_bucket{le="0.1"} 2 1793318400123
_1latency_seconds_bucket{le="1"} 3 1793318400123
_1latency_seconds_bucket{le="+Inf"} 4 1793318400123
_1latency_seconds_sum 2.5 1793318400123
_1latency_seconds_count 4 1793318400123
`
	if got != want {
		t.Errorf("MarshalText =\n%s\nwant\n%s", got, want)
	}
}

func TestExporterPostsTheTextFormat(t *testing.T) {
	var contentType, body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("gzip: %v", err)
			return
		}
		raw, _ := io.ReadAll(zr)
		body = string(raw)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	meter := observertest.NewMeter()
	exporter := NewExporter(otlphttp.Config{
		Endpoint: server.URL + "/api/v1/import/prometheus", Compression: "gzip", Timeout: time.Second,
		Retry: retrying.Config{Retries: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond},
	}, &telemetrydata.Resource{}, meter)
	defer exporter.Close()
	err := exporter.Export(context.Background(), []metrics.Metric{{Name: "orders_total", Kind: metrics.KindSum, Points: []metrics.Point{{Time: testTime, IsInt: true, Int: 1}}}})
	if err == nil {
		t.Fatal("Export = nil, want the 503")
	}
	if exporter.Name() != ExporterName || !strings.HasPrefix(contentType, "text/plain") || body != "orders_total 1 1793318400123\n" {
		t.Errorf("name %q, content type %q, body %q", exporter.Name(), contentType, body)
	}
	if got := meter.Count(observertest.Key(telemetry.MetricExportRetries, telemetry.AttrExporter, ExporterName, telemetry.AttrReason, "http_status")); got != 1 {
		t.Errorf("retries = %d, want 1 counted for the victoriametrics exporter", got)
	}
}
