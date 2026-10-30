package bootstrap_test

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/bootstrap"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

func TestTelemetryInsertsSpansIntoClickHouseWithJSONEachRow(t *testing.T) {
	var mu sync.Mutex
	var queries, bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(zr)
		mu.Lock()
		queries = append(queries, r.URL.Query().Get("query"))
		bodies = append(bodies, string(body))
		mu.Unlock()
	}))
	t.Cleanup(server.Close)
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "checkout"
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterClickHouse}
	cfg.Traces.ClickHouse.Client.Endpoint = server.URL
	ctx := context.Background()
	tel, err := bootstrap.NewTelemetry(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.Tracer().StartSpan(ctx, "checkout")
	span.End()
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(queries) != 1 || queries[0] != "INSERT INTO `default`.`otel_traces` FORMAT JSONEachRow" {
		t.Fatalf("queries = %q, want one INSERT into the default table", queries)
	}
	for _, want := range []string{`"SpanName":"checkout"`, `"ServiceName":"checkout"`, `"TraceId":"` + string(span.TraceID()) + `"`} {
		if !strings.Contains(bodies[0], want) {
			t.Errorf("row %s does not contain %s", bodies[0], want)
		}
	}
}
