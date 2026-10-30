package bootstrap_test

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgtelemetry "github.com/go-jappe/heterolog/pkg/telemetry"
)

// bulkServerは、Bulk APIのcreateをすべて成功させ、書かれた文書を覚えるOpenSearchの代わりです。
type bulkServer struct {
	mu   sync.Mutex
	docs []map[string]any
}

func (s *bulkServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body := io.Reader(r.Body)
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body = zr
	}
	var items []map[string]any
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var action map[string]map[string]string
		if err := json.Unmarshal(scanner.Bytes(), &action); err != nil || !scanner.Scan() {
			http.Error(w, "bad bulk body", http.StatusBadRequest)
			return
		}
		var doc map[string]any
		_ = json.Unmarshal(scanner.Bytes(), &doc)
		s.mu.Lock()
		s.docs = append(s.docs, doc)
		s.mu.Unlock()
		meta := action["create"]
		items = append(items, map[string]any{"create": map[string]any{
			"_index": meta["_index"], "_id": meta["_id"], "status": 201, "result": "created",
		}})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"took": 1, "errors": false, "items": items})
}

func TestTelemetryWritesSpansToOpenSearchWithTheBulkAPI(t *testing.T) {
	server := &bulkServer{}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Resource.ServiceName = "checkout"
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterOpenSearch}
	cfg.Traces.OpenSearch.Client.Endpoint = httpServer.URL
	cfg.Traces.OpenSearch.Client.AuthConfig = pkgopensearch.AuthConfig{Type: pkgopensearch.AuthTypeNone}
	meter := observertest.NewMeter()
	ctx := context.Background()
	tel, err := bootstrap.NewTelemetry(ctx, cfg, loggerruntime.WithObservers(nil, meter))
	if err != nil {
		t.Fatal(err)
	}
	_, span := tel.Tracer().StartSpan(ctx, "checkout")
	span.End()
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.docs) != 1 || server.docs[0]["name"] != "checkout" || server.docs[0]["serviceName"] != "checkout" ||
		server.docs[0]["traceId"] != string(span.TraceID()) {
		t.Fatalf("documents = %v, want the span", server.docs)
	}
	if got := meter.Count(observertest.Key(telemetry.TraceSpans,
		telemetry.AttrExporter, "opensearch", telemetry.AttrOutcome, telemetry.OutcomeSuccess)); got != 1 {
		t.Errorf("written spans = %d, want 1; counters %v", got, meter.Counters())
	}
}

func TestNewTelemetryRejectsOpenSearchWithoutCredentials(t *testing.T) {
	cfg := bootstrap.DefaultTelemetryConfig()
	cfg.Traces.Exporters = []pkgtelemetry.TraceExporter{pkgtelemetry.TraceExporterOpenSearch}
	// 既定はbasic認証で、認証情報は補いません。
	_, err := bootstrap.NewTelemetry(context.Background(), cfg)
	var fieldErr *pkgtelemetry.FieldError
	if !errors.As(err, &fieldErr) || fieldErr.Field != "Traces.OpenSearch.Client" {
		t.Errorf("NewTelemetry = %v, want a FieldError of Traces.OpenSearch.Client", err)
	}
}
